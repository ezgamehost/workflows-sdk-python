import asyncio
import threading
import time
from datetime import UTC, datetime
from typing import Any

import pytest
from helpers import DEFAULT, FakeLive, entry, invocation, sid, types

from ezgh_workflows import Context, NonRetriableError, RetryAfterError, workflow
from ezgh_workflows._runtime.execution import execute
from ezgh_workflows._runtime.live import InvocationSuperseded


def _note(log: list[int], v: Any) -> Any:
    log.append(1)
    return v


def _const(v: Any) -> Any:
    return lambda: v


@workflow("sequence")
async def sequence(ctx: Context) -> int:
    total = 0
    for i in range(ctx.input["n"]):
        total += await ctx.step.run("add", _const(i))
    return total


async def test_first_run_checkpoints_each_step_before_running_it() -> None:
    live = FakeLive()
    cmds = await execute(invocation("sequence", input={"n": 2}), sequence, live)
    assert live.checkpoints[0] == {
        "invocationId": "inv_" + "a" * 26,
        "seq": 1,
        "commands": [{"type": "stepStarted", "stepId": sid("add", 0), "name": "add", "attempt": 1, "retry": DEFAULT}],
    }
    assert live.checkpoints[1]["seq"] == 2
    assert live.checkpoints[1]["commands"][0] == {
        "type": "stepCompleted",
        "stepId": sid("add", 0),
        "name": "add",
        "output": 0,
    }
    assert cmds == [
        {"type": "stepCompleted", "stepId": sid("add", 1), "name": "add", "output": 1},
        {"type": "runCompleted", "output": 1},
    ]


async def test_replay_returns_recorded_results_without_running() -> None:
    ran: list[int] = []

    @workflow("replayed")
    async def replayed(ctx: Context) -> Any:
        return await ctx.step.run("x", lambda: _note(ran, 5))

    cmds = await execute(invocation("replayed", entries=[entry(1, "x", output=7)]), replayed, FakeLive())
    assert cmds == [{"type": "runCompleted", "output": 7}] and ran == []


async def test_journal_pages_and_payloads_load_before_replay() -> None:
    live = FakeLive(
        pages=[{"complete": True, "entries": [entry(2, "add", n=1, outputRef="pay_x")]}],
        payloads={"pay_x": 41, "pay_in": {"n": 2}},
    )
    inv = invocation("sequence", entries=[entry(1, "add", output=0)], complete=False)
    inv["run"]["inputRef"] = "pay_in"
    assert await execute(inv, sequence, live) == [{"type": "runCompleted", "output": 41}]
    assert live.page_requests == [1]


async def test_race_resolves_by_seq_on_replay() -> None:
    @workflow("race")
    async def race(ctx: Context) -> Any:
        a, b = ctx.step.run("a", lambda: "a"), ctx.step.run("b", lambda: "b")
        done, _ = await asyncio.wait({a, b}, return_when=asyncio.FIRST_COMPLETED)
        w = done.pop().result()
        return await ctx.step.run(f"after-{w}", lambda: w)

    journal = [entry(1, "b", output="b"), entry(2, "a", output="a"), entry(3, "after-b", output="b")]
    assert await execute(invocation("race", entries=journal), race, FakeLive()) == [
        {"type": "runCompleted", "output": "b"}
    ]
    journal = [entry(1, "a", output="a"), entry(2, "b", output="b")]
    live = FakeLive()
    await execute(invocation("race", entries=journal), race, live)
    assert live.checkpoints[0]["commands"][0]["name"] == "after-a"
    # protocol 1.0: phase 1, one step at a time in call order
    live = FakeLive()
    cmds = await execute(invocation("race", version="1.0"), race, live)
    assert [c["commands"][-1]["name"] for c in live.checkpoints] == ["a", "b", "after-a"]
    assert cmds[-1] == {"type": "runCompleted", "output": "a"}


async def test_live_values_equal_replayed_values() -> None:
    @workflow("echo")
    async def echo(ctx: Context) -> Any:
        when = await ctx.step.run("date", lambda: datetime(1970, 1, 1, tzinfo=UTC))
        assert when == "1970-01-01T00:00:00.000Z"
        pair = await ctx.step.run("pair", lambda: (1, None))
        assert pair == [1, None]
        return when

    cmds = await execute(invocation("echo"), echo, FakeLive())
    assert cmds[-1] == {"type": "runCompleted", "output": "1970-01-01T00:00:00.000Z"}


async def test_coroutine_steps_and_awaitables_returned_by_plain_functions() -> None:
    async def fetch() -> int:
        await asyncio.sleep(0)
        return 3

    @workflow("coro")
    async def coro(ctx: Context) -> Any:
        a = await ctx.step.run("a", fetch)
        b = await ctx.step.run("b", lambda: fetch())
        return a + b

    assert (await execute(invocation("coro"), coro, FakeLive()))[-1] == {"type": "runCompleted", "output": 6}


async def test_sync_step_functions_run_in_threads_with_step_context() -> None:
    seen: dict[str, Any] = {}

    @workflow("ctxinfo")
    async def ctxinfo(ctx: Context) -> Any:
        def work() -> str:
            seen["thread"] = threading.current_thread().name
            seen["attempt"] = ctx.step.attempt()
            seen["cancelled"] = ctx.step.cancelled()
            return ctx.step.idempotency_key()

        return await ctx.step.run("work", work)

    cmds = await execute(invocation("ctxinfo"), ctxinfo, FakeLive())
    from helpers import RUN_ID

    assert cmds[-1] == {"type": "runCompleted", "output": f"{RUN_ID}:{sid('work')}:1"}
    assert seen == {"thread": "ezgh-workflows-step", "attempt": 1, "cancelled": False}
    from ezgh_workflows._runtime.context import _current

    with pytest.raises(RuntimeError):
        _current()  # outside a step function


async def test_failures_retry_after_and_non_retriable() -> None:
    def slow_down() -> None:
        raise RetryAfterError("slow down", 1e22)

    @workflow("slow")
    async def slow(ctx: Context) -> Any:
        return await ctx.step.run("slow", slow_down)

    cmd = (await execute(invocation("slow"), slow, FakeLive()))[-1]
    assert cmd["type"] == "stepFailed" and cmd["retryable"] is True and cmd["retryAfterMs"] == 86_400_000

    def boom() -> None:
        raise NonRetriableError("nope")

    @workflow("fatal")
    async def fatal(ctx: Context) -> Any:
        return await ctx.step.run("fatal", boom)

    cmd = (await execute(invocation("fatal"), fatal, FakeLive()))[-1]
    assert cmd["retryable"] is False and cmd["error"]["code"] == "workflows.non_retriable"


async def test_retrying_step_runs_at_the_next_attempt_and_failures_carry_their_step() -> None:
    def flaky() -> None:
        raise ValueError("boom")

    @workflow("flaky")
    async def flaky_wf(ctx: Context) -> Any:
        return await ctx.step.run("flaky", flaky, retry={"maxAttempts": 3, "initialMs": 10})

    live = FakeLive()
    retrying = entry(1, "flaky", "retrying", error={"code": "user", "name": "ValueError", "message": "boom"})
    cmds = await execute(invocation("flaky", entries=[retrying]), flaky_wf, live)
    assert live.checkpoints[0]["commands"][0]["attempt"] == 2
    assert live.checkpoints[0]["commands"][0]["retry"] == {
        "maxAttempts": 3,
        "initialMs": 10,
        "maxMs": 600000,
        "multiplier": 2,
    }
    assert cmds[-1]["type"] == "stepFailed" and cmds[-1]["attempt"] == 2
    failed = entry(1, "flaky", "failed", attempt=3, error={"code": "user", "name": "ValueError", "message": "boom"})
    cmds = await execute(invocation("flaky", entries=[failed]), flaky_wf, FakeLive())
    assert cmds == [
        {
            "type": "runFailed",
            "causeStepId": sid("flaky"),
            "error": {"code": "user", "name": "ValueError", "message": "boom"},
        }
    ]


async def test_unencodable_results_fail_the_step_non_retriably() -> None:
    @workflow("unencodable")
    async def unencodable(ctx: Context) -> Any:
        return await ctx.step.run("naive", lambda: datetime(2026, 1, 1))

    cmd = (await execute(invocation("unencodable"), unencodable, FakeLive()))[-1]
    assert (
        cmd["type"] == "stepFailed" and cmd["retryable"] is False and cmd["error"]["code"] == "workflows.non_retriable"
    )

    @workflow("big")
    async def big(ctx: Context) -> Any:
        return await ctx.step.run("big", lambda: "x" * 1_048_577)

    cmd = (await execute(invocation("big"), big, FakeLive()))[-1]
    assert cmd["retryable"] is False and cmd["error"]["code"] == "workflows.output_too_large"

    @workflow("badout")
    async def badout(ctx: Context) -> Any:
        return {1, 2}

    cmd = (await execute(invocation("badout"), badout, FakeLive()))[-1]
    assert cmd["type"] == "runFailed" and cmd["error"]["code"] == "workflows.non_retriable"


async def test_step_timeouts_set_cancelled_and_fail_the_attempt() -> None:
    seen: list[bool] = []

    def long() -> str:
        from ezgh_workflows._runtime.context import _current

        info = _current()
        for _ in range(200):
            if info.is_cancelled():
                seen.append(True)
                return "stopped"
            time.sleep(0.01)
        return "done"

    @workflow("timed")
    async def timed(ctx: Context) -> Any:
        return await ctx.step.run("long", long, timeout_ms=50)

    live = FakeLive()
    cmd = (await execute(invocation("timed"), timed, live))[-1]
    assert live.checkpoints[0]["commands"][0]["timeoutMs"] == 50
    assert cmd["type"] == "stepFailed" and cmd["retryable"] is True and cmd["error"]["code"] == "workflows.step_timeout"
    for _ in range(100):
        if seen:
            break
        await asyncio.sleep(0.01)
    assert seen == [True]


async def test_a_step_raising_its_own_timeout_error_is_not_a_step_timeout() -> None:
    def times_out() -> None:
        raise TimeoutError("upstream")

    @workflow("owntimeout")
    async def owntimeout(ctx: Context) -> Any:
        return await ctx.step.run("t", times_out, timeout_ms=10_000)

    cmd = (await execute(invocation("owntimeout"), owntimeout, FakeLive()))[-1]
    assert cmd["error"]["name"] == "TimeoutError" and cmd["error"]["code"] == "user"


async def test_cancel_throws_at_the_pending_step_and_cleanup_runs() -> None:
    @workflow("cancellable")
    async def cancellable(ctx: Context) -> str:
        try:
            await ctx.step.sleep("wait", "1h")
            return "finished"
        finally:
            await ctx.step.run("cleanup", lambda: "cleaned")

    live = FakeLive()
    first = await execute(invocation("cancellable"), cancellable, live)
    assert types(first) == ["sleep"] and live.checkpoints == []
    inv = invocation("cancellable", entries=[entry(1, "wait", "sleeping", kind="sleep")], cancel=True)
    cmds = await execute(inv, cancellable, live)
    assert live.checkpoints[0]["commands"][-1]["name"] == "cleanup"
    assert cmds[0] == {"type": "stepCompleted", "stepId": sid("cleanup"), "name": "cleanup", "output": "cleaned"}
    assert cmds[-1]["type"] == "runFailed" and cmds[-1]["error"]["code"] == "workflows.cancelled"
    assert cmds[-1]["error"]["name"] == "CancelledError"


async def test_only_run_steps_proceed_while_cancelling() -> None:
    @workflow("sleepy-cleanup")
    async def sleepy_cleanup(ctx: Context) -> str:
        try:
            await ctx.step.sleep("wait", "1h")
            return "finished"
        except Exception:
            try:
                await ctx.step.sleep("more", "1m")
            except Exception as e:
                return type(e).__name__
            return "slept"

    inv = invocation("sleepy-cleanup", entries=[entry(1, "wait", "sleeping", kind="sleep")], cancel=True)
    assert (await execute(inv, sleepy_cleanup, FakeLive()))[-1] == {
        "type": "runCompleted",
        "output": "RunCancelledError",
    }


async def test_duplicate_pending_names_and_reused_ids_fail_the_run() -> None:
    @workflow("dupe")
    async def dupe(ctx: Context) -> Any:
        return await asyncio.gather(ctx.step.run("x", lambda: 1), ctx.step.run("x", lambda: 2))

    cmds = await execute(invocation("dupe"), dupe, FakeLive())
    assert len(cmds) == 1 and cmds[0]["error"]["code"] == "workflows.duplicate_step_name"

    @workflow("dupeid")
    async def dupeid(ctx: Context) -> Any:
        await ctx.step.run("a", lambda: 1, id="same")
        return await ctx.step.run("b", lambda: 2, id="same")

    cmds = await execute(invocation("dupeid"), dupeid, FakeLive())
    assert cmds[-1]["error"]["code"] == "workflows.duplicate_step_name"


async def test_explicit_ids_dont_advance_counters() -> None:
    @workflow("explicit")
    async def explicit(ctx: Context) -> Any:
        a = await ctx.step.run("x", lambda: 1, id="custom-1")
        return a + await ctx.step.run("x", lambda: 2)

    live = FakeLive()
    await execute(invocation("explicit"), explicit, live)
    assert [c["commands"][-1]["stepId"] for c in live.checkpoints] == ["custom-1", sid("x", 0)]


async def test_invalid_arguments_raise_value_error() -> None:
    @workflow("widename")
    async def widename(ctx: Context) -> Any:
        return await ctx.step.run("é" * 200, lambda: 1)

    cmd = (await execute(invocation("widename"), widename, FakeLive()))[-1]
    assert cmd["type"] == "runFailed" and cmd["error"]["name"] == "ValueError" and cmd["error"]["code"] == "user"

    @workflow("badargs")
    async def badargs(ctx: Context) -> Any:
        out = []
        for call in (
            lambda: ctx.step.run("a", lambda: 1, id="bad id"),
            lambda: ctx.step.run("a", lambda: 1, id="ok\n"),
            lambda: ctx.step.run("a", lambda: 1, retry={"maxAttempts": 0}),
            lambda: ctx.step.run("a", lambda: 1, retry={"tries": 2}),
            lambda: ctx.step.run("a", lambda: 1, timeout_ms=0),
            lambda: ctx.step.sleep("s", "2y"),
            lambda: ctx.step.sleep("s", 31_536_000_001),
            lambda: ctx.step.sleep_until("s", datetime(2026, 1, 1)),
            lambda: ctx.step.run("a\u0000b", lambda: 1),
            lambda: ctx.step.run("", lambda: 1),
        ):
            try:
                await call()
                out.append("ok")
            except ValueError:
                out.append("ValueError")
        return out

    cmd = (await execute(invocation("badargs"), badargs, FakeLive()))[-1]
    assert cmd == {"type": "runCompleted", "output": ["ValueError"] * 10}


async def test_superseded_checkpoint_stops_before_the_effect() -> None:
    effects: list[int] = []

    @workflow("effect")
    async def effect(ctx: Context) -> Any:
        return await ctx.step.run("e", lambda: _note(effects, "done"))

    with pytest.raises(InvocationSuperseded):
        await execute(invocation("effect"), effect, FakeLive(supersede_at=1))
    await asyncio.sleep(0.05)
    assert effects == []


async def test_step_budget_ends_the_invocation_without_a_terminal_command() -> None:
    live = FakeLive()
    cmds = await execute(invocation("sequence", input={"n": 5}), sequence, live, max_steps=2)
    assert len(live.checkpoints) == 2 and cmds == [
        {"type": "stepCompleted", "stepId": sid("add", 1), "name": "add", "output": 1}
    ]


async def test_time_budget_ends_the_invocation() -> None:
    clock = [1000.0]
    cmds = await execute(
        invocation("sequence", input={"n": 5}), sequence, FakeLive(), now=lambda: clock[0], deadline=1004.0
    )
    assert cmds == []


async def test_values_are_recorded_and_replayed() -> None:
    @workflow("values")
    async def values(ctx: Context) -> Any:
        t = await ctx.step.now("t")
        rid = await ctx.step.uuid("id")
        return await ctx.step.run(
            "use", lambda: {"year": t.year >= 2026, "aware": t.tzinfo is not None, "idLength": len(rid)}
        )

    live = FakeLive()
    cmds = await execute(invocation("values"), values, live, now=lambda: 1791547200.1239)
    first = live.checkpoints[0]["commands"]
    assert types(first) == ["valueRecorded", "valueRecorded", "stepStarted"]
    assert first[0]["value"] == "2026-10-09T12:00:00.123Z"
    assert len(first[1]["value"]) == 36
    assert cmds[-1] == {"type": "runCompleted", "output": {"year": True, "aware": True, "idLength": 36}}
    journal = [
        entry(1, "t", kind="value", output="2026-10-09T00:00:00.000Z"),
        entry(2, "id", kind="value", output="0199a3d0-2b3c-7d4e-9f50-6b7c8d9e0f1a"),
    ]
    live = FakeLive()
    cmds = await execute(invocation("values", entries=journal), values, live)
    assert types(live.checkpoints[0]["commands"]) == ["stepStarted"]


async def test_sleep_until_in_the_past_sleeps_until_now() -> None:
    @workflow("until")
    async def until(ctx: Context) -> Any:
        await ctx.step.sleep_until("past", datetime(2020, 1, 1, tzinfo=UTC))

    cmds = await execute(invocation("until"), until, FakeLive(), now=lambda: 1791547200.5)
    assert cmds == [{"type": "sleep", "stepId": sid("past"), "name": "past", "until": "2026-10-09T12:00:00.500Z"}]


async def test_schedule_metadata_is_visible() -> None:
    @workflow("scheduled")
    async def scheduled(ctx: Context) -> Any:
        assert ctx.run.schedule is not None
        return {"iso": ctx.run.schedule.scheduled_time.isoformat(), "wire": ctx.run.schedule.to_wire()}

    sched = {"id": "01a12088-d200-7201-8899-aabbccddeeff", "scheduledTime": "2026-10-10T00:00:00Z"}
    out = (await execute(invocation("scheduled", schedule=sched), scheduled, FakeLive()))[-1]
    assert out == {"type": "runCompleted", "output": {"iso": "2026-10-10T00:00:00+00:00", "wire": sched}}


async def test_suspended_workflows_unwind_instead_of_hanging() -> None:
    reached: list[str] = []

    @workflow("unwind")
    async def unwind(ctx: Context) -> Any:
        try:
            await ctx.step.sleep("nap", "1h")
        except Exception:
            reached.append("except")
        finally:
            reached.append("finally")
            try:
                await ctx.step.run("after", lambda: 1)
            except BaseException as e:
                reached.append(type(e).__name__)
                raise

    assert types(await execute(invocation("unwind"), unwind, FakeLive())) == ["sleep"]
    await asyncio.sleep(0.01)
    assert reached == ["finally", "InvocationEnded"]
    pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    assert pending == []


async def test_infinite_retry_after_is_clamped() -> None:
    def forever() -> None:
        raise RetryAfterError("later", float("inf"))

    def never() -> None:
        raise RetryAfterError("now", float("-inf"))

    @workflow("infinite")
    async def infinite(ctx: Context) -> Any:
        await ctx.step.run("a", forever if ctx.input == "pos" else never)

    assert (await execute(invocation("infinite", input="pos"), infinite, FakeLive()))[-1]["retryAfterMs"] == 86_400_000
    assert (await execute(invocation("infinite", input="neg"), infinite, FakeLive()))[-1]["retryAfterMs"] == 0


async def test_code_raising_its_own_cancelled_error_fails_like_any_error() -> None:
    async def cancelled_inside() -> None:
        t = asyncio.ensure_future(asyncio.sleep(10))
        t.cancel()
        await t

    @workflow("stepcancel")
    async def stepcancel(ctx: Context) -> Any:
        return await ctx.step.run("c", cancelled_inside)

    cmd = (await execute(invocation("stepcancel"), stepcancel, FakeLive()))[-1]
    assert cmd["type"] == "stepFailed" and cmd["error"]["name"] == "CancelledError" and cmd["error"]["code"] == "user"

    @workflow("wfcancel")
    async def wfcancel(ctx: Context) -> Any:
        await cancelled_inside()

    cmd = (await execute(invocation("wfcancel"), wfcancel, FakeLive()))[-1]
    assert cmd["type"] == "runFailed" and cmd["error"]["code"] == "user"


async def test_cancelling_the_invocation_tells_the_running_step() -> None:
    seen: list[bool] = []

    def long() -> None:
        from ezgh_workflows._runtime.context import _current

        info = _current()
        for _ in range(300):
            if info.is_cancelled():
                seen.append(True)
                return
            time.sleep(0.01)

    @workflow("abandoned")
    async def abandoned(ctx: Context) -> Any:
        return await ctx.step.run("long", long)

    task = asyncio.ensure_future(execute(invocation("abandoned"), abandoned, FakeLive()))
    await asyncio.sleep(0.2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0.1)
    assert seen == [True]


async def test_only_cleanup_steps_run_while_cancelling_even_beside_a_retrying_one() -> None:
    @workflow("cleanupmix")
    async def cleanupmix(ctx: Context) -> Any:
        try:
            await ctx.step.sleep("wait", "1h")
        finally:
            c = ctx.step.run("cleanup", lambda: 1)
            try:
                await ctx.step.now("t")
            except Exception as e:
                return type(e).__name__
            await c

    journal = [
        entry(1, "wait", "cancelled", kind="sleep"),
        entry(2, "cleanup", "retrying", error={"code": "user", "name": "E", "message": "x"}),
    ]
    live = FakeLive()
    cmds = await execute(invocation("cleanupmix", entries=journal, cancel=True), cleanupmix, live)
    assert all(c["type"] != "valueRecorded" for cp in live.checkpoints for c in cp["commands"])
    assert cmds[-1] == {"type": "runCompleted", "output": "RunCancelledError"}


async def test_quiescence_doesnt_depend_on_how_deep_the_tasks_nest() -> None:
    def make(depth: int) -> Any:
        @workflow(f"deep-{depth}")
        async def deep(ctx: Context) -> Any:
            async def chain(n: int) -> Any:
                if n == 0:
                    return await ctx.step.run("b", lambda: "b")
                return await asyncio.create_task(chain(n - 1))

            return await asyncio.gather(ctx.step.run("a", lambda: "a"), chain(depth))

        return deep

    for depth in (1, 16, 64):
        deep = make(depth)

        cmds = await execute(invocation(f"deep-{depth}"), deep, FakeLive())
        assert [s["name"] for s in cmds[0]["steps"]] == ["a", "b"], depth
        replay = await execute(
            invocation(f"deep-{depth}", entries=[entry(1, "b", output="b"), entry(2, "a", output="a")]),
            deep,
            FakeLive(),
        )
        assert replay == [{"type": "runCompleted", "output": ["a", "b"]}], depth


async def test_live_results_are_what_a_replay_returns() -> None:
    from enum import IntEnum, StrEnum

    class Color(StrEnum):
        RED = "red"

    class Size(IntEnum):
        BIG = 3

    @workflow("enums")
    async def enums(ctx: Context) -> Any:
        c = await ctx.step.run("c", lambda: Color.RED)
        s = await ctx.step.run("s", lambda: {"size": Size.BIG, "t": (1, 2)})
        return [type(c).__name__, type(s["size"]).__name__, type(s["t"]).__name__]

    assert (await execute(invocation("enums"), enums, FakeLive()))[-1]["output"] == ["str", "int", "list"]


async def test_async_workflows_behind_wrappers_and_callables_run_as_async() -> None:
    import functools

    def logged(fn: Any) -> Any:
        @functools.wraps(fn)
        def wrapper(*a: Any, **k: Any) -> Any:
            return fn(*a, **k)

        return wrapper

    @workflow("wrapped")
    @logged
    async def wrapped(ctx: Context) -> str:
        return str(await ctx.step.run("s", lambda: "x"))

    class Handler:
        async def __call__(self, ctx: Context) -> str:
            return str(await ctx.step.run("s", lambda: "y"))

    from ezgh_workflows._runtime.workflow import definition_of

    assert definition_of(wrapped).is_async
    assert (await execute(invocation("wrapped"), wrapped, FakeLive()))[-1] == {"type": "runCompleted", "output": "x"}
    handler = workflow("callable")(Handler())
    assert definition_of(handler).is_async
    assert (await execute(invocation("callable"), handler, FakeLive()))[-1] == {"type": "runCompleted", "output": "y"}


async def test_a_plain_def_returning_an_awaitable_fails_clearly() -> None:
    async def inner() -> int:
        return 1

    def sneaky(ctx: Any) -> Any:
        return inner()

    wf = workflow("sneaky")(sneaky)
    cmd = (await execute(invocation("sneaky"), wf, FakeLive()))[-1]
    assert cmd["type"] == "runFailed" and "async def" in cmd["error"]["message"]


async def test_step_invocations_never_report_the_run_outcome() -> None:
    # A race whose winner is journaled: replay finishes the workflow before reaching the target.
    @workflow("racedone")
    async def racedone(ctx: Context) -> Any:
        a, b = ctx.step.run("a", lambda: "a"), ctx.step.run("b", lambda: "b")
        done, _ = await asyncio.wait({a, b}, return_when=asyncio.FIRST_COMPLETED)
        return done.pop().result()

    journal = [entry(1, "b", output="b"), entry(2, "a", "running", attempt=0)]
    inv = invocation("racedone", kind="step", target=sid("a"), entries=journal)
    assert await execute(inv, racedone, FakeLive()) == []

    @workflow("fatalstep")
    async def fatalstep(ctx: Context) -> Any:
        return await asyncio.gather(ctx.step.run("x", lambda: 1), ctx.step.run("x", lambda: 2))

    inv = invocation("fatalstep", kind="step", target=sid("x"))
    assert await execute(inv, fatalstep, FakeLive()) == []


async def test_step_calls_from_a_step_function_are_refused() -> None:
    @workflow("nested-call")
    async def nested_call(ctx: Context) -> Any:
        def inner() -> str:
            try:
                ctx.step.run("inner", lambda: 1)
            except RuntimeError as e:
                return str(e)
            return "allowed"

        return await ctx.step.run("outer", inner)

    out = (await execute(invocation("nested-call"), nested_call, FakeLive()))[-1]["output"]
    assert "workflow's own code" in out


async def test_paged_journal_keeps_each_steps_highest_seq_entry() -> None:
    # A step that changed between page fetches comes back on a later page at a higher seq.
    @workflow("paged-dup")
    async def paged_dup(ctx: Context) -> Any:
        a = await ctx.step.run("a", lambda: "live-a")
        cb = await ctx.step.create_callback("cb", timeout="1m")
        return [a, await cb.wait()]

    from helpers import RUN_ID

    from ezgh_workflows.protocol import shard_of

    tok = f"{shard_of(RUN_ID):03x}." + "D" * 43
    inline = [
        entry(1, "a", output="A"),
        entry(2, "cb", "waiting", kind="callback", token=tok, createdSeq=2),
    ]
    live = FakeLive(
        pages=[
            # a stale copy of "a" at a lower seq: it loses, even though it comes later
            {"complete": False, "entries": [entry(0, "a", "running", attempt=0)]},
            # "cb" completed between fetches: its new copy (higher seq, same createdSeq) wins
            {"complete": True, "entries": [entry(5, "cb", kind="callback", token=tok, createdSeq=2, output="ok")]},
        ]
    )
    inv = invocation("paged-dup", entries=inline, complete=False)
    assert await execute(inv, paged_dup, live) == [{"type": "runCompleted", "output": ["A", "ok"]}]
    assert live.page_requests == [2, 2]
