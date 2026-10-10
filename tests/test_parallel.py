"""The frontier rule (D2-19): the same commands for the same code in every SDK."""

import asyncio
from typing import Any

from helpers import DEFAULT, FakeLive, entry, invocation, sid, types

from ezgh_workflows import Context, workflow
from ezgh_workflows._runtime.execution import execute


def _const(v: Any) -> Any:
    return lambda: v


@workflow("fan")
async def fan(ctx: Context) -> Any:
    return sum(await asyncio.gather(*(ctx.step.run(f"p{i}", _const(i)) for i in range(3))))


def planned(*names: str) -> dict[str, Any]:
    return {"type": "planParallel", "steps": [{"stepId": sid(n), "name": n, "retry": DEFAULT} for n in names]}


async def test_gather_plans_every_new_step_in_call_order() -> None:
    live = FakeLive()
    assert await execute(invocation("fan"), fan, live) == [planned("p0", "p1", "p2")]
    assert live.checkpoints == []


async def test_planned_and_running_steps_are_awaited_never_rerun() -> None:
    journal = [
        entry(1, "p0", "running", attempt=0),
        entry(2, "p1", "running", attempt=0),
        entry(3, "p2", "running", attempt=0),
    ]
    assert await execute(invocation("fan", entries=journal), fan, FakeLive()) == []
    journal = [entry(1, "p0", output=0), entry(2, "p1", "running"), entry(3, "p2", "running")]
    assert await execute(invocation("fan", entries=journal), fan, FakeLive()) == []
    done = [entry(1, "p2", output=2), entry(2, "p0", output=0), entry(3, "p1", output=1)]
    assert await execute(invocation("fan", entries=done), fan, FakeLive()) == [{"type": "runCompleted", "output": 3}]


async def test_step_invocation_runs_only_its_target_at_journal_attempt_plus_one() -> None:
    journal = [
        entry(1, "p0", "running", attempt=0),
        entry(2, "p1", "running", attempt=0),
        entry(3, "p2", "running", attempt=0),
    ]
    live = FakeLive()
    cmds = await execute(invocation("fan", kind="step", target=sid("p1"), entries=journal), fan, live)
    assert [c["commands"] for c in live.checkpoints] == [
        [{"type": "stepStarted", "stepId": sid("p1"), "name": "p1", "attempt": 1, "retry": DEFAULT}]
    ]
    assert cmds == [{"type": "stepCompleted", "stepId": sid("p1"), "name": "p1", "output": 1}]
    # a retrying parallel step is journaled running; its step invocation runs the next attempt
    journal[1] = entry(2, "p1", "running", attempt=2)
    live = FakeLive()
    await execute(invocation("fan", kind="step", target=sid("p1"), entries=journal), fan, live)
    assert live.checkpoints[0]["commands"][0]["attempt"] == 3
    # an undiscovered target ends the invocation with nothing
    assert await execute(invocation("fan", kind="step", target=sid("zz")), fan, FakeLive()) == []


async def test_nested_gather_is_discovered_before_planning() -> None:
    @workflow("nested")
    async def nested(ctx: Context) -> Any:
        async def pair(p: str) -> Any:
            return await asyncio.gather(ctx.step.run(f"{p}-1", lambda: 1), ctx.step.run(f"{p}-2", lambda: 2))

        return await asyncio.gather(pair("x"), pair("y"))

    cmds = await execute(invocation("nested"), nested, FakeLive())
    assert [s["name"] for s in cmds[0]["steps"]] == ["x-1", "x-2", "y-1", "y-2"]


async def test_sleep_beside_a_new_step_plans_and_suspends() -> None:
    @workflow("racetimeout")
    async def racetimeout(ctx: Context) -> Any:
        async def limit() -> str:
            await ctx.step.sleep("limit", "1m")
            return "limit"

        work = ctx.step.run("work", lambda: "work")
        done, _ = await asyncio.wait({work, asyncio.ensure_future(limit())}, return_when=asyncio.FIRST_COMPLETED)
        return done.pop().result()

    cmds = await execute(invocation("racetimeout"), racetimeout, FakeLive(), now=lambda: 1791547200.0)
    assert cmds == [
        planned("work"),
        {"type": "sleep", "stepId": sid("limit"), "name": "limit", "until": "2026-10-09T12:01:00.000Z"},
    ]
    journal = [entry(1, "work", "running", attempt=0), entry(2, "limit", "sleeping", kind="sleep")]
    assert await execute(invocation("racetimeout", entries=journal), racetimeout, FakeLive()) == []
    journal = [entry(1, "work", output="work"), entry(2, "limit", "sleeping", kind="sleep")]
    assert await execute(invocation("racetimeout", entries=journal), racetimeout, FakeLive()) == [
        {"type": "runCompleted", "output": "work"}
    ]


async def test_one_new_step_beside_a_running_one_is_planned_not_run_inline() -> None:
    @workflow("staggered")
    async def staggered(ctx: Context) -> Any:
        a = ctx.step.run("a", lambda: "a")
        first = await ctx.step.run("first", lambda: 1)
        b = ctx.step.run("b", lambda: "b")
        return [first, *await asyncio.gather(a, b)]

    journal = [entry(1, "a", "running", attempt=0), entry(2, "first", output=1)]
    assert await execute(invocation("staggered", entries=journal), staggered, FakeLive()) == [planned("b")]


async def test_a_lone_new_step_runs_inline() -> None:
    journal = [entry(1, "p0", output=0), entry(2, "p1", output=1)]

    @workflow("tail")
    async def tail(ctx: Context) -> Any:
        a, b = await asyncio.gather(ctx.step.run("p0", lambda: 0), ctx.step.run("p1", lambda: 1))
        return await ctx.step.run("sum", lambda: a + b)

    live = FakeLive()
    cmds = await execute(invocation("tail", entries=journal), tail, live)
    assert types(live.checkpoints[0]["commands"]) == ["stepStarted"]
    assert cmds[-1] == {"type": "runCompleted", "output": 1}


async def test_a_retrying_step_alongside_other_work_waits() -> None:
    @workflow("retrymix")
    async def retrymix(ctx: Context) -> Any:
        return await asyncio.gather(ctx.step.run("r", lambda: 1), ctx.step.run("n", lambda: 2))

    journal = [entry(1, "r", "retrying", error={"code": "user", "name": "E", "message": "x"})]
    assert await execute(invocation("retrymix", entries=journal), retrymix, FakeLive()) == [planned("n")]


async def test_values_are_recorded_even_while_steps_are_with_the_engine() -> None:
    @workflow("valuebusy")
    async def valuebusy(ctx: Context) -> Any:
        a = ctx.step.run("a", lambda: 1)
        t = await ctx.step.uuid("id")
        return [await a, t]

    journal = [entry(1, "a", "running", attempt=0)]
    cmds = await execute(invocation("valuebusy", entries=journal), valuebusy, FakeLive())
    assert types(cmds) == ["valueRecorded"]


async def test_protocol_1_0_runs_gathered_steps_one_at_a_time() -> None:
    live = FakeLive()
    cmds = await execute(invocation("fan", version="1.0"), fan, live)
    assert [c["commands"][-1]["name"] for c in live.checkpoints] == ["p0", "p1", "p2"]
    assert cmds[-1] == {"type": "runCompleted", "output": 3}


async def test_many_steps_across_iterations_keep_call_order() -> None:
    @workflow("wide")
    async def wide(ctx: Context) -> Any:
        tasks = [asyncio.ensure_future(_chain(ctx, i)) for i in range(20)]
        return sum(await asyncio.gather(*tasks))

    cmds = await execute(invocation("wide"), wide, FakeLive())
    assert [s["name"] for s in cmds[0]["steps"]] == [f"c{i}" for i in range(20)]


async def _chain(ctx: Context, i: int) -> int:
    await asyncio.sleep(0)
    return int(await ctx.step.run(f"c{i}", lambda: i))
