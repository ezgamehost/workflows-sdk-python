from typing import Any

from helpers import RUN_ID, FakeLive, entry, invocation, sid, types

from ezgh_workflows import CallbackTimeoutError, ChildFailedError, Context, workflow
from ezgh_workflows._runtime.execution import execute
from ezgh_workflows.protocol import shard_of


@workflow("approval")
async def approval(ctx: Context) -> Any:
    cb = await ctx.step.create_callback("approval", timeout="7d")
    await ctx.step.run("ask", lambda: cb.url)
    try:
        return await cb.wait()
    except CallbackTimeoutError:
        return "timed out"


async def test_create_callback_is_acknowledged_before_the_url_is_used() -> None:
    live = FakeLive()
    cmds = await execute(invocation("approval"), approval, live)
    created = live.checkpoints[0]["commands"][0]
    assert set(created) == {"type", "stepId", "name", "token", "timeoutMs"}
    assert (
        created["type"] == "callbackCreated"
        and created["stepId"] == sid("approval")
        and created["timeoutMs"] == 604_800_000
    )
    token = created["token"]
    assert token.startswith(f"{shard_of(RUN_ID):03x}.") and len(token) == 47
    assert live.checkpoints[1]["commands"][-1]["name"] == "ask"
    assert cmds[0] == {
        "type": "stepCompleted",
        "stepId": sid("ask"),
        "name": "ask",
        "output": f"https://workflows-api.test/public/v1/callbacks/{token}",
    }
    assert cmds[-1] == {"type": "waitForCallback", "stepId": sid("approval")}


async def test_superseded_callback_creation_never_shows_the_url() -> None:
    import pytest

    from ezgh_workflows._runtime.live import InvocationSuperseded

    seen: list[str] = []

    @workflow("cbleak")
    async def cbleak(ctx: Context) -> Any:
        cb = await ctx.step.create_callback("c")
        seen.append(cb.url)

    with pytest.raises(InvocationSuperseded):
        await execute(invocation("cbleak"), cbleak, FakeLive(supersede_at=1))
    assert seen == []


async def test_callback_waiting_completion_and_timeout_replay() -> None:
    tok = f"{shard_of(RUN_ID):03x}." + "A" * 43
    url = f"https://workflows-api.test/public/v1/callbacks/{tok}"
    waiting = [entry(1, "approval", "waiting", kind="callback", token=tok), entry(2, "ask", output=url)]
    assert await execute(invocation("approval", entries=waiting), approval, FakeLive()) == [
        {"type": "waitForCallback", "stepId": sid("approval")}
    ]
    done = [
        entry(1, "approval", "completed", kind="callback", output={"ok": True}, token=tok),
        entry(2, "ask", output=url),
    ]
    assert (await execute(invocation("approval", entries=done), approval, FakeLive()))[-1] == {
        "type": "runCompleted",
        "output": {"ok": True},
    }
    expired = [
        entry(
            1,
            "approval",
            "failed",
            kind="callback",
            token=tok,
            error={"code": "workflows.callback_timeout", "name": "CallbackTimeout", "message": "expired"},
        ),
        entry(2, "ask", output=url),
    ]
    assert (await execute(invocation("approval", entries=expired), approval, FakeLive()))[-1] == {
        "type": "runCompleted",
        "output": "timed out",
    }


async def test_replayed_callbacks_rebuild_the_same_url() -> None:
    tok = f"{shard_of(RUN_ID):03x}." + "B" * 43

    @workflow("cburl")
    async def cburl(ctx: Context) -> Any:
        cb = await ctx.step.create_callback("c", timeout=60_000)
        return {"url": cb.url, "token": cb.token}

    out = await execute(
        invocation("cburl", entries=[entry(1, "c", "waiting", kind="callback", token=tok)]), cburl, FakeLive()
    )
    assert out == [
        {
            "type": "runCompleted",
            "output": {"url": f"https://workflows-api.test/public/v1/callbacks/{tok}", "token": tok},
        }
    ]


async def test_callbacks_and_invoke_need_protocol_1_1() -> None:
    cmds = await execute(invocation("approval", version="1.0"), approval, FakeLive())
    assert types(cmds) == ["runFailed"] and cmds[0]["error"]["code"] == "workflows.non_retriable"
    cmds = await execute(invocation("parent", version="1.0"), parent, FakeLive())
    assert cmds[0]["error"]["code"] == "workflows.non_retriable"


async def test_callback_timeouts_are_bounded() -> None:
    @workflow("cbbad")
    async def cbbad(ctx: Context) -> Any:
        out = []
        for t in ("500ms", "2y", 999):
            try:
                await ctx.step.create_callback("c", timeout=t)
            except ValueError:
                out.append("ValueError")
        return out

    assert (await execute(invocation("cbbad"), cbbad, FakeLive()))[-1]["output"] == ["ValueError"] * 3


@workflow("parent")
async def parent(ctx: Context) -> Any:
    try:
        return await ctx.step.invoke("child", workflow="child-wf", input={"n": 1})
    except ChildFailedError as e:
        cause = e.__cause__
        return {
            "failed": e.message,
            "code": e.code,
            "cause": getattr(cause, "name", None),
            "causeCode": getattr(cause, "code", None),
        }


async def test_invoke_suspends_then_resolves_or_raises() -> None:
    cmds = await execute(invocation("parent"), parent, FakeLive())
    assert cmds == [
        {"type": "invoke", "stepId": sid("child"), "name": "child", "workflow": "child-wf", "input": {"n": 1}}
    ]
    assert (
        await execute(invocation("parent", entries=[entry(1, "child", "waiting", kind="invoke")]), parent, FakeLive())
        == []
    )
    ok = [entry(1, "child", kind="invoke", output=42)]
    assert (await execute(invocation("parent", entries=ok), parent, FakeLive()))[-1] == {
        "type": "runCompleted",
        "output": 42,
    }
    bad = [
        entry(
            1,
            "child",
            "failed",
            kind="invoke",
            error={
                "code": "workflows.child_failed",
                "name": "ChildFailed",
                "message": "child failed",
                "cause": {"code": "workflows.non_retriable", "name": "NonRetriableError", "message": "x"},
            },
        )
    ]
    assert (await execute(invocation("parent", entries=bad), parent, FakeLive()))[-1] == {
        "type": "runCompleted",
        "output": {
            "failed": "child failed",
            "code": "workflows.child_failed",
            "cause": "NonRetriableError",
            "causeCode": "workflows.non_retriable",
        },
    }


async def test_invoke_without_input_omits_it_and_takes_a_workflow_object() -> None:
    @workflow("noinput")
    async def noinput(ctx: Context) -> Any:
        return await ctx.step.invoke("kid", workflow=parent)

    assert await execute(invocation("noinput"), noinput, FakeLive()) == [
        {"type": "invoke", "stepId": sid("kid"), "name": "kid", "workflow": "parent"}
    ]

    @workflow("badinvoke")
    async def badinvoke(ctx: Context) -> Any:
        out = []
        for wf, inp in (("bad name", None), ("ok", {1, 2}), ("ok", "x" * 1_048_577)):
            try:
                await ctx.step.invoke("k", workflow=wf, input=inp)
            except (TypeError, ValueError) as e:
                out.append(type(e).__name__)
        return out

    assert (await execute(invocation("badinvoke"), badinvoke, FakeLive()))[-1]["output"] == [
        "ValueError",
        "TypeError",
        "ValueError",
    ]


async def test_waits_invokes_and_new_steps_end_together_in_call_order() -> None:
    import asyncio

    @workflow("everything")
    async def everything(ctx: Context) -> Any:
        cb = await ctx.step.create_callback("cb")
        return await asyncio.gather(
            ctx.step.run("r1", lambda: 1),
            cb.wait(),
            ctx.step.invoke("kid", workflow="k"),
            ctx.step.run("r2", lambda: 2),
        )

    live = FakeLive()
    cmds = await execute(invocation("everything"), everything, live)
    assert types(live.checkpoints[0]["commands"]) == ["callbackCreated"]
    assert types(cmds) == ["planParallel", "waitForCallback", "invoke"]
    assert [s["name"] for s in cmds[0]["steps"]] == ["r1", "r2"]


async def test_callback_creation_replays_at_created_seq() -> None:
    import asyncio

    @workflow("branches")
    async def branches(ctx: Context) -> Any:
        async def first() -> Any:
            cb = await ctx.step.create_callback("approval", timeout=60_000)
            log = await ctx.step.run("log", lambda: "A")
            await cb.wait()
            return log

        async def second() -> Any:
            await ctx.step.run("prep", lambda: "P")
            return await ctx.step.run("log", lambda: "B")

        a, b = await asyncio.gather(first(), second())
        return {"a": a, "b": b}

    live = FakeLive()
    cmds = await execute(invocation("branches"), branches, live)
    assert types(live.checkpoints[0]["commands"]) == ["callbackCreated"]
    assert [s["name"] for s in cmds[0]["steps"]] == ["prep", "log"] and cmds[0]["steps"][1]["stepId"] == sid("log", 0)
    tok = f"{shard_of(RUN_ID):03x}." + "C" * 43
    journal = [
        entry(5, "approval", kind="callback", output={"ok": True}, token=tok, createdSeq=1),
        entry(2, "log", output="A"),
        entry(3, "prep", output="P"),
        entry(4, "log", n=1, output="B"),
    ]
    assert await execute(invocation("branches", entries=journal), branches, FakeLive()) == [
        {"type": "runCompleted", "output": {"a": "A", "b": "B"}}
    ]


def test_created_seq_must_be_at_least_one() -> None:
    import json

    import pytest

    from ezgh_workflows._runtime.invocation import InvalidInvocation, parse_invocation

    tok = f"{shard_of(RUN_ID):03x}." + "C" * 43
    for bad in (0, -1, "1", True):
        inv = invocation("x", entries=[entry(2, "a", kind="callback", status="waiting", token=tok, createdSeq=bad)])
        with pytest.raises(InvalidInvocation):
            parse_invocation(json.dumps(inv))
    ok = invocation("x", entries=[entry(2, "a", kind="callback", status="waiting", token=tok, createdSeq=1)])
    parse_invocation(json.dumps(ok))
