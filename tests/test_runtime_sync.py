import threading
from typing import Any

from helpers import DEFAULT, FakeLive, entry, invocation, sid, types

from ezgh_workflows import CallbackTimeoutError, RunCancelledError, SyncContext, workflow
from ezgh_workflows._runtime.execution import execute


@workflow("sync-seq")
def sync_seq(ctx: SyncContext) -> int:
    assert threading.current_thread() is not threading.main_thread()
    return int(ctx.step.run("one", lambda: 1) + ctx.step.run("two", lambda: 2))


async def test_sync_workflow_first_run_and_replay() -> None:
    live = FakeLive()
    cmds = await execute(invocation("sync-seq"), sync_seq, live)
    assert len(live.checkpoints) == 2 and cmds[-1] == {"type": "runCompleted", "output": 3}
    replay = await execute(
        invocation("sync-seq", entries=[entry(1, "one", output=1), entry(2, "two", output=2)]), sync_seq, FakeLive()
    )
    assert replay == [{"type": "runCompleted", "output": 3}]


@workflow("sync-par")
def sync_par(ctx: SyncContext) -> Any:
    return ctx.step.parallel(("a", lambda: 1), ("b", lambda: 2))


async def test_sync_parallel_plans_its_branches_and_replays_in_branch_order() -> None:
    cmds = await execute(invocation("sync-par"), sync_par, FakeLive())
    assert cmds == [
        {
            "type": "planParallel",
            "steps": [
                {"stepId": sid("a"), "name": "a", "retry": DEFAULT},
                {"stepId": sid("b"), "name": "b", "retry": DEFAULT},
            ],
        }
    ]
    half = [entry(1, "b", output=2), entry(2, "a", "running", attempt=0)]
    assert await execute(invocation("sync-par", entries=half), sync_par, FakeLive()) == []
    replay = await execute(
        invocation("sync-par", entries=[entry(1, "b", output=2), entry(2, "a", output=1)]), sync_par, FakeLive()
    )
    assert replay == [{"type": "runCompleted", "output": [1, 2]}]


async def test_sync_step_invocation() -> None:
    journal = [entry(1, "a", "running", attempt=0), entry(2, "b", "running", attempt=0)]
    cmds = await execute(invocation("sync-par", kind="step", target=sid("b"), entries=journal), sync_par, FakeLive())
    assert cmds == [{"type": "stepCompleted", "stepId": sid("b"), "name": "b", "output": 2}]


async def test_sync_cancel_reaches_the_thread() -> None:
    @workflow("sync-cancel")
    def sync_cancel(ctx: SyncContext) -> str:
        try:
            ctx.step.sleep("wait", "1h")
            return "finished"
        except RunCancelledError:
            ctx.step.run("cleanup", lambda: "cleaned")
            raise

    assert types(await execute(invocation("sync-cancel"), sync_cancel, FakeLive())) == ["sleep"]
    inv = invocation("sync-cancel", entries=[entry(1, "wait", "sleeping", kind="sleep")], cancel=True)
    cmds = await execute(inv, sync_cancel, FakeLive())
    assert types(cmds) == ["stepCompleted", "runFailed"] and cmds[-1]["error"]["code"] == "workflows.cancelled"


async def test_sync_values_callbacks_and_invoke() -> None:
    @workflow("sync-all")
    def sync_all(ctx: SyncContext) -> Any:
        t = ctx.step.now("t")
        u = ctx.step.uuid("u")
        cb = ctx.step.create_callback("cb", timeout="1m")
        try:
            v = cb.wait()
        except CallbackTimeoutError:
            v = "timeout"
        kid = ctx.step.invoke("kid", workflow="k", input=[1])
        return {"t": t.year, "u": len(u), "v": v, "kid": kid, "url": cb.url.startswith("https://workflows-api.test/")}

    live = FakeLive()
    cmds = await execute(invocation("sync-all"), sync_all, live)
    assert types(live.checkpoints[0]["commands"]) == ["valueRecorded", "valueRecorded", "callbackCreated"]
    assert cmds == [{"type": "waitForCallback", "stepId": sid("cb")}]
    tok = live.checkpoints[0]["commands"][2]["token"]
    journal = [
        entry(1, "t", kind="value", output="2026-10-09T00:00:00.000Z"),
        entry(2, "u", kind="value", output="0199a3d0-2b3c-7d4e-9f50-6b7c8d9e0f1a"),
        entry(
            3,
            "cb",
            "failed",
            kind="callback",
            token=tok,
            error={"code": "workflows.callback_timeout", "name": "CallbackTimeout", "message": "x"},
        ),
    ]
    assert await execute(invocation("sync-all", entries=journal), sync_all, FakeLive()) == [
        {"type": "invoke", "stepId": sid("kid"), "name": "kid", "workflow": "k", "input": [1]}
    ]
    journal.append(entry(4, "kid", kind="invoke", output=7))
    assert (await execute(invocation("sync-all", entries=journal), sync_all, FakeLive()))[-1] == {
        "type": "runCompleted",
        "output": {"t": 2026, "u": 36, "v": "timeout", "kid": 7, "url": True},
    }


async def test_suspended_sync_threads_unwind() -> None:
    import asyncio

    done = threading.Event()

    @workflow("sync-unwind")
    def sync_unwind(ctx: SyncContext) -> None:
        try:
            ctx.step.sleep("nap", "1h")
        finally:
            done.set()

    assert types(await execute(invocation("sync-unwind"), sync_unwind, FakeLive())) == ["sleep"]
    for _ in range(100):
        if done.is_set():
            break
        await asyncio.sleep(0.01)
    assert done.is_set()


async def test_sync_failures_and_slow_code_between_steps() -> None:
    import time

    @workflow("sync-fail")
    def sync_fail(ctx: SyncContext) -> None:
        ctx.step.run("a", lambda: 1)
        time.sleep(0.05)  # the thread is busy between steps: the runtime waits for it
        raise KeyError("missing")

    cmds = await execute(invocation("sync-fail"), sync_fail, FakeLive())
    assert cmds[-1]["type"] == "runFailed" and cmds[-1]["error"]["name"] == "KeyError"
