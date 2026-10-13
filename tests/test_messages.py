import math
from typing import Any

import pytest
from helpers import FakeLive, invocation, sid

from ezgh_workflows import PROTOCOL_VERSIONS, Context, RunCancelledError, SyncContext, workflow
from ezgh_workflows._runtime.execution import execute

Q = "0199a3d0-1111-7000-8000-00000000aaaa"
MSG = {
    "messageId": "0199a3d0-2222-7000-8000-000000000001",
    "body": "paid",
    "attributes": {"orderId": "order:o-1"},
    "sentAt": "2026-10-13T09:00:00.123Z",
    "queueId": Q,
}


def message(status: str, **kw: Any) -> dict[str, Any]:
    e: dict[str, Any] = {
        "seq": 2,
        "createdSeq": 1,
        "stepId": sid("paid"),
        "name": "paid",
        "kind": "message",
        "status": status,
        "attempt": 1,
        "queueId": Q,
        "key": "order:o-1",
    }
    return {**e, **kw}


@workflow("waitmessage")
async def waitmessage(ctx: Context) -> Any:
    m = await ctx.step.wait_for_message("paid", queue_id=Q, key="order:o-1", timeout_seconds=3600)
    return {"received": False} if m is None else {"received": True, "body": m["body"], "messageId": m["messageId"]}


def test_sdk_declares_1_2() -> None:
    assert PROTOCOL_VERSIONS == ["1.0", "1.1", "1.2"]


async def test_the_wait_is_created_in_a_checkpoint_then_the_invocation_suspends() -> None:
    live = FakeLive()
    cmds = await execute(invocation("waitmessage", version="1.2"), waitmessage, live)
    assert [cp["commands"] for cp in live.checkpoints] == [
        [
            {
                "type": "messageWaitCreated",
                "stepId": sid("paid"),
                "name": "paid",
                "queueId": Q,
                "key": "order:o-1",
                "timeoutMs": 3_600_000,
            }
        ]
    ]
    assert cmds == [{"type": "waitForMessage", "stepId": sid("paid")}]


async def test_replay_waiting_completed_timed_out_and_offloaded() -> None:
    live = FakeLive()
    cmds = await execute(
        invocation("waitmessage", version="1.2", entries=[message("waiting", seq=1)]), waitmessage, live
    )
    assert live.checkpoints == [] and cmds == [{"type": "waitForMessage", "stepId": sid("paid")}]

    got = await execute(
        invocation("waitmessage", version="1.2", entries=[message("completed", output=MSG)]), waitmessage, FakeLive()
    )
    assert got == [
        {"type": "runCompleted", "output": {"received": True, "body": "paid", "messageId": MSG["messageId"]}}
    ]

    timed_out = await execute(
        invocation("waitmessage", version="1.2", entries=[message("completed", timedOut=True)]), waitmessage, FakeLive()
    )
    assert timed_out == [{"type": "runCompleted", "output": {"received": False}}]

    ref = "pay_" + "a" * 26
    live = FakeLive(payloads={ref: MSG})
    got = await execute(
        invocation("waitmessage", version="1.2", entries=[message("completed", outputRef=ref)]), waitmessage, live
    )
    assert got[0]["output"]["received"] is True


async def test_a_cancelled_wait_raises_the_cancellation_error() -> None:
    caught: list[BaseException] = []

    @workflow("waitmessage")
    async def wf(ctx: Context) -> Any:
        try:
            return await ctx.step.wait_for_message("paid", queue_id=Q, key="order:o-1", timeout_seconds=3600)
        except RunCancelledError as e:
            caught.append(e)
            raise

    entry = message(
        "cancelled", error={"code": "workflows.cancelled", "name": "CancelledError", "message": "cancelled"}
    )
    [failed] = await execute(invocation("waitmessage", version="1.2", entries=[entry], cancel=True), wf, FakeLive())
    assert caught and failed["type"] == "runFailed" and failed["error"]["code"] == "workflows.cancelled"


def test_sync_workflows_wait_too() -> None:
    import asyncio

    @workflow("waitsync")
    def wf(ctx: SyncContext) -> Any:
        m = ctx.step.wait_for_message("paid", queue_id=Q, key="order:o-1", timeout_seconds=60)
        return None if m is None else m["body"]

    cmds = asyncio.run(execute(invocation("waitsync", version="1.2"), wf, FakeLive()))
    assert cmds == [{"type": "waitForMessage", "stepId": sid("paid")}]
    done = asyncio.run(
        execute(invocation("waitsync", version="1.2", entries=[message("completed", output=MSG)]), wf, FakeLive())
    )
    assert done == [{"type": "runCompleted", "output": "paid"}]


async def test_before_1_2_the_run_fails_non_retriably() -> None:
    [failed] = await execute(invocation("waitmessage", version="1.1"), waitmessage, FakeLive())
    assert failed["type"] == "runFailed" and failed["error"]["code"] == "workflows.non_retriable"
    assert "needs protocol 1.2" in failed["error"]["message"]


@pytest.mark.parametrize(
    ("queue_id", "key", "seconds"),
    [("que_abc", "k", 60), (Q, "has space", 60), (Q, "k", 0.5), (Q, "k", 2_592_001), (Q, "k", math.inf)],
)
async def test_bad_options_fail_the_run(queue_id: str, key: str, seconds: float) -> None:
    @workflow("bad")
    async def bad(ctx: Context) -> Any:
        return await ctx.step.wait_for_message("w", queue_id=queue_id, key=key, timeout_seconds=seconds)

    [failed] = await execute(invocation("bad", version="1.2"), bad, FakeLive())
    assert failed["type"] == "runFailed" and failed["error"]["name"] == "ValueError"
