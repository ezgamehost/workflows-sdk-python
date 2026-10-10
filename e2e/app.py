"""The end-to-end app: served on 127.0.0.1:3200 under a supervisor (scripts/e2e.sh), driven by
e2e/run.py through the real workflows-stack."""

import asyncio
import json
import os
from typing import Any

from ezgh_workflows import Context, NonRetriableError, SyncContext, serve, workflow
from ezgh_workflows.serve import Receive, Scope, Send

STATE = os.environ.get("E2E_STATE", "/tmp/workflows-e2e-state.json")


def read() -> dict[str, Any]:
    try:
        with open(STATE, encoding="utf-8") as f:
            state: dict[str, Any] = json.load(f)
            return state
    except (OSError, ValueError):
        return {"fixed": False, "prepare": 0, "cleanup": 0}


def write(s: dict[str, Any]) -> None:
    with open(STATE, "w", encoding="utf-8") as f:
        json.dump(s, f)


def bump(k: str) -> None:
    s = read()
    s[k] = s.get(k, 0) + 1
    write(s)


def _attempt(ctx: Context) -> int:
    return int(ctx.step.idempotency_key().rsplit(":", 1)[1])


@workflow("e2e-complete")
async def complete(ctx: Context) -> Any:
    a = await ctx.step.run("one", lambda: 1)
    b = await ctx.step.run("two", lambda: 2)
    return {"sum": a + b}


@workflow("e2e-sleep")
async def sleepy(ctx: Context) -> Any:
    large = await ctx.step.run("large", lambda: "y" * 100_000)
    await ctx.step.sleep("nap", "2s")
    return await ctx.step.run("after", lambda: len(large))


@workflow("e2e-retry")
async def retry(ctx: Context) -> Any:
    def flaky() -> int:
        n = _attempt(ctx)
        if n < 3:
            raise RuntimeError(f"attempt {n}")
        return n

    return await ctx.step.run("flaky", flaky, retry={"maxAttempts": 5, "initialMs": 200})


@workflow("e2e-crash")
async def crash(ctx: Context) -> Any:
    def crashy() -> str:
        if _attempt(ctx) == 1:
            os._exit(1)  # the process dies mid-step; the supervisor restarts it
        return "survived"

    return await ctx.step.run("crashy", crashy, retry={"maxAttempts": 3, "initialMs": 200})


@workflow("e2e-cancel")
async def cancel(ctx: Context) -> Any:
    try:
        await ctx.step.sleep("wait", "1h")
        return "finished"
    finally:

        def cleanup() -> str:
            bump("cleanup")
            return "cleaned"

        await ctx.step.run("cleanup", cleanup)


@workflow("e2e-fixable")
async def fixable(ctx: Context) -> Any:
    def prepare() -> bool:
        bump("prepare")
        return True

    def work() -> str:
        if not read().get("fixed"):
            raise NonRetriableError("not fixed yet")
        return "fixed"

    await ctx.step.run("prepare", prepare)
    return await ctx.step.run("work", work)


@workflow("e2e-exact")
async def exact(ctx: Context) -> Any:
    """Recorded JSON replays exactly: key order and NUL characters survive the journal (a sleep
    forces the replay)."""
    v = await ctx.step.run("make", lambda: json.loads('{"b":1,"a":"x\\u0000y","c":{"z":[2,1],"y":null}}'))
    await ctx.step.sleep("nap", "1s")
    return await ctx.step.run("echo", lambda: v)


@workflow("e2e-approval")
async def approval(ctx: Context) -> Any:
    cb = await ctx.step.create_callback("approval", timeout="2m")

    def publish() -> bool:
        s = read()
        s["callbackUrl"] = cb.url
        write(s)
        return True

    await ctx.step.run("publish", publish)
    return await cb.wait()


@workflow("e2e-child")
async def child(ctx: Context) -> Any:
    return await ctx.step.run("times-ten", lambda: ctx.input["n"] * 10)


@workflow("e2e-parent")
async def parent(ctx: Context) -> Any:
    v = await ctx.step.invoke("child", workflow="e2e-child", input=ctx.input)
    return await ctx.step.run("plus-one", lambda: v + 1)


@workflow("e2e-fanout")
async def fanout(ctx: Context) -> Any:
    async def part(i: int) -> int:
        await asyncio.sleep(0.2)
        return i

    def step(i: int) -> Any:
        return lambda: part(i)

    return sum(await asyncio.gather(*(ctx.step.run(f"part-{i}", step(i)) for i in range(5))))


@workflow("e2e-scheduled")
async def scheduled(ctx: Context) -> Any:
    return await ctx.step.run("note", lambda: ctx.run.schedule.to_wire() if ctx.run.schedule else None)


@workflow("e2e-sync")
def sync_parallel(ctx: SyncContext) -> Any:
    a, b = ctx.step.parallel(("a", lambda: 20), ("b", lambda: 22))
    return a + b


WORKFLOWS = [
    complete,
    sleepy,
    retry,
    crash,
    cancel,
    fixable,
    exact,
    approval,
    child,
    parent,
    fanout,
    scheduled,
    sync_parallel,
]
handler = serve(
    workflows=WORKFLOWS,
    signing_secrets=[os.environ.get("EZGH_WORKFLOWS_SIGNING_SECRET")],
    max_invocation_seconds=60,
    deployment="e2e",
)


async def _json(send: Send, body: Any) -> None:
    data = json.dumps(body).encode()
    await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"application/json")]})
    await send({"type": "http.response.body", "body": data})


async def app(scope: Scope, receive: Receive, send: Send) -> None:
    if scope["type"] == "http" and scope.get("path") == "/__e2e/fix":
        s = read()
        s["fixed"] = True
        write(s)
        return await _json(send, "ok")
    if scope["type"] == "http" and scope.get("path") == "/__e2e/state":
        return await _json(send, read())
    return await handler(scope, receive, send)
