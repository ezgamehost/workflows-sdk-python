"""The conformance app: workflows-stack's protocol/conformance/workflows.md, row by row and in its
order, served for cmd/workflows-conformance (scripts/conformance.sh)."""

import asyncio
import os
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from typing import Any

from ezgh_workflows import Context, NonRetriableError, RetryAfterError, WorkflowError, serve, workflow


class Error(Exception):
    """A customer error named Error on the wire, with optional data."""

    def __init__(self, message: str, data: Any = None) -> None:
        super().__init__(message)
        if data is not None:
            self.data = data


def _raise(e: BaseException) -> Any:
    raise e


def _const(v: Any) -> Any:
    return lambda: v


@workflow("sequence")
async def sequence(ctx: Context) -> int:
    total = 0
    for i in range(ctx.input["n"]):
        total += await ctx.step.run("add", _const(i))
    return total


@workflow("echo")
async def echo(ctx: Context) -> Any:
    same = await ctx.step.run("echo", lambda: ctx.input)
    when = await ctx.step.run("date", lambda: datetime(1970, 1, 1, tzinfo=UTC))
    nothing = await ctx.step.run("nothing", lambda: None)
    return {"same": same, "when": when, "nothing": nothing}


@workflow("race")
async def race(ctx: Context) -> Any:
    a, b = ctx.step.run("a", lambda: "a"), ctx.step.run("b", lambda: "b")
    done, _ = await asyncio.wait({a, b}, return_when=asyncio.FIRST_COMPLETED)
    w = done.pop().result()
    return await ctx.step.run(f"after-{w}", lambda: w)


@workflow("flaky")
async def flaky(ctx: Context) -> Any:
    return await ctx.step.run(
        "flaky", lambda: _raise(Error("boom", {"n": 1})), retry={"maxAttempts": 3, "initialMs": 10}
    )


@workflow("fatal")
async def fatal(ctx: Context) -> Any:
    return await ctx.step.run("fatal", lambda: _raise(NonRetriableError("nope")))


@workflow("slow")
async def slow(ctx: Context) -> Any:
    return await ctx.step.run("slow", lambda: _raise(RetryAfterError("slow down", 1500)))


@workflow("sleepy")
async def sleepy(ctx: Context) -> Any:
    await ctx.step.sleep("nap", "2s")
    return await ctx.step.run("after", lambda: "woke")


@workflow("values")
async def values(ctx: Context) -> Any:
    t = await ctx.step.now("t")
    rid = await ctx.step.uuid("id")
    return await ctx.step.run("use", lambda: {"year": t.year >= 2026, "idLength": len(rid)})


@workflow("dupe")
async def dupe(ctx: Context) -> Any:
    return await asyncio.gather(ctx.step.run("x", lambda: 1), ctx.step.run("x", lambda: 2))


@workflow("big")
async def big(ctx: Context) -> Any:
    return await ctx.step.run("big", lambda: "x" * 1_048_577)


@workflow("cancellable")
async def cancellable(ctx: Context) -> Any:
    try:
        await ctx.step.sleep("wait", "1h")
        return "finished"
    finally:
        await ctx.step.run("cleanup", lambda: "cleaned")


@workflow("caught")
async def caught(ctx: Context) -> Any:
    try:
        await ctx.step.run("bad", lambda: 1)
        return "unreachable"
    except Exception as e:
        cause = e.__cause__
        return {
            "isWorkflowError": isinstance(e, WorkflowError),
            "code": getattr(e, "code", None),
            "name": getattr(e, "name", None),
            "data": getattr(e, "data", None),
            "causeName": getattr(cause, "name", None),
            "causeMessage": getattr(cause, "message", None),
        }


@workflow("explicitid")
async def explicitid(ctx: Context) -> Any:
    return await ctx.step.run("x", lambda: 1, id="custom-1")


@workflow("tuned", retry={"maxAttempts": 2})
async def tuned(ctx: Context) -> Any:
    return await ctx.step.run("t", lambda: 1)


def _post(url: str) -> str:
    try:
        with urllib.request.urlopen(urllib.request.Request(url, method="POST", data=b""), timeout=10) as r:
            r.read()
    except urllib.error.HTTPError:
        pass  # any answer
    return "done"


@workflow("effect")
async def effect(ctx: Context) -> Any:
    return await ctx.step.run("effect", lambda: _post(ctx.input["url"]))


@workflow("coded")
async def coded(ctx: Context) -> Any:
    return await ctx.step.run("coded", lambda: _raise(WorkflowError("teapot", code="http.418", data={"status": 418})))


def _wide() -> Any:
    e = Error("wide")
    e.stack = "é" * 5000  # type: ignore[attr-defined]
    raise e


@workflow("widestack")
async def widestack(ctx: Context) -> Any:
    return await ctx.step.run("wide", _wide)


@workflow("widename")
async def widename(ctx: Context) -> Any:
    return await ctx.step.run("é" * 200, lambda: 1)


@workflow("dupeid")
async def dupeid(ctx: Context) -> Any:
    return await asyncio.gather(ctx.step.run("a", lambda: 1, id="same"), ctx.step.run("b", lambda: 2, id="same"))


@workflow("approval")
async def approval(ctx: Context) -> Any:
    cb = await ctx.step.create_callback("approval", timeout=60_000)
    url_ok = await ctx.step.run("notify", lambda: cb.url == ctx.input["apiBase"] + "/public/v1/callbacks/" + cb.token)
    return {"value": await cb.wait(), "urlOk": url_ok}


@workflow("approvaltimeout")
async def approvaltimeout(ctx: Context) -> Any:
    cb = await ctx.step.create_callback("approval", timeout=1000)
    try:
        await cb.wait()
        return "completed"
    except WorkflowError as e:
        return {"code": e.code}


@workflow("parent")
async def parent(ctx: Context) -> Any:
    return await ctx.step.invoke("child", workflow="sequence", input={"n": 3})


@workflow("parentcatch")
async def parentcatch(ctx: Context) -> Any:
    try:
        await ctx.step.invoke("child", workflow="fatal")
        return "unreachable"
    except WorkflowError as e:
        return {"code": e.code, "causeCode": getattr(e.__cause__, "code", None)}


@workflow("fanout")
async def fanout(ctx: Context) -> Any:
    return sum(await asyncio.gather(*(ctx.step.run(f"p{i}", _const(i)) for i in range(3))))


@workflow("racetimeout")
async def racetimeout(ctx: Context) -> Any:
    async def limit() -> str:
        await ctx.step.sleep("limit", "1m")
        return "limit"

    work = ctx.step.run("work", lambda: "work")
    done, _ = await asyncio.wait({work, asyncio.ensure_future(limit())}, return_when=asyncio.FIRST_COMPLETED)
    return done.pop().result()


@workflow("scheduled")
async def scheduled(ctx: Context) -> Any:
    return ctx.run.schedule.to_wire() if ctx.run.schedule is not None else None


@workflow("pulled")
async def pulled(ctx: Context) -> Any:
    return await ctx.step.run("compute", lambda: ctx.input["x"] * 2)


@workflow("longstep")
async def longstep(ctx: Context) -> Any:
    def long() -> str:
        end = time.monotonic() + 10
        while time.monotonic() < end:
            if ctx.step.cancelled():
                return "aborted"
            time.sleep(0.02)
        return "done"

    done = await ctx.step.run("long", long)
    return await ctx.step.run("after", lambda: done)


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


@workflow("cancelfanout")
async def cancelfanout(ctx: Context) -> Any:
    try:
        await asyncio.gather(ctx.step.run("p0", lambda: 0), ctx.step.run("p1", lambda: 1))
        return "done"
    finally:
        await ctx.step.run("cleanup", lambda: "cleaned")


@workflow("toowide")
async def toowide(ctx: Context) -> Any:
    return await asyncio.gather(*(ctx.step.run(f"s{i}", _const(i)) for i in range(1001)))


@workflow("traced")
async def traced(ctx: Context) -> Any:
    return ctx.traceparent


WORKFLOWS = [
    sequence,
    echo,
    race,
    flaky,
    fatal,
    slow,
    sleepy,
    values,
    dupe,
    big,
    cancellable,
    caught,
    explicitid,
    tuned,
    effect,
    coded,
    widestack,
    widename,
    dupeid,
    approval,
    approvaltimeout,
    parent,
    parentcatch,
    fanout,
    racetimeout,
    scheduled,
    pulled,
    longstep,
    branches,
    cancelfanout,
    toowide,
    traced,
]

app = serve(
    workflows=WORKFLOWS,
    signing_secrets=[os.environ.get("CONFORMANCE_SECRET")],
    max_invocation_seconds=300,
    deployment="conformance",
)
