import asyncio
import json
import time
from typing import Any

import pytest
from helpers import invocation

from ezgh_workflows import Context, Worker, WorkflowsApiError, workflow

APP = "0199a3c2-7d30-7f62-9b5c-4eaf8d3f6b92"
ORG, PRJ = "0199a3c2-5b1e-7d40-9f3a-2c8e6b1d4f70", "0199a3c2-6c2f-7e51-8a4b-3d9f7c2e5a81"


@workflow("pulled")
async def pulled(ctx: Context) -> str:
    return str(await ctx.step.run("work", lambda: "done"))


@workflow("longstep")
async def longstep(ctx: Context) -> str:
    def long() -> str:
        end = time.monotonic() + 5
        while time.monotonic() < end:
            if ctx.step.cancelled():
                return "saw cancel"
            time.sleep(0.02)
        return "done"

    return str(await ctx.step.run("long", long))


class FakeLiveHost:
    """Serves poll (scripted answers, then empty), checkpoint, heartbeat and complete over HTTP/1.1."""

    def __init__(self) -> None:
        self.polls: list[dict[str, Any]] = []
        self.poll_auth: list[str] = []
        self.answers: list[tuple[int, Any]] = []  # each poll's answer, in order; then 204s
        self.completed: list[dict[str, Any]] = []
        self.checkpoints: list[dict[str, Any]] = []
        self.checkpoint_status = 204
        self.heartbeats = 0
        self.cancel_after: int | None = None
        self.port = 0

    async def start(self) -> None:
        self.server = await asyncio.start_server(self.handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]

    def task(self, workflow: str, **kw: Any) -> dict[str, Any]:
        inv: dict[str, Any] = invocation(workflow, **kw)
        inv["endpoints"]["live"] = f"http://127.0.0.1:{self.port}"
        inv["token"] = f"tok-{len(self.polls)}-{workflow}"
        return inv

    async def handle(self, r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        head = (await r.readuntil(b"\r\n\r\n")).decode()
        lines = head.split("\r\n")
        _, path, _ = lines[0].split(" ")
        hdrs = {ln.split(":", 1)[0].lower(): ln.split(":", 1)[1].strip() for ln in lines[1:] if ":" in ln}
        length = int(hdrs.get("content-length", "0"))
        body: Any = json.loads(await r.readexactly(length)) if length else None
        status, out = 204, None
        if path == f"/v1/organizations/{ORG}/projects/{PRJ}/workers/poll":
            self.polls.append(body)
            self.poll_auth.append(hdrs.get("authorization", ""))
            if self.answers:
                status, out = self.answers.pop(0)
            else:
                await asyncio.sleep(0.2)
        elif path == "/v1/invocations/checkpoint":
            self.checkpoints.append(body)
            status = self.checkpoint_status
        elif path == "/v1/invocations/heartbeat":
            self.heartbeats += 1
            cancel = self.cancel_after is not None and self.heartbeats >= self.cancel_after
            status, out = 200, {"cancel": {"requestedAt": "x", "graceUntil": "y"} if cancel else None}
        elif path == "/v1/invocations/complete":
            self.completed.append(body)
        data = json.dumps(out).encode() if out is not None else b""
        head_out = f"HTTP/1.1 {status} X\r\nContent-Length: {len(data)}\r\nConnection: close\r\n\r\n"
        w.write(head_out.encode() + data)
        await w.drain()
        w.close()


def worker(host: FakeLiveHost, **kw: Any) -> Worker:
    args: dict[str, Any] = {
        "api_key": "ezgh_x",
        "organization_id": ORG,
        "project_id": PRJ,
        "app": APP,
        "workflows": [pulled, longstep],
        "queue": "conformance",
        "live_url": f"http://127.0.0.1:{host.port}",
        "heartbeat_interval": 0.05,
        "worker_id": "w-1",
        **kw,
    }
    return Worker(**args)


async def until(cond: Any, seconds: float = 5.0) -> None:
    end = time.monotonic() + seconds
    while not cond() and time.monotonic() < end:  # noqa: ASYNC110
        await asyncio.sleep(0.02)


async def test_worker_polls_runs_and_completes() -> None:
    host = FakeLiveHost()
    await host.start()
    host.answers = [(200, {"invocations": [host.task("pulled"), host.task("pulled")]})]
    w = worker(host, max_tasks=4, concurrency=3)
    task = asyncio.create_task(w.run())
    await until(lambda: len(host.completed) == 2)
    w.stop()
    await asyncio.wait_for(task, 5)
    assert host.poll_auth[0] == "Bearer ezgh_x"
    assert host.polls[0] == {
        "app": APP,
        "queue": "conformance",
        "maxTasks": 3,
        "workerId": "w-1",
        "protocolVersions": ["1.0", "1.1", "1.2"],
        "sdk": {"name": "ezgh-workflows", "version": "0.3.0"},
        "workflows": [{"name": "pulled"}, {"name": "longstep"}],
    }
    assert host.polls[1]["workflows"] == host.polls[0]["workflows"]  # every poll announces
    assert host.completed[0]["commands"][-1] == {"type": "runCompleted", "output": "done"}
    assert host.completed[0]["sdk"]["name"] == "ezgh-workflows"
    assert host.checkpoints[0]["commands"][-1]["type"] == "stepStarted"


async def test_heartbeat_cancel_reaches_the_step() -> None:
    host = FakeLiveHost()
    await host.start()
    host.cancel_after = 2
    host.answers = [(200, {"invocations": [host.task("longstep")]})]
    w = worker(host)
    task = asyncio.create_task(w.run())
    await until(lambda: bool(host.completed))
    w.stop()
    await asyncio.wait_for(task, 5)
    assert host.heartbeats >= 2
    assert host.completed[0]["commands"][-1] == {"type": "runCompleted", "output": "saw cancel"}


async def test_superseded_checkpoint_drops_the_invocation() -> None:
    host = FakeLiveHost()
    await host.start()
    host.checkpoint_status = 409
    host.answers = [(200, {"invocations": [host.task("pulled")]})]
    w = worker(host)
    task = asyncio.create_task(w.run())
    await until(lambda: bool(host.checkpoints))
    await asyncio.sleep(0.3)
    w.stop()
    await asyncio.wait_for(task, 5)
    assert host.completed == []


async def test_unknown_workflows_complete_with_workflow_not_found() -> None:
    host = FakeLiveHost()
    await host.start()
    host.answers = [(200, {"invocations": [host.task("gone")]})]
    w = worker(host)
    task = asyncio.create_task(w.run())
    await until(lambda: bool(host.completed))
    w.stop()
    await asyncio.wait_for(task, 5)
    assert host.completed[0]["commands"] == [{"type": "workflowNotFound", "workflow": "gone"}]


async def test_stop_ends_polling_promptly() -> None:
    host = FakeLiveHost()
    await host.start()
    w = worker(host)
    task = asyncio.create_task(w.run())
    await asyncio.sleep(0.3)
    t0 = time.monotonic()
    w.stop()
    await asyncio.wait_for(task, 3)
    assert time.monotonic() - t0 < 1.0


async def test_unauthorized_and_refused_polls_are_fatal() -> None:
    host = FakeLiveHost()
    await host.start()
    host.answers = [(401, {"error": {"code": "unauthorized"}})]
    with pytest.raises(WorkflowsApiError) as e:
        await asyncio.wait_for(worker(host).run(), 5)
    assert e.value.status == 401 and len(host.polls) == 1
    host.answers = [(403, {})]
    with pytest.raises(WorkflowsApiError):
        await asyncio.wait_for(worker(host).run(), 5)
    host.answers = [(400, {"error": {"code": "invalid_request"}})]
    with pytest.raises(WorkflowsApiError) as e:
        await asyncio.wait_for(worker(host).run(), 5)
    assert e.value.code == "invalid_request"


async def test_unavailable_polls_are_retried() -> None:
    host = FakeLiveHost()
    await host.start()
    host.answers = [(503, {}), (200, {"invocations": [host.task("pulled")]})]
    w = worker(host, backoff_max=0.2)
    task = asyncio.create_task(w.run())
    await until(lambda: bool(host.completed))
    w.stop()
    await asyncio.wait_for(task, 5)
    assert len(host.completed) == 1


def test_options_are_checked() -> None:
    base: dict[str, Any] = {
        "api_key": "k",
        "organization_id": ORG,
        "project_id": PRJ,
        "app": APP,
        "workflows": [pulled],
    }
    for bad in (
        {"app": "app-1"},
        {"queue": "Bad_Queue"},
        {"queue": "a" * 64},
        {"max_tasks": 11},
        {"max_tasks": 0},
        {"concurrency": 0},
        {"worker_id": "has space"},
        {"heartbeat_interval": 0},
        {"workflows": [pulled, pulled]},
    ):
        with pytest.raises(ValueError):
            Worker(**{**base, **bad})
    w = Worker(**{**base, "api_key": "ezgh_live_8f3k2m9q7x1v5b6n4c0z2a8s"})
    assert "ezgh_live_8f3k2m9q7x1v5b6n4c0z2a8s" not in repr(w) + str(vars(w)) + repr(w._key)
    with pytest.raises(ValueError):
        Worker(**{**base, "heartbeat_interval": 45})
    import re

    assert re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", w.worker_id)


def test_dev_names_are_sanitized(monkeypatch: pytest.MonkeyPatch) -> None:
    import getpass

    from ezgh_workflows.worker import dev_name

    for k in ("USER", "USERNAME", "LOGNAME"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(getpass, "getuser", lambda: "Matt.Zhao@corp")
    assert dev_name() == "dev-matt-zhao-corp"
    assert dev_name("a_b") == "dev-a-b" and dev_name("___") == "dev-dev" and len(dev_name("x" * 99)) == 62


def test_dev_mode_finds_or_creates_its_app(monkeypatch: pytest.MonkeyPatch) -> None:
    from ezgh_workflows.client import Client

    calls: list[str] = []

    def create(self: Client, name: str, **kw: Any) -> Any:
        calls.append(f"create {name} {kw['kind']} {kw['queue']}")
        raise WorkflowsApiError(409, "app_exists", "exists")

    def list_apps(self: Client, **kw: Any) -> Any:
        return {"apps": [{"id": APP, "name": "dev-me", "kind": "pull", "queue": "dev-me"}], "nextCursor": None}

    monkeypatch.setattr(Client, "create_app", create)
    monkeypatch.setattr(Client, "list_apps", list_apps)
    w = Worker.dev(api_key="k", organization_id=ORG, project_id=PRJ, workflows=[pulled], user="me")
    assert calls == ["create dev-me pull dev-me"] and "dev-me" in repr(w) and APP in repr(w)


async def test_a_superseded_heartbeat_stops_the_step_and_reports_nothing() -> None:
    class Host(FakeLiveHost):
        async def handle(self, r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
            head = (await r.readuntil(b"\r\n\r\n")).decode()
            if " /v1/invocations/heartbeat " in head.split("\r\n")[0] + " ":
                length = next(
                    (int(x.split(":")[1]) for x in head.split("\r\n") if x.lower().startswith("content-length")), 0
                )
                await r.readexactly(length)
                self.heartbeats += 1
                w.write(b"HTTP/1.1 409 X\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                await w.drain()
                w.close()
                return
            # replay the head for the default handling
            reader = asyncio.StreamReader()
            reader.feed_data(
                head.encode()
                + await r.read(
                    int(
                        next(
                            (x.split(":")[1] for x in head.split("\r\n") if x.lower().startswith("content-length")), "0"
                        )
                    )
                )
            )
            reader.feed_eof()
            await super().handle(reader, w)

    host = Host()
    await host.start()
    host.answers = [(200, {"invocations": [host.task("longstep")]})]
    w = worker(host)
    task = asyncio.create_task(w.run())
    await until(lambda: host.heartbeats >= 1)
    await asyncio.sleep(0.5)
    w.stop()
    await asyncio.wait_for(task, 5)
    assert host.completed == []


class ScriptedHost:
    """Answers every poll with one scripted response (status, extra headers, body), optionally
    holding it; records when polls arrive and whether the client went away while held."""

    def __init__(self, status: int, headers: str = "", body: bytes = b"", hold: float = 0.0) -> None:
        self.status, self.headers, self.body, self.hold = status, headers, body, hold
        self.times: list[float] = []
        self.went_away = 0

    async def start(self) -> None:
        self.server = await asyncio.start_server(self.handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]

    async def handle(self, r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        head = (await r.readuntil(b"\r\n\r\n")).decode()
        n = next((int(x.split(":")[1]) for x in head.split("\r\n") if x.lower().startswith("content-length")), 0)
        await r.readexactly(n)
        self.times.append(time.monotonic())
        if self.hold:
            try:
                await asyncio.wait_for(r.read(1), self.hold)
                self.went_away += 1
                w.close()
                return
            except TimeoutError:
                pass
        out = f"HTTP/1.1 {self.status} X\r\n{self.headers}Content-Length: {len(self.body)}\r\nConnection: close\r\n\r\n"
        w.write(out.encode() + self.body)
        await w.drain()
        w.close()


def scripted_worker(host: ScriptedHost, errors: list[str], **kw: Any) -> Worker:
    return Worker(
        api_key="ezgh_x",
        organization_id=ORG,
        project_id=PRJ,
        app=APP,
        workflows=[pulled],
        queue="conformance",
        live_url=f"http://127.0.0.1:{host.port}",
        worker_id="w-1",
        on_error=lambda e, c: errors.append(c),
        **kw,
    )


@pytest.mark.parametrize("status", [301, 308, 101])
async def test_redirects_and_informational_answers_stop_the_worker(status: int) -> None:
    host = ScriptedHost(status, "Location: https://example.invalid/\r\n" if status != 101 else "")
    await host.start()
    with pytest.raises(WorkflowsApiError) as e:
        await asyncio.wait_for(scripted_worker(host, []).run(), 5)
    assert e.value.status in (status, 0) and len(host.times) == 1


@pytest.mark.parametrize("body", [b"[]", b"not json", b'{"invocations": 5}', b"null"])
async def test_a_2xx_that_isnt_a_poll_answer_backs_off(body: bytes) -> None:
    host = ScriptedHost(200, "", body)
    await host.start()
    errors: list[str] = []
    w = scripted_worker(host, errors, backoff_max=0.4)
    task = asyncio.create_task(w.run())
    await asyncio.sleep(1.5)
    w.stop()
    await asyncio.wait_for(task, 5)
    assert 2 <= len(host.times) <= 6 and "poll answer" in errors


async def test_retry_after_paces_unavailable_polls() -> None:
    host = ScriptedHost(503, "Retry-After: 1\r\n", b'{"error":{"code":"unavailable"}}')
    await host.start()
    w = scripted_worker(host, [], backoff_max=0.2)  # a short backoff never cuts below Retry-After
    task = asyncio.create_task(w.run())
    await asyncio.sleep(3.2)
    w.stop()
    await asyncio.wait_for(task, 5)
    gaps = [b - a for a, b in zip(host.times, host.times[1:], strict=False)]
    assert len(host.times) <= 4 and all(g >= 0.95 for g in gaps), gaps


async def test_stop_aborts_a_held_poll() -> None:
    host = ScriptedHost(204, hold=5.0)
    await host.start()
    w = scripted_worker(host, [])
    task = asyncio.create_task(w.run())
    await asyncio.sleep(0.3)
    t0 = time.monotonic()
    w.stop()
    await asyncio.wait_for(task, 3)
    await asyncio.sleep(0.2)
    assert time.monotonic() - t0 < 1.0 and host.went_away == 1


async def test_tasks_received_as_the_worker_stops_still_run() -> None:
    host = FakeLiveHost()
    await host.start()
    w = worker(host)
    host.answers = [(200, {"invocations": [host.task("pulled")]})]
    original = host.handle

    async def slow_poll(r: asyncio.StreamReader, wr: asyncio.StreamWriter) -> None:
        peek = await r.readuntil(b"\r\n\r\n")
        n = next((int(x.split(b":")[1]) for x in peek.split(b"\r\n") if x.lower().startswith(b"content-length")), 0)
        reader = asyncio.StreamReader()
        reader.feed_data(peek + (await r.readexactly(n) if n else b""))
        reader.feed_eof()
        await original(reader, wr)
        if b"/workers/poll" in peek.split(b"\r\n")[0]:
            w.stop()  # stop() arrives as the poll's answer (with a task) does

    host.server.close()
    host.server = await asyncio.start_server(slow_poll, "127.0.0.1", host.port)
    task = asyncio.create_task(w.run())
    await asyncio.wait_for(task, 10)
    assert len(host.completed) == 1, (host.polls, host.checkpoints, host.answers)
    assert host.completed[0]["commands"][-1]["type"] == "runCompleted"


@workflow("traced")
async def traced(ctx: Context) -> str:
    return ctx.traceparent


async def test_traceparent_is_the_pulled_invocations_field() -> None:
    tp = "00-0199a3d02b3c7d4e9f506b7c8d9e0f1a-0123456789abcdef-01"
    host = FakeLiveHost()
    await host.start()
    with_it = {**host.task("traced"), "traceparent": tp, "token": "tok-with"}
    host.answers = [(200, {"invocations": [with_it, {**host.task("traced"), "token": "tok-without"}]})]
    w = worker(host, workflows=[traced], max_tasks=2, concurrency=2)
    task = asyncio.create_task(w.run())
    await until(lambda: len(host.completed) == 2)
    w.stop()
    await asyncio.wait_for(task, 5)
    outputs = sorted(c["commands"][-1]["output"] for c in host.completed)
    assert outputs == ["", tp]
