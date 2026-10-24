import hashlib
import json
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest
from helpers import RUN_ID, FakeLive, entry, invocation, sid, types

from ezgh_workflows import Context, QueuesApiError, QueuesClient, SyncContext, workflow
from ezgh_workflows._runtime.execution import execute
from ezgh_workflows.queues import send_to_queue_digest

Answer = tuple[int, dict[str, str], Any]  # status, headers, JSON body


@dataclass
class Seen:
    method: str
    path: str
    headers: dict[str, str]
    body: Any


@dataclass
class FakeQueues:
    """A local queues-api: it records every request and answers from a script (the last answer
    repeats; none answers a message ID per message)."""

    url: str = ""
    seen: list[Seen] = field(default_factory=list)
    answers: list[Answer] = field(default_factory=list)

    def answer(self, body: Any) -> Answer:
        n = len(self.seen) - 1
        if self.answers:
            return self.answers[min(n, len(self.answers) - 1)]
        return ok(len(body["messages"]))


def ok(n: int) -> Answer:
    return 200, {}, {"messages": [{"messageId": f"0199a3d0-0000-7000-8000-00000000000{i}"} for i in range(n)]}


def err(status: int, code: str, retry_after: str | None = None) -> Answer:
    return (
        status,
        {"Retry-After": retry_after} if retry_after is not None else {},
        {"error": {"code": code, "message": f"{code} here"}},
    )


@pytest.fixture
def fake() -> Iterator[FakeQueues]:
    f = FakeQueues()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
            body = json.loads(raw) if raw else None
            f.seen.append(Seen(self.command, self.path, {k.lower(): v for k, v in self.headers.items()}, body))
            status, headers, payload = f.answer(body)
            data = json.dumps(payload).encode() if payload is not None else b""
            self.send_response(status)
            for k, v in headers.items():
                self.send_header(k, v)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args: Any) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    f.url = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        yield f
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture
def sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    out: list[float] = []
    monkeypatch.setattr("ezgh_workflows.queues._sleep", out.append)
    return out


KEY = "ezgh_" + "a" * 40
PATH = "/v1/organizations/org-1/projects/prj-1/queues/que_abc/messages"


def client(fake: FakeQueues, **kw: Any) -> QueuesClient:
    return QueuesClient(api_key=KEY, organization_id="org-1", project_id="prj-1", base_url=fake.url + "/", **kw)


def test_send_posts_the_messages_with_the_key_and_idempotency_key(fake: FakeQueues) -> None:
    out = client(fake).send("que_abc", [{"body": "hi", "attributes": {"orderId": "o-1"}}], idempotency_key="k-1")
    assert out == {"messages": [{"messageId": "0199a3d0-0000-7000-8000-000000000000"}]}
    [s] = fake.seen
    assert (s.method, s.path) == ("POST", PATH)
    assert s.headers["authorization"] == f"Bearer {KEY}"
    assert s.headers["content-type"] == "application/json"
    assert s.headers["idempotency-key"] == "k-1"
    assert s.body == {"messages": [{"body": "hi", "attributes": {"orderId": "o-1"}}]}


def test_default_base_url_and_escaping(monkeypatch: pytest.MonkeyPatch) -> None:
    urls: list[str] = []

    def fake_request(method: str, url: str, headers: dict[str, str], body: bytes | None, timeout: float) -> Any:
        from ezgh_workflows._http import Response

        urls.append(url)
        return Response(200, {}, json.dumps(ok(1)[2]).encode())

    monkeypatch.setattr("ezgh_workflows.queues.request", fake_request)
    QueuesClient(api_key="k", organization_id="o/1", project_id="p").send("que_a b", [{"body": "x"}])
    assert urls == ["https://queues-api.ezghcloud.com/v1/organizations/o%2F1/projects/p/queues/que_a%20b/messages"]


def test_resends_429_and_5xx_after_retry_after_with_the_same_key(fake: FakeQueues, sleeps: list[float]) -> None:
    fake.answers = [
        err(429, "too_many_requests", "1"),
        err(502, "bad_gateway"),
        err(409, "idempotency_in_progress", "0"),
        ok(1),
    ]
    client(fake).send("que_abc", [{"body": "x"}])
    assert sleeps == [1.0, 1.0, 0.0]  # Retry-After: 1, then backoff 0.5 * 2**1, then Retry-After: 0
    keys = {s.headers["idempotency-key"] for s in fake.seen}
    assert len(fake.seen) == 4 and len(keys) == 1
    assert len(keys.pop()) == 36  # a random UUID when none is given


def test_retry_after_is_capped_by_max_retry_delay(fake: FakeQueues, sleeps: list[float]) -> None:
    fake.answers = [err(503, "unavailable", "30"), ok(1)]
    client(fake, max_retry_delay=2).send("que_abc", [{"body": "x"}])
    assert sleeps == [2.0]


@pytest.mark.parametrize(
    ("answer", "retries", "status", "code", "requests", "retry_after"),
    [
        (err(400, "invalid_request"), 3, 400, "invalid_request", 1, None),
        (err(429, "quota_exceeded"), 3, 429, "quota_exceeded", 1, None),
        (err(429, "too_many_requests", "0"), 2, 429, "too_many_requests", 3, 0.0),
        (err(500, "internal_error"), 1, 500, "internal_error", 2, None),
        ((404, {}, None), 3, 404, "unknown", 1, None),
        ((302, {"Location": "https://elsewhere.test/"}, None), 3, 302, "redirect", 1, None),
    ],
)
def test_errors_carry_status_code_and_retry_after(
    fake: FakeQueues,
    sleeps: list[float],
    answer: Answer,
    retries: int,
    status: int,
    code: str,
    requests: int,
    retry_after: float | None,
) -> None:
    fake.answers = [answer]
    with pytest.raises(QueuesApiError) as e:
        client(fake, retries=retries).send("que_abc", [{"body": "x"}])
    assert (e.value.status, e.value.code, e.value.retry_after_seconds) == (status, code, retry_after)
    assert len(fake.seen) == requests


def test_network_errors_are_resent_then_reported(sleeps: list[float]) -> None:
    c = QueuesClient(api_key="k", organization_id="o", project_id="p", base_url="http://127.0.0.1:9", retries=2)
    with pytest.raises(QueuesApiError) as e:
        c.send("que_abc", [{"body": "x"}])
    assert (e.value.status, e.value.code) == (0, "network_error") and sleeps == [0.5, 1.0]


def test_bad_arguments_are_refused_before_sending(fake: FakeQueues) -> None:
    with pytest.raises(ValueError):
        QueuesClient(api_key="", organization_id="o", project_id="p")
    c = client(fake)
    for bad in ([], [{"body": "x"}] * 11, "x", [1]):
        with pytest.raises(ValueError):
            c.send("que_abc", bad)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        c.send("que_abc", [{"body": "x"}], idempotency_key="has space")
    assert fake.seen == []


def test_the_api_key_never_prints(fake: FakeQueues) -> None:
    c = client(fake)
    assert KEY not in repr(c) and KEY not in str(c) and KEY not in repr(c._cred)


# The key derivation vector, the same in every EZGH Workflows SDK.
def test_key_derivation_vector() -> None:
    assert sid("notify") == "70d6d439c310423a9f3007a881987180"
    digest = "ac73d9ecbcab73fa5c63cae08e17eac274aca628299cc68bd17498109cb268c0"
    assert send_to_queue_digest("0199a3d0-2b3c-7d4e-9f50-6b7c8d9e0f1a", "70d6d439c310423a9f3007a881987180") == digest


def notify_wf(c: QueuesClient) -> Any:
    @workflow("notify")
    async def notify(ctx: Context) -> int:
        sent = await ctx.step.send_to_queue(
            "notify",
            c,
            "que_abc",
            [
                {"body": "a", "groupId": "acct-9"},
                {"body": "b"},
                {"body": "c", "groupId": "acct-9", "dedupId": "mine"},
            ],
        )
        return len(sent["messages"])

    return notify


DIGEST = hashlib.sha256(f"{RUN_ID}:{sid('notify')}".encode()).hexdigest()


async def test_sends_once_with_derived_keys_records_and_replays_without_sending(fake: FakeQueues) -> None:
    wf = notify_wf(client(fake))
    cmds = await execute(invocation("notify"), wf, FakeLive())
    [s] = fake.seen
    assert s.path == PATH and s.headers["idempotency-key"] == f"wfq_{DIGEST}"
    assert s.body == {
        "messages": [
            {"body": "a", "groupId": "acct-9", "dedupId": f"{DIGEST}:0"},
            {"body": "b"},
            {"body": "c", "groupId": "acct-9", "dedupId": "mine"},
        ]
    }
    assert cmds == [
        {"type": "stepCompleted", "stepId": sid("notify"), "name": "notify", "output": ok(3)[2]},
        {"type": "runCompleted", "output": 3},
    ]
    # Memoized: a replay returns the recorded result without calling the server.
    replay = await execute(invocation("notify", entries=[entry(1, "notify", output=cmds[0]["output"])]), wf, FakeLive())
    assert replay == [{"type": "runCompleted", "output": 3}]
    assert len(fake.seen) == 1


async def test_a_retried_attempt_sends_the_same_key(fake: FakeQueues) -> None:
    retrying = entry(1, "notify", "retrying", error={"code": "user", "name": "QueuesApiError", "message": "down"})
    cmds = await execute(invocation("notify", entries=[retrying]), notify_wf(client(fake)), FakeLive())
    assert cmds[-1] == {"type": "runCompleted", "output": 3}
    assert fake.seen[0].headers["idempotency-key"] == f"wfq_{DIGEST}"


def test_sync_workflows_send_too_and_other_steps_get_other_keys(fake: FakeQueues) -> None:
    c = client(fake)

    @workflow("two")
    def two(ctx: SyncContext) -> list[str]:
        a = ctx.step.send_to_queue("n", c, "que_abc", [{"body": "1"}])
        b = ctx.step.send_to_queue("n", c, "que_abc", [{"body": "2"}])
        return [a["messages"][0]["messageId"], b["messages"][0]["messageId"]]

    import asyncio

    cmds = asyncio.run(execute(invocation("two"), two, FakeLive()))
    assert types(cmds)[-1] == "runCompleted"
    assert [s.headers["idempotency-key"] for s in fake.seen] == [
        f"wfq_{send_to_queue_digest(RUN_ID, sid('n', 0))}",
        f"wfq_{send_to_queue_digest(RUN_ID, sid('n', 1))}",
    ]
    assert "dedupId" not in fake.seen[0].body["messages"][0]


async def test_a_refusal_is_non_retriable_and_an_outage_retryable(fake: FakeQueues, sleeps: list[float]) -> None:
    fake.answers = [err(403, "access_denied")]
    [failed] = await execute(invocation("notify"), notify_wf(client(fake)), FakeLive())
    assert failed["type"] == "stepFailed" and failed["retryable"] is False
    assert failed["error"]["code"] == "workflows.non_retriable" and failed["error"]["name"] == "NonRetriableError"
    assert failed["error"]["cause"]["name"] == "QueuesApiError"
    assert failed["error"]["cause"]["data"] == {"status": 403, "code": "access_denied"}

    fake.answers = [err(503, "unavailable", "0")]
    [down] = await execute(invocation("notify"), notify_wf(client(fake)), FakeLive())
    assert down["type"] == "stepFailed" and down["retryable"] is True
    assert down["error"]["name"] == "QueuesApiError" and down["error"]["data"] == {"status": 503, "code": "unavailable"}


async def test_bad_arguments_fail_the_run_before_any_step(fake: FakeQueues) -> None:
    c = client(fake)
    bad_calls: list[Callable[[Context], Any]] = [
        lambda ctx: ctx.step.send_to_queue("n", c, "que_abc", []),
        lambda ctx: ctx.step.send_to_queue("n", object(), "que_abc", [{"body": "x"}]),  # type: ignore[arg-type]
    ]
    for call in bad_calls:

        @workflow("bad")
        async def bad(ctx: Context, call: Callable[[Context], Any] = call) -> Any:
            return await call(ctx)

        [failed] = await execute(invocation("bad"), bad, FakeLive())
        assert failed["type"] == "runFailed" and failed["error"]["name"] in ("ValueError", "TypeError")
    assert fake.seen == []
