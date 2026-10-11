"""The final cross-SDK review's items (Python), test-first."""

import asyncio
import json
import threading
import time
from typing import Any

import pytest
from helpers import FakeLive, entry, invocation, sid, types

import ezgh_workflows._http as http
from ezgh_workflows import (
    Client,
    Context,
    NonRetriableError,
    RetryAfterError,
    WorkflowError,
    WorkflowsApiError,
    workflow,
)
from ezgh_workflows._runtime.execution import execute


def _slow_x() -> str:
    time.sleep(0.2)
    return "x"


# 2. A heartbeat that reported a cancel: no new step starts; the invocation ends.
async def test_no_step_starts_after_a_heartbeat_reported_a_cancel() -> None:
    @workflow("seq2")
    async def seq2(ctx: Context) -> Any:
        await ctx.step.run("one", _slow_x)
        await ctx.step.run("two", lambda: "y")
        return "done"

    ev = threading.Event()
    threading.Timer(0.05, ev.set).start()
    live = FakeLive()
    cmds = await execute(invocation("seq2"), seq2, live, cancel_event=ev)
    assert [c["commands"][-1]["name"] for c in live.checkpoints] == ["one"]
    assert cmds == [{"type": "stepCompleted", "stepId": sid("one"), "name": "one", "output": "x"}]


# 4. While cancelling, cleanup doesn't wait behind a sibling the engine owns.
async def test_cleanup_runs_beside_running_siblings_while_cancelling() -> None:
    @workflow("cc")
    async def cc(ctx: Context) -> Any:
        try:
            await asyncio.gather(ctx.step.run("a", lambda: 1), ctx.step.run("b", lambda: 2))
        finally:
            await ctx.step.run("cleanup", lambda: "cleaned")

    journal = [entry(1, "a", "running", attempt=0), entry(2, "b", "running", attempt=0)]
    live = FakeLive()
    cmds = await execute(invocation("cc", entries=journal, cancel=True), cc, live)
    assert [c["commands"][-1]["name"] for c in live.checkpoints] == ["cleanup"]
    assert cmds[0] == {"type": "stepCompleted", "stepId": sid("cleanup"), "name": "cleanup", "output": "cleaned"}


async def test_while_cancelling_the_first_non_run_call_is_refused_wherever_it_is() -> None:
    @workflow("cc2")
    async def cc2(ctx: Context) -> Any:
        try:
            await ctx.step.sleep("wait", "1h")
        except Exception:
            r = ctx.step.run("cleanup", lambda: "cleaned")
            try:
                await ctx.step.sleep("more", "1m")
            except Exception as e:
                return [type(e).__name__, await r]
        return "no"

    journal = [entry(1, "wait", "sleeping", kind="sleep")]
    cmds = await execute(invocation("cc2", entries=journal, cancel=True), cc2, FakeLive())
    assert cmds[-1] == {"type": "runCompleted", "output": ["RunCancelledError", "cleaned"]}


# 5. More than 1,000 steps at once fails the run non-retriably before any runs.
async def test_a_plan_over_1000_steps_fails_the_run() -> None:
    @workflow("fan1001")
    async def fan1001(ctx: Context) -> Any:
        return await asyncio.gather(*(ctx.step.run(f"p{i}", lambda: 1) for i in range(1001)))

    live = FakeLive()
    cmds = await execute(invocation("fan1001"), fan1001, live)
    assert live.checkpoints == [] and types(cmds) == ["runFailed"]
    assert cmds[0]["error"]["code"] == "workflows.non_retriable"


# 7. A result past the transport's cap fails the run non-retriably.
async def test_a_result_past_the_transport_cap_fails_the_run() -> None:
    big = "x" * (900 * 1024)

    @workflow("kids")
    async def kids(ctx: Context) -> Any:
        return await asyncio.gather(*(ctx.step.invoke(f"k{i}", workflow="child", input=big) for i in range(10)))

    cmds = await execute(invocation("kids"), kids, FakeLive())
    assert types(cmds) == ["runFailed"] and cmds[0]["error"]["code"] == "workflows.non_retriable"
    cmds = await execute(invocation("kids"), kids, FakeLive(), max_result_bytes=16 << 20)
    assert types(cmds) == ["invoke"] * 10


# 13. Error classification walks the cause chain.
async def test_non_retriable_and_retry_after_are_found_in_the_cause_chain() -> None:
    def wrapped_fatal() -> None:
        try:
            raise NonRetriableError("root")
        except NonRetriableError as e:
            raise RuntimeError("wrapper") from e

    def wrapped_slow() -> None:
        try:
            raise RetryAfterError("slow", 2500)
        except RetryAfterError as e:
            raise ValueError("wrapper") from e

    def coded() -> None:
        raise WorkflowError("x", code="workflows.non_retriable")

    @workflow("chains")
    async def chains(ctx: Context) -> Any:
        return await ctx.step.run("s", {"fatal": wrapped_fatal, "slow": wrapped_slow, "coded": coded}[ctx.input])

    cmd = (await execute(invocation("chains", input="fatal"), chains, FakeLive()))[-1]
    assert cmd["retryable"] is False
    cmd = (await execute(invocation("chains", input="slow"), chains, FakeLive()))[-1]
    assert cmd["retryable"] is True and cmd["retryAfterMs"] == 2500
    cmd = (await execute(invocation("chains", input="coded"), chains, FakeLive()))[-1]
    assert cmd["retryable"] is False


# 10. serve() refuses a workflow timeout longer than the invocation.
def test_serve_refuses_a_timeout_longer_than_the_invocation() -> None:
    from conftest import vector

    from ezgh_workflows import serve

    secret = vector("signatures")["sign"][0]["secrets"][0]

    @workflow("long", timeout_ms=600_000)
    async def long(ctx: Context) -> None:
        return None

    with pytest.raises(ValueError):
        serve(workflows=[long], signing_secrets=[secret], max_invocation_seconds=300)
    serve(workflows=[long], signing_secrets=[secret], max_invocation_seconds=600)


# 11. Dev names match TS and Go.
def test_dev_names_match_the_other_sdks(monkeypatch: pytest.MonkeyPatch) -> None:
    from ezgh_workflows.worker import dev_name

    for k in ("USER", "USERNAME", "LOGNAME"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("USER", "Matt.Zhao@corp")
    assert dev_name() == "dev-matt-zhao-corp"
    assert dev_name("a__b--c") == "dev-a-b-c" and dev_name("___") == "dev-dev"
    assert dev_name("x" * 99) == "dev-" + "x" * 58
    assert dev_name("é-" * 40) == "dev-dev"


# 15. Deployment IDs from Fly.io and Render; never Lambda's $LATEST.
def test_deployment_ids_from_more_platforms(monkeypatch: pytest.MonkeyPatch) -> None:
    from ezgh_workflows._env import detect_deployment

    for k in (
        "VERCEL_DEPLOYMENT_ID",
        "AWS_LAMBDA_FUNCTION_NAME",
        "AWS_LAMBDA_FUNCTION_VERSION",
        "K_REVISION",
        "FLY_MACHINE_VERSION",
        "RENDER_GIT_COMMIT",
    ):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("RENDER_GIT_COMMIT", "abc123")
    assert detect_deployment() == "abc123"
    monkeypatch.setenv("FLY_MACHINE_VERSION", "fly-7")
    assert detect_deployment() == "fly-7"
    monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "fn")
    monkeypatch.setenv("AWS_LAMBDA_FUNCTION_VERSION", "$LATEST")
    assert detect_deployment() == "fly-7"


# Client items: 1, 12, 14 and the 409 idempotency_in_progress retry.
class Recorder:
    def __init__(self) -> None:
        self.answers: list[Any] = []
        self.calls: list[tuple[str, str, dict[str, str], bytes | None]] = []

    def __call__(
        self, method: str, url: str, headers: dict[str, str], body: bytes | None, timeout: float
    ) -> http.Response:
        self.calls.append((method, url, dict(headers), body))
        a = self.answers.pop(0)
        if isinstance(a, Exception):
            raise a
        status, payload, *rest = a
        hdrs = rest[0] if rest else ({"retry-after": "0"} if status in (429, 503) else {})
        return http.Response(status, hdrs, json.dumps(payload).encode())


@pytest.fixture
def rec(monkeypatch: pytest.MonkeyPatch) -> Recorder:
    r = Recorder()
    monkeypatch.setattr("ezgh_workflows.client.request", r)
    monkeypatch.setattr("ezgh_workflows.client._sleep", lambda s: None)
    return r


def client() -> Client:
    return Client(api_key="ezgh_secret", organization_id="org", project_id="prj", base_url="http://api.test")


def test_secret_bearing_calls_never_succeed_without_their_secret(rec: Recorder) -> None:
    rec.answers = [(503, {"error": {"code": "unavailable"}})]
    with pytest.raises(WorkflowsApiError) as e:
        client().create_app("a", endpoint="https://x")
    assert e.value.code == "outcome_unknown" and len(rec.calls) == 1
    rec.answers = [(201, {"app": {"id": "a", "kind": "push"}, "signingSecret": None})]
    with pytest.raises(WorkflowsApiError) as e:
        client().create_app("a", endpoint="https://x")
    assert e.value.code == "outcome_unknown"
    rec.answers = [(200, {"app": {"id": "a", "kind": "push"}, "signingSecret": None})]
    with pytest.raises(WorkflowsApiError):
        client().rotate_secret("a")
    rec.answers = [(201, {"app": {"id": "p", "kind": "pull"}, "signingSecret": None})]
    assert client().create_app("p", kind="pull", queue="q")["app"]["id"] == "p"  # pull apps have no secret
    rec.answers = [
        (429, {"error": {"code": "rate_limited"}}),
        (201, {"app": {"id": "a", "kind": "push"}, "signingSecret": "whsec_x"}),
    ]
    assert client().create_app("a", endpoint="https://x")["signingSecret"] == "whsec_x"  # a 429 took no effect


def test_idempotency_in_progress_is_retried_after_retry_after(rec: Recorder) -> None:
    slept: list[float] = []
    import ezgh_workflows.client as client_module

    client_module._sleep = slept.append
    rec.answers = [(409, {"error": {"code": "idempotency_in_progress"}}, {"retry-after": "2"}), (201, {"id": "r"})]
    assert client().start("app", "wf")["id"] == "r"
    assert slept == [2.0] and len({c[2]["Idempotency-Key"] for c in rec.calls}) == 1
    rec.answers = [(409, {"error": {"code": "run_key_conflict"}})]
    with pytest.raises(WorkflowsApiError):
        client().start("app", "wf")


def test_limits_are_checked_client_side() -> None:
    with pytest.raises(ValueError):
        client().list_apps(limit=0)
    with pytest.raises(ValueError):
        client().list_schedules(limit=-1)
    with pytest.raises(ValueError):
        client().events("r", limit=0)


def test_complete_callback_url(monkeypatch: pytest.MonkeyPatch) -> None:
    from ezgh_workflows import AsyncClient, complete_callback_url

    sent: list[Any] = []

    def fake(method: str, url: str, headers: dict[str, str], body: bytes | None, timeout: float) -> http.Response:
        sent.append((method, url, headers, body))
        return http.Response(
            200 if len(sent) == 1 else 409, {}, b'{"error":{"code":"callback_completed","message":"no"}}'
        )

    monkeypatch.setattr("ezgh_workflows.client.request", fake)
    url = "https://workflows-api.test/public/v1/callbacks/001." + "A" * 43
    complete_callback_url(url, {"ok": True})
    assert sent[0][0] == "POST" and sent[0][1] == url and sent[0][3] == b'{"ok":true}'
    assert "Authorization" not in sent[0][2] and sent[0][2]["Content-Type"] == "application/json"
    with pytest.raises(WorkflowsApiError) as e:
        asyncio.run(AsyncClient(api_key="k", organization_id="o", project_id="p").complete_callback_url(url, None))
    assert e.value.code == "callback_completed" and "A" * 43 not in str(e.value) and sent[1][3] == b"null"


# 14. A 401 poll is fatal at once.
async def test_a_401_poll_is_fatal_at_once() -> None:
    from test_worker import ScriptedHost, scripted_worker

    host = ScriptedHost(401, "", b'{"error":{"code":"unauthorized"}}')
    await host.start()
    with pytest.raises(WorkflowsApiError) as e:
        await asyncio.wait_for(scripted_worker(host, []).run(), 5)
    assert e.value.status == 401 and len(host.times) == 1
