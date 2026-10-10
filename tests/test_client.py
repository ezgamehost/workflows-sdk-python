import asyncio
import inspect
import json
from typing import Any

import pytest

import ezgh_workflows._http as http
from ezgh_workflows import AsyncClient, Client, WorkflowsApiError


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
        status, payload = a
        return http.Response(status, {"retry-after": "0"} if status in (429, 503) else {}, json.dumps(payload).encode())


@pytest.fixture
def rec(monkeypatch: pytest.MonkeyPatch) -> Recorder:
    r = Recorder()
    monkeypatch.setattr("ezgh_workflows.client.request", r)
    monkeypatch.setattr("ezgh_workflows.client._sleep", lambda s: None)
    return r


def client() -> Client:
    return Client(api_key="ezgh_secret", organization_id="org", project_id="prj", base_url="http://api.test/")


BASE = "http://api.test/v1/organizations/org/projects/prj"


def test_writes_resend_with_the_same_idempotency_key(rec: Recorder) -> None:
    rec.answers = [(503, {"error": {"code": "unavailable"}}), (201, {"id": "r1"})]
    assert client().start("app", "wf", {"a": 1}, run_key="k")["id"] == "r1"
    keys = {c[2]["Idempotency-Key"] for c in rec.calls}
    assert len(rec.calls) == 2 and len(keys) == 1
    assert rec.calls[0][1] == BASE + "/runs" and rec.calls[0][2]["Authorization"] == "Bearer ezgh_secret"
    assert json.loads(rec.calls[0][3] or b"") == {"appId": "app", "workflow": "wf", "input": {"a": 1}, "runKey": "k"}


def test_network_errors_are_retried_then_reported(rec: Recorder) -> None:
    rec.answers = [http.HttpError("ConnectionResetError"), (200, {"id": "x"})]
    assert client().get("x")["id"] == "x"
    rec.answers = [http.HttpError("TimeoutError")] * 4
    with pytest.raises(WorkflowsApiError) as e:
        client().get("x")
    assert (e.value.status, e.value.code) == (0, "network_error")


def test_secret_bearing_calls_are_not_resent_after_network_errors(rec: Recorder) -> None:
    rec.answers = [http.HttpError("ConnectionResetError")]
    with pytest.raises(WorkflowsApiError) as e:
        client().create_app("a", endpoint="https://x")
    assert (e.value.status, e.value.code) == (0, "outcome_unknown") and len(rec.calls) == 1
    rec.answers = [http.HttpError("ConnectionResetError")]
    with pytest.raises(WorkflowsApiError):
        client().rotate_secret("a")


def test_errors_carry_status_and_code(rec: Recorder) -> None:
    rec.answers = [(409, {"error": {"code": "run_key_conflict", "message": "different input"}})]
    with pytest.raises(WorkflowsApiError) as e:
        client().start("app", "wf", {"a": 2}, run_key="k")
    assert (e.value.status, e.value.code, str(e.value)) == (409, "run_key_conflict", "different input")
    rec.answers = [(429, {})] * 4
    with pytest.raises(WorkflowsApiError) as e:
        client().get("x")
    assert e.value.status == 429 and len(rec.calls) == 5


def test_the_api_key_never_prints() -> None:
    c = client()
    assert "ezgh_secret" not in repr(c) and "ezgh_secret" not in repr(
        AsyncClient(api_key="ezgh_secret", organization_id="o", project_id="p")
    )
    assert "ezgh_secret" not in repr(c._key) and "ezgh_secret" not in str(c._key)


def test_paths_are_escaped(rec: Recorder) -> None:
    rec.answers = [(200, {"id": "x"})]
    client().get("a/b")
    assert rec.calls[0][1] == BASE + "/runs/a%2Fb"
    assert "Idempotency-Key" not in rec.calls[0][2]


def test_app_schedule_and_callback_shapes(rec: Recorder) -> None:
    rec.answers = [(201, {"app": {"id": "a"}, "signingSecret": None})]
    client().create_app("dev", kind="pull", queue="dev-me", partitions=4, idempotency_key="k1")
    assert json.loads(rec.calls[-1][3] or b"") == {"name": "dev", "kind": "pull", "queue": "dev-me", "partitions": 4}
    assert rec.calls[-1][2]["Idempotency-Key"] == "k1"
    rec.answers = [(200, {"id": "r"})]
    client().complete_callback("r", "s", {"approved": True})
    assert rec.calls[-1][1] == BASE + "/runs/r/callbacks/s" and json.loads(rec.calls[-1][3] or b"") == {
        "approved": True
    }
    rec.answers = [(201, {"id": "s"})]
    client().create_schedule(app_id="a", workflow="wf", cron="* * * * *", input={"x": 1})
    assert json.loads(rec.calls[-1][3] or b"") == {
        "appId": "a",
        "workflow": "wf",
        "name": "wf",
        "cron": "* * * * *",
        "timeZone": "UTC",
        "overlap": "skip",
        "jitterSeconds": 0,
        "catchupWindowSeconds": 3600,
        "paused": False,
        "input": {"x": 1},
    }
    rec.answers = [(200, {"id": "s"})]
    client().update_schedule("s", revision=3, cron="0 * * * *", time_zone="Europe/London")
    method, url, headers, body = rec.calls[-1]
    assert (method, url, headers["If-Match"]) == ("PATCH", BASE + "/schedules/s", '"3"')
    assert json.loads(body or b"") == {"cron": "0 * * * *", "timeZone": "Europe/London"}
    with pytest.raises(ValueError):
        client().update_schedule("s", app_id="other")
    rec.answers = [(201, {"id": "run"}), (200, {"schedules": [], "nextCursor": None}), (204, None)]
    client().trigger_schedule("s", idempotency_key="t1")
    assert rec.calls[-1][2]["Idempotency-Key"] == "t1" and rec.calls[-1][1] == BASE + "/schedules/s/trigger"
    client().list_schedules(cursor="c/1", limit=10)
    assert rec.calls[-1][1] == BASE + "/schedules?cursor=c%2F1&limit=10"
    rec.answers.insert(0, (200, {"x": 1}))
    assert client().schedule_input("s") == {"x": 1}
    assert rec.calls[-1][1] == BASE + "/schedules/s/payloads/input"
    client().delete_schedule("s")
    assert rec.calls[-1][0] == "DELETE"


def test_async_client_matches(rec: Recorder) -> None:
    public = {n for n in dir(Client) if not n.startswith("_") and callable(getattr(Client, n))}
    mirrored = {
        n for n in dir(AsyncClient) if not n.startswith("_") and inspect.iscoroutinefunction(getattr(AsyncClient, n))
    }
    assert public == mirrored
    for n in public:
        a = inspect.signature(getattr(Client, n))
        b = inspect.signature(getattr(AsyncClient, n))
        assert list(a.parameters) == list(b.parameters), n
    rec.answers = [(200, {"id": "x"})]
    c = AsyncClient(api_key="k", organization_id="org", project_id="prj", base_url="http://api.test")
    assert asyncio.run(c.get("x"))["id"] == "x"


def test_options_are_checked() -> None:
    for kw in ({"retries": -1}, {"max_retry_delay": float("nan")}, {"api_key": ""}):
        args: dict[str, Any] = {"api_key": "k", "organization_id": "o", "project_id": "p", **kw}
        with pytest.raises(ValueError):
            Client(**args)
