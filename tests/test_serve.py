import importlib
import json
import time
from typing import Any

import pytest
from conftest import vector
from helpers import FakeLive, invocation

from ezgh_workflows import Context, workflow
from ezgh_workflows._runtime.live import CheckpointFailed, InvocationSuperseded
from ezgh_workflows.protocol import decode_secret, sign
from ezgh_workflows.serve import serve

serve_module = importlib.import_module("ezgh_workflows.serve")  # the package exports the function as serve

SECRET: str = vector("signatures")["sign"][0]["secrets"][0]
OTHER: str = vector("signatures")["sign"][1]["secrets"][1]


@workflow("hello")
async def hello(ctx: Context) -> str:
    return str(await ctx.step.run("greet", lambda: "hi"))


@workflow("tuned", retry={"maxAttempts": 2}, timeout_ms=60_000)
async def tuned(ctx: Context) -> int:
    return 1


async def call(app: Any, body: bytes, headers: dict[str, str], method: str = "POST") -> tuple[int, Any]:
    sent: list[dict[str, Any]] = []
    chunks = [body[i : i + 65536] for i in range(0, len(body), 65536)] or [b""]

    async def receive() -> dict[str, Any]:
        c = chunks.pop(0) if chunks else b""
        return {"type": "http.request", "body": c, "more_body": bool(chunks)}

    async def send(m: dict[str, Any]) -> None:
        sent.append(m)

    scope = {
        "type": "http",
        "method": method,
        "path": "/",
        "headers": [(k.lower().encode(), v.encode()) for k, v in headers.items()],
    }
    await app(scope, receive, send)
    assert sent[0]["type"] == "http.response.start"
    status = sent[0]["status"]
    text = b"".join(m.get("body", b"") for m in sent[1:])
    assert dict(sent[0]["headers"])[b"content-length"] == str(len(text)).encode()
    return status, json.loads(text) if text else {}


def signed(body: bytes, t: int | None = None, secret: str = SECRET) -> dict[str, str]:
    return {
        "EZGH-Signature": sign([decode_secret(secret)], t or int(time.time()), body),
        "Content-Type": "application/json",
    }


def app(**kw: Any) -> Any:
    return serve(
        workflows=[hello, tuned], signing_secrets=[SECRET, None, ""], max_invocation_seconds=60, deployment="test", **kw
    )


async def test_sync_reply_lists_full_policies_and_versions() -> None:
    body = json.dumps(
        {
            "protocolVersion": "1.1",
            "kind": "sync",
            "invocationId": "inv_" + "a" * 26,
            "token": "",
            "endpoints": {"live": "http://x"},
            "deadline": "2099-01-01T00:00:00Z",
            "cancel": None,
        }
    ).encode()
    status, reply = await call(app(), body, signed(body))
    assert status == 200 and reply["protocolVersions"] == ["1.0", "1.1"] and reply["maxInvocationSeconds"] == 60
    assert reply["workflows"] == [
        {"name": "hello"},
        {
            "name": "tuned",
            "retry": {"maxAttempts": 2, "initialMs": 1000, "maxMs": 600000, "multiplier": 2},
            "timeoutMs": 60000,
        },
    ]
    assert reply["sdk"]["name"] == "ezgh-workflows" and reply["deployment"] == "test"


async def test_status_codes() -> None:
    body = json.dumps(invocation("hello")).encode()
    assert (await call(app(), body, signed(body), method="GET"))[0] == 405
    assert (await call(app(), body, {"EZGH-Signature": "t=1,v1=" + "0" * 64}))[1] == {"code": "expired_signature"}
    assert (await call(app(), body, signed(body, secret=OTHER)))[1] == {"code": "invalid_signature"}
    assert (await call(app(), body, {}))[0] == 401
    assert (await call(app(), body, signed(body, t=int(time.time()) - 400)))[1] == {"code": "expired_signature"}
    big = b"x" * (8 * 1024 * 1024 + 1)
    assert (await call(app(), big, signed(big)))[0] == 413
    assert (await call(app(), big, {**signed(b"x"), "Content-Length": str(len(big))}))[0] == 413
    bad = b"{}"
    status, res = await call(app(), bad, signed(bad))
    assert status == 400 and res["code"] == "invalid_invocation"


async def test_rotation_accepts_either_secret() -> None:
    body = json.dumps(invocation("missing")).encode()
    a = serve(workflows=[hello], signing_secrets=[OTHER, SECRET])
    two = {"EZGH-Signature": sign([decode_secret(OTHER), decode_secret(SECRET)], int(time.time()), body)}
    assert (await call(a, body, two))[0] == 200


def test_malformed_secrets_and_options_fail_at_construction() -> None:
    cases: list[dict[str, Any]] = [
        {"signing_secrets": ["whsec_short"]},
        {"signing_secrets": []},
        {"signing_secrets": [SECRET], "max_invocation_seconds": 901},
        {"signing_secrets": [SECRET], "max_steps_per_invocation": 0},
    ]
    for kw in cases:
        with pytest.raises(ValueError):
            serve(workflows=[hello], **kw)
    with pytest.raises(ValueError):
        serve(workflows=[hello, hello], signing_secrets=[SECRET])


async def test_no_configured_secret_answers_500() -> None:
    body = json.dumps(invocation("hello")).encode()
    a = serve(workflows=[hello], signing_secrets=[None])
    assert (await call(a, body, signed(body)))[0] == 500


async def test_unknown_workflow_answers_workflow_not_found() -> None:
    body = json.dumps(invocation("missing")).encode()
    status, res = await call(app(), body, signed(body))
    assert status == 200 and res["commands"] == [{"type": "workflowNotFound", "workflow": "missing"}]
    assert res["deployment"] == "test" and set(res) == {"commands", "sdk", "deployment"}


async def test_results_and_live_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    lives: list[FakeLive] = []
    behaviour: dict[str, Any] = {}

    class Live(FakeLive):
        def __init__(self, base: str, token: str, *, deadline: float, reserve: float) -> None:
            super().__init__()
            behaviour["deadline"], behaviour["reserve"], behaviour["token"] = deadline, reserve, token
            lives.append(self)

        async def checkpoint(self, cp: Any) -> None:
            if "raise" in behaviour:
                raise behaviour["raise"]
            await super().checkpoint(cp)

    monkeypatch.setattr(serve_module, "HttpLive", Live)
    body = json.dumps(invocation("hello")).encode()
    t = int(time.time())
    status, res = await call(app(), body, signed(body, t=t))
    assert status == 200 and res["commands"][-1] == {"type": "runCompleted", "output": "hi"}
    assert lives[0].checkpoints[0]["commands"][0]["type"] == "stepStarted" and behaviour["token"] == "t"
    # the deadline is local: receipt + (deadline - t), never later than the deadline itself
    near = invocation("hello")
    near["deadline"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t + 100))
    nb = json.dumps(near).encode()
    await call(app(), nb, signed(nb, t=t - 50))
    assert behaviour["deadline"] <= t + 100 and behaviour["reserve"] >= 5.0
    behaviour["raise"] = CheckpointFailed("down", "payload_unavailable")
    status, res = await call(app(), body, signed(body))
    assert status == 503 and res == {
        "code": "payload_unavailable",
        "error": {"code": "payload_unavailable", "message": "down"},
    }
    behaviour["raise"] = InvocationSuperseded("gone")
    assert (await call(app(), body, signed(body)))[0:2] == (409, {"code": "invocation_superseded"})
    behaviour["raise"] = RuntimeError("bug")
    assert (await call(app(), body, signed(body)))[0] == 500


async def test_lifespan_is_acknowledged() -> None:
    sent: list[dict[str, Any]] = []
    msgs = [{"type": "lifespan.startup"}, {"type": "lifespan.shutdown"}]

    async def receive() -> dict[str, Any]:
        return msgs.pop(0)

    async def send(m: dict[str, Any]) -> None:
        sent.append(m)

    await app()({"type": "lifespan"}, receive, send)
    assert [m["type"] for m in sent] == ["lifespan.startup.complete", "lifespan.shutdown.complete"]


def test_deployment_detection(monkeypatch: pytest.MonkeyPatch) -> None:
    from ezgh_workflows._env import detect_deployment

    for k in ("VERCEL_DEPLOYMENT_ID", "AWS_LAMBDA_FUNCTION_NAME", "AWS_LAMBDA_FUNCTION_VERSION", "K_REVISION"):
        monkeypatch.delenv(k, raising=False)
    assert detect_deployment() is None
    monkeypatch.setenv("K_REVISION", "svc-0001")
    assert detect_deployment() == "svc-0001"
    monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "fn")
    monkeypatch.setenv("AWS_LAMBDA_FUNCTION_VERSION", "7")
    assert detect_deployment() == "fn:7"
    monkeypatch.setenv("VERCEL_DEPLOYMENT_ID", "dpl_" + "é" * 300)
    d = detect_deployment()
    assert d is not None and len(d.encode()) == 256
    from ezgh_workflows._env import clean_deployment

    assert clean_deployment("a\u0000b") == "ab" and clean_deployment("") is None
