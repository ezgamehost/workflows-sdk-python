"""serve(): an ASGI app for push invocations (design, "Transports").

Answers (plan D25): 200 SyncReply or InvocationResult, 401 bad signature, 400 bad invocation, 409
superseded, 503 checkpoint failed, 500 SDK error, 405 anything but POST, 413 over 8 MiB.
"""

import asyncio
import time
from collections.abc import Awaitable, Callable, MutableMapping
from typing import Any

from ._env import clean_deployment, detect_deployment
from ._runtime.execution import execute
from ._runtime.invocation import InvalidInvocation, parse_invocation, parse_time
from ._runtime.live import CheckpointFailed, HttpLive, InvocationSuperseded
from ._runtime.workflow import Workflow, definition_of, workflow_info
from ._version import SDK_NAME, SDK_VERSION
from .protocol import PROTOCOL_VERSIONS
from .protocol._codec import dumps_bytes
from .protocol._signature import SIGNATURE_HEADER, SignatureError, decode_secret, parse_signature, verify_parsed

Scope = MutableMapping[str, Any]
Message = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]
MAX_BODY = 8 * 1024 * 1024
_HEADER = SIGNATURE_HEADER.lower().encode("latin-1")


class _TooLarge(Exception):
    pass


class _Disconnected(Exception):
    pass


async def _respond(send: Send, status: int, body: object) -> None:
    data = dumps_bytes(body)
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(data)).encode())],
        }
    )
    await send({"type": "http.response.body", "body": data})


def _header(scope: Scope, name: bytes) -> bytes | None:
    for k, v in scope.get("headers", []):
        if bytes(k).lower() == name:
            return bytes(v)
    return None


async def _read_body(scope: Scope, receive: Receive) -> bytes:
    declared = _header(scope, b"content-length")
    if declared is not None and declared.isdigit() and int(declared) > MAX_BODY:
        raise _TooLarge
    chunks: list[bytes] = []
    size = 0
    while True:
        m = await receive()
        if m["type"] == "http.disconnect":
            raise _Disconnected
        b = bytes(m.get("body", b""))
        size += len(b)
        if size > MAX_BODY:
            raise _TooLarge
        chunks.append(b)
        if not m.get("more_body"):
            return b"".join(chunks)


async def _lifespan(receive: Receive, send: Send) -> None:
    while True:
        m = await receive()
        if m["type"] == "lifespan.startup":
            await send({"type": "lifespan.startup.complete"})
        elif m["type"] == "lifespan.shutdown":
            await send({"type": "lifespan.shutdown.complete"})
            return


def serve(
    *,
    workflows: list[Workflow],
    signing_secrets: list[str | None],
    max_invocation_seconds: int = 300,
    deployment: str | None = None,
    max_steps_per_invocation: int = 50,
    api_base_url: str | None = None,
) -> ASGIApp:
    """An ASGI app for push invocations: mount it at the endpoint URL you register with EZGH.

    signing_secrets: the app's active secrets (whsec_...); pass two during a rotation. None or empty
    entries (an unset environment variable at build time) are skipped; with none left, requests
    answer 500. max_invocation_seconds: how long your platform lets a request run (1-900).
    deployment: recorded with each invocation (detected on Vercel, AWS Lambda and Cloud Run).
    api_base_url: the public API base for callback URLs when an invocation doesn't carry one."""
    by_name: dict[str, Workflow] = {}
    for wf in workflows:
        d = definition_of(wf)
        if d.name in by_name:
            raise ValueError(f'Two workflows are named "{d.name}"')
        by_name[d.name] = wf
    if not isinstance(signing_secrets, list | tuple) or not signing_secrets:
        raise ValueError("serve() needs at least one signing secret")
    keys = [decode_secret(s) for s in signing_secrets if s]  # a malformed secret fails now
    if not isinstance(max_invocation_seconds, int) or not 1 <= max_invocation_seconds <= 900:
        raise ValueError("max_invocation_seconds is a whole number from 1 to 900")
    if not isinstance(max_steps_per_invocation, int) or max_steps_per_invocation < 1:
        raise ValueError("max_steps_per_invocation is a whole number >= 1")
    dep = clean_deployment(deployment) if deployment is not None else detect_deployment()
    sdk = {"name": SDK_NAME, "version": SDK_VERSION}
    infos = [workflow_info(wf) for wf in by_name.values()]

    def result(commands: list[Any]) -> dict[str, Any]:
        r: dict[str, Any] = {"commands": commands, "sdk": sdk}
        if dep:
            r["deployment"] = dep
        return r

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "lifespan":
            return await _lifespan(receive, send)
        if scope["type"] != "http":
            return None
        received = time.time()
        if scope.get("method") != "POST":
            return await _respond(send, 405, {"code": "method_not_allowed"})
        if not keys:
            return await _respond(send, 500, {"code": "sdk_error", "message": "No signing secret is configured"})
        # The header and its timestamp first: a stale or malformed signature costs no body read.
        header = (_header(scope, _HEADER) or b"").decode("latin-1")
        try:
            sig = parse_signature(header, received)
        except SignatureError as e:
            return await _respond(
                send, 401, {"code": "expired_signature" if e.reason == "expired" else "invalid_signature"}
            )
        try:
            body = await _read_body(scope, receive)
        except _TooLarge:
            return await _respond(send, 413, {"code": "payload_too_large"})
        except _Disconnected:
            return None
        try:
            verify_parsed(sig, keys, body)
        except SignatureError:
            return await _respond(send, 401, {"code": "invalid_signature"})
        try:
            inv = parse_invocation(body)
        except InvalidInvocation as e:
            return await _respond(send, 400, {"code": "invalid_invocation", "message": str(e)})
        if inv["kind"] == "sync":
            reply: dict[str, Any] = {
                "protocolVersions": list(PROTOCOL_VERSIONS),
                "sdk": sdk,
                "maxInvocationSeconds": max_invocation_seconds,
                "workflows": infos,
            }
            if dep:
                reply["deployment"] = dep
            return await _respond(send, 200, reply)
        name = inv["run"]["workflow"]
        wf = by_name.get(name)
        if wf is None:
            return await _respond(send, 200, result([{"type": "workflowNotFound", "workflow": name}]))
        # The deadline on this machine's clock: the engine's budget (deadline - t) from receipt,
        # and never later than the deadline as this clock reads it.
        engine_deadline = parse_time(inv["deadline"])
        deadline = min(received + (engine_deadline - sig.t), engine_deadline)
        reserve = max(5.0, max(0.0, deadline - received) * 0.1)
        try:
            io = HttpLive(inv["endpoints"]["live"], inv["token"], deadline=deadline, reserve=reserve)
            commands = await execute(
                inv, wf, io, deadline=deadline, max_steps=max_steps_per_invocation, api_base_url=api_base_url
            )
        except InvocationSuperseded:
            return await _respond(send, 409, {"code": "invocation_superseded"})
        except CheckpointFailed as e:
            # The code goes top level and in the kit's error shape, which dispatch reads:
            # payload_unavailable costs the run no step attempt.
            return await _respond(send, 503, {"code": e.code, "error": {"code": e.code, "message": str(e)}})
        except asyncio.CancelledError:
            raise
        except Exception:
            return await _respond(send, 500, {"code": "sdk_error"})
        return await _respond(send, 200, result(commands))

    return app
