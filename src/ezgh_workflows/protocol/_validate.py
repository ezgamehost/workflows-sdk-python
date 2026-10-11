"""Checks mirroring workflows-stack internal/protocol/validate.go for what the SDK itself sends
about the app: poll requests, the SDK's identity and the workflows it announces."""

from collections.abc import Mapping
from typing import Any

from ._bytes import utf8_len
from ._ids import QUEUE_NAME, WORKER_ID, WORKFLOW_NAME, is_uuid

MAX_POLL_TASKS = 10
MAX_WORKFLOWS = 1000
MAX_SDK_NAME_BYTES = 200
MAX_SDK_VERSION_BYTES = 100
_MAX_SAFE_INTEGER = 2**53 - 1


def _number(v: Any, lo: float, hi: float, integer: bool) -> bool:
    if not isinstance(v, int | float) or isinstance(v, bool):
        return False
    if v != v or not lo <= v <= hi:  # NaN, or out of range
        return False
    return not integer or float(v).is_integer()


def check_retry(p: Mapping[str, Any]) -> None:
    """RetryPolicy.Validate: maxAttempts 1-100, initialMs >= 0, maxMs 0-86,400,000, multiplier 1-10."""
    if not (
        _number(p.get("maxAttempts"), 1, 100, True)
        and _number(p.get("initialMs"), 0, _MAX_SAFE_INTEGER, True)
        and _number(p.get("maxMs"), 0, 86_400_000, True)
        and _number(p.get("multiplier"), 1, 10, False)
    ):
        raise ValueError(
            "A retry policy has integer maxAttempts 1-100, integer initialMs >= 0, "
            "integer maxMs 0-86,400,000 and multiplier 1-10"
        )


def check_sdk(sdk: Any) -> None:
    """ValidateSDK: a name and version, at most 200 and 100 bytes."""
    if not isinstance(sdk, Mapping):
        raise ValueError("sdk is {name, version}")
    name, version = sdk.get("name"), sdk.get("version")
    if not isinstance(name, str) or not isinstance(version, str) or not name.strip() or not version.strip():
        raise ValueError("sdk name and version are required")
    if utf8_len(name) > MAX_SDK_NAME_BYTES or utf8_len(version) > MAX_SDK_VERSION_BYTES:
        raise ValueError("sdk name (at most 200 bytes) or version (at most 100) is too long")


def check_workflows(ws: Any) -> None:
    """ValidateWorkflows: at most 1,000, each name valid and listed once, timeoutMs not negative,
    retry policies valid."""
    if ws is None:
        return
    if not isinstance(ws, list) or len(ws) > MAX_WORKFLOWS:
        raise ValueError(f"An app serves at most {MAX_WORKFLOWS} workflows")
    seen: set[str] = set()
    for w in ws:
        name = w.get("name") if isinstance(w, Mapping) else None
        if not isinstance(name, str) or not WORKFLOW_NAME.fullmatch(name) or name in seen:
            raise ValueError(f"workflow name {name!r} is invalid or repeated")
        seen.add(name)
        t = w.get("timeoutMs", 0)
        if not isinstance(t, int) or isinstance(t, bool) or t < 0:
            raise ValueError(f"workflow {name!r} has a negative timeoutMs")
        if w.get("retry") is not None:
            check_retry(w["retry"])


def check_poll_request(r: Mapping[str, Any]) -> None:
    """PollRequest.Validate: what the live host's poll endpoint refuses, as ValueError."""
    t = r.get("maxTasks")
    if not is_uuid(r.get("app")):
        raise ValueError(f"app {r.get('app')!r} isn't an app ID")
    if not isinstance(r.get("queue"), str) or not QUEUE_NAME.fullmatch(r["queue"]):
        raise ValueError(f"queue {r.get('queue')!r} doesn't match ^[a-z0-9][a-z0-9-]{{0,62}}$")
    if not isinstance(t, int) or isinstance(t, bool) or not 1 <= t <= MAX_POLL_TASKS:
        raise ValueError(f"maxTasks {t!r} is outside 1-10")
    if not isinstance(r.get("workerId"), str) or not WORKER_ID.fullmatch(r["workerId"]):
        raise ValueError(f"workerId {r.get('workerId')!r} doesn't match ^[A-Za-z0-9._:-]{{1,128}}$")
    pv = r.get("protocolVersions")
    if not isinstance(pv, list) or not pv:
        raise ValueError("protocolVersions is empty")
    check_sdk(r.get("sdk"))
    check_workflows(r.get("workflows"))
