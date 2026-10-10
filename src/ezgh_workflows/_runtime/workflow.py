"""@workflow: names, default retry policies and timeouts."""

import inspect
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any
from weakref import WeakKeyDictionary

from ..protocol._codec import DEFAULT_RETRY
from ..protocol._ids import WORKFLOW_NAME
from ..protocol._types import RetryPolicy

# The longest step timeout: a pull invocation's deadline is at most 24 hours.
MAX_TIMEOUT_MS = 86_400_000
_MAX_SAFE_INTEGER = 2**53 - 1


@dataclass(frozen=True)
class WorkflowDefinition:
    name: str
    fn: Callable[..., Any]
    is_async: bool
    retry: RetryPolicy | None
    timeout_ms: int | None


class Workflow:
    """A workflow, as @workflow returns it: opaque apart from its name."""

    __slots__ = ("__weakref__", "name")

    def __init__(self, name: str) -> None:
        self.name = name

    def __repr__(self) -> str:
        return f"<Workflow {self.name}>"


_DEFINITIONS: "WeakKeyDictionary[Workflow, WorkflowDefinition]" = WeakKeyDictionary()


def definition_of(wf: Workflow) -> WorkflowDefinition:
    d = _DEFINITIONS.get(wf) if isinstance(wf, Workflow) else None
    if d is None:
        raise TypeError("Not a workflow: define workflows with @workflow(name)")
    return d


def _number(v: Any, lo: float, hi: float, integer: bool) -> bool:
    if not isinstance(v, int | float) or isinstance(v, bool):
        return False
    if v != v or not lo <= v <= hi:  # NaN, or out of range
        return False
    return not integer or float(v).is_integer()


def check_retry(p: Mapping[str, Any]) -> None:
    """The protocol's bounds (validate.go): maxAttempts 1-100, initialMs >= 0, maxMs 0-86,400,000,
    multiplier 1-10."""
    if not (
        _number(p["maxAttempts"], 1, 100, True)
        and _number(p["initialMs"], 0, _MAX_SAFE_INTEGER, True)
        and _number(p["maxMs"], 0, 86_400_000, True)
        and _number(p["multiplier"], 1, 10, False)
    ):
        raise ValueError(
            "A retry policy has integer maxAttempts 1-100, integer initialMs >= 0, "
            "integer maxMs 0-86,400,000 and multiplier 1-10"
        )


def merge_retry(base: RetryPolicy, over: Mapping[str, Any] | None) -> RetryPolicy:
    """over's fields (camelCase, as on the wire) laid over base, checked."""
    if over is not None and not isinstance(over, Mapping):
        raise TypeError("retry is a dict such as {'maxAttempts': 3}")
    merged: dict[str, Any] = dict(base)
    for k, v in (over or {}).items():
        if k not in merged:
            raise ValueError(f"Unknown retry field {k!r}; use maxAttempts, initialMs, maxMs and multiplier")
        if v is not None:
            merged[k] = v
    check_retry(merged)
    m = merged["multiplier"]
    return {
        "maxAttempts": int(merged["maxAttempts"]),
        "initialMs": int(merged["initialMs"]),
        "maxMs": int(merged["maxMs"]),
        "multiplier": int(m) if isinstance(m, int) else m,
    }


def check_timeout(ms: Any) -> int:
    if not isinstance(ms, int) or isinstance(ms, bool) or not 1 <= ms <= MAX_TIMEOUT_MS:
        raise ValueError("timeout_ms is a whole number of milliseconds from 1 to 86,400,000")
    return ms


def workflow(
    name: str, *, retry: Mapping[str, Any] | None = None, timeout_ms: int | None = None
) -> Callable[[Callable[..., Any]], Workflow]:
    """Declares a workflow: an async def taking a Context, or a plain def taking a SyncContext.

    retry (a partial policy, merged with the default) and timeout_ms are the defaults for its steps,
    reported to EZGH at sync."""
    if not isinstance(name, str) or not WORKFLOW_NAME.fullmatch(name):
        raise ValueError(f"Workflow names match ^[A-Za-z0-9_-]{{1,64}}$; got {name!r}")
    merged = None if retry is None else merge_retry(DEFAULT_RETRY, retry)
    t = None if timeout_ms is None else check_timeout(timeout_ms)

    def wrap(fn: Callable[..., Any]) -> Workflow:
        if not callable(fn):
            raise TypeError("@workflow decorates a function")
        wf = Workflow(name)
        _DEFINITIONS[wf] = WorkflowDefinition(name, fn, inspect.iscoroutinefunction(fn), merged, t)
        return wf

    return wrap


def workflow_info(wf: Workflow) -> dict[str, Any]:
    """The workflow as a sync reply or a poll announces it."""
    d = definition_of(wf)
    info: dict[str, Any] = {"name": d.name}
    if d.retry:
        info["retry"] = dict(d.retry)
    if d.timeout_ms:
        info["timeoutMs"] = d.timeout_ms
    return info
