"""Errors: the SDK's classes and their wire shape (design, "Errors")."""

import traceback
from typing import Any

from ..protocol._bytes import truncate_utf8
from ..protocol._codec import encode_value, json_byte_length
from ..protocol._types import WireError

MAX_TEXT_BYTES = 8192
MAX_NAME_BYTES = 256
MAX_DATA_BYTES = 65_536
# validate.go's MaxErrorDepth: at most 8 levels in all (the error and 7 causes).
MAX_DEPTH = 8
PLATFORM_CODES = frozenset(
    {
        "workflows.callback_timeout",
        "workflows.cancelled",
        "workflows.child_failed",
        "workflows.step_timeout",
        "workflows.invocation_lost",
        "workflows.non_retriable",
        "workflows.output_too_large",
        "workflows.duplicate_step_name",
        "workflows.workflow_not_found",
        "workflows.invocation_failed",
        "workflows.timed_out",
    }
)


class _Unset:
    def __repr__(self) -> str:
        return "<unset>"


_UNSET: Any = _Unset()


class WorkflowError(Exception):
    """An error with a code and JSON data. Recorded failures are rebuilt as this class (or the
    subclass their code names), so isinstance works the same live and on replay.

    A code other than "user" or the platform's own "workflows.*" codes goes on the wire as "user",
    with the custom code as the name (when no name was given) and as data["code"].
    """

    code: str = "user"
    wire_name: str = "WorkflowError"

    def __init__(
        self, message: str = "", *, code: str | None = None, name: str | None = None, data: Any = _UNSET
    ) -> None:
        super().__init__(message)
        if code is not None:
            self.code = code
        self.name: str = name if name is not None else type(self).wire_name
        self.data: Any = None if data is _UNSET else data
        self._has_data = data is not _UNSET
        self._wire_stack: Any = _UNSET  # set by decode_error: the recorded stack (None: none)

    @property
    def message(self) -> str:
        return str(self.args[0]) if self.args else ""


class NonRetriableError(WorkflowError):
    """Retrying won't help: the step fails at once (workflows.non_retriable)."""

    code = "workflows.non_retriable"
    wire_name = "NonRetriableError"


class RunCancelledError(WorkflowError):
    """Raised into the pending step call when the run is cancelled (workflows.cancelled; its wire
    name is CancelledError). An Exception, deliberately not asyncio.CancelledError: catch it, or use
    finally, to run step.run cleanup steps; it never cancels asyncio tasks."""

    code = "workflows.cancelled"
    wire_name = "CancelledError"


class StepTimeoutError(WorkflowError):
    """A step ran past its timeout_ms (workflows.step_timeout); it's retried like any failure."""

    code = "workflows.step_timeout"
    wire_name = "StepTimeout"


class DuplicateStepNameError(WorkflowError):
    code = "workflows.duplicate_step_name"
    wire_name = "DuplicateStepName"


class OutputTooLargeError(WorkflowError):
    code = "workflows.output_too_large"
    wire_name = "OutputTooLarge"


class CallbackTimeoutError(WorkflowError):
    """A callback wasn't completed before its timeout: what Callback.wait() raises."""

    code = "workflows.callback_timeout"
    wire_name = "CallbackTimeout"


class ChildFailedError(WorkflowError):
    """A child workflow (step.invoke) failed; __cause__ is the child's error."""

    code = "workflows.child_failed"
    wire_name = "ChildFailed"


class RetryAfterError(Exception):
    """A retryable step error with the delay before the next attempt, in milliseconds (code user)."""

    def __init__(self, message: str, delay_ms: float) -> None:
        super().__init__(message)
        self.delay_ms = delay_ms


_BY_CODE: dict[str, type[WorkflowError]] = {
    c.code: c
    for c in (
        NonRetriableError,
        RunCancelledError,
        StepTimeoutError,
        DuplicateStepNameError,
        OutputTooLargeError,
        CallbackTimeoutError,
        ChildFailedError,
    )
}


def _json_data(v: Any) -> Any:
    """v as JSON when it encodes within 64 KiB, else unset (data that doesn't fit is dropped)."""
    try:
        encoded = encode_value(v)
        return encoded if json_byte_length(encoded) <= MAX_DATA_BYTES else _UNSET
    except (TypeError, ValueError, RecursionError):
        return _UNSET


def encode_error(e: object, depth: int = 0) -> WireError:
    """The wire shape of anything raised: messages and stacks cut to 8 KiB of UTF-8, names to 256
    bytes, data over 64 KiB of JSON dropped, at most 8 levels of cause."""
    if not isinstance(e, BaseException):
        return {"code": "user", "name": "Error", "message": truncate_utf8(_safe_str(e), MAX_TEXT_BYTES)}
    if isinstance(e, WorkflowError):
        code, name = e.code, e.name
        data = _json_data(e.data) if e._has_data else _UNSET
    else:
        code, name = "user", type(e).__name__
        raw = getattr(e, "data", _UNSET)
        data = _json_data(raw) if raw is not _UNSET else _UNSET
    if code != "user" and code not in PLATFORM_CODES:
        if isinstance(e, WorkflowError) and name == "WorkflowError":
            name = code
        if data is _UNSET:
            data = {"code": code}
        elif isinstance(data, dict) and "code" not in data:
            data = {**data, "code": code}
        code = "user"
    w: WireError = {
        "code": code,
        "name": truncate_utf8(_safe_str(name), MAX_NAME_BYTES),
        "message": truncate_utf8(_message(e), MAX_TEXT_BYTES),
    }
    if data is not _UNSET:
        w["data"] = data
    stack = _stack(e)
    if stack:
        w["stack"] = truncate_utf8(stack, MAX_TEXT_BYTES)
    if e.__cause__ is not None and depth < MAX_DEPTH - 1:
        w["cause"] = encode_error(e.__cause__, depth + 1)
    return w


def _message(e: BaseException) -> str:
    return e.message if isinstance(e, WorkflowError) else _safe_str(e)


def _stack(e: BaseException) -> str | None:
    recorded = getattr(e, "_wire_stack", _UNSET)
    if recorded is not _UNSET:
        return recorded if isinstance(recorded, str) else None
    custom = getattr(e, "stack", None)
    if isinstance(custom, str):
        return custom
    if e.__traceback__ is None:
        return None
    try:
        return "".join(traceback.format_exception(type(e), e, e.__traceback__, chain=False))
    except Exception:
        return None


def _safe_str(v: object) -> str:
    try:
        return str(v)
    except Exception:
        return "A value that can't be shown was raised"


def decode_error(w: WireError) -> WorkflowError:
    """Rebuilds the SDK's classes from code; encode_error(decode_error(w)) == w."""
    cls = _BY_CODE.get(w["code"], WorkflowError)
    e = cls(w["message"], code=w["code"], name=w["name"], data=w.get("data", _UNSET))
    e._wire_stack = w.get("stack")
    if "cause" in w:
        e.__cause__ = decode_error(w["cause"])
    return e
