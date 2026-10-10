from typing import Any

from ezgh_workflows import (
    CallbackTimeoutError,
    ChildFailedError,
    NonRetriableError,
    RetryAfterError,
    RunCancelledError,
    WorkflowError,
)
from ezgh_workflows._runtime.errors import (
    DuplicateStepNameError,
    OutputTooLargeError,
    StepTimeoutError,
    decode_error,
    encode_error,
)


def test_platform_errors_have_wire_names_and_codes() -> None:
    cases: list[tuple[Exception, str, str]] = [
        (NonRetriableError("x"), "workflows.non_retriable", "NonRetriableError"),
        (RunCancelledError("x"), "workflows.cancelled", "CancelledError"),
        (StepTimeoutError("x"), "workflows.step_timeout", "StepTimeout"),
        (DuplicateStepNameError("x"), "workflows.duplicate_step_name", "DuplicateStepName"),
        (OutputTooLargeError("x"), "workflows.output_too_large", "OutputTooLarge"),
        (CallbackTimeoutError("x"), "workflows.callback_timeout", "CallbackTimeout"),
        (ChildFailedError("x"), "workflows.child_failed", "ChildFailed"),
        (RetryAfterError("x", 10), "user", "RetryAfterError"),
    ]
    for e, code, name in cases:
        w = encode_error(e)
        assert (w["code"], w["name"], w["message"]) == (code, name, "x")


def test_cancellation_is_an_exception_not_asyncio_cancellation() -> None:
    import asyncio

    assert issubclass(RunCancelledError, Exception) and not issubclass(RunCancelledError, asyncio.CancelledError)


def test_custom_codes_go_out_as_user_with_the_code_kept() -> None:
    w = encode_error(WorkflowError("teapot", code="http.418", data={"status": 418}))
    assert w["code"] == "user" and w["name"] == "http.418" and w["data"] == {"status": 418, "code": "http.418"}
    w2 = encode_error(WorkflowError("x", code="workflows.made_up"))
    assert w2["code"] == "user" and w2["data"] == {"code": "workflows.made_up"}
    w3 = encode_error(WorkflowError("x", code="c.1", name="Mine", data=[1]))
    assert (w3["name"], w3["data"]) == ("Mine", [1])


def test_builtins_cause_chains_and_byte_limits() -> None:
    try:
        try:
            raise KeyError("root")
        except KeyError as root:
            raise ValueError("é" * 5000) from root
    except ValueError as e:
        w = encode_error(e)
    assert w["name"] == "ValueError" and w["cause"]["name"] == "KeyError"
    assert len(w["message"].encode()) <= 8192 and len(w.get("stack", "").encode()) <= 8192
    assert "stack" in w


def test_custom_stack_attribute_is_used_and_cut() -> None:
    class Error(Exception):
        pass

    e = Error("wide")
    e.stack = "é" * 5000  # type: ignore[attr-defined]
    w = encode_error(e)
    assert w["name"] == "Error" and w["stack"] == "é" * 4096


def test_decoding_rebuilds_classes_and_round_trips() -> None:
    wire: Any = {
        "code": "workflows.child_failed",
        "name": "ChildFailed",
        "message": "child",
        "cause": {"code": "user", "name": "Error", "message": "root"},
    }
    e = decode_error(wire)
    assert isinstance(e, ChildFailedError) and isinstance(e, WorkflowError)
    assert isinstance(e.__cause__, WorkflowError) and e.__cause__.name == "Error"
    assert encode_error(e) == wire
    assert isinstance(
        decode_error({"code": "workflows.callback_timeout", "name": "x", "message": ""}), CallbackTimeoutError
    )


def test_data_over_64_kib_is_dropped_and_depth_is_capped() -> None:
    w = encode_error(WorkflowError("big", data={"x": "y" * 70_000}))
    assert "data" not in w
    w = encode_error(WorkflowError("nan", data=float("nan")))
    assert "data" not in w
    e: BaseException = ValueError("0")
    for i in range(1, 12):
        n = ValueError(str(i))
        n.__cause__ = e
        e = n
    w = encode_error(e)
    levels = 1
    while "cause" in w:
        w, levels = w["cause"], levels + 1
    assert levels == 8  # validate.go: at most MaxErrorDepth (8) levels, the error included


def test_non_exceptions_and_unprintable_errors() -> None:
    class Bad(Exception):
        def __str__(self) -> str:
            raise RuntimeError("no")

    assert encode_error("plain")["message"] == "plain"
    assert encode_error(Bad())["message"]
