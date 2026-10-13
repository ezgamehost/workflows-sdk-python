"""The wire shapes: exactly workflows-stack's internal/protocol JSON. Optional fields are left out,
never null, except Invocation.cancel."""

from typing import Any, Literal, NotRequired, TypedDict

JSONValue = Any


class RetryPolicy(TypedDict):
    maxAttempts: int
    initialMs: int
    maxMs: int
    multiplier: float


class WireError(TypedDict):
    code: str
    name: str
    message: str
    data: NotRequired[JSONValue]
    stack: NotRequired[str]
    cause: NotRequired["WireError"]


class JournalEntry(TypedDict):
    seq: int
    stepId: str
    name: str
    kind: str  # run | sleep | value | callback | invoke (a later version may add more)
    status: str  # running | retrying | completed | failed | cancelled | sleeping | waiting (or later)
    attempt: int
    output: NotRequired[JSONValue]
    outputRef: NotRequired[str]
    error: NotRequired[WireError]
    token: NotRequired[str]  # kind callback: the callback token


class Journal(TypedDict):
    complete: bool
    entries: list[JournalEntry]


class ScheduleInfo(TypedDict):
    id: str
    scheduledTime: str


class RunInfo(TypedDict):
    id: str
    runKey: NotRequired[str]
    workflow: str
    input: NotRequired[JSONValue]
    inputRef: NotRequired[str]
    attempt: int
    startedAt: str
    schedule: NotRequired[ScheduleInfo]


class Cancel(TypedDict):
    requestedAt: str
    graceUntil: str


class Endpoints(TypedDict):
    live: str
    api: NotRequired[str]  # the public API base: a callback URL is api + "/public/v1/callbacks/" + token


class Invocation(TypedDict):
    protocolVersion: str
    kind: Literal["orchestrate", "step", "sync"]
    invocationId: str
    token: str
    endpoints: Endpoints
    run: NotRequired[RunInfo]
    journal: NotRequired[Journal]
    targetStepId: NotRequired[str]
    deadline: str
    cancel: Cancel | None
    # A pulled invocation's W3C traceparent, sent while the project's observability setting is on
    # (a push request carries it as the traceparent header instead).
    traceparent: NotRequired[str]


Command = dict[str, Any]  # one of the command shapes; built only by the runtime


class Checkpoint(TypedDict):
    invocationId: str
    seq: int
    commands: list[Command]


class SDKInfo(TypedDict):
    name: str
    version: str


class InvocationResult(TypedDict):
    commands: list[Command]
    deployment: NotRequired[str]
    sdk: SDKInfo


class WorkflowInfo(TypedDict):
    name: str
    retry: NotRequired[RetryPolicy]
    timeoutMs: NotRequired[int]


class SyncReply(TypedDict):
    protocolVersions: list[str]
    sdk: SDKInfo
    maxInvocationSeconds: int
    workflows: list[WorkflowInfo]
    deployment: NotRequired[str]


class PollRequest(TypedDict):
    app: str
    queue: str
    maxTasks: int
    workerId: str
    protocolVersions: list[str]
    sdk: SDKInfo
    workflows: NotRequired[list[WorkflowInfo]]


class PollResponse(TypedDict):
    invocations: list[Invocation]


class HeartbeatResponse(TypedDict):
    cancel: Cancel | None
