"""What workflow code sees: Context (async def workflows) and SyncContext (plain def)."""

import asyncio
import threading
from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

from ..protocol._types import ScheduleInfo
from ..queues import (
    QueueMessage,
    QueuesApiError,
    QueuesClient,
    SentMessages,
    check_messages,
    send_to_queue_digest,
    with_dedup_ids,
)
from .errors import NonRetriableError

if TYPE_CHECKING:
    from .execution import Execution


class InvocationEnded(BaseException):
    """Raised into workflow code that is still waiting on a step when its invocation has ended
    (the run suspended; EZGH invokes it again later). Like GeneratorExit, it's a BaseException, so
    except Exception doesn't catch it while finally blocks run; step calls made then raise it too."""


@dataclass(frozen=True)
class StepRun:
    attempt: int
    idempotency_key: str
    timed_out: threading.Event
    run_cancel: threading.Event | None = None

    def is_cancelled(self) -> bool:
        return self.timed_out.is_set() or (self.run_cancel is not None and self.run_cancel.is_set())


CURRENT_STEP: ContextVar[StepRun | None] = ContextVar("ezgh_workflows_step", default=None)


def _current() -> StepRun:
    s = CURRENT_STEP.get()
    if s is None:
        raise RuntimeError("Only available inside a step function (ctx.step.run)")
    return s


@dataclass(frozen=True)
class ScheduleMeta:
    """Set on runs a schedule started: the schedule and its scheduled time (the cron instant, not
    the jittered fire time)."""

    id: str
    scheduled_time: datetime
    _wire: ScheduleInfo = field(repr=False, compare=False)

    def to_wire(self) -> ScheduleInfo:
        """run.schedule exactly as the invocation carried it: {"id", "scheduledTime"}."""
        return {"id": self._wire["id"], "scheduledTime": self._wire["scheduledTime"]}


@dataclass(frozen=True)
class RunMeta:
    id: str
    run_key: str | None
    attempt: int
    schedule: ScheduleMeta | None


@dataclass(frozen=True)
class Callback:
    """A callback: share url (or token) with whoever completes it, then await wait(). The URL is a
    credential: keep it private."""

    url: str
    token: str
    _wait: Callable[[], "asyncio.Future[Any]"] = field(repr=False, compare=False)

    def wait(self) -> "asyncio.Future[Any]":
        """Suspends the run until the callback is completed (its JSON body) or times out
        (CallbackTimeoutError)."""
        return self._wait()


@dataclass(frozen=True)
class SyncCallback:
    url: str
    token: str
    _wait: Callable[[], Any] = field(repr=False, compare=False)

    def wait(self) -> Any:
        return self._wait()


class _StepInfo:
    def attempt(self) -> int:
        """Inside a step function: this attempt's number (from 1)."""
        return _current().attempt

    def idempotency_key(self) -> str:
        """Inside a step function: f"{run_id}:{step_id}:{attempt}", for idempotent calls elsewhere."""
        return _current().idempotency_key

    def cancelled(self) -> bool:
        """Inside a step function: true once the step's timeout passed or (pull) the run is
        cancelling. A long step can poll it and stop early."""
        return _current().is_cancelled()


def _queue_sender(
    ex: "Execution", client: QueuesClient, queue_id: str, messages: Sequence[QueueMessage]
) -> Callable[[], SentMessages]:
    """send_to_queue's step function: client.send with the step's Idempotency-Key and FIFO dedupIds."""
    if not isinstance(client, QueuesClient):
        raise TypeError("step.send_to_queue needs a QueuesClient")
    if not isinstance(queue_id, str) or not queue_id:
        raise ValueError("step.send_to_queue needs a queue_id")
    check_messages(messages)
    msgs: list[QueueMessage] = [dict(m) for m in messages]  # type: ignore[misc]
    run_id: str = ex.inv["run"]["id"]

    def send() -> SentMessages:
        # The step's idempotency key is f"{run_id}:{step_id}:{attempt}": without the attempt, it's
        # the same on every attempt of this step (and different for every other step and run).
        key = _current().idempotency_key
        digest = send_to_queue_digest(run_id, key[len(run_id) + 1 : key.rindex(":")])
        try:
            return client.send(queue_id, with_dedup_ids(msgs, digest), idempotency_key="wfq_" + digest)
        except QueuesApiError as e:
            if e.refused():
                raise NonRetriableError(f"Sending to the queue {queue_id} was refused: {e.status} {e.code}: {e}") from e
            raise

    return send


class Step(_StepInfo):
    """The durable primitives. Every call registers its step at once and returns an
    asyncio.Future, so asyncio.gather and asyncio.wait work on them directly."""

    def __init__(self, ex: "Execution") -> None:
        self._ex = ex

    def run(
        self,
        name: str,
        fn: Callable[[], Any | Awaitable[Any]],
        *,
        retry: Mapping[str, Any] | None = None,
        timeout_ms: int | None = None,
        id: str | None = None,
    ) -> "asyncio.Future[Any]":
        """Runs fn at least once and records its JSON result; on replay the result returns without
        running fn. Plain functions run in a thread; coroutine functions on the caller's loop."""
        return self._ex.call_async(lambda: self._ex.register_run(name, fn, retry, timeout_ms, id))

    def sleep(self, name: str, duration: str | int, *, id: str | None = None) -> "asyncio.Future[None]":
        """A durable sleep (at most a year): "500ms", "30s", "5m", "2h", "3d", or milliseconds."""
        return self._ex.call_async(lambda: self._ex.register_sleep(name, duration, None, id))

    def sleep_until(self, name: str, at: datetime | str, *, id: str | None = None) -> "asyncio.Future[None]":
        return self._ex.call_async(lambda: self._ex.register_sleep(name, None, at, id))

    def now(self, name: str) -> "asyncio.Future[datetime]":
        """The current time (UTC, millisecond precision), recorded: the same on every replay."""
        return self._ex.call_async(lambda: self._ex.register_value(name, "now"))

    def uuid(self, name: str) -> "asyncio.Future[str]":
        """A random UUID, recorded: the same on every replay."""
        return self._ex.call_async(lambda: self._ex.register_value(name, "uuid"))

    def create_callback(
        self, name: str, *, timeout: str | int = "7d", id: str | None = None
    ) -> "asyncio.Future[Callback]":
        """A callback URL (protocol 1.1): recorded before the URL is visible. timeout is 1 s to 1 year."""
        return self._ex.call_async(lambda: self._ex.register_callback(name, timeout, id, sync=False))

    def invoke(
        self, name: str, *, workflow: "str | Any", input: Any = None, id: str | None = None
    ) -> "asyncio.Future[Any]":
        """Runs a child workflow of the same app and returns its output, or raises ChildFailedError
        (protocol 1.1)."""
        return self._ex.call_async(lambda: self._ex.register_invoke(name, workflow, input, id))

    def send_to_queue(
        self, name: str, client: QueuesClient, queue_id: str, messages: Sequence[QueueMessage]
    ) -> "asyncio.Future[SentMessages]":
        """Sends 1-10 messages to an EZGH queue in a step (step.run around client.send) and returns
        the recorded result. Every attempt sends Idempotency-Key "wfq_" + hex(SHA-256(run_id + ":" +
        step_id)), and each message with a groupId (FIFO) and no dedupId of its own gets the dedupId
        <that hex>:<index>, so the step sends its messages once. A 4xx refusal (other than 408, 429
        and 409 idempotency_in_progress) fails the step without retries (NonRetriableError, caused
        by the QueuesApiError)."""
        ex = self._ex
        return ex.call_async(
            lambda: ex.register_run(name, _queue_sender(ex, client, queue_id, messages), None, None, None)
        )


class SyncStep(_StepInfo):
    """Step's calls, blocking, for plain def workflows; parallel() runs several steps at once."""

    def __init__(self, ex: "Execution") -> None:
        self._ex = ex

    def run(
        self,
        name: str,
        fn: Callable[[], Any],
        *,
        retry: Mapping[str, Any] | None = None,
        timeout_ms: int | None = None,
        id: str | None = None,
    ) -> Any:
        return self._ex.call_sync([lambda: self._ex.register_run(name, fn, retry, timeout_ms, id)])[0]

    def sleep(self, name: str, duration: str | int, *, id: str | None = None) -> None:
        self._ex.call_sync([lambda: self._ex.register_sleep(name, duration, None, id)])

    def sleep_until(self, name: str, at: datetime | str, *, id: str | None = None) -> None:
        self._ex.call_sync([lambda: self._ex.register_sleep(name, None, at, id)])

    def now(self, name: str) -> datetime:
        v: datetime = self._ex.call_sync([lambda: self._ex.register_value(name, "now")])[0]
        return v

    def uuid(self, name: str) -> str:
        v: str = self._ex.call_sync([lambda: self._ex.register_value(name, "uuid")])[0]
        return v

    def create_callback(self, name: str, *, timeout: str | int = "7d", id: str | None = None) -> SyncCallback:
        cb: SyncCallback = self._ex.call_sync([lambda: self._ex.register_callback(name, timeout, id, sync=True)])[0]
        return cb

    def invoke(self, name: str, *, workflow: "str | Any", input: Any = None, id: str | None = None) -> Any:
        return self._ex.call_sync([lambda: self._ex.register_invoke(name, workflow, input, id)])[0]

    def send_to_queue(
        self, name: str, client: QueuesClient, queue_id: str, messages: Sequence[QueueMessage]
    ) -> SentMessages:
        """Step.send_to_queue, blocking."""
        ex = self._ex
        out: SentMessages = ex.call_sync(
            [lambda: ex.register_run(name, _queue_sender(ex, client, queue_id, messages), None, None, None)]
        )[0]
        return out

    def parallel(self, *branches: tuple[str, Callable[[], Any]]) -> list[Any]:
        """Runs each (name, fn) as its own step, concurrently; returns the results in branch order.
        The first failure (in the order they were recorded) is raised."""
        ex = self._ex

        def reg(n: str, f: Callable[[], Any]) -> Callable[[], Any]:
            return lambda: ex.register_run(n, f, None, None, None)

        return ex.call_sync([reg(n, f) for n, f in branches])


@dataclass(frozen=True)
class Context:
    """An async workflow's view of its run: input, run metadata, and step."""

    input: Any
    run: RunMeta
    step: Step
    # The invocation's W3C traceparent, exactly as received (the push request's traceparent header,
    # or a pulled invocation's traceparent field), while the project's observability setting is on;
    # "" otherwise. Parent your own spans on it: the SDK has no OpenTelemetry dependency.
    traceparent: str = ""


@dataclass(frozen=True)
class SyncContext:
    """A plain def workflow's view of its run: input, run metadata, and step (blocking calls)."""

    input: Any
    run: RunMeta
    step: SyncStep
    # The invocation's W3C traceparent, exactly as received (the push request's traceparent header,
    # or a pulled invocation's traceparent field), while the project's observability setting is on;
    # "" otherwise. Parent your own spans on it: the SDK has no OpenTelemetry dependency.
    traceparent: str = ""
