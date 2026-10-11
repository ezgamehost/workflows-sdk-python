"""One invocation of a workflow: replay against the journal, then run forward to the frontier.

Mirrors workflows-sdk-ts's Execution (the reference) with the phase 2 rules:
- recorded outcomes settle one at a time, lowest seq first, with the workflow run to quiescence
  before each, so asyncio.wait(FIRST_COMPLETED) sees the same winner as the first time;
- at the frontier (protocol 1.1) the oldest new call decides: a value is recorded, a callback is
  created (acknowledged before its URL is visible); otherwise one lone actionable step runs inline
  and anything more is planned (planParallel) and suspended (sleep, invoke, waitForCallback) in one
  result: the frontier rule, D2-19, the same in every SDK. Protocol 1.0 keeps phase 1's
  one-at-a-time behaviour;
- a step invocation runs exactly its target step;
- a failed attempt ends the invocation with stepFailed; the step and time budgets end it with no
  terminal command; the workflow's return or raise ends it with runCompleted or runFailed.

Quiescence is exact and depth-independent. An async workflow runs on an event loop of its own, in
a thread of its own: it is idle when that loop has no ready callbacks (every task of the workflow
waits on a future nothing has resolved). A sync workflow is idle when its thread waits on step
calls none of which has an outcome yet. The driver (this module, on the caller's loop) runs steps,
talks to workflows-live and settles calls; the two sides share state under one lock.
"""

import asyncio
import contextlib
import inspect
import json
import math
import threading
import time
import uuid as uuidlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

from ..protocol._bytes import utf8_len
from ..protocol._codec import (
    DEFAULT_RETRY,
    MAX_OUTPUT_BYTES,
    dumps_bytes,
    encode_value,
    iso_from_ms,
    json_byte_length,
    parse_duration,
)
from ..protocol._ids import STEP_ID_OVERRIDE, WORKFLOW_NAME, new_callback_token, shard_of, step_id
from ..protocol._types import Command, Invocation, JournalEntry, RetryPolicy, WireError
from .context import (
    CURRENT_STEP,
    Callback,
    Context,
    InvocationEnded,
    RunMeta,
    ScheduleMeta,
    Step,
    StepRun,
    SyncCallback,
    SyncContext,
    SyncStep,
)
from .errors import (
    DuplicateStepNameError,
    NonRetriableError,
    OutputTooLargeError,
    RetryAfterError,
    RunCancelledError,
    StepTimeoutError,
    decode_error,
    encode_error,
)
from .invocation import parse_time, valid_entry
from .live import CheckpointFailed, LiveIO
from .threads import run_in_thread
from .workflow import Workflow, check_timeout, definition_of, merge_retry

MAX_SLEEP_MS = 31_536_000_000  # a year: a run times out after a year at most
MIN_CALLBACK_TIMEOUT_MS = 1_000
MAX_RETRY_AFTER_MS = 86_400_000
MAX_PLANNED_STEPS = 1000  # validate.go MaxPlannedSteps
DEFAULT_API_BASE = "https://workflows-api.ezghcloud.com"
RECORDED = frozenset({"completed", "failed", "cancelled"})
_CAUSE_STEP = "_ezgh_workflows_step_id"
# After the invocation ends, how long the workflow's own loop may take to unwind before its
# remaining tasks are cancelled.
UNWIND_SECONDS = 5.0


def _retrieve(f: "asyncio.Future[Any]") -> None:
    """Marks a future's exception retrieved: a call the workflow never awaits (a race's loser)
    isn't logged; whoever awaits it still sees the exception."""
    if not f.cancelled():
        f.exception()


def _quiet_handler(loop: asyncio.AbstractEventLoop, context: dict[str, Any]) -> None:
    """The workflow loop's exception handler: a task the workflow never awaited (a race's loser)
    ending with InvocationEnded when the invocation suspends is expected; anything else is logged."""
    if isinstance(context.get("exception"), InvocationEnded | asyncio.CancelledError):
        return
    loop.default_exception_handler(context)


def _apply(fut: "asyncio.Future[Any]", ok: bool, value: Any) -> None:
    if fut.done():
        return
    if ok:
        fut.set_result(value)
    else:
        fut.set_exception(value)


class _StepCancelled(Exception):
    """The step's own code raised asyncio.CancelledError."""


def _being_cancelled() -> bool:
    """Whether the current task is being cancelled (by its owner), as opposed to code inside it
    raising asyncio.CancelledError of its own."""
    task = asyncio.current_task()
    return task is not None and task.cancelling() > 0


def _supports_parallel(version: str) -> bool:
    major, _, minor = version.partition(".")
    try:
        return (int(major), int(minor.split(".")[0] or "0")) >= (1, 1)
    except ValueError:
        return major == "1"


def _json_copy(v: Any) -> Any:
    """v (already encoded) through the wire and back: live values equal replayed ones, so a
    StrEnum or numpy float comes back as the plain str or float a replay gives."""
    return json.loads(dumps_bytes(v))


@dataclass(eq=False)
class _Call:
    order: int
    name: str
    kind: str  # run | sleep | value | callback | wait | invoke
    step_id: str
    future: "asyncio.Future[Any] | None" = None  # async workflows: on the workflow's loop
    explicit: str | None = None
    fn: Callable[[], Any] | None = None
    retry: RetryPolicy | None = None
    timeout_ms: int | None = None
    until_ms: int | None = None
    produce: str | None = None  # value: now | uuid
    callback_timeout_ms: int | None = None
    sync_handle: bool = False
    workflow: str | None = None
    has_input: bool = False
    input: Any = None
    outcome: tuple[bool, Any] | None = None  # set by the driver, under the lock
    settled_n: int = 0

    @property
    def settled(self) -> bool:
        return self.outcome is not None


@dataclass
class _Inline:
    call: _Call
    attempt: int


class Execution:
    def __init__(
        self,
        inv: Invocation,
        wf: Workflow,
        io: LiveIO,
        now: Callable[[], float],
        max_steps: int,
        deadline: float,
        cancel_event: threading.Event | None,
        api_base_url: str | None,
    ) -> None:
        self.inv, self.io, self.now, self.max_steps, self.deadline = inv, io, now, max_steps, deadline
        self.d = definition_of(wf)
        self.cancel_event = cancel_event
        self.started = now()
        self.loop = asyncio.get_running_loop()  # the driver's
        self.parallel = _supports_parallel(inv["protocolVersion"])
        api = inv["endpoints"].get("api") or api_base_url or DEFAULT_API_BASE
        self.api_base = api.rstrip("/")
        self.lock = threading.Condition()  # guards the call registry, shared with the workflow side
        self.calls: list[_Call] = []
        self.counters: dict[str, int] = {}
        self.explicit_ids: set[str] = set()
        self.settled_count = 0
        self.journal: dict[str, JournalEntry] = {}
        self.outbox: list[Command] = []
        self.cp_seq = 0
        self.executed = 0
        self.cancel_thrown = False
        self.fatal: WireError | None = None
        self.finished: tuple[bool, Any] | None = None
        self.crashed: BaseException | None = None  # the workflow ended with a BaseException
        self.gone = False  # the workflow ended without a result (cancelled)
        self.closed = False
        self.activity = asyncio.Event()
        # async workflows: their loop and thread
        self.wf_loop: asyncio.AbstractEventLoop | None = None
        self.wf_thread: int | None = None
        self.wf_closed: asyncio.Event | None = None
        # sync workflows: the calls the thread waits on (and whether it needs all of them)
        self.sync_waiting: list[_Call] | None = None
        self.thread_done = False
        self.sync_thread: int | None = None
        self._sync_step: SyncStep | None = None

    # ---- notifications to the driver ----

    def _notify(self) -> None:
        with contextlib.suppress(RuntimeError):  # the driver's loop is closed: nobody listens
            self.loop.call_soon_threadsafe(self.activity.set)

    def _post(self, fn: Callable[..., None], *args: Any) -> None:
        with contextlib.suppress(RuntimeError):
            self.loop.call_soon_threadsafe(fn, *args)

    # ---- registration (on the workflow's side, under the lock) ----

    def _register(self, kind: str, name: str, explicit: str | None, **extra: Any) -> _Call:
        if not isinstance(name, str) or not name or utf8_len(name) > 256 or "\u0000" in name:
            raise ValueError("A step name is 1-256 bytes of UTF-8, without NUL")
        if explicit is not None and (not isinstance(explicit, str) or not STEP_ID_OVERRIDE.fullmatch(explicit)):
            raise ValueError(f"Step ids match ^[A-Za-z0-9._:-]{{1,128}}$; got {explicit!r}")
        with self.lock:
            if self.closed:
                raise InvocationEnded()
            if kind in ("callback", "invoke") and not self.parallel and self.fatal is None:
                what = "create_callback" if kind == "callback" else "invoke"
                self.fatal = encode_error(
                    NonRetriableError(
                        f"step.{what} needs protocol 1.1; this invocation is {self.inv['protocolVersion']}"
                    )
                )
            if explicit is not None:
                sid = explicit
                # An explicit id names one step for the whole run: reusing it, even after the first
                # step settled, would replay the first step's outcome as the second's.
                if self.fatal is None and explicit in self.explicit_ids:
                    self.fatal = encode_error(
                        DuplicateStepNameError(f'The step id "{explicit}" is used twice; give each step its own id')
                    )
                self.explicit_ids.add(explicit)
            else:
                n = self.counters.get(name, 0)
                self.counters[name] = n + 1
                sid = step_id(name, n)
                if self.fatal is None and any(
                    not c.settled and c.explicit is None and c.kind != "wait" and c.name == name for c in self.calls
                ):
                    self.fatal = encode_error(
                        DuplicateStepNameError(
                            f'Two steps named "{name}" are pending at once; give each a distinct name or an id'
                        )
                    )
            return self._append_locked(_Call(len(self.calls), name, kind, sid, explicit=explicit, **extra))

    def _append_locked(self, c: _Call) -> _Call:
        if self.wf_loop is not None:
            c.future = self.wf_loop.create_future()
            c.future.add_done_callback(_retrieve)
        self.calls.append(c)
        self._notify()
        return c

    def register_run(
        self,
        name: str,
        fn: Callable[[], Any],
        retry: Mapping[str, Any] | None,
        timeout_ms: int | None,
        explicit: str | None,
    ) -> _Call:
        if not callable(fn):
            raise TypeError("step.run needs a function")
        policy = merge_retry(self.d.retry or DEFAULT_RETRY, retry)
        t = check_timeout(timeout_ms) if timeout_ms is not None else self.d.timeout_ms
        return self._register("run", name, explicit, fn=fn, retry=policy, timeout_ms=t)

    def register_sleep(
        self, name: str, duration: str | int | None, at: datetime | str | None, explicit: str | None
    ) -> _Call:
        now_ms = math.floor(self.now() * 1000)
        try:
            if duration is not None:
                ms = parse_duration(duration)
                if ms > MAX_SLEEP_MS:
                    raise ValueError("A sleep is at most a year")
                until = now_ms + ms
            else:
                if isinstance(at, datetime):
                    if at.tzinfo is None or at.utcoffset() is None:
                        raise ValueError("sleep_until needs an aware datetime (for example tzinfo=UTC)")
                    ts = at.timestamp()
                elif isinstance(at, str):
                    ts = parse_time(at)
                else:
                    raise TypeError("sleep_until needs an aware datetime or an RFC 3339 string")
                t_ms = math.floor(ts * 1000)
                if t_ms - now_ms > MAX_SLEEP_MS:
                    raise ValueError("A sleep is at most a year")
                until = max(t_ms, now_ms)  # a time in the past sleeps until now
        except OverflowError as e:
            raise ValueError(str(e)) from e
        return self._register("sleep", name, explicit, until_ms=until)

    def register_value(self, name: str, what: str) -> _Call:
        return self._register("value", name, None, produce=what)

    def register_callback(self, name: str, timeout: str | int, explicit: str | None, *, sync: bool) -> _Call:
        ms = parse_duration(timeout)
        if not MIN_CALLBACK_TIMEOUT_MS <= ms <= MAX_SLEEP_MS:
            raise ValueError("A callback timeout is from 1 second to 1 year")
        return self._register("callback", name, explicit, callback_timeout_ms=ms, sync_handle=sync)

    def register_invoke(self, name: str, workflow: Any, input: Any, explicit: str | None) -> _Call:
        wf_name = workflow.name if isinstance(workflow, Workflow) else workflow
        if not isinstance(wf_name, str) or not WORKFLOW_NAME.fullmatch(wf_name):
            raise ValueError(f"step.invoke needs a workflow name matching ^[A-Za-z0-9_-]{{1,64}}$; got {wf_name!r}")
        encoded = None
        if input is not None:
            try:
                encoded = encode_value(input)
            except RecursionError as e:
                raise TypeError("step.invoke's input isn't JSON") from e
            if json_byte_length(encoded) > MAX_OUTPUT_BYTES:
                raise ValueError("step.invoke's input is at most 1 MiB of JSON")
        return self._register("invoke", name, explicit, workflow=wf_name, has_input=input is not None, input=encoded)

    def register_wait(self, create: _Call) -> _Call:
        """Callback.wait(): a pending call on the callback's own step ID that neither advances the
        name's counter nor counts as a duplicate."""
        with self.lock:
            if self.closed:
                raise InvocationEnded()
            return self._append_locked(_Call(len(self.calls), create.name, "wait", create.step_id))

    # ---- the workflow side's two ways of waiting ----

    def call_async(self, register: Callable[[], _Call]) -> "asyncio.Future[Any]":
        """An async step method: register now, return the call's future on the workflow's loop."""
        loop = self.wf_loop
        if loop is None or threading.get_ident() != self.wf_thread:
            raise RuntimeError("ctx.step calls are made from the workflow's own code, not from step functions")
        try:
            c = register()
        except (ValueError, TypeError, InvocationEnded) as e:
            fut: asyncio.Future[Any] = loop.create_future()
            fut.set_exception(e)
            fut.add_done_callback(_retrieve)
            return fut
        assert c.future is not None
        return c.future

    def call_sync(self, registers: list[Callable[[], _Call]]) -> list[Any]:
        """Sync step methods: register every call, then block until all have outcomes or one
        failed (the first failure, in settling order, is raised)."""
        if self.wf_loop is not None or threading.get_ident() != self.sync_thread:
            raise RuntimeError("ctx.step calls are made from the workflow's own code, not from step functions")
        calls = [r() for r in registers]
        with self.lock:
            self.sync_waiting = calls
            self._notify()
            while not self._sync_ready(calls):
                self.lock.wait()
            self.sync_waiting = None
        failed = sorted((c for c in calls if c.outcome is not None and not c.outcome[0]), key=lambda c: c.settled_n)
        if failed:
            raise failed[0].outcome[1]  # type: ignore[index]
        return [c.outcome[1] for c in calls if c.outcome is not None]

    @staticmethod
    def _sync_ready(calls: list[_Call]) -> bool:
        return all(c.settled for c in calls) or any(c.outcome is not None and not c.outcome[0] for c in calls)

    # ---- settling (the driver) ----

    def _settle(self, c: _Call, ok: bool, value: Any) -> None:
        with self.lock:
            if c.outcome is not None:
                return
            self.settled_count += 1
            c.outcome, c.settled_n = (ok, value), self.settled_count
            self.lock.notify_all()
        if c.future is not None and self.wf_loop is not None:
            with contextlib.suppress(RuntimeError):  # the workflow's loop is gone
                self.wf_loop.call_soon_threadsafe(_apply, c.future, ok, value)

    def _resolve(self, c: _Call, value: Any) -> None:
        self._settle(c, True, value)

    def _reject(self, c: _Call, e: BaseException) -> None:
        self._settle(c, False, e)

    def _handle(self, c: _Call, token: str) -> Callback | SyncCallback:
        url = f"{self.api_base}/public/v1/callbacks/{token}"
        if c.sync_handle:
            return SyncCallback(url, token, lambda: self.call_sync([lambda: self.register_wait(c)])[0])
        return Callback(url, token, lambda: self.call_async(lambda: self.register_wait(c)))

    @staticmethod
    def _order_seq(c: _Call, e: JournalEntry) -> int:
        """Where a recorded outcome replays: its seq; a callback's creation at createdSeq when the
        journal carries it (the entry's seq is its completion's)."""
        if c.kind == "callback":
            created = cast(dict[str, Any], e).get("createdSeq")
            if isinstance(created, int) and not isinstance(created, bool):
                return created
        return int(e["seq"])

    def _recorded_entry(self, c: _Call) -> JournalEntry | None:
        """The journal entry that settles c: a callback's creation once its token is journaled
        (whatever its status); every other call once its step has an outcome."""
        e = self.journal.get(c.step_id)
        if e is None:
            return None
        if c.kind == "callback":
            return e if e.get("token") else None
        return e if e.get("status") in RECORDED else None

    def _settle_recorded(self, c: _Call, e: JournalEntry) -> None:
        if c.kind == "callback":
            return self._resolve(c, self._handle(c, e.get("token", "")))
        status = e.get("status")
        if status == "completed":
            if c.kind == "sleep":
                return self._resolve(c, None)
            out = e.get("output")
            if c.kind == "value" and c.produce == "now" and isinstance(out, str):
                try:
                    return self._resolve(c, datetime.fromtimestamp(parse_time(out), UTC))
                except (ValueError, OverflowError, OSError):
                    return self._resolve(c, out)
            return self._resolve(c, out)
        if status == "cancelled":
            self.cancel_thrown = True
            return self._reject(c, RunCancelledError("The run was cancelled"))
        wire = e.get("error") or {"code": "user", "name": "Error", "message": "Step failed"}
        err = decode_error(wire)
        setattr(err, _CAUSE_STEP, c.step_id)
        self._reject(c, err)

    # ---- I/O ----

    async def _load(self) -> None:
        """Pages the rest of an incomplete journal and fetches offloaded outputs and the input, so
        workflow code never waits on I/O during replay."""
        j = self.inv["journal"]
        entries: list[dict[str, Any]] = [dict(e) for e in j["entries"]]
        complete = j["complete"]
        while not complete:
            last = max((int(e["seq"]) for e in entries), default=0)
            page = await self.io.journal_page(last)
            if not all(valid_entry(e) for e in page["entries"]):
                raise CheckpointFailed("workflows-live sent a malformed journal entry")
            entries.extend(dict(e) for e in page["entries"])
            complete = bool(page.get("complete")) or not page["entries"]
        refs = [e for e in entries if isinstance(e.get("outputRef"), str)]
        for i in range(0, len(refs), 8):
            chunk = refs[i : i + 8]
            values = await asyncio.gather(*(self.io.payload(e["outputRef"]) for e in chunk))
            for e, v in zip(chunk, values, strict=True):
                e["output"] = v
                del e["outputRef"]
        run = self.inv["run"]
        if isinstance(run.get("inputRef"), str):
            run["input"] = await self.io.payload(run["inputRef"])
        for e in entries:
            self.journal[e["stepId"]] = cast(JournalEntry, e)

    async def _checkpoint(self, commands: list[Command]) -> None:
        self.cp_seq += 1
        await self.io.checkpoint({"invocationId": self.inv["invocationId"], "seq": self.cp_seq, "commands": commands})

    def _take_outbox(self) -> list[Command]:
        out, self.outbox = self.outbox, []
        return out

    def _end(self, commands: list[Command]) -> list[Command]:
        return [*self._take_outbox(), *commands]

    def _time_left_ok(self) -> bool:
        total = self.deadline - self.started
        return self.deadline - self.now() > max(5.0, total * 0.1)

    # ---- running a step ----

    async def _call_step(self, fn: Callable[[], Any]) -> Any:
        r = await fn() if inspect.iscoroutinefunction(fn) else await run_in_thread(fn)
        while inspect.isawaitable(r):  # a plain function that returned an awaitable
            r = await r
        return r

    async def _run_step(self, c: _Call, attempt: int) -> Command | None:
        assert c.fn is not None and c.retry is not None
        started: Command = {
            "type": "stepStarted",
            "stepId": c.step_id,
            "name": c.name,
            "attempt": attempt,
            "retry": c.retry,
        }
        if c.timeout_ms:
            started["timeoutMs"] = c.timeout_ms
        await self._checkpoint([*self._take_outbox(), started])
        key = f"{self.inv['run']['id']}:{c.step_id}:{attempt}"
        info = StepRun(attempt, key, threading.Event(), self.cancel_event)
        token = CURRENT_STEP.set(info)
        try:
            work = asyncio.ensure_future(self._call_step(c.fn))  # copies the context: CURRENT_STEP
            work.add_done_callback(_retrieve)
            try:
                if c.timeout_ms:
                    done, _ = await asyncio.wait({work}, timeout=c.timeout_ms / 1000)
                    if not done:
                        info.timed_out.set()
                        work.cancel()  # a thread still running is abandoned, not killed
                        raise StepTimeoutError(f'Step "{c.name}" ran past {c.timeout_ms} ms')
                raw = await asyncio.shield(work)
            except asyncio.CancelledError:
                if _being_cancelled():
                    # The invocation itself is cancelled (superseded, shut down): tell the step.
                    info.timed_out.set()
                    work.cancel()
                    raise
                raise _StepCancelled() from None
            try:
                encoded = encode_value(raw)
            except (TypeError, RecursionError) as e:
                raise NonRetriableError(str(e)) from e
            if json_byte_length(encoded) > MAX_OUTPUT_BYTES:
                raise OutputTooLargeError(f'Step "{c.name}" returned more than 1 MiB of JSON')
            value = _json_copy(encoded)  # the live value is the replayed one
            self.executed += 1
            self._resolve(c, value)
            self.outbox.append({"type": "stepCompleted", "stepId": c.step_id, "name": c.name, "output": value})
            return None
        except _StepCancelled:
            # The step's own code raised asyncio.CancelledError (not the invocation's): a failure.
            return self._step_failed(c, attempt, asyncio.CancelledError("The step raised asyncio.CancelledError"))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            return self._step_failed(c, attempt, e)
        finally:
            CURRENT_STEP.reset(token)

    @staticmethod
    def _step_failed(c: _Call, attempt: int, e: BaseException) -> Command:
        retryable = not isinstance(e, NonRetriableError | OutputTooLargeError)
        cmd: Command = {
            "type": "stepFailed",
            "stepId": c.step_id,
            "name": c.name,
            "attempt": attempt,
            "error": encode_error(e),
            "retryable": retryable,
        }
        if isinstance(e, RetryAfterError):
            d = e.delay_ms
            if isinstance(d, int | float) and not isinstance(d, bool) and d == d:
                cmd["retryAfterMs"] = int(min(MAX_RETRY_AFTER_MS, max(0.0, d)))  # clamps infinities too
        return cmd

    def _terminal(self) -> list[Command]:
        assert self.finished is not None
        ok, val = self.finished
        if ok:
            try:
                output = encode_value(val)
            except (TypeError, RecursionError) as e:
                return [{"type": "runFailed", "error": encode_error(NonRetriableError(str(e)))}]
            if json_byte_length(output) > MAX_OUTPUT_BYTES:
                err = OutputTooLargeError("The workflow returned more than 1 MiB of JSON")
                return [{"type": "runFailed", "error": encode_error(err)}]
            return [{"type": "runCompleted", "output": output}]
        cmd: Command = {"type": "runFailed", "error": encode_error(val)}
        cause = getattr(val, _CAUSE_STEP, None)
        if isinstance(cause, str):
            cmd["causeStepId"] = cause
        return [cmd]

    # ---- the workflow ----

    def _meta(self) -> RunMeta:
        r = self.inv["run"]
        s = r.get("schedule")
        sched = None
        if s is not None:
            sched = ScheduleMeta(s["id"], datetime.fromtimestamp(parse_time(s["scheduledTime"]), UTC), s)
        return RunMeta(r["id"], r.get("runKey"), r["attempt"], sched)

    def _ended(self, kind: str, value: Any) -> None:
        """On the driver's loop: the workflow returned, raised, crashed or went away."""
        if not self.closed:
            if kind == "ok" and self.finished is None:
                self.finished = (True, value)
            elif kind == "error" and self.finished is None:
                self.finished = (False, value)
            elif kind == "crash":
                self.crashed = value
            elif kind == "gone":
                self.gone = True
        self.activity.set()

    @staticmethod
    def _not_awaitable(r: Any) -> Any:
        if inspect.isawaitable(r):
            if inspect.iscoroutine(r):
                r.close()
            raise TypeError(
                "A plain def workflow returned an awaitable: declare it async def (or make it return a value)"
            )
        return r

    async def _run_async(self) -> None:
        """The workflow, on its own loop."""
        try:
            r = self.d.fn(Context(self.inv["run"].get("input"), self._meta(), Step(self)))
            result = await r if inspect.isawaitable(r) else r
        except InvocationEnded:
            self._post(self._ended, "gone", None)
        except asyncio.CancelledError as e:
            if _being_cancelled():
                self._post(self._ended, "gone", None)
                raise
            self._post(self._ended, "error", e)  # the workflow's own code raised it: a failure
        except Exception as e:
            self._post(self._ended, "error", e)
        except BaseException as e:
            self._post(self._ended, "crash", e)
            raise
        else:
            self._post(self._ended, "ok", result)

    async def _wf_main(self) -> None:
        assert self.wf_closed is not None
        task = asyncio.ensure_future(self._run_async())
        task.add_done_callback(lambda t: None if t.cancelled() else t.exception())
        await self.wf_closed.wait()
        # The invocation is over; pending calls got InvocationEnded. Let the workflow unwind.
        current = asyncio.current_task()
        rest = [t for t in asyncio.all_tasks() if t is not current]
        if rest:
            _, still = await asyncio.wait(rest, timeout=UNWIND_SECONDS)
            for t in still:
                t.cancel()
            if still:
                await asyncio.wait(still, timeout=UNWIND_SECONDS)

    def _start_async(self) -> None:
        started = threading.Event()

        def main() -> None:
            # A loop of the workflow's own: its ready queue says exactly when the workflow is idle.
            loop = asyncio.SelectorEventLoop()
            loop.set_exception_handler(_quiet_handler)
            asyncio.set_event_loop(loop)
            self.wf_loop, self.wf_thread = loop, threading.get_ident()
            self.wf_closed = asyncio.Event()
            started.set()
            try:
                loop.run_until_complete(self._wf_main())
            except BaseException:
                pass
            finally:
                with contextlib.suppress(Exception):
                    loop.run_until_complete(loop.shutdown_asyncgens())
                loop.close()

        threading.Thread(target=main, name=f"ezgh-workflows-{self.d.name}", daemon=True).start()
        started.wait()

    def _start_sync(self) -> None:
        self._sync_step = SyncStep(self)
        ctx = SyncContext(self.inv["run"].get("input"), self._meta(), self._sync_step)
        fn = self.d.fn

        def target() -> None:
            self.sync_thread = threading.get_ident()
            kind: str
            value: Any = None
            try:
                kind, value = "ok", self._not_awaitable(fn(ctx))
            except InvocationEnded:
                kind = "gone"
            except Exception as e:
                kind, value = "error", e
            except BaseException as e:
                kind, value = "crash", e
            self._post(self._ended, kind, value)  # queued before the driver can see thread_done
            with self.lock:
                self.thread_done = True
                self.lock.notify_all()
            self._notify()

        threading.Thread(target=target, name=f"ezgh-workflows-{self.d.name}", daemon=True).start()

    def _workflow_over(self) -> bool:
        return self.finished is not None or self.crashed is not None or self.gone

    def _budget_left(self) -> float:
        total = self.deadline - self.started
        return max(0.0, self.deadline - self.now() - max(5.0, total * 0.1))

    async def _async_idle(self) -> None:
        """Until the workflow's loop has no ready callbacks: every task waits on a future nothing
        has resolved (or the workflow ended)."""
        loop = self.wf_loop
        assert loop is not None
        answer: asyncio.Future[None] = self.loop.create_future()

        def reply() -> None:
            if not answer.done():
                answer.set_result(None)

        hops = [0]

        def probe() -> None:
            ready = getattr(loop, "_ready", None)
            hops[0] += 1
            idle = len(ready) == 0 if ready is not None else hops[0] > 64
            if idle:
                self._post(reply)
            else:
                loop.call_soon(probe)

        with contextlib.suppress(RuntimeError):
            loop.call_soon_threadsafe(probe)
        await answer

    def _sync_idle(self) -> bool:
        with self.lock:
            w = self.sync_waiting
            return self.thread_done or (w is not None and not self._sync_ready(w))

    async def _quiesce(self) -> bool:
        """Until the workflow is idle; False when the time budget ran out first (code busy outside
        any step), so the invocation ends with what it has."""
        if self.d.is_async:
            try:
                await asyncio.wait_for(self._async_idle(), self._budget_left())
            except TimeoutError:
                return False
            # Anything the workflow posted before going idle has run on this loop by now.
            await asyncio.sleep(0)
            return True
        while True:
            self.activity.clear()
            if self._sync_idle():
                await asyncio.sleep(0)  # let the thread's posted notices run first
                return True
            try:
                await asyncio.wait_for(self.activity.wait(), self._budget_left())
            except TimeoutError:
                return self._sync_idle()

    def _close(self) -> None:
        """The invocation is over: anything still waiting gets InvocationEnded, so the workflow
        unwinds through its finally blocks instead of hanging; step calls made then raise it."""
        with self.lock:
            self.closed = True
            unsettled = [c for c in self.calls if not c.settled]
        for c in unsettled:
            self._reject(c, InvocationEnded())
        if self.wf_loop is not None and self.wf_closed is not None:
            with contextlib.suppress(RuntimeError):
                self.wf_loop.call_soon_threadsafe(self.wf_closed.set)

    async def run(self) -> list[Command]:
        try:
            await self._load()
            if self.d.is_async:
                self._start_async()
            else:
                self._start_sync()
            return await self._drive()
        finally:
            self._close()

    async def _drive(self) -> list[Command]:
        inv = self.inv
        while True:
            if not await self._quiesce():
                return self._end([])
            step_inv = inv["kind"] == "step"
            if self.fatal is not None:
                # A step invocation may only report its target step (the engine refuses the rest).
                return self._end([] if step_inv else [{"type": "runFailed", "error": self.fatal}])
            if self.finished is not None:
                return self._end([] if step_inv else self._terminal())
            if self.crashed is not None:
                raise self.crashed
            if self.gone:
                raise asyncio.CancelledError()  # the workflow was cancelled under us: report nothing
            with self.lock:
                pending = [c for c in self.calls if not c.settled]
            if not pending:
                self.activity.clear()
                try:
                    await asyncio.wait_for(self.activity.wait(), self._budget_left())
                except TimeoutError:
                    return self._end([])
                continue
            recorded = [
                (self._order_seq(c, e), c.order, c, e) for c in pending if (e := self._recorded_entry(c)) is not None
            ]
            if recorded:
                _, _, rc, re_ = min(recorded, key=lambda r: (r[0], r[1]))
                self._settle_recorded(rc, re_)
                continue
            pending.sort(key=lambda c: c.order)
            if step_inv:
                return await self._step_invocation(pending)
            c = pending[0]
            if inv["cancel"] and not self.cancel_thrown:
                self.cancel_thrown = True
                self._reject(c, RunCancelledError("The run was cancelled"))
                continue
            if self.cancel_thrown and c.kind != "run":
                self._reject(c, RunCancelledError("The run is cancelling: only step.run cleanup steps can run"))
                continue
            # The deciding call: at 1.1 the oldest call with no journal entry (D2-19; calls the
            # engine owns don't hold values back), at 1.0 the oldest pending call, as in phase 1.
            parallel = self.parallel and not self.cancel_thrown
            head = next((p for p in pending if p.step_id not in self.journal), None) if parallel else c
            if head is not None and head.kind == "value":
                if head.produce == "now":
                    recorded_value: str = iso_from_ms(math.floor(self.now() * 1000))
                    live: Any = datetime.fromtimestamp(parse_time(recorded_value), UTC)
                else:
                    recorded_value = live = str(uuidlib.uuid4())
                self.outbox.append(
                    {"type": "valueRecorded", "stepId": head.step_id, "name": head.name, "value": recorded_value}
                )
                self._resolve(head, live)
                continue
            if head is not None and head.kind == "callback":
                await self._create_callback(head)
                continue
            nxt = self._plan(pending) if parallel else self._sequential(c)
            if isinstance(nxt, list):
                return self._end(nxt)
            if self.executed >= self.max_steps or not self._time_left_ok():
                return self._end([])
            failed = await self._run_step(nxt.call, nxt.attempt)
            if failed is not None:
                return self._end([failed])

    async def _create_callback(self, c: _Call) -> None:
        """Mints the token (the run's shard), records it in an acknowledged checkpoint, and only
        then resolves the handle, so its URL is never seen before the engine knows it."""
        shard = shard_of(self.inv["run"]["id"])
        if shard is None:
            self._reject(c, ValueError("The run ID carries no shard; callbacks need a v7 or v8 run ID"))
            return
        token, _ = new_callback_token(shard)
        assert c.callback_timeout_ms is not None
        created: Command = {
            "type": "callbackCreated",
            "stepId": c.step_id,
            "name": c.name,
            "token": token,
            "timeoutMs": c.callback_timeout_ms,
        }
        await self._checkpoint([*self._take_outbox(), created])
        # A wait() later in this invocation finds the callback waiting.
        self.journal[c.step_id] = {
            "seq": 2**62,
            "stepId": c.step_id,
            "name": c.name,
            "kind": "callback",
            "status": "waiting",
            "attempt": 1,
            "token": token,
        }
        self._resolve(c, self._handle(c, token))

    def _planned(self, c: _Call) -> Command:
        s: Command = {"stepId": c.step_id, "name": c.name, "retry": c.retry}
        if c.timeout_ms:
            s["timeoutMs"] = c.timeout_ms
        return s

    def _plan(self, pending: list[_Call]) -> "_Inline | list[Command]":
        """The frontier rule (protocol 1.1, D2-19, identical in every SDK): one actionable step with
        nothing else pending runs inline; otherwise every new step is planned together and every
        suspension sent with it, in call order. A retrying step alongside other work waits
        (planParallel skips existing steps); a running entry (a planned step at attempt 0 too) is
        awaited, never re-run."""
        runs: list[_Call] = []
        retrying: list[_Call] = []
        busy = 0
        suspends: list[Command] = []
        waited: set[str] = set()
        for p in pending:
            e = self.journal.get(p.step_id)
            if p.kind == "run":
                if e is None:
                    runs.append(p)
                elif e.get("status") == "retrying":
                    retrying.append(p)
                else:
                    busy += 1
            elif p.kind == "sleep":
                if e is None:
                    assert p.until_ms is not None
                    suspends.append(
                        {"type": "sleep", "stepId": p.step_id, "name": p.name, "until": iso_from_ms(p.until_ms)}
                    )
                else:
                    busy += 1
            elif p.kind == "invoke":
                if e is None:
                    cmd: Command = {"type": "invoke", "stepId": p.step_id, "name": p.name, "workflow": p.workflow}
                    if p.has_input:  # no input: the field is omitted, as in every SDK
                        cmd["input"] = p.input
                    suspends.append(cmd)
                else:
                    busy += 1
            elif p.kind == "wait" and p.step_id not in waited and (e is None or e.get("status") not in RECORDED):
                waited.add(p.step_id)
                suspends.append({"type": "waitForCallback", "stepId": p.step_id})
            # value and callback calls are handled when they're the oldest new call.
        if busy == 0 and not suspends and len(runs) + len(retrying) == 1:
            c = runs[0] if runs else retrying[0]
            e = self.journal.get(c.step_id)
            return _Inline(c, int(e.get("attempt", 0)) + 1 if e is not None and e.get("status") == "retrying" else 1)
        commands: list[Command] = []
        if runs:
            commands.append({"type": "planParallel", "steps": [self._planned(r) for r in runs[:MAX_PLANNED_STEPS]]})
        return [*commands, *suspends]

    def _sequential(self, c: _Call) -> "_Inline | list[Command]":
        """Protocol 1.0 (and cleanup while cancelling): phase 1, exactly."""
        e = self.journal.get(c.step_id)
        # Any other recorded status (sleeping, running, waiting, or one a later version adds) means
        # the engine is still waiting on it: end the invocation; only a retrying step runs again.
        if e is not None and e.get("status") != "retrying":
            return []
        if c.kind == "sleep":
            assert c.until_ms is not None
            return [{"type": "sleep", "stepId": c.step_id, "name": c.name, "until": iso_from_ms(c.until_ms)}]
        if c.kind != "run":
            return []
        return _Inline(c, int(e.get("attempt", 0)) + 1 if e is not None else 1)

    async def _step_invocation(self, pending: list[_Call]) -> list[Command]:
        """A step invocation runs exactly its target step, at the journal's attempt + 1, and ends."""
        target = next((c for c in pending if c.kind == "run" and c.step_id == self.inv.get("targetStepId")), None)
        if target is None or not self._time_left_ok():
            return self._end([])
        e = self.journal.get(target.step_id)
        failed = await self._run_step(target, (int(e.get("attempt", 0)) if e is not None else 0) + 1)
        return self._end([failed] if failed is not None else [])


async def execute(
    inv: Invocation,
    wf: Workflow,
    io: LiveIO,
    *,
    now: Callable[[], float] = time.time,
    max_steps: int = 50,
    deadline: float | None = None,
    cancel_event: threading.Event | None = None,
    api_base_url: str | None = None,
) -> list[Command]:
    """Runs one invocation of wf and returns its InvocationResult commands. Raises
    InvocationSuperseded or CheckpointFailed when a checkpoint isn't applied.

    deadline: the invocation's deadline on this machine's clock (epoch seconds); default the
    invocation's own. cancel_event: set while the run is cancelling (pull heartbeats), visible to
    steps through ctx.step.cancelled()."""
    if not isinstance(max_steps, int) or max_steps < 1:
        raise ValueError("max_steps is a whole number >= 1")
    ex = Execution(
        inv,
        wf,
        io,
        now,
        max_steps,
        deadline if deadline is not None else parse_time(inv["deadline"]),
        cancel_event,
        api_base_url,
    )
    return await ex.run()
