"""One invocation of a workflow: replay against the journal, then run forward to the frontier.

Mirrors workflows-sdk-ts's Execution (the reference) with the phase 2 rules:
- recorded outcomes settle one at a time, lowest seq first, with the event loop run to quiescence
  before each, so asyncio.wait(FIRST_COMPLETED) sees the same winner as the first time;
- at the frontier (protocol 1.1) the oldest new call decides: a value is recorded, a callback is
  created (acknowledged before its URL is visible); otherwise one lone actionable step runs inline
  and anything more is planned (planParallel) and suspended (sleep, invoke, waitForCallback) in one
  result: the frontier rule, D2-19, the same in every SDK. Protocol 1.0 keeps phase 1's
  one-at-a-time behaviour;
- a step invocation runs exactly its target step;
- a failed attempt ends the invocation with stepFailed; the step and time budgets end it with no
  terminal command; the workflow's return or raise ends it with runCompleted or runFailed.
"""

import asyncio
import contextlib
import contextvars
import inspect
import math
import threading
import time
import uuid as uuidlib
import weakref
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

from ..protocol._bytes import utf8_len
from ..protocol._codec import (
    DEFAULT_RETRY,
    MAX_OUTPUT_BYTES,
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
from .invocation import parse_time
from .live import LiveIO
from .workflow import Workflow, check_timeout, definition_of, merge_retry

# Quiescence: this many consecutive event-loop turns with no new step call (at least 8, plan Y2).
IDLE_SPINS = 16
MAX_SLEEP_MS = 31_536_000_000  # a year: a run times out after a year at most
MIN_CALLBACK_TIMEOUT_MS = 1_000
MAX_RETRY_AFTER_MS = 86_400_000
MAX_PLANNED_STEPS = 1000  # validate.go MaxPlannedSteps
DEFAULT_API_BASE = "https://workflows-api.ezghcloud.com"
RECORDED = frozenset({"completed", "failed", "cancelled"})
_CAUSE_STEP = "_ezgh_workflows_step_id"


def _retrieve(f: "asyncio.Future[Any]") -> None:
    """Marks a future's exception retrieved: a call the workflow never awaits (a race's loser)
    isn't logged; whoever awaits it still sees the exception."""
    if not f.cancelled():
        f.exception()


_FILTERED: "weakref.WeakSet[asyncio.AbstractEventLoop]" = weakref.WeakSet()


def _quiet_invocation_ended(loop: asyncio.AbstractEventLoop) -> None:
    """A task the workflow created and never awaited (a race's loser) ends with InvocationEnded
    when its invocation suspends; that's expected, so the loop doesn't log it. Every other
    exception goes to the handler that was there before."""
    try:
        if loop in _FILTERED:
            return
        _FILTERED.add(loop)
    except TypeError:  # a loop that can't be weakly referenced: leave its handler alone
        return
    previous = loop.get_exception_handler()

    def handler(lp: asyncio.AbstractEventLoop, context: dict[str, Any]) -> None:
        if isinstance(context.get("exception"), InvocationEnded):
            return
        if previous is not None:
            previous(lp, context)
        else:
            lp.default_exception_handler(context)

    loop.set_exception_handler(handler)


def _supports_parallel(version: str) -> bool:
    major, _, minor = version.partition(".")
    try:
        return (int(major), int(minor.split(".")[0] or "0")) >= (1, 1)
    except ValueError:
        return major == "1"


def run_in_thread(fn: Callable[[], Any]) -> "asyncio.Future[Any]":
    """fn in a fresh daemon thread with the caller's contextvars. A step that outlives its timeout
    is abandoned, not killed, and never holds a slot of a shared executor."""
    loop = asyncio.get_running_loop()
    fut: asyncio.Future[Any] = loop.create_future()
    ctx = contextvars.copy_context()

    def target() -> None:
        ok, value = True, None
        try:
            value = ctx.run(fn)
        except BaseException as e:
            ok, value = False, e if not isinstance(e, StopIteration) else RuntimeError(str(e))

        def settle() -> None:
            if fut.done():
                return
            if ok:
                fut.set_result(value)
            else:
                fut.set_exception(cast(BaseException, value))

        with contextlib.suppress(RuntimeError):  # the loop is closed: nobody is waiting any more
            loop.call_soon_threadsafe(settle)

    threading.Thread(target=target, name="ezgh-workflows-step", daemon=True).start()
    return fut


@dataclass(eq=False)
class _Call:
    order: int
    name: str
    kind: str  # run | sleep | value | callback | wait | invoke
    step_id: str
    future: "asyncio.Future[Any]"
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

    @property
    def settled(self) -> bool:
        return self.future.done()


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
        self.loop = asyncio.get_running_loop()
        _quiet_invocation_ended(self.loop)
        self.parallel = _supports_parallel(inv["protocolVersion"])
        api = inv["endpoints"].get("api") or api_base_url or DEFAULT_API_BASE
        self.api_base = api.rstrip("/")
        self.calls: list[_Call] = []
        self.counters: dict[str, int] = {}
        self.explicit_ids: set[str] = set()
        self.journal: dict[str, JournalEntry] = {}
        self.outbox: list[Command] = []
        self.cp_seq = 0
        self.executed = 0
        self.cancel_thrown = False
        self.fatal: WireError | None = None
        self.finished: tuple[bool, Any] | None = None
        self.crashed: BaseException | None = None  # the workflow ended with a BaseException
        self.closed = False
        self.activity = asyncio.Event()
        self.parked = asyncio.Event()  # sync workflows: the thread waits on a call, or has ended
        self.thread_done = False
        self.task: asyncio.Task[None] | None = None
        self._sync_step: SyncStep | None = None

    # ---- registration (on the loop thread) ----

    def _failed(self, e: BaseException) -> "asyncio.Future[Any]":
        fut: asyncio.Future[Any] = self.loop.create_future()
        fut.set_exception(e)
        fut.add_done_callback(_retrieve)
        return fut

    def _register(self, kind: str, name: str, explicit: str | None, **extra: Any) -> "asyncio.Future[Any]":
        if self.closed:
            return self._failed(InvocationEnded())
        if not isinstance(name, str) or not name or utf8_len(name) > 256 or "\u0000" in name:
            return self._failed(ValueError("A step name is 1-256 bytes of UTF-8, without NUL"))
        if explicit is not None and (not isinstance(explicit, str) or not STEP_ID_OVERRIDE.fullmatch(explicit)):
            return self._failed(ValueError(f"Step ids match ^[A-Za-z0-9._:-]{{1,128}}$; got {explicit!r}"))
        if kind in ("callback", "invoke") and not self.parallel and self.fatal is None:
            what = "create_callback" if kind == "callback" else "invoke"
            self.fatal = encode_error(
                NonRetriableError(f"step.{what} needs protocol 1.1; this invocation is {self.inv['protocolVersion']}")
            )
        if explicit is not None:
            sid = explicit
            # An explicit id names one step for the whole run: reusing it, even after the first step
            # settled, would replay the first step's outcome as the second's.
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
        return self._append(_Call(len(self.calls), name, kind, sid, self.loop.create_future(), explicit, **extra))

    def _append(self, c: _Call) -> "asyncio.Future[Any]":
        self.calls.append(c)
        c.future.add_done_callback(_retrieve)
        self.activity.set()
        return c.future

    def register_run(
        self,
        name: str,
        fn: Callable[[], Any],
        retry: Mapping[str, Any] | None,
        timeout_ms: int | None,
        explicit: str | None,
    ) -> "asyncio.Future[Any]":
        try:
            if not callable(fn):
                raise TypeError("step.run needs a function")
            policy = merge_retry(self.d.retry or DEFAULT_RETRY, retry)
            t = check_timeout(timeout_ms) if timeout_ms is not None else self.d.timeout_ms
        except (TypeError, ValueError) as e:
            return self._failed(e)
        return self._register("run", name, explicit, fn=fn, retry=policy, timeout_ms=t)

    def register_sleep(
        self, name: str, duration: str | int | None, at: datetime | str | None, explicit: str | None
    ) -> "asyncio.Future[Any]":
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
        except (TypeError, ValueError, OverflowError) as e:
            return self._failed(e if not isinstance(e, OverflowError) else ValueError(str(e)))
        return self._register("sleep", name, explicit, until_ms=until)

    def register_value(self, name: str, what: str) -> "asyncio.Future[Any]":
        return self._register("value", name, None, produce=what)

    def register_callback(
        self, name: str, timeout: str | int, explicit: str | None, *, sync: bool
    ) -> "asyncio.Future[Any]":
        try:
            ms = parse_duration(timeout)
            if not MIN_CALLBACK_TIMEOUT_MS <= ms <= MAX_SLEEP_MS:
                raise ValueError("A callback timeout is from 1 second to 1 year")
        except ValueError as e:
            return self._failed(e)
        return self._register("callback", name, explicit, callback_timeout_ms=ms, sync_handle=sync)

    def register_invoke(self, name: str, workflow: Any, input: Any, explicit: str | None) -> "asyncio.Future[Any]":
        try:
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
        except (TypeError, ValueError) as e:
            return self._failed(e)
        return self._register("invoke", name, explicit, workflow=wf_name, has_input=input is not None, input=encoded)

    def _wait_call(self, create: _Call) -> "asyncio.Future[Any]":
        """Callback.wait(): a pending call on the callback's own step ID that neither advances the
        name's counter nor counts as a duplicate."""
        if self.closed:
            return self._failed(InvocationEnded())
        return self._append(_Call(len(self.calls), create.name, "wait", create.step_id, self.loop.create_future()))

    # ---- sync workflows: is the thread waiting on a call? ----

    def sync_parked(self) -> None:
        self.parked.set()

    def sync_resumed(self) -> None:
        if not self.thread_done:
            self.parked.clear()

    # ---- settling ----

    @staticmethod
    def _resolve(c: _Call, value: Any) -> None:
        if not c.future.done():
            c.future.set_result(value)

    @staticmethod
    def _reject(c: _Call, e: BaseException) -> None:
        if not c.future.done():
            c.future.set_exception(e)

    def _handle(self, c: _Call, token: str) -> Callback | SyncCallback:
        url = f"{self.api_base}/public/v1/callbacks/{token}"
        if c.sync_handle:
            sync_step = self._sync_step
            assert sync_step is not None
            return SyncCallback(url, token, lambda: sync_step._block(lambda: self._wait_call(c)))
        return Callback(url, token, lambda: self._wait_call(c))

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
            if c.timeout_ms:
                done, _ = await asyncio.wait({work}, timeout=c.timeout_ms / 1000)
                if not done:
                    info.timed_out.set()
                    work.cancel()  # a thread still running is abandoned, not killed
                    raise StepTimeoutError(f'Step "{c.name}" ran past {c.timeout_ms} ms')
            raw = await work
            try:
                value = encode_value(raw)
            except (TypeError, RecursionError) as e:
                raise NonRetriableError(str(e)) from e
            if json_byte_length(value) > MAX_OUTPUT_BYTES:
                raise OutputTooLargeError(f'Step "{c.name}" returned more than 1 MiB of JSON')
            self.executed += 1
            self._resolve(c, value)
            self.outbox.append({"type": "stepCompleted", "stepId": c.step_id, "name": c.name, "output": value})
            return None
        except asyncio.CancelledError:
            raise
        except Exception as e:
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
                    cmd["retryAfterMs"] = int(min(MAX_RETRY_AFTER_MS, max(0.0, math.floor(d))))
            return cmd
        finally:
            CURRENT_STEP.reset(token)

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

    def _finish(self, ok: bool, value: Any) -> None:
        if not self.closed and self.finished is None:
            self.finished = (ok, value)
        self.activity.set()

    async def _run_async(self) -> None:
        try:
            result = await self.d.fn(Context(self.inv["run"].get("input"), self._meta(), Step(self)))
        except InvocationEnded:
            return
        except Exception as e:
            self._finish(False, e)
        else:
            self._finish(True, result)
        finally:
            self.activity.set()

    def _start_sync(self) -> None:
        self._sync_step = SyncStep(self, self.loop)
        ctx = SyncContext(self.inv["run"].get("input"), self._meta(), self._sync_step)
        fn, loop = self.d.fn, self.loop

        def target() -> None:
            outcome: tuple[str, Any]
            try:
                outcome = ("ok", fn(ctx))
            except InvocationEnded:
                outcome = ("ended", None)
            except Exception as e:
                outcome = ("error", e)
            except BaseException as e:
                outcome = ("crash", e)

            def done() -> None:
                self.thread_done = True
                if outcome[0] == "ok":
                    self._finish(True, outcome[1])
                elif outcome[0] == "error":
                    self._finish(False, outcome[1])
                elif outcome[0] == "crash" and not self.closed:
                    self.crashed = outcome[1]
                self.parked.set()
                self.activity.set()

            with contextlib.suppress(RuntimeError):
                loop.call_soon_threadsafe(done)

        threading.Thread(target=target, name=f"ezgh-workflows-{self.d.name}", daemon=True).start()

    def _workflow_over(self) -> bool:
        if self.finished is not None or self.crashed is not None:
            return True
        return self.task is not None and self.task.done()

    async def _wait(self, ev: asyncio.Event) -> bool:
        """ev.wait(), bounded by the invocation's time budget: False when the budget ran out while
        the workflow was busy outside any step (the invocation then ends with what it has)."""
        total = self.deadline - self.started
        left = self.deadline - self.now() - max(5.0, total * 0.1)
        try:
            await asyncio.wait_for(ev.wait(), max(0.0, left))
        except TimeoutError:
            return ev.is_set()
        return True

    async def _quiesce(self) -> bool:
        while True:
            if not self.d.is_async and not await self._wait(self.parked):
                return False
            seen, spins = len(self.calls), 0
            while spins < IDLE_SPINS:
                await asyncio.sleep(0)
                if self._workflow_over():
                    return True
                if len(self.calls) != seen:
                    seen, spins = len(self.calls), 0
                else:
                    spins += 1
            if self.d.is_async or self.parked.is_set():
                return True

    def _close(self) -> None:
        """The invocation is over: anything still waiting gets InvocationEnded, so the workflow
        task (or thread) unwinds through its finally blocks instead of hanging."""
        self.closed = True
        for c in self.calls:
            self._reject(c, InvocationEnded())

    async def run(self) -> list[Command]:
        try:
            await self._load()
            if self.d.is_async:
                self.task = asyncio.ensure_future(self._run_async())
                self.task.add_done_callback(lambda t: None if t.cancelled() else t.exception())
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
            if self.fatal is not None:
                return self._end([{"type": "runFailed", "error": self.fatal}])
            if self.finished is not None:
                return self._end(self._terminal())
            if self.crashed is not None:
                raise self.crashed
            if self.task is not None and self.task.done():
                # The workflow ended without a result: cancelled, or a BaseException. Report nothing.
                if self.task.cancelled():
                    raise asyncio.CancelledError()
                exc = self.task.exception()
                raise exc if exc is not None else asyncio.CancelledError()
            pending = [c for c in self.calls if not c.settled]
            if not pending:
                self.activity.clear()
                if not await self._wait(self.activity):
                    return self._end([])
                continue
            recorded = [(e["seq"], c.order, c, e) for c in pending if (e := self._recorded_entry(c)) is not None]
            if recorded:
                _, _, rc, re_ = min(recorded, key=lambda r: (r[0], r[1]))
                self._settle_recorded(rc, re_)
                continue
            pending.sort(key=lambda c: c.order)
            if inv["kind"] == "step":
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
            head = next((p for p in pending if p.step_id not in self.journal), None) if self.parallel else c
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
            nxt = self._plan(pending) if self.parallel and not self.cancel_thrown else self._sequential(c)
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
