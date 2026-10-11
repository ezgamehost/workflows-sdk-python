"""Worker: runs a pull app's workflows (design-phase-2, "Pull workers").

It long-polls workflows-live for tasks with an API key, runs each invocation (checkpoints, journal
pages and payloads with the invocation token, as push does), heartbeats while it runs, and
completes it with its InvocationResult.
"""

import asyncio
import contextlib
import functools
import getpass
import json
import logging
import os
import random
import re
import signal
import socket
import threading
import time
import urllib.parse
import uuid
from collections.abc import Callable
from email.utils import parsedate_to_datetime
from typing import Any

from ._env import clean_deployment, detect_deployment
from ._http import AbortableRequest, HttpError, Response, request
from ._runtime.execution import MAX_PULL_RESULT_BYTES, execute
from ._runtime.invocation import InvalidInvocation, parse_invocation, parse_time
from ._runtime.live import CheckpointFailed, HttpLive, InvocationSuperseded, retry_after_seconds
from ._runtime.threads import run_in_thread
from ._runtime.workflow import Workflow, definition_of, workflow_info
from ._version import SDK_NAME, SDK_VERSION
from .client import DEFAULT_BASE_URL, Client, WorkflowsApiError, _Secret
from .protocol import PROTOCOL_VERSIONS
from .protocol._codec import dumps_bytes
from .protocol._ids import QUEUE_NAME, WORKER_ID, is_uuid
from .protocol._validate import check_poll_request

DEFAULT_LIVE_URL = "https://workflows-live.ezghcloud.com"
POLL_TIMEOUT = 70.0  # the live host holds a poll up to 55 s
MAX_POLL_TASKS = 10  # validate.go MaxPollTasks
COMPLETE_GRACE = 30.0  # completes are retried until the deadline plus this
STOP_READ_SECONDS = 0.25  # on stop(), how long an in-flight poll may finish before it's aborted
MAX_HEARTBEAT_INTERVAL = 30.0  # the engine fails an invocation after 60 s without a heartbeat
log = logging.getLogger("ezgh_workflows")


def dev_name(user: str | None = None) -> str:
    """dev-<user>, the same in every SDK: the user (else $USER, $USERNAME, $LOGNAME, the OS user)
    lowercased, each run of characters outside [a-z0-9] replaced by one "-", cut to 58 characters,
    trimmed of "-"; dev-dev when nothing is left. A valid queue and app name."""
    raw = user or os.environ.get("USER") or os.environ.get("USERNAME") or os.environ.get("LOGNAME") or ""
    if not raw:
        with contextlib.suppress(Exception):
            raw = getpass.getuser()
    clean = re.sub(r"[^a-z0-9]+", "-", raw.lower())[:58].strip("-")
    return f"dev-{clean or 'dev'}"


def _default_worker_id() -> str:
    host = re.sub(r"[^A-Za-z0-9._:-]", "-", socket.gethostname() or "worker")[:100] or "worker"
    return f"{host}-{uuid.uuid4().hex[:8]}"


def _unauthorized(status: int) -> WorkflowsApiError:
    return WorkflowsApiError(
        status,
        "unauthorized" if status == 401 else "forbidden",
        "The API key can't poll this app: it needs workflows.workers.poll (and workflows.apps.sync to announce "
        "workflows) on the app",
    )


class Worker:
    """Runs workflows for a pull app: polls workflows-live, runs invocations, heartbeats and
    completes them. await run() until stop() (or SIGTERM/SIGINT when run in the main thread).

    app is the pull app's ID; queue its task queue. Up to concurrency invocations run at once, and a
    poll asks for at most max_tasks (1-10). Every poll announces the workflows (announce=False
    leaves them out, so the key needs no workflows.apps.sync)."""

    def __init__(
        self,
        *,
        api_key: str,
        organization_id: str,
        project_id: str,
        app: str,
        workflows: list[Workflow],
        queue: str = "default",
        max_tasks: int = 1,
        concurrency: int = 10,
        worker_id: str | None = None,
        live_url: str = DEFAULT_LIVE_URL,
        announce: bool = True,
        deployment: str | None = None,
        heartbeat_interval: float = 20.0,
        shutdown_grace: float = 30.0,
        backoff_max: float = 30.0,
        max_steps_per_invocation: int = 50,
        api_base_url: str | None = None,
        on_error: Callable[[BaseException, str], None] | None = None,
    ) -> None:
        if not isinstance(api_key, str) or not api_key:
            raise ValueError("api_key is required")
        if not is_uuid(app):
            raise ValueError(f"app is the pull app's ID (a lowercase UUID); got {app!r}")
        if not isinstance(queue, str) or not QUEUE_NAME.fullmatch(queue):
            raise ValueError(f"A queue matches ^[a-z0-9][a-z0-9-]{{0,62}}$; got {queue!r}")
        if not isinstance(max_tasks, int) or not 1 <= max_tasks <= MAX_POLL_TASKS:
            raise ValueError("max_tasks is a whole number from 1 to 10")
        if not isinstance(concurrency, int) or concurrency < 1:
            raise ValueError("concurrency is a whole number >= 1")
        wid = worker_id if worker_id is not None else _default_worker_id()
        if not isinstance(wid, str) or not WORKER_ID.fullmatch(wid):
            raise ValueError(f"A worker ID matches ^[A-Za-z0-9._:-]{{1,128}}$; got {wid!r}")
        for name, v in (
            ("heartbeat_interval", heartbeat_interval),
            ("shutdown_grace", shutdown_grace),
            ("backoff_max", backoff_max),
        ):
            if not isinstance(v, int | float) or not v >= 0 or v != v:
                raise ValueError(f"{name} is a number of seconds >= 0")
        if not 0 < heartbeat_interval <= MAX_HEARTBEAT_INTERVAL:
            raise ValueError(
                f"heartbeat_interval is 0-{MAX_HEARTBEAT_INTERVAL:g} seconds, well inside the engine's 60 s "
                "heartbeat timeout"
            )
        if not isinstance(max_steps_per_invocation, int) or max_steps_per_invocation < 1:
            raise ValueError("max_steps_per_invocation is a whole number >= 1")
        self._by_name: dict[str, Workflow] = {}
        for wf in workflows:
            d = definition_of(wf)
            if d.name in self._by_name:
                raise ValueError(f'Two workflows are named "{d.name}"')
            self._by_name[d.name] = wf
        self._key = _Secret(api_key)  # never in repr()
        org, prj = urllib.parse.quote(organization_id, safe=""), urllib.parse.quote(project_id, safe="")
        self._poll_url = f"{live_url.rstrip('/')}/v1/organizations/{org}/projects/{prj}/workers/poll"
        self._app, self._queue, self._max_tasks, self._concurrency = app, queue, max_tasks, concurrency
        self._id = wid
        self._infos = [workflow_info(wf) for wf in self._by_name.values()] if announce else None
        self._deployment = clean_deployment(deployment) if deployment is not None else detect_deployment()
        self._hb, self._grace, self._backoff_max = float(heartbeat_interval), float(shutdown_grace), float(backoff_max)
        self._max_steps, self._api_base = max_steps_per_invocation, api_base_url
        self._on_error = on_error
        check_poll_request(self._poll_body(max_tasks))  # what the live host would refuse fails now
        self._stop = threading.Event()
        self._stopping: asyncio.Event | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._inflight: set[asyncio.Task[None]] = set()
        self._cancels: set[threading.Event] = set()  # each running invocation's step cancellation

    def __repr__(self) -> str:
        return f"<ezgh_workflows.Worker app={self._app} queue={self._queue} id={self._id}>"

    @property
    def worker_id(self) -> str:
        return self._id

    @classmethod
    def dev(
        cls,
        *,
        api_key: str,
        organization_id: str,
        project_id: str,
        workflows: list[Workflow],
        base_url: str = DEFAULT_BASE_URL,
        user: str | None = None,
        **kw: Any,
    ) -> "Worker":
        """A worker for local development: finds or creates the pull app named dev-<user> (its
        queue too), so two laptops never take each other's runs."""
        name = dev_name(user)
        c = Client(api_key=api_key, organization_id=organization_id, project_id=project_id, base_url=base_url)
        try:
            app_id = c.create_app(name, kind="pull", queue=name)["app"]["id"]
        except WorkflowsApiError as e:
            # It exists (409 app_exists), or the create's outcome is unknown: find it by name.
            if not ((e.status == 409 and e.code == "app_exists") or e.code == "outcome_unknown"):
                raise
            app_id = _find_app(c, name, name)
        return cls(
            api_key=api_key,
            organization_id=organization_id,
            project_id=project_id,
            app=app_id,
            workflows=workflows,
            queue=name,
            **kw,
        )

    def stop(self) -> None:
        """Stops polling at once (a long poll in flight is aborted); running invocations get
        shutdown_grace seconds. Thread-safe."""
        self._stop.set()
        loop, ev = self._loop, self._stopping
        if loop is not None and ev is not None:
            with contextlib.suppress(RuntimeError):
                loop.call_soon_threadsafe(ev.set)

    def run_sync(self) -> None:
        """asyncio.run(self.run())."""
        asyncio.run(self.run())

    def _error(self, e: BaseException, context: str) -> None:
        if self._on_error is not None:
            try:
                self._on_error(e, context)
            except Exception:
                log.exception("ezgh_workflows: on_error raised")
        else:
            log.warning("ezgh_workflows worker: %s: %s", context, e)

    def _poll_body(self, max_tasks: int) -> dict[str, Any]:
        body: dict[str, Any] = {
            "app": self._app,
            "queue": self._queue,
            "maxTasks": max_tasks,
            "workerId": self._id,
            "protocolVersions": list(PROTOCOL_VERSIONS),
            "sdk": {"name": SDK_NAME, "version": SDK_VERSION},
        }
        if self._infos is not None:
            body["workflows"] = self._infos
        return body

    async def _sleep(self, seconds: float) -> None:
        assert self._stopping is not None
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stopping.wait(), max(0.0, seconds))

    async def run(self) -> None:
        """Polls and runs invocations until stop(). Raises WorkflowsApiError when the key can't poll
        (401 or 403) or the poll is refused as malformed (another 4xx)."""
        loop = asyncio.get_running_loop()
        self._loop, self._stopping = loop, asyncio.Event()
        if self._stop.is_set():
            self._stopping.set()
        signals: list[signal.Signals] = []
        if threading.current_thread() is threading.main_thread():
            for sig in (signal.SIGTERM, signal.SIGINT):
                try:
                    loop.add_signal_handler(sig, self.stop)
                    signals.append(sig)
                except (NotImplementedError, RuntimeError, ValueError):
                    pass
        try:
            await self._poll_loop()
        finally:
            for sig in signals:
                with contextlib.suppress(Exception):
                    loop.remove_signal_handler(sig)
            await self._drain()

    async def _drain(self) -> None:
        if self._inflight:
            _, pending = await asyncio.wait(set(self._inflight), timeout=self._grace)
            for ev in list(self._cancels):  # running steps see ctx.step.cancelled()
                ev.set()
            for t in pending:  # abandoned: the engine's heartbeat timeout re-dispatches them
                t.cancel()
            if pending:
                await asyncio.wait(pending, timeout=5)

    async def _poll_loop(self) -> None:
        assert self._stopping is not None
        delay = 0.5
        while not self._stopping.is_set():
            free = self._concurrency - len(self._inflight)
            if free <= 0:
                stop = asyncio.ensure_future(self._stopping.wait())
                await asyncio.wait({*self._inflight, stop}, return_when=asyncio.FIRST_COMPLETED)
                stop.cancel()
                continue
            body = dumps_bytes(self._poll_body(min(self._max_tasks, free)))
            headers = {"Authorization": f"Bearer {self._key.value}", "Content-Type": "application/json"}
            # A daemon thread whose socket stop() shuts down: a poll the live host holds never
            # delays stop() or process exit.
            req = AbortableRequest()
            poll = run_in_thread(
                functools.partial(req.request, "POST", self._poll_url, headers, body, POLL_TIMEOUT),
                "ezgh-workflows-poll",
            )
            stop = asyncio.ensure_future(self._stopping.wait())
            await asyncio.wait({poll, stop}, return_when=asyncio.FIRST_COMPLETED)
            stop.cancel()
            if not poll.done():
                # Stopping. An answer already on its way is still read (its tasks run within the
                # grace period); a poll the live host is holding is aborted.
                await asyncio.wait({poll}, timeout=STOP_READ_SECONDS)
                if not poll.done():
                    req.abort()
                    await asyncio.wait({poll}, timeout=2.0)
                if not poll.done():
                    return
            received = time.time()
            try:
                res: Response = poll.result()
            except HttpError as e:
                if self._stopping.is_set():
                    return
                self._error(e, "poll")
                await self._backoff(delay, None)
                delay = min(self._backoff_max, delay * 2)
                continue
            if res.status in (401, 403):
                raise _unauthorized(res.status)  # the key or its permissions: fatal, as in every SDK
            if res.status == 429 or res.status >= 500:
                self._error(WorkflowsApiError(res.status, "unavailable", f"HTTP {res.status}"), "poll")
                await self._backoff(delay, retry_after_seconds(res))
                delay = min(self._backoff_max, delay * 2)
                continue
            if res.status >= 400 or res.status < 200 or res.status >= 300:
                # 1xx, 3xx (the worker follows no redirects) and other 4xx: misconfigured, not transient.
                raise WorkflowsApiError(res.status, _code(res), f"workflows-live refused the poll (HTTP {res.status})")
            if res.status == 204:
                delay = 0.5
                continue
            invs = _invocations(res.body)
            if invs is None:  # a 2xx that isn't a poll answer: something between us and the live host
                self._error(ValueError(f"HTTP {res.status} with a body that isn't a poll answer"), "poll answer")
                await self._backoff(delay, None)
                delay = min(self._backoff_max, delay * 2)
                continue
            delay = 0.5
            skew = _skew(res, received)
            for raw in invs:  # received even while stopping: they run within the grace period
                task = asyncio.ensure_future(self._handle(raw, received, skew))
                self._inflight.add(task)
                task.add_done_callback(self._inflight.discard)

    async def _backoff(self, delay: float, retry_after: float | None) -> None:
        """Waits max(Retry-After, the capped backoff with +-20 % jitter), or until stop()."""
        await self._sleep(max(retry_after or 0.0, min(self._backoff_max, delay) * random.uniform(0.8, 1.2)))

    async def _post(self, url: str, token: str, body: Any, until: float) -> Response | None:
        """POST with the invocation token; 429, 5xx and network errors are retried until `until`."""
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        data = dumps_bytes(body)
        attempt = 0
        while True:
            left = until - time.time()
            if left <= 0:
                return None
            try:
                res: Response = await run_in_thread(
                    functools.partial(request, "POST", url, headers, data, min(left, 60.0)), "ezgh-workflows-http"
                )
                if res.status != 429 and res.status < 500:
                    return res
                wait = retry_after_seconds(res) or 0.0
            except HttpError:
                wait = 0.0
            await asyncio.sleep(min(max(0.0, until - time.time()), max(wait, min(5.0, 0.2 * 2**attempt))))
            attempt += 1

    async def _handle(self, raw: Any, received: float, skew: float) -> None:
        try:
            inv = parse_invocation(json.dumps(raw))
        except (InvalidInvocation, TypeError, ValueError) as e:
            self._error(e, "an invocation the worker can't read")
            return
        if inv["kind"] == "sync":
            return
        live = inv["endpoints"]["live"].rstrip("/")
        sent = parse_time(inv["deadline"])
        # The local deadline: the engine's, moved by the clock skew the poll answer's Date shows.
        deadline = min(sent, sent - skew)
        result: dict[str, Any] = {"sdk": {"name": SDK_NAME, "version": SDK_VERSION}}
        if self._deployment:
            result["deployment"] = self._deployment
        name = inv["run"]["workflow"]
        wf = self._by_name.get(name)
        if wf is None:
            result["commands"] = [{"type": "workflowNotFound", "workflow": name}]
            await self._post(f"{live}/v1/invocations/complete", inv["token"], result, deadline + COMPLETE_GRACE)
            return
        cancel = threading.Event()
        self._cancels.add(cancel)
        try:
            await self._run(inv, wf, live, deadline, received, result, cancel)
        finally:
            self._cancels.discard(cancel)

    async def _run(
        self,
        inv: Any,
        wf: Workflow,
        live: str,
        deadline: float,
        received: float,
        result: dict[str, Any],
        cancel: threading.Event,
    ) -> None:
        superseded = [False]
        io = HttpLive(live, inv["token"], deadline=deadline, reserve=max(5.0, max(0.0, deadline - received) * 0.1))
        work = asyncio.ensure_future(
            execute(
                inv,
                wf,
                io,
                deadline=deadline,
                cancel_event=cancel,
                max_steps=self._max_steps,
                api_base_url=self._api_base,
                max_result_bytes=MAX_PULL_RESULT_BYTES,
            )
        )
        beat = asyncio.ensure_future(self._heartbeat(live, inv["token"], deadline, cancel, work, superseded))
        try:
            try:
                result["commands"] = await work
            except asyncio.CancelledError:
                if superseded[0]:
                    return
                raise
            except InvocationSuperseded:
                return
            except CheckpointFailed as e:
                self._error(e, "checkpoint")  # the engine's invocation timers recover it
                return
            except Exception as e:
                self._error(e, "invocation")
                return
            # Heartbeats go on until the result is delivered: a complete retried through an outage
            # mustn't let the invocation's heartbeat timeout pass.
            res = await self._post(f"{live}/v1/invocations/complete", inv["token"], result, deadline + COMPLETE_GRACE)
        finally:
            beat.cancel()
        if res is None:
            self._error(TimeoutError("workflows-live didn't take the result"), "complete")
        elif res.status >= 400 and res.status != 409:  # 409: superseded, the result is dropped
            self._error(WorkflowsApiError(res.status, _code(res), f"HTTP {res.status}"), "complete")

    async def _heartbeat(
        self,
        live: str,
        token: str,
        deadline: float,
        cancel: threading.Event,
        work: "asyncio.Future[Any]",
        superseded: list[bool],
    ) -> None:
        while True:
            await asyncio.sleep(self._hb)
            res = await self._post(f"{live}/v1/invocations/heartbeat", token, {}, min(deadline, time.time() + self._hb))
            if res is None:
                continue
            if res.status == 409:  # superseded: stop and report nothing
                superseded[0] = True
                cancel.set()  # a running step sees ctx.step.cancelled()
                work.cancel()
                return
            try:
                if res.status < 300 and json.loads(res.body or b"{}").get("cancel"):
                    cancel.set()
            except (ValueError, AttributeError, UnicodeDecodeError):
                pass


def _invocations(body: bytes) -> list[Any] | None:
    """A poll answer's invocations: {"invocations": [...]}, else None."""
    try:
        v = json.loads(body) if body else None
    except (ValueError, UnicodeDecodeError):
        return None
    if not isinstance(v, dict):
        return None
    invs = v.get("invocations")
    if invs is None:
        return []
    return invs if isinstance(invs, list) else None


def _code(res: Response) -> str:
    try:
        err = json.loads(res.body).get("error")
        code = err.get("code") if isinstance(err, dict) else None
        return code if isinstance(code, str) else "unknown"
    except (ValueError, AttributeError, UnicodeDecodeError):
        return "unknown"


def _skew(res: Response, received: float) -> float:
    """How far the live host's clock is ahead of ours (its Date header), or 0."""
    v = res.headers.get("date")
    if not v:
        return 0.0
    try:
        return parsedate_to_datetime(v).timestamp() - received
    except (TypeError, ValueError, IndexError):
        return 0.0


def _find_app(c: Client, name: str, queue: str) -> str:
    cursor: str | None = None
    while True:
        page = c.list_apps(cursor=cursor, limit=100)
        for a in page.get("apps", []):
            if a.get("name") == name:
                if a.get("kind") != "pull" or a.get("queue") != queue:
                    raise WorkflowsApiError(
                        409, "app_exists", f'The app "{name}" exists but isn\'t a pull app with the queue {queue}'
                    )
                return a["id"]
        cursor = page.get("nextCursor")
        if not cursor:
            raise WorkflowsApiError(404, "not_found", f'The dev app "{name}" was neither created nor found')
