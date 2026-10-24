"""The Workflows API for one project: apps, runs, callbacks and schedules.

Client is blocking (urllib); AsyncClient makes the same calls in a thread. Responses are TypedDicts
with the API's camelCase keys, so fields a later API adds pass through untouched.
"""

import asyncio
import json
import math
import time
import urllib.parse
import uuid
from typing import Any, Literal, NotRequired, TypedDict

from ._http import HttpError, request
from ._instance import KeySource, key_source
from .protocol._codec import dumps_bytes, encode_value

_sleep = time.sleep
_NO_BODY: Any = object()
DEFAULT_BASE_URL = "https://workflows-api.ezghcloud.com"


class WorkflowsApiError(Exception):
    """An error answer from the Workflows API: the HTTP status and the platform error code.
    Status 0 with code "outcome_unknown": the request may or may not have taken effect (a network
    error on create_app or rotate_secret, which aren't resent)."""

    def __init__(self, status: int, code: str, message: str, retry_after_seconds: float | None = None) -> None:
        super().__init__(message)
        self.status, self.code, self.retry_after_seconds = status, code, retry_after_seconds

    def __repr__(self) -> str:
        return f"WorkflowsApiError({self.status}, {self.code!r}, {str(self)!r})"


class RunStep(TypedDict):
    seq: int
    stepId: str
    name: str
    kind: str
    status: str
    attempt: int
    hasOutput: bool
    error: dict[str, Any] | None


class Run(TypedDict):
    id: str
    runKey: str | None
    appId: str
    workflow: str
    status: str  # running | cancelling | completed | failed | cancelled | timedOut (or later)
    attempt: int
    error: dict[str, Any] | None
    causeStepId: str | None
    steps: list[RunStep]
    createdAt: str
    updatedAt: str
    endedAt: str | None


class App(TypedDict):
    id: str
    slug: str
    resourceName: str
    name: str
    kind: str  # push | pull
    endpoint: str | None
    syncStatus: str
    workflows: list[dict[str, Any]]
    maxInvocationSeconds: int | None
    protocolVersion: str | None
    sdk: dict[str, str] | None
    deployment: str | None
    health: str
    createdAt: str
    updatedAt: str
    queue: NotRequired[str | None]
    partitions: NotRequired[int | None]


class AppWithSecret(TypedDict):
    """A new or rotated signing secret is returned once; an Idempotency-Key replay answers None."""

    app: App
    signingSecret: str | None


class AppList(TypedDict):
    apps: list[App]
    nextCursor: str | None


class Schedule(TypedDict):
    id: str
    resourceName: str
    appId: str
    workflow: str
    name: str
    cron: str
    timeZone: str
    paused: bool
    revision: int
    updatedAt: str
    overlap: NotRequired[str]
    jitterSeconds: NotRequired[int]
    catchupWindowSeconds: NotRequired[int]
    nextFireAt: NotRequired[str | None]
    nextScheduledTime: NotRequired[str | None]
    lastScheduledTime: NotRequired[str | None]
    activeRunId: NotRequired[str | None]
    missed: NotRequired[int]
    skipped: NotRequired[int]
    hasInput: NotRequired[bool]
    createdAt: NotRequired[str]


class ScheduleList(TypedDict):
    schedules: list[Schedule]
    nextCursor: str | None


class RunEvents(TypedDict):
    events: list[dict[str, Any]]
    nextCursor: str | None


Overlap = Literal["skip", "allow", "bufferOne"]
_SCHEDULE_FIELDS = {
    "workflow": "workflow",
    "name": "name",
    "cron": "cron",
    "time_zone": "timeZone",
    "input": "input",
    "overlap": "overlap",
    "jitter_seconds": "jitterSeconds",
    "catchup_window_seconds": "catchupWindowSeconds",
}


def _q(s: str) -> str:
    if not isinstance(s, str) or not s:
        raise ValueError("IDs are non-empty strings")
    return urllib.parse.quote(s, safe="")


def _query(**kw: Any) -> str:
    q = {k: str(v) for k, v in kw.items() if v is not None}
    return "?" + urllib.parse.urlencode(q) if q else ""


def _retry_after(v: str | None) -> float | None:
    if not v:
        return None
    try:
        n = float(v)
    except ValueError:
        return None
    return n if math.isfinite(n) and n > 0 else None


def _api_error(status: int, payload: Any, retry_after: str | None) -> WorkflowsApiError:
    err = payload.get("error") if isinstance(payload, dict) else None
    err = err if isinstance(err, dict) else {}
    return WorkflowsApiError(
        status, str(err.get("code", "unknown")), str(err.get("message", f"HTTP {status}")), _retry_after(retry_after)
    )


def _limit(limit: int | None) -> int | None:
    if limit is not None and (not isinstance(limit, int) or isinstance(limit, bool) or limit < 1):
        raise ValueError("limit is a whole number >= 1")
    return limit


def _with_secret(out: Any, what: str, kind: str | None = None) -> Any:
    """A push app's secret comes back once: an answer without it (an Idempotency-Key replay) isn't
    success, because the caller would never see the secret. Pull apps have none."""
    app = out.get("app") if isinstance(out, dict) else None
    kind = kind or (app.get("kind") if isinstance(app, dict) else None)
    if isinstance(out, dict) and out.get("signingSecret") is None and kind != "pull":
        raise WorkflowsApiError(
            0,
            "outcome_unknown",
            f"The {what} answered without the signing secret (it's returned once; this was a replay). Check with "
            "get_app, and rotate the app's secret if you didn't receive it.",
        )
    return out


def complete_callback_url(url: str, value: Any, *, timeout: float = 60.0, retries: int = 3) -> None:
    """Completes a callback by its URL (Callback.url) with value (any JSON, at most 1 MiB). The token
    in the URL is the only credential: no API key is sent. A repeat with the same value succeeds;
    another value is WorkflowsApiError 409 callback_completed, an expired callback 410
    callback_expired, an unknown token 404. Errors never include the URL."""
    if not isinstance(url, str) or not url.startswith(("https://", "http://")):
        raise ValueError("A callback URL is an http(s) URL, as Callback.url gives it")
    data = dumps_bytes(encode_value(value))
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    attempt = 0
    while True:
        try:
            res = request("POST", url, headers, data, timeout)
        except HttpError as e:
            # Completing is idempotent (the same value answers 200 again): resending is safe.
            if attempt >= retries:
                raise WorkflowsApiError(
                    0, "network_error", f"Completing the callback failed in transit ({e.reason})"
                ) from None
            _sleep(0.5 * 2**attempt)
            attempt += 1
            continue
        if 200 <= res.status < 300:
            return
        try:
            payload = json.loads(res.body) if res.body else None
        except (ValueError, UnicodeDecodeError):
            payload = None
        err = _api_error(res.status, payload, res.headers.get("retry-after"))
        if res.status not in (429, 503) or attempt >= retries:
            raise err
        _sleep(min(10.0, err.retry_after_seconds or 1.0))
        attempt += 1


class Client:
    """Calls the Workflows API for one project (blocking). Run calls take an API key or a
    workflows:runs SSO token; app and schedule management need an API key with those actions.

    Without api_key, on an EZGH compute instance with a Bot attached, the Bot's short-lived key
    from the metadata service is used (fetched when needed; a 401 fetches a new one and retries once).

    429, 503 and network errors are retried up to retries times (after Retry-After, at most
    max_retry_delay seconds); writes are resent with the same Idempotency-Key."""

    __slots__ = ("__weakref__", "_base", "_cred", "_max_delay", "_retries")

    def __init__(
        self,
        *,
        api_key: str | None = None,
        organization_id: str,
        project_id: str,
        base_url: str = DEFAULT_BASE_URL,
        retries: int = 3,
        max_retry_delay: float = 10.0,
    ) -> None:
        self._cred: KeySource = key_source(api_key)
        if not isinstance(retries, int) or isinstance(retries, bool) or retries < 0:
            raise ValueError("retries is a whole number >= 0")
        if not isinstance(max_retry_delay, int | float) or not math.isfinite(max_retry_delay) or max_retry_delay < 0:
            raise ValueError("max_retry_delay is a number of seconds >= 0")
        self._base = f"{base_url.rstrip('/')}/v1/organizations/{_q(organization_id)}/projects/{_q(project_id)}"
        self._retries, self._max_delay = retries, float(max_retry_delay)

    def __repr__(self) -> str:
        return f"<ezgh_workflows.Client {self._base}>"

    def _call(
        self,
        method: str,
        path: str,
        body: Any = _NO_BODY,
        idempotency_key: str | None = None,
        *,
        resend: bool = True,
        headers: dict[str, str] | None = None,
    ) -> Any:
        h = {"Accept": "application/json", **(headers or {})}
        data = None
        if body is not _NO_BODY:  # JSON null is a body too
            h["Content-Type"] = "application/json"
            data = dumps_bytes(body)
        if method != "GET":
            h["Idempotency-Key"] = idempotency_key or str(uuid.uuid4())
        attempt = 0
        reauthed = False  # an instance key the API refused is replaced once per call
        while True:
            key = self._cred.key()
            h["Authorization"] = f"Bearer {key}"
            try:
                res = request(method, self._base + path, h, data, 60.0)
            except HttpError as e:
                if not resend:
                    raise WorkflowsApiError(
                        0,
                        "outcome_unknown",
                        f"The request failed in transit ({e.reason}), so it may have taken effect; a resend "
                        "would return no secret. Check with get_app, and rotate the app's secret if you "
                        "didn't receive it.",
                    ) from None
                if attempt >= self._retries:
                    raise WorkflowsApiError(0, "network_error", f"The request failed in transit ({e.reason})") from None
                _sleep(min(self._max_delay, 0.5 * 2**attempt))
                attempt += 1
                continue
            try:
                payload = json.loads(res.body) if res.body else None
            except (ValueError, UnicodeDecodeError):
                payload = None
            if 200 <= res.status < 300:
                return payload
            api_err = _api_error(res.status, payload, res.headers.get("retry-after"))
            if res.status == 401 and not reauthed and self._cred.refused(key):
                reauthed = True
                continue
            if not resend and res.status >= 500:
                # The server may have done it before failing: a resend would replay it without the
                # secret, so the outcome is unknown.
                raise WorkflowsApiError(
                    res.status,
                    "outcome_unknown",
                    f"The API answered {res.status} ({api_err.code}), so the request may have taken effect; a "
                    "resend would return no secret. Check with get_app, and rotate the app's secret if you "
                    "didn't receive it.",
                    api_err.retry_after_seconds,
                )
            # 429 and 503 took no effect; a 409 idempotency_in_progress means the first request with
            # this Idempotency-Key is still running: all are retried with the same key.
            retry = res.status in (429, 503) or (res.status == 409 and api_err.code == "idempotency_in_progress")
            if not retry or attempt >= self._retries:
                raise api_err
            ra = api_err.retry_after_seconds
            _sleep(min(self._max_delay, ra if ra is not None else 1.0))
            attempt += 1

    # ---- apps ----

    def create_app(
        self,
        name: str,
        *,
        kind: Literal["push", "pull"] = "push",
        endpoint: str | None = None,
        queue: str | None = None,
        partitions: int | None = None,
        idempotency_key: str | None = None,
    ) -> AppWithSecret:
        """Creates an app. A push app's signing secret is returned once; a network error isn't
        resent (a replay would answer signingSecret None): it raises outcome_unknown."""
        body: dict[str, Any] = {"name": name, "kind": kind}
        for k, v in (("endpoint", endpoint), ("queue", queue), ("partitions", partitions)):
            if v is not None:
                body[k] = v
        out: AppWithSecret = _with_secret(
            self._call("POST", "/apps", body, idempotency_key, resend=False), "create", kind
        )
        return out

    def get_app(self, app_id: str) -> App:
        out: App = self._call("GET", f"/apps/{_q(app_id)}")
        return out

    def list_apps(self, *, cursor: str | None = None, limit: int | None = None) -> AppList:
        out: AppList = self._call("GET", "/apps" + _query(cursor=cursor, limit=_limit(limit)))
        return out

    def sync(self, app_id: str, *, endpoint: str | None = None, deployment: str | None = None) -> App:
        """Asks EZGH to call the app's endpoint and record its workflows; run it after each deploy.
        A failed sync is a 409 sync_failed."""
        body = {k: v for k, v in (("endpoint", endpoint), ("deployment", deployment)) if v is not None}
        out: App = self._call("POST", f"/apps/{_q(app_id)}/sync", body)
        return out

    def rotate_secret(self, app_id: str) -> AppWithSecret:
        """A new signing secret; the old ones stay valid for 24 hours. Not resent after a network error."""
        out: AppWithSecret = _with_secret(
            self._call("POST", f"/apps/{_q(app_id)}/rotate-secret", {}, resend=False), "rotation"
        )
        return out

    # ---- runs ----

    def start(
        self,
        app_id: str,
        workflow: str,
        input: Any = None,
        *,
        run_key: str | None = None,
        idempotency_key: str | None = None,
    ) -> Run:
        """Starts a run. With run_key, starting again with the same workflow and input returns the
        same run, and different input is a 409 run_key_conflict."""
        body: dict[str, Any] = {"appId": app_id, "workflow": workflow}
        if input is not None:
            body["input"] = encode_value(input)
        if run_key is not None:
            body["runKey"] = run_key
        out: Run = self._call("POST", "/runs", body, idempotency_key)
        return out

    def get(self, run_id: str) -> Run:
        out: Run = self._call("GET", f"/runs/{_q(run_id)}")
        return out

    def cancel(self, run_id: str, *, grace_period_seconds: int | None = None) -> Run:
        """The run is cancelling; its code gets grace_period_seconds (0-3600, default 300) to clean up."""
        body = {} if grace_period_seconds is None else {"gracePeriodSeconds": grace_period_seconds}
        out: Run = self._call("POST", f"/runs/{_q(run_id)}/cancel", body)
        return out

    def retry(self, run_id: str) -> Run:
        """Retries a failed run from its failed step; completed steps don't run again."""
        out: Run = self._call("POST", f"/runs/{_q(run_id)}/retry", {})
        return out

    def output(self, run_id: str) -> Any:
        """The run's output; 409 run_not_completed until it completes."""
        return self._call("GET", f"/runs/{_q(run_id)}/payloads/output")

    def input(self, run_id: str) -> Any:
        return self._call("GET", f"/runs/{_q(run_id)}/payloads/input")

    def step_output(self, run_id: str, step_id: str) -> Any:
        return self._call("GET", f"/runs/{_q(run_id)}/steps/{_q(step_id)}/output")

    def events(self, run_id: str, *, cursor: str | None = None, limit: int | None = None) -> RunEvents:
        out: RunEvents = self._call("GET", f"/runs/{_q(run_id)}/events" + _query(cursor=cursor, limit=_limit(limit)))
        return out

    def complete_callback(self, run_id: str, step_id: str, value: Any) -> Run:
        """Completes a run's waiting callback with value (any JSON, at most 1 MiB): the body is the
        value itself. 409 callback_completed for a different value, 410 callback_expired."""
        out: Run = self._call("POST", f"/runs/{_q(run_id)}/callbacks/{_q(step_id)}", encode_value(value))
        return out

    def complete_callback_url(self, url: str, value: Any) -> None:
        """complete_callback_url(url, value): by the callback's URL, with no API key."""
        complete_callback_url(url, value)

    # ---- schedules ----

    def create_schedule(
        self,
        *,
        app_id: str,
        workflow: str,
        cron: str,
        time_zone: str = "UTC",
        name: str | None = None,
        input: Any = None,
        overlap: Overlap = "skip",
        jitter_seconds: int = 0,
        catchup_window_seconds: int = 3600,
        paused: bool = False,
        idempotency_key: str | None = None,
    ) -> Schedule:
        """A schedule starting runs of workflow on a cron timetable (5 fields or @hourly, @daily,
        @weekly, @monthly, @yearly) in an IANA time zone. name defaults to the workflow's."""
        body: dict[str, Any] = {
            "appId": app_id,
            "workflow": workflow,
            "name": name if name is not None else workflow,
            "cron": cron,
            "timeZone": time_zone,
            "overlap": overlap,
            "jitterSeconds": jitter_seconds,
            "catchupWindowSeconds": catchup_window_seconds,
            "paused": paused,
        }
        if input is not None:
            body["input"] = encode_value(input)
        out: Schedule = self._call("POST", "/schedules", body, idempotency_key)
        return out

    def get_schedule(self, schedule_id: str) -> Schedule:
        out: Schedule = self._call("GET", f"/schedules/{_q(schedule_id)}")
        return out

    def list_schedules(self, *, cursor: str | None = None, limit: int | None = None) -> ScheduleList:
        out: ScheduleList = self._call("GET", "/schedules" + _query(cursor=cursor, limit=_limit(limit)))
        return out

    def update_schedule(self, schedule_id: str, *, revision: int | None = None, **fields: Any) -> Schedule:
        """Changes workflow, name, cron, time_zone, input, overlap, jitter_seconds or
        catchup_window_seconds. With revision, the update applies only at that revision (If-Match;
        412 precondition_failed otherwise)."""
        body: dict[str, Any] = {}
        for k, v in fields.items():
            if k not in _SCHEDULE_FIELDS:
                raise ValueError(f"update_schedule can't change {k!r}; it takes {', '.join(_SCHEDULE_FIELDS)}")
            body[_SCHEDULE_FIELDS[k]] = encode_value(v) if k == "input" else v
        headers = {"If-Match": f'"{int(revision)}"'} if revision is not None else None
        out: Schedule = self._call("PATCH", f"/schedules/{_q(schedule_id)}", body, headers=headers)
        return out

    def pause_schedule(self, schedule_id: str) -> Schedule:
        out: Schedule = self._call("POST", f"/schedules/{_q(schedule_id)}/pause", {})
        return out

    def resume_schedule(self, schedule_id: str) -> Schedule:
        """Re-arms the schedule from now, with no catch-up for the paused period."""
        out: Schedule = self._call("POST", f"/schedules/{_q(schedule_id)}/resume", {})
        return out

    def delete_schedule(self, schedule_id: str) -> None:
        self._call("DELETE", f"/schedules/{_q(schedule_id)}")

    def schedule_input(self, schedule_id: str) -> Any:
        """The schedule's input (null when it has none)."""
        return self._call("GET", f"/schedules/{_q(schedule_id)}/payloads/input")

    def trigger_schedule(self, schedule_id: str, *, idempotency_key: str | None = None) -> Run:
        """Starts a run now. The run is derived from the Idempotency-Key, so the same key gives the
        same run (201 new, 200 existing)."""
        out: Run = self._call("POST", f"/schedules/{_q(schedule_id)}/trigger", {}, idempotency_key)
        return out


class AsyncClient:
    """Client's calls as coroutines (each runs the blocking call in a thread)."""

    __slots__ = ("_c",)

    def __init__(
        self,
        *,
        api_key: str | None = None,
        organization_id: str,
        project_id: str,
        base_url: str = DEFAULT_BASE_URL,
        retries: int = 3,
        max_retry_delay: float = 10.0,
    ) -> None:
        self._c = Client(
            api_key=api_key,
            organization_id=organization_id,
            project_id=project_id,
            base_url=base_url,
            retries=retries,
            max_retry_delay=max_retry_delay,
        )

    def __repr__(self) -> str:
        return repr(self._c).replace("Client", "AsyncClient")

    async def create_app(
        self,
        name: str,
        *,
        kind: Literal["push", "pull"] = "push",
        endpoint: str | None = None,
        queue: str | None = None,
        partitions: int | None = None,
        idempotency_key: str | None = None,
    ) -> AppWithSecret:
        return await asyncio.to_thread(
            self._c.create_app,
            name,
            kind=kind,
            endpoint=endpoint,
            queue=queue,
            partitions=partitions,
            idempotency_key=idempotency_key,
        )

    async def get_app(self, app_id: str) -> App:
        return await asyncio.to_thread(self._c.get_app, app_id)

    async def list_apps(self, *, cursor: str | None = None, limit: int | None = None) -> AppList:
        return await asyncio.to_thread(self._c.list_apps, cursor=cursor, limit=limit)

    async def sync(self, app_id: str, *, endpoint: str | None = None, deployment: str | None = None) -> App:
        return await asyncio.to_thread(self._c.sync, app_id, endpoint=endpoint, deployment=deployment)

    async def rotate_secret(self, app_id: str) -> AppWithSecret:
        return await asyncio.to_thread(self._c.rotate_secret, app_id)

    async def start(
        self,
        app_id: str,
        workflow: str,
        input: Any = None,
        *,
        run_key: str | None = None,
        idempotency_key: str | None = None,
    ) -> Run:
        return await asyncio.to_thread(
            self._c.start, app_id, workflow, input, run_key=run_key, idempotency_key=idempotency_key
        )

    async def get(self, run_id: str) -> Run:
        return await asyncio.to_thread(self._c.get, run_id)

    async def cancel(self, run_id: str, *, grace_period_seconds: int | None = None) -> Run:
        return await asyncio.to_thread(self._c.cancel, run_id, grace_period_seconds=grace_period_seconds)

    async def retry(self, run_id: str) -> Run:
        return await asyncio.to_thread(self._c.retry, run_id)

    async def output(self, run_id: str) -> Any:
        return await asyncio.to_thread(self._c.output, run_id)

    async def input(self, run_id: str) -> Any:
        return await asyncio.to_thread(self._c.input, run_id)

    async def step_output(self, run_id: str, step_id: str) -> Any:
        return await asyncio.to_thread(self._c.step_output, run_id, step_id)

    async def events(self, run_id: str, *, cursor: str | None = None, limit: int | None = None) -> RunEvents:
        return await asyncio.to_thread(self._c.events, run_id, cursor=cursor, limit=limit)

    async def complete_callback(self, run_id: str, step_id: str, value: Any) -> Run:
        return await asyncio.to_thread(self._c.complete_callback, run_id, step_id, value)

    async def complete_callback_url(self, url: str, value: Any) -> None:
        await asyncio.to_thread(complete_callback_url, url, value)

    async def create_schedule(
        self,
        *,
        app_id: str,
        workflow: str,
        cron: str,
        time_zone: str = "UTC",
        name: str | None = None,
        input: Any = None,
        overlap: Overlap = "skip",
        jitter_seconds: int = 0,
        catchup_window_seconds: int = 3600,
        paused: bool = False,
        idempotency_key: str | None = None,
    ) -> Schedule:
        return await asyncio.to_thread(
            self._c.create_schedule,
            app_id=app_id,
            workflow=workflow,
            cron=cron,
            time_zone=time_zone,
            name=name,
            input=input,
            overlap=overlap,
            jitter_seconds=jitter_seconds,
            catchup_window_seconds=catchup_window_seconds,
            paused=paused,
            idempotency_key=idempotency_key,
        )

    async def get_schedule(self, schedule_id: str) -> Schedule:
        return await asyncio.to_thread(self._c.get_schedule, schedule_id)

    async def list_schedules(self, *, cursor: str | None = None, limit: int | None = None) -> ScheduleList:
        return await asyncio.to_thread(self._c.list_schedules, cursor=cursor, limit=limit)

    async def update_schedule(self, schedule_id: str, *, revision: int | None = None, **fields: Any) -> Schedule:
        return await asyncio.to_thread(self._c.update_schedule, schedule_id, revision=revision, **fields)

    async def pause_schedule(self, schedule_id: str) -> Schedule:
        return await asyncio.to_thread(self._c.pause_schedule, schedule_id)

    async def resume_schedule(self, schedule_id: str) -> Schedule:
        return await asyncio.to_thread(self._c.resume_schedule, schedule_id)

    async def delete_schedule(self, schedule_id: str) -> None:
        await asyncio.to_thread(self._c.delete_schedule, schedule_id)

    async def schedule_input(self, schedule_id: str) -> Any:
        return await asyncio.to_thread(self._c.schedule_input, schedule_id)

    async def trigger_schedule(self, schedule_id: str, *, idempotency_key: str | None = None) -> Run:
        return await asyncio.to_thread(self._c.trigger_schedule, schedule_id, idempotency_key=idempotency_key)
