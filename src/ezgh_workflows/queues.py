"""EZGH Queues: a client for the public send API (queues-api), which step.send_to_queue uses.

Messages and results are TypedDicts with the API's camelCase keys.
"""

import hashlib
import json
import math
import time
import urllib.parse
import uuid
from collections.abc import Mapping, Sequence
from typing import Any, NotRequired, TypedDict

from ._http import HttpError, request
from ._instance import key_source
from .protocol._codec import dumps_bytes

_sleep = time.sleep
DEFAULT_QUEUES_BASE_URL = "https://queues-api.ezghcloud.com"


class QueueMessage(TypedDict):
    """A message to send. body is UTF-8 text; body and attributes together are at most 256 KiB.
    groupId is required on FIFO queues and refused on standard ones; dedupId is FIFO only.
    delaySeconds (0-900) is standard queues only."""

    body: str
    attributes: NotRequired[dict[str, str]]
    delaySeconds: NotRequired[int]
    groupId: NotRequired[str]
    dedupId: NotRequired[str]


class SentMessage(TypedDict):
    """One message's result. seq: FIFO only. duplicate: True when a FIFO dedupId was seen in the
    last 5 minutes, so the message wasn't stored again (messageId is the original's)."""

    messageId: str
    seq: NotRequired[str]
    duplicate: NotRequired[bool]


class SentMessages(TypedDict):
    """One result per message, in order."""

    messages: list[SentMessage]


class ReceivedMessage(TypedDict):
    """A queue message a wait received (step.wait_for_message, protocol 1.2). groupId and seq: FIFO
    queues only."""

    messageId: str
    body: str
    attributes: dict[str, str]
    sentAt: str
    queueId: str
    groupId: NotRequired[str]
    seq: NotRequired[str]


class QueuesApiError(Exception):
    """An error answer from the Queues API: the HTTP status, the platform error code and
    Retry-After. Status 0 with code "network_error": every attempt failed in transit."""

    def __init__(self, status: int, code: str, message: str, retry_after_seconds: float | None = None) -> None:
        super().__init__(message)
        self.status, self.code, self.retry_after_seconds = status, code, retry_after_seconds

    @property
    def data(self) -> dict[str, Any]:
        """What a step failure records, so a replay's WorkflowError keeps the status and code."""
        return {"status": self.status, "code": self.code}

    def resendable(self) -> bool:
        """An answer a resend with the same Idempotency-Key may fix. A 429 quota_exceeded isn't:
        it isn't a failure to retry on a timer."""
        if self.status == 429:
            return self.code != "quota_exceeded"
        if self.status == 409:
            return self.code == "idempotency_in_progress"
        return self.status >= 500

    def refused(self) -> bool:
        """A 3xx or 4xx that retrying the step won't fix (all but 408, 429 and 409
        idempotency_in_progress)."""
        return (
            300 <= self.status < 500
            and self.status not in (408, 429)
            and not (self.status == 409 and self.code == "idempotency_in_progress")
        )

    def __repr__(self) -> str:
        return f"QueuesApiError({self.status}, {self.code!r}, {str(self)!r})"


def _retry_after(v: str | None) -> float | None:
    if not v:
        return None
    try:
        n = float(v)
    except ValueError:
        return None
    return n if math.isfinite(n) and n >= 0 else None


def _api_error(status: int, payload: Any, retry_after: str | None) -> QueuesApiError:
    if 300 <= status < 400:
        return QueuesApiError(
            status, "redirect", "The Queues API answered a redirect; requests are never redirected, so check base_url"
        )
    err = payload.get("error") if isinstance(payload, dict) else None
    err = err if isinstance(err, dict) else {}
    return QueuesApiError(
        status, str(err.get("code", "unknown")), str(err.get("message", f"HTTP {status}")), _retry_after(retry_after)
    )


def _valid_key(k: object) -> bool:
    return isinstance(k, str) and 1 <= len(k) <= 200 and all(0x21 <= ord(ch) <= 0x7E for ch in k)


def check_messages(messages: object) -> None:
    """1-10 messages, each a mapping."""
    if (
        isinstance(messages, str | bytes | Mapping)
        or not isinstance(messages, Sequence)
        or not 1 <= len(messages) <= 10
        or not all(isinstance(m, Mapping) for m in messages)
    ):
        raise ValueError("Send 1 to 10 messages, each a dict with a body")


class QueuesClient:
    """Sends messages to EZGH Queues for one project (blocking). Pass it to step.send_to_queue, or
    call send directly outside a workflow (in async code, through asyncio.to_thread).

    A 429 (but not quota_exceeded), any 5xx, a 409 idempotency_in_progress and network errors are
    resent up to retries times, after Retry-After (at most max_retry_delay seconds), always with
    the same Idempotency-Key."""

    __slots__ = ("__weakref__", "_base", "_cred", "_max_delay", "_retries", "_timeout")

    def __init__(
        self,
        *,
        api_key: str | None = None,
        organization_id: str,
        project_id: str,
        base_url: str = DEFAULT_QUEUES_BASE_URL,
        retries: int = 3,
        max_retry_delay: float = 10.0,
        timeout: float = 30.0,
    ) -> None:
        self._cred = key_source(api_key)
        for what, v in (("organization_id", organization_id), ("project_id", project_id)):
            if not isinstance(v, str) or not v:
                raise ValueError(f"{what} is required")
        if not isinstance(retries, int) or isinstance(retries, bool) or retries < 0:
            raise ValueError("retries is a whole number >= 0")
        if not isinstance(max_retry_delay, int | float) or not math.isfinite(max_retry_delay) or max_retry_delay < 0:
            raise ValueError("max_retry_delay is a number of seconds >= 0")
        if not isinstance(timeout, int | float) or not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("timeout is a number of seconds > 0")
        org, prj = urllib.parse.quote(organization_id, safe=""), urllib.parse.quote(project_id, safe="")
        self._base = f"{base_url.rstrip('/')}/v1/organizations/{org}/projects/{prj}"
        self._retries, self._max_delay, self._timeout = retries, float(max_retry_delay), float(timeout)

    def __repr__(self) -> str:
        return f"<ezgh_workflows.QueuesClient {self._base}>"

    def send(
        self, queue_id: str, messages: Sequence[QueueMessage], *, idempotency_key: str | None = None
    ) -> SentMessages:
        """Sends 1-10 messages to a queue (queues.messages.send) in one request: all are stored, or
        none. Every attempt carries the same Idempotency-Key (yours, or a random one), which the API
        honors for 48 hours: a resend gets the first answer instead of sending again."""
        if not isinstance(queue_id, str) or not queue_id:
            raise ValueError("send needs a queue_id")
        check_messages(messages)
        key = str(uuid.uuid4()) if idempotency_key is None else idempotency_key
        if not _valid_key(key):
            raise ValueError("An idempotency_key is 1-200 printable ASCII characters")
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Idempotency-Key": key,
        }
        data = dumps_bytes({"messages": [dict(m) for m in messages]})
        url = f"{self._base}/queues/{urllib.parse.quote(queue_id, safe='')}/messages"
        attempt = 0
        reauthed = False  # an instance key the API refused is replaced once per call
        while True:
            key = self._cred.key()
            headers["Authorization"] = f"Bearer {key}"
            try:
                res = request("POST", url, headers, data, self._timeout)
            except HttpError as e:
                if attempt >= self._retries:
                    why = f"Sending to the queue failed in transit ({e.reason})"
                    raise QueuesApiError(0, "network_error", why) from None
                _sleep(min(self._max_delay, 0.5 * 2**attempt))
                attempt += 1
                continue
            try:
                payload = json.loads(res.body) if res.body else None
            except (ValueError, UnicodeDecodeError):
                payload = None
            if 200 <= res.status < 300:
                if not isinstance(payload, dict) or not isinstance(payload.get("messages"), list):
                    why = "The Queues API answered without a messages list"
                    raise QueuesApiError(res.status, "invalid_response", why)
                out: SentMessages = payload  # type: ignore[assignment]
                return out
            err = _api_error(res.status, payload, res.headers.get("retry-after"))
            if res.status == 401 and not reauthed and self._cred.refused(key):
                reauthed = True
                continue
            if not err.resendable() or attempt >= self._retries:
                raise err
            ra = err.retry_after_seconds
            _sleep(min(self._max_delay, ra if ra is not None else 0.5 * 2**attempt))
            attempt += 1


def send_to_queue_digest(run_id: str, step_id: str) -> str:
    """step.send_to_queue's key material: lowercase hex of SHA-256(run_id + ":" + step_id). The step
    sends the Idempotency-Key "wfq_" + digest, and gives each FIFO message (one with a groupId)
    without a dedupId the dedupId digest + ":" + its index."""
    return hashlib.sha256(f"{run_id}:{step_id}".encode()).hexdigest()


def with_dedup_ids(messages: Sequence[QueueMessage], digest: str) -> list[QueueMessage]:
    """A copy of messages in which each one with a groupId and no dedupId has the dedupId
    digest:index. Standard queues refuse dedupId, so messages without a groupId go as they are."""
    out: list[QueueMessage] = []
    for i, m in enumerate(messages):
        c: QueueMessage = dict(m)  # type: ignore[assignment]
        if "groupId" in c and "dedupId" not in c:
            c["dedupId"] = f"{digest}:{i}"
        out.append(c)
    return out
