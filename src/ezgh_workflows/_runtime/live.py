"""workflows-live during an invocation: checkpoints, journal pages and payloads (design, D2, D25)."""

import asyncio
import json
import time
import urllib.parse
from collections.abc import Callable
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import Any, Literal, Protocol

from .._http import HttpError, Response, request
from ..protocol._codec import dumps_bytes
from ..protocol._types import Checkpoint, Journal


class LiveIO(Protocol):
    async def checkpoint(self, cp: Checkpoint) -> None: ...
    async def journal_page(self, after_seq: int) -> Journal: ...
    async def payload(self, pid: str) -> Any: ...


class InvocationSuperseded(Exception):
    """The engine has moved on: another invocation owns the run. Stop at once."""


class CheckpointFailed(Exception):
    """workflows-live refused or couldn't take a call after retries: the step must not run. code is
    what the SDK tells dispatch: payload_unavailable when the live host's last answer was 503
    payload_unavailable, else checkpoint_failed."""

    def __init__(
        self, message: str, code: Literal["checkpoint_failed", "payload_unavailable"] = "checkpoint_failed"
    ) -> None:
        super().__init__(message)
        self.code: Literal["checkpoint_failed", "payload_unavailable"] = code


MAX_BACKOFF = 5.0


def retry_after_seconds(res: Response) -> float | None:
    v = res.headers.get("retry-after")
    if not v:
        return None
    try:
        n = float(v)
        return n if n >= 0 and n == n else None
    except ValueError:
        try:
            return max(0.0, parsedate_to_datetime(v).timestamp() - time.time())
        except (TypeError, ValueError, IndexError):
            return None


def error_code(res: Response) -> str | None:
    """The kit's {"error": {"code"}} from a small error body."""
    if len(res.body) > 64 * 1024:
        return None
    try:
        err = json.loads(res.body).get("error")
        code = err.get("code") if isinstance(err, dict) else None
        return code if isinstance(code, str) else None
    except (ValueError, AttributeError, UnicodeDecodeError):
        return None


@dataclass
class HttpLive:
    """workflows-live with Authorization: Bearer <invocation token>. 409 means superseded; 429,
    5xx and network errors are retried after max(Retry-After, backoff) while more than reserve
    seconds would remain before the deadline; any other error answer fails the invocation."""

    base: str
    token: str
    deadline: float | None = None  # epoch seconds on this machine's clock
    reserve: float = 0.0
    now: Callable[[], float] = time.time
    attempts: int = 4  # without a deadline
    first_delay: float = 0.2

    async def _call(self, method: str, path: str, body: bytes | None = None) -> Response:
        headers = {"Authorization": f"Bearer {self.token}"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        what, last, payload_down = path.split("?")[0], "", False
        url = self.base.rstrip("/") + path

        def failed(msg: str) -> CheckpointFailed:
            return CheckpointFailed(msg, "payload_unavailable" if payload_down else "checkpoint_failed")

        attempt = 0
        while True:
            timeout = 30.0 if self.deadline is None else self.deadline - self.now()
            if timeout <= 0:
                raise failed(f"No time left for {what}: {last or 'the deadline passed'}")
            wait: float | None = None
            res: Response | None
            try:
                res = await asyncio.to_thread(request, method, url, headers, body, timeout)
            except HttpError as e:
                last, payload_down, res = e.reason, False, None
            if res is not None:
                if 200 <= res.status < 300:
                    return res
                if res.status == 409:
                    raise InvocationSuperseded("The engine superseded this invocation")
                if res.status != 429 and res.status < 500:
                    raise CheckpointFailed(f"workflows-live answered {res.status} for {what}")
                code = error_code(res) if res.status == 503 else None
                last = f"HTTP {res.status} {code}" if code else f"HTTP {res.status}"
                payload_down = code == "payload_unavailable"
                wait = retry_after_seconds(res)
            # Retry-After never shortens the backoff (a 0 or a past date mustn't spin).
            delay = max(wait or 0.0, min(MAX_BACKOFF, self.first_delay * 2**attempt))
            if self.deadline is None:
                more = attempt + 1 < self.attempts
            else:
                more = self.deadline - self.now() - delay > self.reserve
            if not more:
                raise failed(f"workflows-live didn't take {what}: {last}")
            await asyncio.sleep(delay)
            attempt += 1

    async def checkpoint(self, cp: Checkpoint) -> None:
        await self._call("POST", "/v1/invocations/checkpoint", dumps_bytes(cp))

    async def journal_page(self, after_seq: int) -> Journal:
        res = await self._call("GET", f"/v1/invocations/journal?afterSeq={after_seq}")
        try:
            page = json.loads(res.body)
        except (ValueError, UnicodeDecodeError) as e:
            raise CheckpointFailed("workflows-live sent a journal page that isn't JSON") from e
        if not (isinstance(page, dict) and isinstance(page.get("entries"), list)):
            raise CheckpointFailed("workflows-live sent a malformed journal page")
        return page  # type: ignore[return-value]

    async def payload(self, pid: str) -> Any:
        res = await self._call("GET", "/v1/invocations/payloads/" + urllib.parse.quote(pid, safe=""))
        try:
            return json.loads(res.body)
        except (ValueError, UnicodeDecodeError) as e:
            raise CheckpointFailed("workflows-live sent a payload that isn't JSON") from e
