"""Container credentials: in an EZGH Kubernetes pod whose service account has a pod identity
association, a client given no API key uses the Bot's short-lived key from the node's pod identity
agent, presenting the pod's projected service account token."""

import http.client
import ipaddress
import json
import os
import threading
import time
import urllib.parse
from collections.abc import Callable
from datetime import datetime

from ._http import Response
from ._instance import _REFRESH_AHEAD, _RETRY_AFTER, InstanceCredentialsError, KeySource, _error_code

# The variables EZGH Kubernetes injects into such a pod.
URI_ENV = "EZGH_CONTAINER_CREDENTIALS_FULL_URI"
TOKEN_FILE_ENV = "EZGH_CONTAINER_AUTHORIZATION_TOKEN_FILE"
_AGENT_IPS = (ipaddress.ip_address("169.254.170.23"), ipaddress.ip_address("fd65:7a67:6800::23"))


class ContainerCredentialsError(InstanceCredentialsError):
    """The pod's credentials couldn't be fetched from the pod identity agent."""


def check_container_uri(raw: str) -> None:
    """Allow only the pod identity agent's addresses, or loopback, over plain http, so the service
    account token never leaves the node."""
    try:
        u = urllib.parse.urlsplit(raw)
        ip = ipaddress.ip_address(u.hostname or "")
        _ = u.port  # a bad port raises ValueError here, not mid-request
        if u.scheme == "http" and u.username is None and (ip.is_loopback or ip in _AGENT_IPS):
            return
    except ValueError:
        pass
    raise ContainerCredentialsError(
        f"{URI_ENV} must be http://169.254.170.23, http://[fd65:7a67:6800::23] or a loopback address"
    )


def _get(uri: str, token: str) -> Response:
    """GET straight to the agent (http.client: no proxy, no redirects), 1 s to connect, 10 s per read."""
    u = urllib.parse.urlsplit(uri)
    conn = http.client.HTTPConnection(u.hostname or "", u.port or 80, timeout=1.0)
    try:
        conn.connect()
        if conn.sock is not None:
            conn.sock.settimeout(10.0)
        path = (u.path or "/") + (f"?{u.query}" if u.query else "")
        conn.request("GET", path, headers={"Authorization": f"Bearer {token}"})
        r = conn.getresponse()
        return Response(r.status, {k.lower(): v for k, v in r.getheaders()}, r.read(1 << 16))
    except (OSError, http.client.HTTPException) as e:
        raise ContainerCredentialsError(f"Pod identity agent unreachable ({type(e).__name__})") from None
    finally:
        conn.close()


class ContainerKey(KeySource):
    """The key of the Bot the pod's service account is associated with: fetched when needed,
    refreshed 5 minutes before it expires, and kept until it really expires when the agent is down."""

    def __init__(self, uri: str, token_file: str, now: Callable[[], float] = time.time) -> None:
        self._uri, self._token_file, self._now = uri, token_file, now
        self._lock = threading.Lock()
        self._key, self._expires, self._next_try = "", 0.0, 0.0
        self._last_err: Exception | None = None
        self._retry_after = 0.0  # the agent's Retry-After on the last answer

    def __repr__(self) -> str:
        return "<ezgh_workflows container credentials>"

    def key(self) -> str:
        with self._lock:
            now = self._now()
            if self._key and now + _REFRESH_AHEAD < self._expires:
                return self._key
            err: Exception | None = None
            if now >= self._next_try:
                try:
                    return self._fetch()
                except ContainerCredentialsError as e:
                    err = self._last_err = e
                    self._next_try = now + max(_RETRY_AFTER, self._retry_after)
            # A failed fetch keeps the old key until it actually expires.
            if self._key and now < self._expires:
                return self._key
            # Backing off: report the failure that started it, without asking again.
            raise (
                err
                or self._last_err
                or ContainerCredentialsError("Container credentials expired and the pod identity agent isn't answering")
            )

    def refused(self, key: str) -> bool:
        with self._lock:
            if self._key == key:
                self._key, self._expires, self._next_try = "", 0.0, 0.0
        return True

    def _fetch(self) -> str:
        check_container_uri(self._uri)
        if not self._token_file:
            raise ContainerCredentialsError(f"{URI_ENV} is set but {TOKEN_FILE_ENV} is not")
        # Read afresh every time: kubelet rotates the projected token.
        try:
            with open(self._token_file, encoding="utf-8") as f:
                token = f.read().strip()
        except (OSError, UnicodeDecodeError) as e:
            raise ContainerCredentialsError(f"Reading the service account token ({TOKEN_FILE_ENV}): {e}") from None
        if not token:
            raise ContainerCredentialsError(f"The pod's service account token ({TOKEN_FILE_ENV}) is empty")
        res = _get(self._uri, token)
        try:
            self._retry_after = max(0.0, float(res.headers.get("retry-after", "0")))
        except ValueError:
            self._retry_after = 0.0
        if res.status != 200:
            code = _error_code(res.body)
            raise ContainerCredentialsError(f"Pod identity agent answered {res.status}{f' ({code})' if code else ''}")
        try:
            c = json.loads(res.body)
            key, expires = c["apiKey"], datetime.fromisoformat(c["expiresAt"]).timestamp()
        except (ValueError, KeyError, TypeError):
            raise ContainerCredentialsError("Pod identity agent sent an unreadable answer") from None
        if not isinstance(key, str) or not key:
            raise ContainerCredentialsError("Pod identity agent sent an unreadable answer")
        self._key, self._expires = key, expires
        return key


def container_env() -> tuple[str, str] | None:
    """The URI and token file when EZGH_CONTAINER_CREDENTIALS_FULL_URI is set. No network."""
    uri = os.environ.get(URI_ENV, "")
    return (uri, os.environ.get(TOKEN_FILE_ENV, "")) if uri else None
