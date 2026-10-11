"""The package's only network code: blocking HTTP with urllib (no proxies, no redirects)."""

import contextlib
import http.client
import socket
import ssl
import threading
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field

_CTX = ssl.create_default_context()
MAX_RESPONSE = 32 * 1024 * 1024


@dataclass
class Response:
    status: int
    headers: dict[str, str]
    body: bytes = field(repr=False)


class HttpError(Exception):
    """The request got no HTTP answer (DNS, connect, TLS, timeout, reset)."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args: object, **kwargs: object) -> None:
        return None


_OPENER = urllib.request.build_opener(
    urllib.request.ProxyHandler({}), _NoRedirect, urllib.request.HTTPSHandler(context=_CTX)
)


def _headers(h: object) -> dict[str, str]:
    items = getattr(h, "items", None)
    return {str(k).lower(): str(v) for k, v in items()} if callable(items) else {}


def request(method: str, url: str, headers: dict[str, str], body: bytes | None, timeout: float) -> Response:
    """One blocking request. Any HTTP status is a Response; no answer at all raises HttpError."""
    req = urllib.request.Request(
        url, data=body, method=method, headers={"User-Agent": "ezgh-workflows-python", **headers}
    )
    try:
        with _OPENER.open(req, timeout=max(0.001, timeout)) as r:
            data = r.read(MAX_RESPONSE + 1)
            return Response(r.status, _headers(r.headers), data[:MAX_RESPONSE])
    except urllib.error.HTTPError as e:
        try:
            data = e.read(MAX_RESPONSE + 1) if e.fp else b""
        except (OSError, http.client.HTTPException):
            data = b""
        return Response(e.code, _headers(e.headers), data[:MAX_RESPONSE])
    except (urllib.error.URLError, TimeoutError, http.client.HTTPException, OSError, ValueError) as e:
        raise HttpError(type(e).__name__) from None


class AbortableRequest:
    """One blocking request (http.client: no proxies, no redirects) that another thread can abort
    by shutting its socket down: a held long poll ends at once when the worker stops."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._conn: http.client.HTTPConnection | None = None
        self._aborted = False

    def request(self, method: str, url: str, headers: dict[str, str], body: bytes | None, timeout: float) -> Response:
        u = urllib.parse.urlsplit(url)
        if u.scheme not in ("http", "https") or not u.hostname:
            raise HttpError("InvalidURL")
        conn: http.client.HTTPConnection
        if u.scheme == "https":
            conn = http.client.HTTPSConnection(u.hostname, u.port, timeout=max(0.001, timeout), context=_CTX)
        else:
            conn = http.client.HTTPConnection(u.hostname, u.port, timeout=max(0.001, timeout))
        with self._lock:
            if self._aborted:
                raise HttpError("Aborted")
            self._conn = conn
        try:
            path = (u.path or "/") + (f"?{u.query}" if u.query else "")
            conn.request(method, path, body=body, headers={"User-Agent": "ezgh-workflows-python", **headers})
            r = conn.getresponse()
            data = r.read(MAX_RESPONSE + 1)
            return Response(r.status, _headers(r.headers), data[:MAX_RESPONSE])
        except (OSError, http.client.HTTPException, ValueError) as e:
            raise HttpError("Aborted" if self._aborted else type(e).__name__) from None
        finally:
            conn.close()

    def abort(self) -> None:
        with self._lock:
            self._aborted = True
            conn = self._conn
        sock = conn.sock if conn is not None else None
        if sock is not None:
            with contextlib.suppress(OSError):
                sock.shutdown(socket.SHUT_RDWR)
