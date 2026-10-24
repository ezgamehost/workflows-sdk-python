"""Instance credentials: on an EZGH compute instance with a Bot attached, a client given no API
key uses the Bot's short-lived key from the instance's metadata service."""

import json
import threading
import time
from collections.abc import Callable
from datetime import datetime

from ._http import HttpError, Response, request

METADATA_URL = "http://169.254.169.254"
PRODUCT_NAME_PATH = "/sys/class/dmi/id/product_name"  # tests point these two elsewhere
_PRODUCT = "EZGH Compute"
_REFRESH_AHEAD = 5 * 60.0
_TOKEN_TTL = 6 * 3600
_RETRY_AFTER = 5.0


class InstanceCredentialsError(Exception):
    """The instance's credentials couldn't be fetched."""


class NoBotError(InstanceCredentialsError):
    """This instance has no Bot attached."""

    def __init__(self) -> None:
        super().__init__("This instance has no Bot attached")


def on_instance() -> bool:
    """This machine is an EZGH compute instance (SMBIOS product name). No network."""
    try:
        with open(PRODUCT_NAME_PATH, encoding="utf-8") as f:
            return f.read().strip() == _PRODUCT
    except (OSError, UnicodeDecodeError):
        return False


class KeySource:
    def key(self) -> str:
        raise NotImplementedError

    def refused(self, key: str) -> bool:
        """The API answered 401 to key: True if one retry with a new key is worthwhile."""
        return False


class StaticKey(KeySource):
    __slots__ = ("_k",)

    def __init__(self, key: str) -> None:
        self._k = key

    def key(self) -> str:
        return self._k

    def __repr__(self) -> str:
        return "<redacted>"


class InstanceKey(KeySource):
    """The key of the instance's Bot: fetched when needed, refreshed 5 minutes before it expires,
    and kept until it really expires when the metadata service is down."""

    def __init__(self, now: Callable[[], float] = time.time) -> None:
        self._now = now
        self._lock = threading.Lock()
        self._token, self._token_exp = "", 0.0
        self._key, self._expires, self._next_try = "", 0.0, 0.0
        self._retry_after = 0.0  # the metadata service's Retry-After (503, 429) on the last failure

    def __repr__(self) -> str:
        return "<ezgh_workflows instance credentials>"

    def key(self) -> str:
        with self._lock:
            now = self._now()
            if self._key and now + _REFRESH_AHEAD < self._expires:
                return self._key
            err: Exception | None = None
            if not self._key or now >= self._next_try:
                try:
                    return self._fetch()
                except InstanceCredentialsError as e:
                    err = e
                    self._next_try = now + max(_RETRY_AFTER, self._retry_after)
                    if isinstance(e, NoBotError):
                        self._key = ""
            # A failed fetch keeps the old key until it actually expires.
            if self._key and now < self._expires:
                return self._key
            raise err or InstanceCredentialsError(
                "Instance credentials expired and the metadata service is not answering"
            )

    def refused(self, key: str) -> bool:
        with self._lock:
            if self._key == key:
                self._key, self._expires, self._next_try = "", 0.0, 0.0
        return True

    def _fetch(self) -> str:
        for attempt in range(2):
            if not self._token or self._now() >= self._token_exp:
                self._new_token()
            res = self._call("GET", "/ezgh/v1/credentials", {"EZGH-Metadata-Token": self._token}, 10.0)
            if res.status == 401 and attempt == 0:
                self._token = ""  # the token is gone (migration, metadata restart): get one new one
                continue
            if res.status == 404 and _error_code(res.body) == "no_bot":
                raise NoBotError  # any other 404 (not_found: mid-migration) is transient, like a 503
            if res.status != 200:
                try:
                    self._retry_after = max(0.0, float(res.headers.get("retry-after", "0")))
                except ValueError:
                    self._retry_after = 0.0
                raise InstanceCredentialsError(f"Instance metadata answered {res.status}")
            try:
                c = json.loads(res.body)
                key, expires = c["apiKey"], datetime.fromisoformat(c["expiresAt"]).timestamp()
            except (ValueError, KeyError, TypeError):
                raise InstanceCredentialsError("Instance metadata sent an unreadable answer") from None
            if not isinstance(key, str) or not key:
                raise InstanceCredentialsError("Instance metadata sent an unreadable answer")
            self._key, self._expires = key, expires
            return key
        raise InstanceCredentialsError("Instance metadata refused its own token")

    def _new_token(self) -> None:
        res = self._call("PUT", "/ezgh/v1/token", {"EZGH-Metadata-Token-TTL-Seconds": str(_TOKEN_TTL)}, 1.0)
        token = res.body.decode("utf-8", "replace").strip() if res.status == 200 else ""
        if not token:
            raise InstanceCredentialsError(f"Instance metadata answered {res.status} to the token request")
        self._token, self._token_exp = token, self._now() + _TOKEN_TTL - _REFRESH_AHEAD

    def _call(self, method: str, path: str, headers: dict[str, str], timeout: float) -> Response:
        try:
            return request(method, METADATA_URL + path, {"EZGH-Metadata": "true", **headers}, None, timeout)
        except HttpError as e:
            raise InstanceCredentialsError(f"Instance metadata unreachable ({e.reason})") from None


def _error_code(body: bytes) -> str:
    try:
        return str(json.loads(body)["error"]["code"])
    except (ValueError, KeyError, TypeError):
        return ""


def key_source(api_key: object) -> KeySource:
    """The API key if given, else the instance's credentials on an instance, else ValueError."""
    if isinstance(api_key, str) and api_key:
        return StaticKey(api_key)
    if (api_key is None or api_key == "") and on_instance():
        return InstanceKey()
    raise ValueError("api_key is required")
