"""Values on the wire are JSON (design, "Return values are JSON")."""

import json
import math
import re
from collections.abc import Mapping
from datetime import UTC, date, datetime
from typing import Any

from ._types import RetryPolicy

MAX_OUTPUT_BYTES = 1_048_576
DEFAULT_RETRY: RetryPolicy = {"maxAttempts": 5, "initialMs": 1000, "maxMs": 600_000, "multiplier": 2}


def iso_ms(dt: datetime) -> str:
    """An aware datetime as JavaScript's toISOString: UTC, exactly 3 fractional digits, Z."""
    u = dt.astimezone(UTC)
    return u.strftime("%Y-%m-%dT%H:%M:%S.") + f"{u.microsecond // 1000:03d}Z"


def iso_from_ms(ms: int) -> str:
    """Epoch milliseconds as toISOString (exact: no float rounding)."""
    secs, rem = divmod(ms, 1000)
    return datetime.fromtimestamp(secs, UTC).strftime("%Y-%m-%dT%H:%M:%S.") + f"{rem:03d}Z"


def encode_value(v: object) -> Any:
    """v as the wire carries it: dict (str keys), list, tuple, str, int, finite float, bool, None,
    an aware datetime (toISOString) and a date (YYYY-MM-DD). Anything else raises TypeError."""
    if v is None or isinstance(v, bool | str | int):
        return v
    if isinstance(v, float):
        if not math.isfinite(v):
            raise TypeError("NaN and infinities aren't JSON; return a finite number or a string")
        return v
    if isinstance(v, datetime):
        if v.tzinfo is None or v.utcoffset() is None:
            raise TypeError("A naive datetime is ambiguous; use an aware datetime (for example tzinfo=UTC)")
        return iso_ms(v)
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, Mapping):
        out: dict[str, Any] = {}
        for k, item in v.items():
            if not isinstance(k, str):
                raise TypeError(f"JSON object keys are strings; got a {type(k).__name__} key")
            out[k] = encode_value(item)
        return out
    if isinstance(v, list | tuple):
        return [encode_value(x) for x in v]
    raise TypeError(f"A {type(v).__name__} can't be a step result or workflow output; return JSON values")


_SURROGATE = re.compile("[\ud800-\udfff]")


def dumps(v: object) -> str:
    """Compact JSON, like JSON.stringify: non-ASCII kept, lone surrogates escaped (so the text is
    always valid UTF-8). Raises ValueError for NaN or infinities."""
    s = json.dumps(v, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    return _SURROGATE.sub(lambda m: f"\\u{ord(m.group()):04x}", s)


def dumps_bytes(v: object) -> bytes:
    return dumps(v).encode("utf-8")


def json_byte_length(v: object) -> int:
    return len(dumps_bytes(v))


_DURATION = re.compile(r"([0-9]+)(ms|s|m|h|d)")
_UNITS = {"ms": 1, "s": 1000, "m": 60_000, "h": 3_600_000, "d": 86_400_000}


def parse_duration(d: str | int | float) -> int:
    """Milliseconds, or a string such as 500ms, 30s, 5m, 2h or 3d."""
    if isinstance(d, bool):
        raise ValueError("A duration is a number of milliseconds or a string such as 30s")
    if isinstance(d, int | float):
        if not math.isfinite(d) or d < 0:
            raise ValueError("A duration is a non-negative number of milliseconds")
        return int(d)
    m = _DURATION.fullmatch(d.strip()) if isinstance(d, str) else None
    if not m:
        raise ValueError(f"{d!r} isn't a duration: use 500ms, 30s, 5m, 2h or 3d")
    return int(m.group(1)) * _UNITS[m.group(2)]


def backoff(p: RetryPolicy, attempt: int) -> int:
    """The delay before attempt+1: min(maxMs, floor(initialMs * multiplier^(attempt-1)))."""
    try:
        ms = math.floor(p["initialMs"] * p["multiplier"] ** (max(1, attempt) - 1))
    except OverflowError:
        return p["maxMs"]
    return p["maxMs"] if ms > p["maxMs"] else max(0, ms)
