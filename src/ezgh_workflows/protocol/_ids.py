"""Step IDs, run-ID shards, invocation tokens, callback tokens and derived run IDs."""

import hashlib
import os
import re
from datetime import UTC, datetime

from ._bytes import b64url, from_b64url_strict, utf8_bytes

# Patterns are used with fullmatch (Go's $ never matches before a trailing newline; Python's does).
_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
STEP_ID_OVERRIDE = re.compile(r"[A-Za-z0-9._:-]{1,128}")
WORKFLOW_NAME = re.compile(r"[A-Za-z0-9_-]{1,64}")
RUN_KEY = STEP_ID_OVERRIDE
QUEUE_NAME = re.compile(r"[a-z0-9][a-z0-9-]{0,62}")
WORKER_ID = re.compile(r"[A-Za-z0-9._:-]{1,128}")
_INV_ID = re.compile(r"inv_[a-z2-7]{26}")
_SHARD_HEX = re.compile(r"[0-9a-f]{3}")
SHARDS = 1024


def is_uuid(s: object) -> bool:
    return isinstance(s, str) and _UUID.fullmatch(s) is not None


def step_id(name: str, occurrence: int) -> str:
    """Lowercase hex of the first 16 bytes of SHA-256(name + NUL + decimal occurrence)."""
    return hashlib.sha256(utf8_bytes(f"{name}\u0000{occurrence}")).digest()[:16].hex()


def shard_of(run_id: str) -> int | None:
    """The shard (0-1023) of a v7 or v8 run ID: rand_a holds 0b00 and the 10-bit shard."""
    if not is_uuid(run_id):
        return None
    h = run_id.replace("-", "")
    b6, b7, b8 = int(h[12:14], 16), int(h[14:16], 16), int(h[16:18], 16)
    if (b6 >> 4) not in (7, 8) or (b8 & 0xC0) != 0x80 or (b6 & 0x0C) != 0:
        return None
    return ((b6 & 0x03) << 8) | b7


def is_invocation_token(s: str) -> bool:
    """'<shard 3 hex>.<run UUID>.<invocation ID>.<43 base64url>', the shard being the run ID's."""
    if not isinstance(s, str):
        return False
    parts = s.split(".")
    if len(parts) != 4:
        return False
    shard, run_id, inv, secret = parts
    if not _SHARD_HEX.fullmatch(shard) or not _INV_ID.fullmatch(inv):
        return False
    rs = shard_of(run_id)
    if rs is None or rs != int(shard, 16):
        return False
    try:
        return len(from_b64url_strict(secret)) == 32
    except ValueError:
        return False


def format_callback_token(shard: int, secret: bytes) -> str:
    if not 0 <= shard < SHARDS or len(secret) != 32:
        raise ValueError("a callback token has a shard 0-1023 and a 32-byte secret")
    return f"{shard:03x}.{b64url(secret)}"


def new_callback_token(shard: int) -> tuple[str, str]:
    """A callback token for a run on shard ('<shard 3 hex>.<43 base64url of 32 random bytes>') and
    the hex SHA-256 of the 32 secret bytes, which the engine stores (design-phase-2, "Callbacks")."""
    secret = os.urandom(32)
    return format_callback_token(shard, secret), hashlib.sha256(secret).hexdigest()


def derived_run_id(domain: str, a: str, b: str) -> str:
    """D2-8: SHA-256(domain NUL a NUL b)[0:16] as a UUIDv8 whose rand_a carries the shard
    (big-endian uint16 of sum[16:18] mod 1024)."""
    s = hashlib.sha256(utf8_bytes(f"{domain}\u0000{a}\u0000{b}")).digest()
    shard = int.from_bytes(s[16:18], "big") % SHARDS
    u = bytearray(s[:16])
    u[6] = 0x80 | ((shard >> 8) & 0x03)
    u[7] = shard & 0xFF
    u[8] = 0x80 | (u[8] & 0x3F)
    h = u.hex()
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}"


def rfc3339_nano(t: "datetime | str") -> str:
    """A time as Go's RFC3339Nano in UTC (fractional zeros trimmed, Z), as derived IDs hash it."""
    if isinstance(t, str):
        t = datetime.fromisoformat(t.replace("z", "Z"))
    if t.tzinfo is None:
        raise ValueError("an aware time is needed")
    u = t.astimezone(UTC)
    frac = f"{u.microsecond:06d}".rstrip("0")
    return u.strftime("%Y-%m-%dT%H:%M:%S") + (f".{frac}" if frac else "") + "Z"


def scheduled_run_id(schedule_id: str, scheduled_time: "datetime | str") -> str:
    """The run ID of a schedule's firing at scheduled_time (domain workflows.schedule)."""
    return derived_run_id("workflows.schedule", schedule_id, rfc3339_nano(scheduled_time))


def triggered_run_id(schedule_id: str, idempotency_key: str) -> str:
    """The run ID of a schedule trigger with this Idempotency-Key (domain workflows.trigger)."""
    return derived_run_id("workflows.trigger", schedule_id, idempotency_key)


def child_run_id(parent: str, step: str) -> str:
    """The run ID of the child a step.invoke starts (domain workflows.child)."""
    return derived_run_id("workflows.child", parent, step)
