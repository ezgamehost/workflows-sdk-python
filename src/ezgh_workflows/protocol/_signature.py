"""EZGH-Signature: t=<unix seconds>,v1=<hex HMAC-SHA256(key, t + "." + body)>, one v1 per secret."""

import hashlib
import hmac
import re
from dataclasses import dataclass
from typing import Literal

from ._bytes import from_b64url_strict

SIGNATURE_HEADER = "EZGH-Signature"
SECRET_PREFIX = "whsec_"
MAX_SIGNATURE_AGE_SECONDS = 300
MAX_SIGNATURES = 8
MAX_HEADER = 1024
_MALFORMED = "A signing secret is whsec_ and 43 base64url characters, as the API returned it"


class SignatureError(Exception):
    def __init__(self, reason: Literal["expired", "invalid"], message: str) -> None:
        super().__init__(message)
        self.reason: Literal["expired", "invalid"] = reason


def decode_secret(secret: str) -> bytes:
    """The 32-byte HMAC key of a whsec_ secret; ValueError for anything else."""
    s = secret.strip() if isinstance(secret, str) else ""
    if not s.startswith(SECRET_PREFIX) or len(s) != len(SECRET_PREFIX) + 43:
        raise ValueError(_MALFORMED)
    try:
        b = from_b64url_strict(s[len(SECRET_PREFIX) :])
    except ValueError as e:
        raise ValueError(_MALFORMED) from e
    if len(b) != 32:
        raise ValueError(_MALFORMED)
    return b


def _mac(secret: bytes, t: int, body: bytes) -> bytes:
    return hmac.new(secret, f"{t}.".encode() + body, hashlib.sha256).digest()


def sign(secrets: list[bytes], t: int, body: bytes) -> str:
    return ",".join([f"t={t}", *(f"v1={_mac(s, t, body).hex()}" for s in secrets)])


@dataclass(frozen=True)
class ParsedSignature:
    t: int
    signatures: tuple[bytes, ...]


_T = re.compile(r"[0-9]{1,12}")
_V1 = re.compile(r"[0-9a-fA-F]{64}")


def parse_signature(header: str, now: float) -> ParsedSignature:
    """The header's shape and timestamp, checked before the body is read."""
    if not isinstance(header, str) or not header or len(header) > MAX_HEADER:
        raise SignatureError("invalid", "The EZGH-Signature header is missing or malformed")
    t: int | None = None
    sigs: list[bytes] = []
    for raw in header.split(","):
        k, eq, v = raw.strip().partition("=")
        if not eq:
            raise SignatureError("invalid", "The EZGH-Signature header is malformed")
        if k == "t":
            if not _T.fullmatch(v):
                raise SignatureError("invalid", "The EZGH-Signature timestamp is malformed")
            t = int(v)
        elif k == "v1" and _V1.fullmatch(v):
            if len(sigs) == MAX_SIGNATURES:
                raise SignatureError("invalid", f"The EZGH-Signature header has more than {MAX_SIGNATURES} signatures")
            sigs.append(bytes.fromhex(v))
    if t is None or not sigs:
        raise SignatureError("invalid", "The EZGH-Signature header is missing or malformed")
    if abs(now - t) > MAX_SIGNATURE_AGE_SECONDS:
        raise SignatureError("expired", "The EZGH-Signature timestamp is more than 5 minutes from now")
    return ParsedSignature(t, tuple(sigs))


def verify_parsed(sig: ParsedSignature, secrets: list[bytes], body: bytes) -> None:
    """One HMAC per secret; every candidate compared in constant time, with no early exit."""
    ok = False
    for s in secrets:
        want = _mac(s, sig.t, body)
        for got in sig.signatures:
            ok |= hmac.compare_digest(want, got)
    if not ok:
        raise SignatureError("invalid", "The EZGH-Signature doesn't match any signing secret")


def verify(header: str, secrets: list[bytes], body: bytes, now: float) -> None:
    verify_parsed(parse_signature(header, now), secrets, body)
