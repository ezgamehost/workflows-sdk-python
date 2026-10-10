"""UTF-8 byte counting and strict base64url: every limit the engine enforces is in bytes."""

import base64
import binascii

_B64URL = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_")


def utf8_bytes(s: str) -> bytes:
    """s as UTF-8; a lone surrogate (only reachable from decoded JSON) counts as its 3-byte form."""
    return s.encode("utf-8", "surrogatepass")


def utf8_len(s: str) -> int:
    return len(utf8_bytes(s))


def truncate_utf8(s: str, max_bytes: int) -> str:
    """s cut to at most max_bytes of UTF-8, never inside a code point."""
    b = utf8_bytes(s)
    if len(b) <= max_bytes:
        return s
    cut = max_bytes
    while cut > 0 and (b[cut] & 0xC0) == 0x80:
        cut -= 1
    return b[:cut].decode("utf-8", "surrogatepass")


def b64url(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode("ascii")


def from_b64url_strict(s: str) -> bytes:
    """Unpadded base64url, canonical only (Go's RawURLEncoding.Strict()): one text per byte string."""
    if not s or any(ch not in _B64URL for ch in s) or len(s) % 4 == 1:
        raise ValueError("not base64url")
    try:
        out = base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))
    except (binascii.Error, ValueError) as e:
        raise ValueError("not base64url") from e
    if b64url(out) != s:
        raise ValueError("not canonical base64url")
    return out
