"""workflows-stack's protocol vectors (tests/vectors, vendored verbatim by scripts/sync-vectors.sh)."""

import hashlib
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from conftest import vector

from ezgh_workflows.protocol import (
    SignatureError,
    backoff,
    child_run_id,
    decode_secret,
    encode_value,
    is_invocation_token,
    shard_of,
    sign,
    step_id,
    verify,
)
from ezgh_workflows.protocol._bytes import b64url
from ezgh_workflows.protocol._ids import derived_run_id, format_callback_token

SKIP = object()
_fn: Callable[[], int] = lambda: 1  # noqa: E731
# codec.json's tags name JavaScript values: each maps to its Python counterpart, or SKIP where
# Python has none (a dict has no undefined fields).
PY_CODEC: dict[str, Any] = {
    "date-epoch": datetime(1970, 1, 1, tzinfo=UTC),
    "undefined": None,
    "object-with-undefined-field": SKIP,
    "array-with-undefined": [1, None],
    "nested-date": {"at": datetime(2026, 10, 9, 12, tzinfo=UTC)},
    "bigint": Decimal("1"),  # refused, like a JS bigint
    "function": _fn,  # refused
}


def test_step_ids() -> None:
    for c in vector("step-ids")["cases"]:
        assert step_id(c["name"], c["occurrence"]) == c["stepId"], c


def test_signing() -> None:
    for c in vector("signatures")["sign"]:
        assert sign([decode_secret(s) for s in c["secrets"]], c["t"], c["body"].encode()) == c["header"]


def test_verifying() -> None:
    for c in vector("signatures")["verify"]:
        secrets = [decode_secret(s) for s in c["secrets"]]
        if c["result"] == "ok":
            verify(c["header"], secrets, c["body"].encode(), c["now"])
        else:
            with pytest.raises(SignatureError) as err:
                verify(c["header"], secrets, c["body"].encode(), c["now"])
            assert err.value.reason == c["result"], c


def test_invocation_tokens() -> None:
    v = vector("invocation-tokens")
    for t in v["valid"]:
        assert is_invocation_token(t["token"]), t
        assert hashlib.sha256(bytes.fromhex(t["secretHex"])).hexdigest() == t["hash"]
    for t in v["invalid"]:
        assert not is_invocation_token(t["token"]), t


def test_callback_tokens() -> None:
    for c in vector("callback-tokens")["valid"]:
        secret = bytes.fromhex(c["secretHex"])
        assert format_callback_token(c["shard"], secret) == c["token"]
        assert hashlib.sha256(secret).hexdigest() == c["hashHex"]
        assert b64url(secret) == c["token"].split(".")[1]


def test_run_ids_carry_their_shard() -> None:
    v = vector("run-ids")
    for c in v["valid"]:
        assert shard_of(c["id"]) == c["shard"], c
    for c in v["invalid"]:
        assert shard_of(c["id"]) is None, c


def test_derived_run_ids() -> None:
    v = vector("derived-ids")
    for c in v["child"]:
        assert child_run_id(c["parent"], c["stepId"]) == c["id"], c
        assert shard_of(c["id"]) == c["shard"]
    for c in v["scheduled"]:
        assert derived_run_id("workflows.schedule", c["scheduleId"], c["scheduledTime"]) == c["id"], c
        assert shard_of(c["id"]) == c["shard"]
    for c in v["triggered"]:
        assert derived_run_id("workflows.trigger", c["scheduleId"], c["idempotencyKey"]) == c["id"], c


def test_backoff() -> None:
    v = vector("backoff")
    for i, ms in enumerate(v["delaysMs"]):
        assert backoff(v["policy"], i + 1) == ms, i + 1


def test_codec() -> None:
    for c in vector("codec")["cases"]:
        assert c["tag"] in PY_CODEC, c["tag"]
        value = PY_CODEC[c["tag"]]
        if value is SKIP:
            continue
        if "error" in c:
            with pytest.raises(TypeError):
                encode_value(value)
        else:
            assert encode_value(value) == c["encoded"], c["tag"]
