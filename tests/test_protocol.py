from datetime import UTC, date, datetime, timedelta, timezone

import pytest

from ezgh_workflows.protocol import (
    MAX_OUTPUT_BYTES,
    SignatureError,
    decode_secret,
    dumps,
    encode_value,
    json_byte_length,
    new_callback_token,
    parse_duration,
    parse_signature,
    step_id,
    truncate_utf8,
    utf8_len,
)
from ezgh_workflows.protocol._ids import STEP_ID_OVERRIDE, WORKFLOW_NAME


def test_aware_datetimes_encode_like_to_iso_string() -> None:
    assert encode_value(datetime(2026, 10, 9, 12, 0, 0, 123456, tzinfo=UTC)) == "2026-10-09T12:00:00.123Z"
    assert encode_value(datetime(2026, 10, 9, 14, tzinfo=timezone(timedelta(hours=2)))) == "2026-10-09T12:00:00.000Z"
    assert encode_value(date(2026, 10, 9)) == "2026-10-09"


@pytest.mark.parametrize(
    "bad", [datetime(2026, 1, 1), float("nan"), float("inf"), {1, 2}, frozenset(), b"x", {1: "int key"}, object()]
)
def test_refused_values(bad: object) -> None:
    with pytest.raises(TypeError):
        encode_value(bad)


def test_tuples_become_lists_and_key_order_is_kept() -> None:
    assert encode_value({"b": (1, 2), "a": None}) == {"b": [1, 2], "a": None}
    assert dumps(encode_value({"b": 1, "a": "x\u0000y"})) == '{"b":1,"a":"x\\u0000y"}'


def test_lone_surrogates_are_escaped_like_json_stringify() -> None:
    assert dumps("a\ud800b") == '"a\\ud800b"'
    assert dumps("é") == '"é"'


def test_byte_lengths_are_utf8() -> None:
    assert utf8_len("é") == 2
    assert truncate_utf8("é" * 5000, 8192) == "é" * 4096
    assert truncate_utf8("aé", 2) == "a"
    assert json_byte_length("x" * MAX_OUTPUT_BYTES) == MAX_OUTPUT_BYTES + 2


def test_durations() -> None:
    assert [parse_duration(d) for d in ("500ms", "30s", "5m", "2h", "3d", 250)] == [
        500,
        30_000,
        300_000,
        7_200_000,
        259_200_000,
        250,
    ]
    for bad in ("1w", "-1s", -5, float("nan"), "", "1.5s", "\u0663s", True):
        with pytest.raises(ValueError):
            parse_duration(bad)


def test_patterns_match_go_regexps_exactly() -> None:
    # Go's $ never matches before a trailing newline; Python's $ does, so the SDK uses fullmatch.
    assert STEP_ID_OVERRIDE.fullmatch("abc\n") is None and WORKFLOW_NAME.fullmatch("wf\n") is None
    assert STEP_ID_OVERRIDE.fullmatch("a.b:c-d_1") and WORKFLOW_NAME.fullmatch("A-b_9")


def test_secrets_are_strict() -> None:
    good = "whsec_" + "A" * 43
    assert len(decode_secret(good)) == 32
    for bad in ("A" * 49, "whsec_" + "A" * 42, "whsec_" + "A" * 42 + "B", "whsec_" + "+" * 43):
        with pytest.raises(ValueError):
            decode_secret(bad)


def test_signature_header_is_checked_before_the_body() -> None:
    with pytest.raises(SignatureError) as e:
        parse_signature("t=100,v1=" + "0" * 64, now=100 + 301)
    assert e.value.reason == "expired"
    with pytest.raises(SignatureError) as e:
        parse_signature("t=100,v1=" + "0" * 64, now=100 - 301)
    assert e.value.reason == "expired"
    for bad in (
        "t=100," + ",".join(["v1=" + "0" * 64] * 9),
        "t=+100,v1=" + "0" * 64,
        "t=\u0661\u0660\u0660,v1=" + "0" * 64,
        "x" * 1025,
        "",
        "t=100",
    ):
        with pytest.raises(SignatureError) as e:
            parse_signature(bad, now=100)
        assert e.value.reason == "invalid", bad


def test_callback_tokens_carry_the_run_shard() -> None:
    token, digest = new_callback_token(0x2A5)
    shard, secret = token.split(".")
    assert shard == "2a5" and len(secret) == 43 and len(digest) == 64
    other, _ = new_callback_token(0x2A5)
    assert other != token
    with pytest.raises(ValueError):
        new_callback_token(1024)


def test_step_ids_hash_utf8() -> None:
    assert len(step_id("é", 3)) == 32
