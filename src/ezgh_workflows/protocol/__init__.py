"""Wire helpers of the EZGH Workflows protocol.

sign, verify, decode_secret, SignatureError, PROTOCOL_VERSIONS and the wire types are the stable
API (for verifying EZGH requests in a custom transport); the other helpers are re-exported for the
SDK's own modules and may change without notice.
"""

from ._bytes import truncate_utf8, utf8_len
from ._codec import (
    DEFAULT_RETRY,
    MAX_OUTPUT_BYTES,
    backoff,
    dumps,
    encode_value,
    json_byte_length,
    parse_duration,
)
from ._ids import (
    child_run_id,
    is_invocation_token,
    new_callback_token,
    scheduled_run_id,
    shard_of,
    step_id,
    triggered_run_id,
)
from ._signature import (
    SIGNATURE_HEADER,
    ParsedSignature,
    SignatureError,
    decode_secret,
    parse_signature,
    sign,
    verify,
    verify_parsed,
)
from ._types import (
    Cancel,
    Checkpoint,
    Command,
    HeartbeatResponse,
    Invocation,
    InvocationResult,
    Journal,
    JournalEntry,
    PollRequest,
    PollResponse,
    RetryPolicy,
    RunInfo,
    ScheduleInfo,
    SyncReply,
    WireError,
    WorkflowInfo,
)

PROTOCOL_VERSIONS = ["1.0", "1.1", "1.2"]

__all__ = [
    "DEFAULT_RETRY",
    "MAX_OUTPUT_BYTES",
    "PROTOCOL_VERSIONS",
    "SIGNATURE_HEADER",
    "Cancel",
    "Checkpoint",
    "Command",
    "HeartbeatResponse",
    "Invocation",
    "InvocationResult",
    "Journal",
    "JournalEntry",
    "ParsedSignature",
    "PollRequest",
    "PollResponse",
    "RetryPolicy",
    "RunInfo",
    "ScheduleInfo",
    "SignatureError",
    "SyncReply",
    "WireError",
    "WorkflowInfo",
    "backoff",
    "child_run_id",
    "decode_secret",
    "dumps",
    "encode_value",
    "is_invocation_token",
    "json_byte_length",
    "new_callback_token",
    "parse_duration",
    "parse_signature",
    "scheduled_run_id",
    "shard_of",
    "sign",
    "step_id",
    "triggered_run_id",
    "truncate_utf8",
    "utf8_len",
    "verify",
    "verify_parsed",
]
