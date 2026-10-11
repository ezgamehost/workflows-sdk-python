"""Parsing invocations: checks what the runtime relies on and ignores unknown fields."""

import json
from datetime import datetime
from typing import cast

from ..protocol._types import Invocation

KINDS = frozenset({"orchestrate", "step", "sync"})


class InvalidInvocation(Exception):
    pass


def parse_time(s: str) -> float:
    """An RFC 3339 time as epoch seconds; ValueError if it isn't one (with a zone)."""
    if not isinstance(s, str):
        raise ValueError("not a time")
    dt = datetime.fromisoformat(s.replace("z", "Z"))
    if dt.tzinfo is None:
        raise ValueError("an RFC 3339 time has a zone")
    return dt.timestamp()


def _int(v: object) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def valid_entry(e: object) -> bool:
    """What replay relies on in a journal entry: seq, stepId, status, and a callback's createdSeq
    (protocol 1.1, P2: where its creation replays) a whole number >= 1 when present."""
    if not (isinstance(e, dict) and _int(e.get("seq")) and isinstance(e.get("stepId"), str)):
        return False
    if not isinstance(e.get("status"), str):
        return False
    created = e.get("createdSeq")
    return created is None or (_int(created) and created >= 1)


def parse_invocation(text: str | bytes) -> Invocation:
    try:
        v = json.loads(text)
    except (ValueError, UnicodeDecodeError, RecursionError) as e:
        raise InvalidInvocation("The body isn't JSON") from e
    if not isinstance(v, dict):
        raise InvalidInvocation("The body isn't an invocation")
    pv = v.get("protocolVersion")
    if not isinstance(pv, str) or pv.split(".")[0] != "1":
        raise InvalidInvocation(f"Unsupported protocol version {pv!r}")
    if v.get("kind") not in KINDS:
        raise InvalidInvocation(f"Unknown invocation kind {v.get('kind')!r}")
    if not isinstance(v.get("invocationId"), str) or not isinstance(v.get("deadline"), str):
        raise InvalidInvocation("Missing invocationId or deadline")
    try:
        parse_time(v["deadline"])
    except ValueError as e:
        raise InvalidInvocation("deadline isn't an RFC 3339 time") from e
    if v["kind"] != "sync":
        run, journal = v.get("run"), v.get("journal")
        if not (
            isinstance(run, dict)
            and isinstance(run.get("id"), str)
            and isinstance(run.get("workflow"), str)
            and _int(run.get("attempt"))
        ):
            raise InvalidInvocation("Missing run")
        if not (
            isinstance(journal, dict)
            and isinstance(journal.get("complete"), bool)
            and isinstance(journal.get("entries"), list)
        ):
            raise InvalidInvocation("Missing journal")
        for entry in journal["entries"]:
            if not valid_entry(entry):
                raise InvalidInvocation("A journal entry needs seq, stepId and status (and createdSeq >= 1 if set)")
        sched = run.get("schedule")
        if sched is not None:
            if not (isinstance(sched, dict) and isinstance(sched.get("id"), str)):
                raise InvalidInvocation("run.schedule needs id and scheduledTime")
            try:
                parse_time(sched.get("scheduledTime"))  # type: ignore[arg-type]
            except ValueError as err:
                raise InvalidInvocation("run.schedule.scheduledTime isn't an RFC 3339 time") from err
        ep = v.get("endpoints")
        if not isinstance(v.get("token"), str) or not v["token"] or not isinstance(ep, dict):
            raise InvalidInvocation("Missing token or endpoints.live")
        if not isinstance(ep.get("live"), str):
            raise InvalidInvocation("Missing token or endpoints.live")
        if v["kind"] == "step" and not (isinstance(v.get("targetStepId"), str) and v["targetStepId"]):
            raise InvalidInvocation("A step invocation needs targetStepId")
    c = v.get("cancel")
    if c is not None and not isinstance(c, dict):
        raise InvalidInvocation("cancel isn't an object")
    v["cancel"] = c
    return cast(Invocation, v)
