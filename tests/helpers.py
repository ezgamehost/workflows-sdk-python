import json
from dataclasses import dataclass, field
from typing import Any

from conftest import vector

from ezgh_workflows._runtime.live import InvocationSuperseded
from ezgh_workflows.protocol import step_id

RUN_ID: str = vector("run-ids")["valid"][2]["id"]  # shard 513
DEFAULT = {"maxAttempts": 5, "initialMs": 1000, "maxMs": 600000, "multiplier": 2}


@dataclass
class FakeLive:
    """What workflows-live would receive: checkpoints (JSON round-tripped), and scripted pages."""

    checkpoints: list[dict[str, Any]] = field(default_factory=list)
    pages: list[dict[str, Any]] = field(default_factory=list)
    payloads: dict[str, Any] = field(default_factory=dict)
    supersede_at: int | None = None
    page_requests: list[int] = field(default_factory=list)

    async def checkpoint(self, cp: Any) -> None:
        if self.supersede_at is not None and len(self.checkpoints) + 1 >= self.supersede_at:
            raise InvocationSuperseded("superseded")
        self.checkpoints.append(json.loads(json.dumps(cp)))

    async def journal_page(self, after_seq: int) -> Any:
        self.page_requests.append(after_seq)
        return self.pages.pop(0) if self.pages else {"complete": True, "entries": []}

    async def payload(self, pid: str) -> Any:
        return self.payloads[pid]


def sid(name: str, n: int = 0) -> str:
    return step_id(name, n)


def entry(
    seq: int, name: str, status: str = "completed", *, n: int = 0, kind: str = "run", attempt: int = 1, **kw: Any
) -> dict[str, Any]:
    return {
        "seq": seq,
        "stepId": kw.pop("step_id", sid(name, n)),
        "name": name,
        "kind": kind,
        "status": status,
        "attempt": attempt,
        **kw,
    }


def invocation(
    workflow: str,
    *,
    input: Any = None,
    entries: list[dict[str, Any]] | None = None,
    kind: str = "orchestrate",
    target: str | None = None,
    cancel: bool = False,
    schedule: dict[str, str] | None = None,
    version: str = "1.1",
    complete: bool = True,
) -> Any:
    run: dict[str, Any] = {"id": RUN_ID, "workflow": workflow, "attempt": 1, "startedAt": "2026-10-10T00:00:00Z"}
    if input is not None:
        run["input"] = input
    if schedule is not None:
        run["schedule"] = schedule
    inv: dict[str, Any] = {
        "protocolVersion": version,
        "kind": kind,
        "invocationId": "inv_" + "a" * 26,
        "token": "t",
        "endpoints": {"live": "http://live.test", "api": "https://workflows-api.test"},
        "run": run,
        "journal": {"complete": complete, "entries": entries or []},
        "deadline": "2099-01-01T00:00:00Z",
        "cancel": {"requestedAt": "2026-10-10T00:00:00Z", "graceUntil": "2099-01-01T00:00:00Z"} if cancel else None,
    }
    if target is not None:
        inv["targetStepId"] = target
    return inv


def types(cmds: list[dict[str, Any]]) -> list[str]:
    return [c["type"] for c in cmds]
