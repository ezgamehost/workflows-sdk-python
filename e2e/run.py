"""The end-to-end driver: each check through the public API (Client) against a local
workflows-stack; exits 1 at the first failure."""

import json
import os
import re
import sys
import time
import urllib.request
from collections.abc import Callable
from datetime import datetime
from typing import Any, NoReturn

from ezgh_workflows import Client, WorkflowsApiError
from ezgh_workflows.client import Run, RunStep
from ezgh_workflows.protocol import child_run_id, scheduled_run_id, shard_of, triggered_run_id

API = os.environ.get("E2E_API", "http://127.0.0.1:6100")
APP = os.environ.get("E2E_APP", "http://127.0.0.1:3200")
# A fresh engine leases its shards over about 30 s and answers 503 until then: retry generously.
client = Client(
    api_key=os.environ["E2E_API_KEY"],
    organization_id=os.environ["E2E_ORG"],
    project_id=os.environ["E2E_PROJECT"],
    base_url=API,
    retries=90,
    max_retry_delay=2,
)
APP_ID = os.environ["E2E_APP_ID"]
PULL_APP_ID = os.environ["E2E_PULL_APP_ID"]
TERMINAL = ("completed", "failed", "cancelled", "timedOut")


def fail(what: str, detail: Any = None) -> NoReturn:
    print(f"FAILED: {what}", json.dumps(detail, indent=2, default=str) if detail is not None else "", file=sys.stderr)
    sys.exit(1)


def check(cond: object, what: str, detail: Any = None) -> None:
    if not cond:
        fail(what, detail)


def until(run_id: str, done: Callable[[Run], bool], seconds: float = 60) -> Run:
    end = time.monotonic() + seconds
    r = client.get(run_id)
    while not done(r) and time.monotonic() < end:
        time.sleep(0.5)
        r = client.get(run_id)
    check(done(r), f"run {r['workflow']} reached the expected state (it's {r['status']})", r)
    return r


def terminal(r: Run) -> bool:
    return r["status"] in TERMINAL


def step_named(r: Run, name: str) -> RunStep | None:
    return next((s for s in r.get("steps") or [] if s["name"] == name), None)


def step_status(r: Run, name: str) -> str | None:
    s = step_named(r, name)
    return s["status"] if s is not None else None


def state() -> dict[str, Any]:
    with urllib.request.urlopen(f"{APP}/__e2e/state", timeout=10) as res:
        out: dict[str, Any] = json.loads(res.read())
        return out


def exists(run_id: str, seconds: float = 60) -> Run:
    """Waits for a run created asynchronously (a scheduled firing, a child) to answer GET."""
    end = time.monotonic() + seconds
    while True:
        try:
            return client.get(run_id)
        except WorkflowsApiError as e:
            if e.status != 404 or time.monotonic() > end:
                raise
            time.sleep(0.5)


def post(url: str, body: Any) -> int:
    req = urllib.request.Request(
        url, data=json.dumps(body).encode(), method="POST", headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as res:
            return int(res.status)
    except urllib.error.HTTPError as e:
        return int(e.code)


def main() -> None:
    synced = client.sync(APP_ID)
    sdk = synced.get("sdk") or {}
    check(
        synced["syncStatus"] == "synced"
        and len(synced["workflows"]) == 13
        and synced["deployment"] == "e2e"
        and sdk.get("name") == "ezgh-workflows",
        "sync records the app's workflows",
        synced,
    )
    print("ok   sync")

    done = until(client.start(APP_ID, "e2e-complete")["id"], terminal)
    check(done["status"] == "completed" and client.output(done["id"]) == {"sum": 3}, "e2e-complete completes", done)
    print("ok   completion")

    run_key = f"key-{int(time.time() * 1000)}"
    keyed = client.start(APP_ID, "e2e-complete", {"a": 1}, run_key=run_key)
    again = client.start(APP_ID, "e2e-complete", {"a": 1}, run_key=run_key)
    check(again["id"] == keyed["id"], "the same runKey and input return the same run", [keyed, again])
    try:
        client.start(APP_ID, "e2e-complete", {"a": 2}, run_key=run_key)
        fail("the same runKey with different input is 409 run_key_conflict")
    except WorkflowsApiError as e:
        check((e.status, e.code) == (409, "run_key_conflict"), "the same runKey with different input is 409", repr(e))
    print("ok   idempotent start")

    slept = until(client.start(APP_ID, "e2e-sleep")["id"], terminal)
    nap = step_named(slept, "nap")
    check(
        slept["status"] == "completed"
        and client.output(slept["id"]) == 100_000
        and nap
        and nap["status"] == "completed",
        "e2e-sleep resumes after its timer, with the large payload replayed",
        slept,
    )
    print("ok   sleep and payload replay")

    retried = until(client.start(APP_ID, "e2e-retry")["id"], terminal)
    flaky = step_named(retried, "flaky")
    check(
        retried["status"] == "completed" and client.output(retried["id"]) == 3 and flaky and flaky["attempt"] == 3,
        "e2e-retry succeeds on attempt 3",
        retried,
    )
    print("ok   step retries")

    crashed = until(client.start(APP_ID, "e2e-crash")["id"], terminal, 90)
    crashy = step_named(crashed, "crashy")
    check(
        crashed["status"] == "completed"
        and client.output(crashed["id"]) == "survived"
        and crashy
        and crashy["attempt"] == 2,
        "a crash mid-step counts as an attempt of that step",
        crashed,
    )
    print("ok   crash mid-step is a step attempt")

    cancelling = client.start(APP_ID, "e2e-cancel")
    until(cancelling["id"], lambda r: step_status(r, "wait") == "sleeping")
    client.cancel(cancelling["id"])
    cancelled = until(cancelling["id"], terminal)
    s1 = state()
    cleanup = step_named(cancelled, "cleanup")
    check(
        cancelled["status"] == "cancelled" and cleanup and cleanup["status"] == "completed" and s1["cleanup"] == 1,
        "cancel runs the cleanup step and ends cancelled",
        [cancelled, s1],
    )
    print("ok   cancel with cleanup")

    fixable = until(client.start(APP_ID, "e2e-fixable")["id"], terminal)
    check(fixable["status"] == "failed", "e2e-fixable fails before the fix", fixable)
    urllib.request.urlopen(f"{APP}/__e2e/fix", timeout=10).read()
    client.retry(fixable["id"])
    fixed = until(
        fixable["id"],
        lambda r: r["status"] == "completed" or (r["status"] == "failed" and r["attempt"] > fixable["attempt"]),
    )
    s2 = state()
    check(
        fixed["status"] == "completed" and client.output(fixed["id"]) == "fixed" and s2["prepare"] == 1,
        "retry resumes from the failed step without re-running completed ones",
        [fixed, s2],
    )
    print("ok   retry from failure")

    if os.environ.get("E2E_EXACT_JSON") == "1":
        ex = until(client.start(APP_ID, "e2e-exact")["id"], terminal)
        text = json.dumps(client.output(ex["id"]), separators=(",", ":"))
        check(
            ex["status"] == "completed" and text == '{"b":1,"a":"x\\u0000y","c":{"z":[2,1],"y":null}}',
            "recorded JSON replays with its key order and NUL characters",
            [ex, text],
        )
        print("ok   exact JSON replay")
    else:
        print("skip exact JSON replay (this workflows-stack has no json-column migration 0005 yet)")

    # A plain def workflow fanning out with parallel().
    sp = until(client.start(APP_ID, "e2e-sync")["id"], terminal)
    check(
        sp["status"] == "completed"
        and client.output(sp["id"]) == 42
        and all(step_status(sp, n) == "completed" for n in ("a", "b")),
        "e2e-sync's parallel steps both complete",
        sp,
    )
    print("ok   sync workflow parallel")

    fan = until(client.start(APP_ID, "e2e-fanout")["id"], terminal)
    check(
        fan["status"] == "completed"
        and client.output(fan["id"]) == 10
        and all(step_status(fan, f"part-{i}") == "completed" for i in range(5)),
        "e2e-fanout's five steps complete",
        fan,
    )
    print("ok   parallel fan-out")

    # A callback completed over its public URL.
    approval = client.start(APP_ID, "e2e-approval")
    url: str | None = None
    end = time.monotonic() + 60
    while not url and time.monotonic() < end:
        url = state().get("callbackUrl")
        if not url:
            time.sleep(0.25)
    # The URL is endpoints.api (the stack's PUBLIC_BASE_URL: an https origin, even locally) plus
    # /public/v1/callbacks/<token>, the token on the run's shard; the driver posts that path to the
    # local API.
    m = re.fullmatch(r"https://[^/]+/public/v1/callbacks/([0-9a-f]{3})\.[A-Za-z0-9_-]{43}", url or "")
    check(m and int(m.group(1), 16) == shard_of(approval["id"]), "e2e-approval published a callback URL", url)
    assert url is not None
    url = API + "/public/v1/callbacks/" + url.split("/public/v1/callbacks/", 1)[1]
    check(post(url, {"approved": True}) == 200, "completing the callback succeeds")
    check(post(url, {"approved": True}) == 200, "repeating the same completion answers 200")
    check(post(url, {"approved": False}) == 409, "a different completion is 409 callback_completed")
    approved = until(approval["id"], terminal)
    check(
        approved["status"] == "completed" and client.output(approved["id"]) == {"approved": True},
        "e2e-approval resumes with the callback's body",
        approved,
    )
    print("ok   callback by HTTP")

    # The pull worker announces e2e-pulled on its polls: a start is 409 workflow_not_announced until
    # one lands; then the run completes on the worker.
    end = time.monotonic() + 90
    while True:
        try:
            started = client.start(PULL_APP_ID, "e2e-pulled", {"n": 7})
            break
        except WorkflowsApiError as e:
            check(
                e.code == "workflow_not_announced" and time.monotonic() < end,
                "the pull worker announced e2e-pulled",
                repr(e),
            )
            time.sleep(0.5)
    pulled = until(started["id"], terminal)
    check(
        pulled["status"] == "completed" and client.output(pulled["id"]) == 49, "the pull worker runs e2e-pulled", pulled
    )
    print("ok   pull worker")

    # A schedule every minute: the first firing starts a run whose ID derives from the scheduled time.
    sched = client.create_schedule(
        app_id=APP_ID, workflow="e2e-scheduled", name=f"e2e-{int(time.time())}", cron="* * * * *"
    )
    fired = sched
    end = time.monotonic() + 90
    while not fired.get("lastScheduledTime"):
        check(time.monotonic() < end, "the schedule fired within 90 s", fired)
        time.sleep(1)
        fired = client.get_schedule(sched["id"])
    last = fired.get("lastScheduledTime")
    assert last is not None
    scheduled_id = scheduled_run_id(sched["id"], last)
    exists(scheduled_id)
    scheduled = until(scheduled_id, terminal)
    note = client.output(scheduled_id)
    check(
        scheduled["status"] == "completed"
        and isinstance(note, dict)
        and note.get("id") == sched["id"]
        and datetime.fromisoformat(note["scheduledTime"]) == datetime.fromisoformat(last),
        "the scheduled run sees its schedule and scheduled time",
        [scheduled, note, fired],
    )
    key = f"e2e-trigger-{sched['id']}"
    triggered = client.trigger_schedule(sched["id"], idempotency_key=key)
    again2 = client.trigger_schedule(sched["id"], idempotency_key=key)
    check(
        triggered["id"] == triggered_run_id(sched["id"], key) and again2["id"] == triggered["id"],
        "trigger derives its run ID from the key and is idempotent",
        [triggered, again2],
    )
    check(until(triggered["id"], terminal)["status"] == "completed", "the triggered run completes")
    check(client.pause_schedule(sched["id"])["paused"] is True, "pause pauses")
    client.delete_schedule(sched["id"])
    try:
        client.get_schedule(sched["id"])
        fail("a deleted schedule is gone")
    except WorkflowsApiError as e:
        check(e.status == 404, "a deleted schedule is 404", repr(e))
    print("ok   schedule")

    parent = until(client.start(APP_ID, "e2e-parent", {"n": 4})["id"], terminal)
    check(
        parent["status"] == "completed" and client.output(parent["id"]) == 41,
        "e2e-parent completes with its child's output plus one",
        parent,
    )
    child_step = step_named(parent, "child")
    check(child_step, "the parent journals its child step", parent)
    assert child_step is not None
    child_id = child_run_id(parent["id"], child_step["stepId"])
    child = exists(child_id)
    check(
        child["status"] == "completed" and child["workflow"] == "e2e-child" and client.output(child_id) == 40,
        "the child run exists under its derived ID and completed",
        child,
    )
    print("ok   child workflow")

    print("PASSED")


if __name__ == "__main__":
    try:
        main()
    except WorkflowsApiError as e:
        fail(f"the API answered {e.status} {e.code}: {e}")
