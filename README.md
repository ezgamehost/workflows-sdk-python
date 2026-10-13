# ezgh-workflows

The Python SDK for [EZGH Workflows](https://docs.ezghcloud.com/workflows): durable workflows written
as code, with steps, sleeps, retries, callbacks, child workflows, parallel steps and schedules.

- Python 3.12 and 3.13; no runtime dependencies (the standard library only).
- Workflows are `async def` (with `Context`) or plain `def` (with `SyncContext`).
- Speaks the EZGH Workflows protocol `1.0` and `1.1` (`ezgh_workflows.PROTOCOL_VERSIONS`).

**Status:** not yet published to PyPI. Verified end to end against workflows-stack `ec53771`
(conformance: every push and pull scenario; the end-to-end suite: runs, retries, crashes, cancel,
callbacks, child workflows, parallel steps, schedules and a pull worker against `workflows-stack all`).

## Writing workflows

```python
import asyncio
import os

from ezgh_workflows import Context, SyncContext, serve, workflow


@workflow("onboarding", retry={"maxAttempts": 3})
async def onboarding(ctx: Context) -> dict:
    user = await ctx.step.run("load-user", lambda: load(ctx.input["userId"]))
    await ctx.step.sleep("cooldown", "3d")
    cb = await ctx.step.create_callback("approval", timeout="7d")
    await ctx.step.run("ask", lambda: ask_manager(cb.url))
    decision = await cb.wait()                      # raises CallbackTimeoutError after 7 days
    crm, billing = await asyncio.gather(            # parallel steps
        ctx.step.run("crm", sync_crm),
        ctx.step.run("billing", bill),
    )
    report = await ctx.step.invoke("provision", workflow="provision-account", input={"id": user["id"]})
    return {"crm": crm, "billing": billing, "report": report, "decision": decision}


@workflow("nightly")                                # a plain def: blocking calls, parallel() to fan out
def nightly(ctx: SyncContext) -> int:
    rows = ctx.step.run("load", load_rows)
    a, b = ctx.step.parallel(("part-a", lambda: part(rows, 0)), ("part-b", lambda: part(rows, 1)))
    return a + b


app = serve(workflows=[onboarding, nightly], signing_secrets=[os.environ.get("EZGH_WORKFLOWS_SIGNING_SECRET")])
```

**Steps.** `ctx.step` has:

| Call | What it does |
| --- | --- |
| `run(name, fn, *, retry=None, timeout_ms=None, id=None)` | Runs `fn` at least once and records its JSON result; on replay the result returns without running `fn`. |
| `sleep(name, "3d")`, `sleep_until(name, when)` | A durable sleep (at most a year): the invocation ends and EZGH invokes the workflow again later. |
| `now(name)`, `uuid(name)` | A recorded time (an aware UTC `datetime`) or UUID: the same on every replay. |
| `create_callback(name, *, timeout="7d", id=None)` | A callback URL (`cb.url`, `cb.token`); `await cb.wait()` suspends until it's completed. |
| `invoke(name, *, workflow, input=None, id=None)` | Runs a child workflow of the same app and returns its output, or raises `ChildFailedError`. |
| `send_to_queue(name, client, queue_id, messages)` | Sends 1–10 messages to an EZGH queue, once (see [Sending to a queue](#sending-to-a-queue)). |
| `parallel((name, fn), ...)` | Sync workflows only: runs several steps at once. |

Inside a step function, `ctx.step.attempt()`, `ctx.step.idempotency_key()` (`<runId>:<stepId>:<attempt>`,
for idempotent calls to other systems) and `ctx.step.cancelled()` (true once the step's timeout passed or,
on a pull worker, the run is cancelling) describe the running attempt. Step functions take no arguments.

- **Names.** Step names are 1–256 bytes of UTF-8. Steps pending at once need distinct names (or ids);
  a repeated name in a loop gets its own step ID by occurrence. An explicit `id` matches
  `^[A-Za-z0-9._:-]{1,128}$` and names one step for the whole run.
- **Code outside steps re-runs on every invocation** and must be free of side effects. Use `step.now`
  and `step.uuid` instead of clocks and randomness.
- `ctx.run` is `RunMeta(id, run_key, attempt, schedule)`; `schedule` is `ScheduleMeta(id,
  scheduled_time)` for runs a schedule started.
- `ctx.traceparent` is the invocation's W3C traceparent, `""` while the project's Workflows
  observability setting is off (see [Tracing](#tracing)).

**Errors.** Raise `NonRetriableError` to fail a step at once, or `RetryAfterError(message, delay_ms)`
to retry after a given delay. A step that exhausts its retries raises into the workflow, where it can
be caught. Recorded failures come back as `WorkflowError` (or the subclass their code names:
`StepTimeoutError`, `CallbackTimeoutError`, `ChildFailedError`, ...), with `code`, `name`, `message`,
`data` and `__cause__`, the same on first execution and on replay.

**Cancellation.** A cancelled run raises `RunCancelledError` at the step the workflow is waiting on.
Catch it (or use `finally`) to run `step.run` cleanup steps; other primitives raise at once.

### Sending to a queue

`ctx.step.send_to_queue(name, client, queue_id, messages)` sends 1–10 messages to an
[EZGH Queues](https://ezghcloud.com) queue in a step, and returns the recorded result
(`{"messages": [{"messageId", "seq"?, "duplicate"?}]}`, one per message, in order). It's `step.run`
around `client.send`, so a replay returns the result without sending again. The client is a
`QueuesClient` with your own API key (it needs `queues.messages.send` on the queue): push apps hold
no API key, so the step never borrows a credential. Messages are dicts with the API's camelCase keys.

```python
from ezgh_workflows import QueuesClient

queues = QueuesClient(api_key=os.environ["EZGH_API_KEY"], organization_id=ORG, project_id=PROJECT)
# base_url defaults to https://queues-api.ezghcloud.com


@workflow("order")
async def order(ctx: Context) -> None:
    placed = await ctx.step.run("place", lambda: shop.place(ctx.input))
    await ctx.step.send_to_queue(
        "notify", queues, "que_…",
        [{"body": json.dumps(placed), "attributes": {"orderId": placed["id"]}, "groupId": placed["accountId"]}],
    )
```

- **Sent once.** Every attempt of the step sends the `Idempotency-Key`
  `wfq_<hex(SHA-256(run_id + ":" + step_id))>`, which the Queues API honors for 48 hours: an attempt
  that resends after a crash (between the send and the step's record) gets the first answer instead
  of a second copy, on standard and FIFO queues alike. So keep `messages` deterministic: the same
  key with a different body is a `409 idempotency_conflict`.
- **FIFO dedup IDs.** A message with a `groupId` (a FIFO queue's message) and no `dedupId` of its
  own is sent with `dedupId` `<that hex>:<index>`; FIFO queues also drop a repeat within their
  5-minute window. Messages without a `groupId` are sent as they are, since standard queues refuse
  `dedupId`.
- **Errors.** The client resends a 429 (but not `quota_exceeded`), any 5xx, a 409
  `idempotency_in_progress` and network errors after `Retry-After` (`retries`, default 3, at most
  `max_retry_delay`, default 10 s, apart), then raises `QueuesApiError` with `status`, `code` and
  `retry_after_seconds`. In the step, what's still failing is retried by the step's retry policy,
  but another 4xx (a bad message, a missing queue, a refused permission) fails the step at once:
  `NonRetriableError` caused by the `QueuesApiError`.

Sync workflows call it the same way, without `await`. `queues.send(queue_id, messages,
idempotency_key=None)` works outside workflows too (blocking; without a key, each call gets a random
one, kept across its resends).

### Tracing

While the project's Workflows observability setting is on, EZGH sends each invocation a W3C
`traceparent` (the push request's header, or a pulled invocation's field), and `ctx.traceparent` is
that value exactly as received. It's `""` while the setting is off, so a parent-based sampler never
waits for a parent that won't arrive. The SDK has no OpenTelemetry dependency; with
`opentelemetry-api`, parent your own spans on it. Start them inside step functions: code outside
steps re-runs on every replay.

```python
from opentelemetry import trace
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

tracer = trace.get_tracer("billing")


@workflow("charge")
async def charge(ctx: Context) -> str:
    def charge_card() -> str:
        parent = TraceContextTextMapPropagator().extract({"traceparent": ctx.traceparent})
        with tracer.start_as_current_span("charge card", context=parent):
            return payments.charge(ctx.input)

    return await ctx.step.run("charge-card", charge_card)
```

### Limits and guarantees

- **Steps run at least once.** A step can run again if the process dies after it ran but before
  its result was recorded; pass `ctx.step.idempotency_key()` to APIs that deduplicate.
- **Retries:** a step that raises is retried with backoff (default 5 attempts, from 1 s doubling to
  at most 10 minutes; override per step with `retry=`, or per workflow; partial policies merge with
  the defaults). Bounds: `maxAttempts` 1–100, `maxMs` up to 24 hours, `multiplier` 1–10. Raise
  `NonRetriableError` to fail at once, or `RetryAfterError(message, delay_ms)` to pick the delay
  (0 to 24 hours), directly or as any exception's `__cause__`; an error whose `code` is
  `workflows.non_retriable` isn't retried either. Messages and stacks are cut to 8 KiB, `data` over
  64 KiB is dropped, and at most 8 levels of `__cause__` are kept.
- **Sizes:** a step's result, the workflow's output, a child's input and a callback's body are each
  at most 1 MiB of JSON (a bigger result fails the step without retries). An invocation's result is
  at most 16 MiB (push) or 8 MiB (pull): one bigger, say many large child inputs at once, fails the
  run non-retriably.
- **Parallel steps:** a run has at most 32 step invocations at once (further steps wait their
  turn), and at most 1,000 steps can start together: more fails the run (start them in batches, or
  fan out through child workflows). A run has at most 1,000 steps.
- **Callbacks:** the timeout is 1 s to a year (default 7 days). The first completion wins; the same
  body again succeeds, another body is `409 callback_completed`, after the timeout `410
  callback_expired`. Complete one with `complete_callback_url(cb.url, value)` (no API key: the
  token is the URL's only credential, so keep it private) or `Client.complete_callback(run_id,
  step_id, value)`.
- **Child workflows:** `step.invoke` runs a workflow of the same app; children nest at most 10
  deep, and each counts as one of its parent's steps. A failed child raises `ChildFailedError`
  (`workflows.child_failed`), whose `__cause__` is the child's error.
- **Timeouts:** `timeout_ms` (a step's, or the workflow's default) is at most 24 hours. A step
  can't outlive its invocation, so `serve()` refuses a workflow default longer than
  `max_invocation_seconds`; long steps need a pull worker (below).
- **Invocation time:** an invocation stops starting steps when less than max(5 s, 10 %) of its
  time is left, and EZGH invokes the workflow again.
- **Protocol 1.1** is needed for parallel steps, callbacks and child workflows; at 1.0 steps run
  one after another and `create_callback` and `invoke` fail the run with `NonRetriableError`.

### Decisions in this SDK

- **Step methods return `asyncio.Future`s, registered at call time,** not coroutines. Call order is
  registration order, so step IDs and the frontier are deterministic, and `asyncio.gather`,
  `asyncio.wait(..., return_when=FIRST_COMPLETED)` and `asyncio.as_completed` work on them directly.
- **Steps discovered together run in parallel** (protocol 1.1): when the workflow is idle, every new
  step it's waiting on is planned at once, each as its own invocation (at most 32 at once per run).
  Replay resolves recorded outcomes one at a time in the order they were recorded, so a race sees
  the same winner every time.
- **An async workflow's own code runs on an event loop of its own, in a dedicated thread,** so the
  SDK knows exactly when the workflow is idle (every task waits on a step nobody has resolved),
  however deeply its tasks nest: that decides which steps are planned together. Workflow code
  outside steps must not await objects bound to your server's loop (it shouldn't do I/O at all);
  step functions run where you'd expect: **plain step functions in a thread of their own,** so a
  long step never blocks anything (a step that outlives its `timeout_ms` is abandoned, not
  killed), and **coroutine step functions on your server's loop**. Calling `ctx.step` from inside a
  step function raises `RuntimeError`. **Sync workflows run in a dedicated thread** whose step calls
  block.
- **Cancellation is `RunCancelledError`, an `Exception`, deliberately not `asyncio.CancelledError`:**
  `except Exception` and `finally` see it, and it never cancels asyncio tasks. Its wire name is
  `CancelledError` and its code `workflows.cancelled`. An `asyncio.CancelledError` reaching the
  runtime (the process shutting down) aborts the invocation and reports nothing.
- **When an invocation ends while workflow code still waits on a step** (it suspended), the waiting
  call raises `InvocationEnded`, a `BaseException` like `GeneratorExit`: `except Exception` doesn't
  catch it, `finally` blocks run, and step calls made then raise it too. EZGH invokes the workflow
  again later and replays it.
- **Values on the wire are JSON:** `dict` (with `str` keys), `list`, `tuple` (sent as a list), `str`,
  `int`, finite `float`, `bool`, `None`, an **aware** `datetime` (sent as UTC ISO 8601 with exactly 3
  fractional digits and `Z`, like JavaScript's `toISOString`; microseconds are truncated) and a `date`
  (`YYYY-MM-DD`). Anything else (a naive `datetime`, `Decimal`, `set`, `bytes`, NaN, ...) fails the
  step with `workflows.non_retriable`. The live value is replaced by the wire value, so the live and
  replayed values are identical (a `datetime` comes back as a `str` both times, and a `StrEnum` or
  `IntEnum` as a plain `str` or `int`).
- **Invalid arguments raise `ValueError`** (names, durations, ids, policies) and wrong types
  `TypeError`; either fails the run with code `user`.
- **Every limit is the engine's, in UTF-8 bytes** (workflows-stack `internal/protocol/validate.go`):
  the SDK never sends what the engine would refuse.

## Serving (push)

`serve(workflows=[...], signing_secrets=[...])` returns an ASGI app. Run it with any ASGI server
(`uvicorn app:app`), or mount it in Starlette or FastAPI; see [examples](examples/README.md),
including AWS Lambda through an ASGI adapter.

- `signing_secrets`: the app's active `whsec_…` secrets; pass two during a rotation. `None` and empty
  entries (an unset environment variable at build time) are skipped.
- `max_invocation_seconds` (1–900, default 300): at most your platform's request time limit.
- `deployment`: recorded with each invocation; detected from `VERCEL_DEPLOYMENT_ID`,
  `AWS_LAMBDA_FUNCTION_NAME:AWS_LAMBDA_FUNCTION_VERSION` (not `$LATEST`), `K_REVISION`,
  `FLY_MACHINE_VERSION` or `RENDER_GIT_COMMIT`.
- `max_steps_per_invocation` (default 50).

Every request's `EZGH-Signature` is verified in constant time and refused when its timestamp is more
than 5 minutes from now. The invocation's deadline is local (receipt time plus the engine's budget),
so clock skew doesn't matter.

## Pull workers

A pull app's workflows run on your own machines: a `Worker` long-polls EZGH for tasks with an API
key, so it needs no public endpoint, and its steps may run for hours (heartbeats keep the
invocation alive). **A pull invocation's deadline is the longest `timeout_ms` the app's workers
announce, at least 15 minutes and at most 24 hours,** so give a workflow whose steps can run
longer than 15 minutes a `timeout_ms` that covers them (`@workflow("etl", timeout_ms=3 * 3_600_000)`).

```python
from ezgh_workflows import Worker

worker = Worker(
    api_key=os.environ["EZGH_API_KEY"],     # workflows.workers.poll (and workflows.apps.sync to announce)
    organization_id=ORG,
    project_id=PROJECT,
    app=PULL_APP_ID,                        # client.create_app("reports", kind="pull", queue="default")
    queue="default",
    workflows=[onboarding, nightly],
    concurrency=10,                         # invocations at once
    max_tasks=1,                            # tasks per poll (1-10)
)
worker.run_sync()                           # or: await worker.run(); worker.stop() from anywhere
```

- Every poll announces the worker's workflows; the app's list is what live workers announced in the
  last 10 minutes.
- While an invocation runs, the worker heartbeats every `heartbeat_interval` seconds (default 20; the
  engine's timeout is 60). When the run is cancelled, the running step's `ctx.step.cancelled()`
  turns true, so a long step can stop early, and no further step starts in that invocation.
- `stop()`, SIGTERM or SIGINT stop polling at once: a long poll in flight is aborted, and tasks it
  had already received still run. Running invocations get `shutdown_grace` seconds (default 30),
  and the engine re-dispatches anything abandoned after its heartbeat timeout.
- A poll answered 401, 403, another 4xx, a redirect or a 1xx raises `WorkflowsApiError`
  from `run()`. 429, 5xx, network errors and 2xx answers that aren't poll answers are retried
  after `max(Retry-After, backoff)`, the backoff doubling from 0.5 s to `backoff_max` with ±20 %
  jitter. `heartbeat_interval` is at most 30 s (the engine's heartbeat timeout is 60 s).
- **Dev mode:** `Worker.dev(api_key=..., organization_id=..., project_id=..., workflows=[...])`
  finds or creates the pull app `dev-<user>` with the queue of the same name, so two laptops never
  take each other's runs. The name is the same in every SDK: `$USER` (else `$USERNAME`,
  `$LOGNAME`, the OS user) lowercased, each run of other characters made one `-`, at most 58
  characters (`dev-dev` if nothing is left).

## Client

```python
from ezgh_workflows import Client, AsyncClient, WorkflowsApiError

client = Client(api_key=os.environ["EZGH_API_KEY"], organization_id=ORG, project_id=PROJECT)
created = client.create_app("orders", endpoint="https://example.com/ezgh-workflows/")
client.sync(created["app"]["id"])
run = client.start(created["app"]["id"], "onboarding", {"userId": "u_1"}, run_key="signup-u_1")
```

Apps (`create_app`, `get_app`, `list_apps`, `sync`, `rotate_secret`), runs (`start`, `get`, `cancel`,
`retry`, `output`, `input`, `step_output`, `events`), callbacks (`complete_callback`) and schedules
(`create_schedule`, `get_schedule`, `list_schedules`, `update_schedule`, `pause_schedule`,
`resume_schedule`, `delete_schedule`, `trigger_schedule`, `schedule_input`). `AsyncClient` has the same methods as
coroutines. Responses are `TypedDict`s with the API's camelCase keys. Errors raise
`WorkflowsApiError` with `status`, `code` and `retry_after_seconds`; 429, 503, `409
idempotency_in_progress` and network errors are retried after `Retry-After` (`retries`, default 3,
at most `max_retry_delay` apart), and writes are resent with the same `Idempotency-Key`.
`create_app` and `rotate_secret` aren't resent after a network error or a 5xx, because a replay
returns no secret, and they never succeed without one: they raise
`WorkflowsApiError(..., "outcome_unknown")`; check with `get_app`, and rotate the secret if you
didn't receive it. `complete_callback_url(url, value)` (also on both clients) completes a
callback by its URL without an API key.

| Credential | What it can do |
| --- | --- |
| Signing secret (`whsec_…`) | Verifies EZGH's requests to a push app. Only `serve()` uses it. |
| API key (`ezgh_…`) | Everything the key's owner may do: apps, runs, schedules, callbacks, and pull workers' polls. |
| `workflows:runs` SSO token | Run calls (start, inspect, cancel, retry, complete a callback), not app management. |
| Callback URL | Completes one callback, once. Treat it as a credential and keep it private. |

## Development

```sh
uv sync
uv run ruff check . && uv run ruff format --check .
uv run mypy
uv run pytest
scripts/conformance.sh ~/code/workflows-stack    # workflows-stack's conformance harness (push and pull)
DATABASE_URL=postgres://… scripts/e2e.sh ~/code/workflows-stack   # end to end against workflows-stack all
```

The protocol vectors in `tests/vectors` are workflows-stack's, vendored verbatim by
`scripts/sync-vectors.sh`; `.workflows-stack-rev` names the commit.

## License

Apache-2.0.
