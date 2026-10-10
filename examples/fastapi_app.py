"""A FastAPI app that serves EZGH Workflows at /ezgh-workflows and starts runs from a route."""

import os
from typing import Any

from fastapi import FastAPI

from ezgh_workflows import AsyncClient, Context, SyncContext, serve, workflow


@workflow("onboarding", retry={"maxAttempts": 3})
async def onboarding(ctx: Context) -> dict[str, Any]:
    user = await ctx.step.run("load-user", lambda: {"id": ctx.input["userId"]})
    await ctx.step.sleep("cooldown", "1h")
    return {"user": user}


@workflow("nightly")
def nightly(ctx: SyncContext) -> int:
    a, b = ctx.step.parallel(("part-a", lambda: 1), ("part-b", lambda: 2))
    return int(a + b)


app = FastAPI()
app.mount(
    "/ezgh-workflows",
    serve(workflows=[onboarding, nightly], signing_secrets=[os.environ.get("EZGH_WORKFLOWS_SIGNING_SECRET")]),
)
client = AsyncClient(
    api_key=os.environ.get("EZGH_API_KEY", ""),
    organization_id=os.environ.get("EZGH_ORGANIZATION_ID", ""),
    project_id=os.environ.get("EZGH_PROJECT_ID", ""),
)


@app.post("/signups/{user_id}")
async def signup(user_id: str) -> dict[str, str]:
    run = await client.start(
        os.environ["EZGH_WORKFLOWS_APP_ID"], "onboarding", {"userId": user_id}, run_key=f"signup-{user_id}"
    )
    return {"runId": run["id"]}
