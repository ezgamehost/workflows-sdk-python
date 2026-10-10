"""Mount EZGH Workflows into an existing Starlette application (FastAPI: app.mount works the same)."""

import os

from starlette.applications import Starlette
from starlette.routing import Mount

from ezgh_workflows import Context, serve, workflow


@workflow("hello")
async def hello(ctx: Context) -> str:
    return await ctx.step.run("greet", lambda: f"hello {ctx.input['name']}")


app = Starlette(
    routes=[
        Mount(
            "/ezgh-workflows",
            app=serve(workflows=[hello], signing_secrets=[os.environ.get("EZGH_WORKFLOWS_SIGNING_SECRET")]),
        )
    ]
)
