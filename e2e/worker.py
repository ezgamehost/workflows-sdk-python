"""The e2e pull worker: runs e2e-pulled for the pull app e2e/setup.py created."""

import os

from ezgh_workflows import Context, Worker, workflow


@workflow("e2e-pulled")
async def pulled(ctx: Context) -> int:
    return int(await ctx.step.run("square", lambda: ctx.input["n"] ** 2))


def main() -> None:
    Worker(
        api_key=os.environ["E2E_API_KEY"],
        organization_id=os.environ["E2E_ORG"],
        project_id=os.environ["E2E_PROJECT"],
        app=os.environ["E2E_PULL_APP_ID"],
        queue="e2e",
        workflows=[pulled],
        live_url=os.environ.get("E2E_LIVE", "http://127.0.0.1:6150"),
        max_tasks=4,
        heartbeat_interval=2.0,
        backoff_max=1.0,
        shutdown_grace=2.0,
    ).run_sync()


if __name__ == "__main__":
    main()
