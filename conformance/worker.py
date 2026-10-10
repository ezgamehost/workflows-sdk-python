"""The conformance app's workflows on a pull worker, for the harness's pull scenarios
(workflows-stack protocol/conformance/workflows.md, "Pull scenarios")."""

import os

from ezgh_workflows import Worker

from .app import WORKFLOWS

ORGANIZATION = "0199a3c2-5b1e-7d40-9f3a-2c8e6b1d4f70"
PROJECT = "0199a3c2-6c2f-7e51-8a4b-3d9f7c2e5a81"
PULL_APP = os.environ.get("CONFORMANCE_PULL_APP", "0199a3c2-7d30-7f62-9b5c-4eaf8d3f6b92")


def main() -> None:
    Worker(
        api_key="conformance-key",
        organization_id=ORGANIZATION,
        project_id=PROJECT,
        app=PULL_APP,
        queue="conformance",
        workflows=WORKFLOWS,
        live_url=os.environ["CONFORMANCE_LIVE"],
        max_tasks=4,
        concurrency=4,
        heartbeat_interval=0.3,
        backoff_max=1.0,
        deployment="conformance",
    ).run_sync()


if __name__ == "__main__":
    main()
