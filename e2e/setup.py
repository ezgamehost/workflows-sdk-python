"""Creates the e2e push app and pull app through the Client (which retries the cold engine's 503s
with the same Idempotency-Key) and prints "<appId> <signingSecret> <pullAppId>"."""

import os
import time

from ezgh_workflows import Client


def main() -> None:
    client = Client(
        api_key=os.environ["E2E_API_KEY"],
        organization_id=os.environ["E2E_ORG"],
        project_id=os.environ["E2E_PROJECT"],
        base_url=os.environ.get("E2E_API", "http://127.0.0.1:6100"),
        retries=90,
        max_retry_delay=2,
    )
    stamp = int(time.time() * 1000)
    created = client.create_app(f"e2e-{stamp}", endpoint=os.environ.get("E2E_APP", "http://127.0.0.1:3200/"))
    secret = created["signingSecret"]
    if not secret:
        raise SystemExit("create_app returned no signing secret")
    pull = client.create_app(f"e2e-pull-{stamp}", kind="pull", queue="e2e", partitions=2)
    print(created["app"]["id"], secret, pull["app"]["id"])


if __name__ == "__main__":
    main()
