# ezgh-workflows

The Python SDK for [EZGH Workflows](https://docs.ezghcloud.com/workflows): durable workflows written
as code, with steps, sleeps, retries, callbacks, child workflows, parallel steps and schedules.

**Status: in development.** This repository is being built; the sections below say what exists so
far. It isn't published to PyPI yet.

- Python 3.12 and 3.13; no runtime dependencies (the standard library only).
- Speaks the EZGH Workflows protocol `1.0` and `1.1`.

## What's here

- `ezgh_workflows.protocol`: the wire protocol's helpers (signatures, step IDs, the value codec,
  callback tokens), checked against workflows-stack's protocol vectors (`tests/vectors`, vendored
  verbatim by `scripts/sync-vectors.sh`; `.workflows-stack-rev` names the commit).

## Development

```sh
uv sync
uv run ruff check . && uv run ruff format --check .
uv run mypy
uv run pytest
```

## License

Apache-2.0.
