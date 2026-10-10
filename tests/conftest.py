import json
from pathlib import Path
from typing import Any

VECTORS = Path(__file__).parent / "vectors"


def vector(name: str) -> Any:
    return json.loads((VECTORS / f"{name}.json").read_text(encoding="utf-8"))
