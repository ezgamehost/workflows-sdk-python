"""Deployment IDs from the platform's environment (design, "Runs continue on whatever code is deployed")."""

import os

from .protocol._bytes import truncate_utf8

MAX_DEPLOYMENT_BYTES = 256  # validate.go MaxDeploymentLen


def clean_deployment(v: str | None) -> str | None:
    """A deployment label as the engine stores it: no NUL, at most 256 bytes; empty is none."""
    if not v:
        return None
    return truncate_utf8(v.replace("\u0000", ""), MAX_DEPLOYMENT_BYTES) or None


def detect_deployment() -> str | None:
    """Vercel's deployment ID, AWS Lambda's function:version, or Cloud Run's revision."""
    try:
        env = os.environ
        lam = env.get("AWS_LAMBDA_FUNCTION_NAME"), env.get("AWS_LAMBDA_FUNCTION_VERSION")
        v = (
            env.get("VERCEL_DEPLOYMENT_ID")
            or (f"{lam[0]}:{lam[1]}" if lam[0] and lam[1] else None)
            or env.get("K_REVISION")
        )
    except Exception:
        return None
    return clean_deployment(v)
