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
    """Vercel's deployment ID, AWS Lambda's function:version (but not $LATEST), Cloud Run's
    revision, Fly.io's machine version or Render's commit, in that order."""
    try:
        env = os.environ
        if env.get("VERCEL_DEPLOYMENT_ID"):
            return clean_deployment(env["VERCEL_DEPLOYMENT_ID"])
        fn, ver = env.get("AWS_LAMBDA_FUNCTION_NAME"), env.get("AWS_LAMBDA_FUNCTION_VERSION")
        if fn and ver and ver != "$LATEST":
            return clean_deployment(f"{fn}:{ver}")
        for k in ("K_REVISION", "FLY_MACHINE_VERSION", "RENDER_GIT_COMMIT"):
            if env.get(k):
                return clean_deployment(env[k])
    except Exception:
        return None
    return None
