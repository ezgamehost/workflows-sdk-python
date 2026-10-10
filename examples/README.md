# Examples

`serve()` returns a plain ASGI app, so it runs on any ASGI server and mounts into any ASGI framework.
None of these frameworks are dependencies of `ezgh-workflows`.

- **uvicorn:** `uvicorn app:app`, where `app = serve(workflows=[...], signing_secrets=[...])`.
- **Starlette:** `Mount("/ezgh-workflows", app=serve(...))` ([starlette_mount.py](starlette_mount.py)).
- **FastAPI:** `app.mount("/ezgh-workflows", serve(...))` ([fastapi_app.py](fastapi_app.py)). Register
  `https://<your host>/ezgh-workflows/` as the app's endpoint.
- **AWS Lambda:** wrap the ASGI app with an ASGI-to-Lambda adapter (for example Mangum:
  `handler = Mangum(serve(...), lifespan="off")`) behind a function URL or API Gateway. Set
  `max_invocation_seconds` at most the function's timeout.

**`max_invocation_seconds` must not exceed your platform's request time limit** (Lambda's function
timeout, Cloud Run's request timeout, your load balancer's idle timeout). EZGH sets every
invocation's deadline inside it, and the SDK ends an invocation early, checkpointed, when its
deadline approaches; a platform that cuts the request first loses the step in flight.
