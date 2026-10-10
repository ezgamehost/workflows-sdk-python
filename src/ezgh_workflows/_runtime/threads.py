"""Fresh daemon threads for blocking work: never the event loop's shared default executor, which
user code fills (asyncio.to_thread) and which would starve the SDK's heartbeats and checkpoints."""

import asyncio
import contextlib
import contextvars
import threading
from collections.abc import Callable
from typing import Any, cast


def run_in_thread(fn: Callable[[], Any], name: str = "ezgh-workflows-step") -> "asyncio.Future[Any]":
    """fn in a fresh daemon thread with the caller's contextvars. A step that outlives its timeout
    is abandoned, not killed, and never holds a slot of a shared executor."""
    loop = asyncio.get_running_loop()
    fut: asyncio.Future[Any] = loop.create_future()
    ctx = contextvars.copy_context()

    def target() -> None:
        ok, value = True, None
        try:
            value = ctx.run(fn)
        except BaseException as e:
            ok, value = False, e if not isinstance(e, StopIteration) else RuntimeError(str(e))

        def settle() -> None:
            if fut.done():
                return
            if ok:
                fut.set_result(value)
            else:
                fut.set_exception(cast(BaseException, value))

        with contextlib.suppress(RuntimeError):  # the loop is closed: nobody is waiting any more
            loop.call_soon_threadsafe(settle)

    threading.Thread(target=target, name=name, daemon=True).start()
    return fut
