"""Deadline propagates from ASGI request context into synchronous worker threads."""

import contextvars, time

DEADLINE = contextvars.ContextVar("oa_deadline", default=None)


def remaining(default):
    deadline = DEADLINE.get()
    if deadline is None:
        return default
    value = deadline - time.monotonic()
    if value <= 0:
        raise TimeoutError("request deadline exceeded")
    return min(default, value)
