"""Error handling across the Resonate durability boundary.

This example shows how a failure raised in a durable function reaches whoever
awaits it.

When a durable function like ``bar`` fails, its exception does not propagate
in-process the way a normal Python call would. Resonate **encodes the failure,
writes it to a durable promise, and later decodes it** for whoever awaits the
result. That awaiter may be the same process, a different worker (via
``ctx.rpc``), or a process that recovered after a crash and never saw the
original exception at all.

The wire form carries two things: a transport-safe ``message`` string every
SDK can read, and a best-effort pickle of the original exception. So a Python
awaiter that shares the runtime and can import the class gets the **original
type and attributes back** -- ``foo`` below catches ``UsernameTakenError`` and
``ValueError`` directly. When the pickle cannot round-trip (a non-Python
producer, an unimportable class, an unpicklable payload), the awaiter falls
back to a plain :class:`ApplicationError` carrying the message -- which is why
``foo`` keeps an ``ApplicationError`` arm as the cross-boundary fallback.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import time
from typing import TYPE_CHECKING, Literal

from resonate.error import ApplicationError
from resonate.resonate import Resonate
from resonate.retry import Never

if TYPE_CHECKING:
    from resonate.context import Context


class UsernameTakenError(Exception):
    """Raised when a username is already registered.

    Kept as a plain ``Exception`` subclass so it round-trips through the codec's
    pickle cleanly. An exception whose ``__init__`` *reformats* its argument
    (e.g. ``super().__init__(f"PREFIX: {msg}")``) would double-format on
    unpickle -- pickle reconstructs via ``cls(*self.args)``, and ``args`` here
    already holds the formatted string. Discriminating by *type* (below) makes
    such message munging unnecessary anyway.
    """


def bar(ctx: Context, username: str, age: int) -> str:
    existing_users = ["admin", "coder123", "python_fan"]

    if age < 0:
        msg = "Age cannot be negative."
        raise ValueError(msg)

    if username.lower() in existing_users:
        msg = f"The username '{username}' is already in use."
        raise UsernameTakenError(msg)

    return f"Registration successful for {username}!"


async def foo(
    ctx: Context, username: str, age: int, mode: Literal["run", "rpc"]
) -> None:
    try:
        v = await (
            ctx.run(bar, username, age)
            if mode == "run"
            else ctx.rpc("bar", username, age)
        )
        print(f"🎉 Success: {v}")
    except UsernameTakenError as e:
        # The original domain type is reconstructed across the boundary, so we
        # can catch it directly instead of parsing a message prefix.
        print(f"⚠️ Business Rule Error: {e}")
        print("💡 Suggestion: Please try adding numbers to your username.")
    except ValueError as e:
        print(f"💥 Validation Error: {e}")
    except ApplicationError as e:
        msg = """ApplicationError is a fallback error.
        Used when the raised error cannot be deserialized by pickle.
        Not applicable in this example."""
        raise AssertionError(msg) from e


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("run", "rpc"), default="run")
    parser.add_argument(
        "--error",
        choices=("none", "taken", "value"),
        default="none",
        help="The type of error scenario you want to trigger",
    )
    args = parser.parse_args()

    if args.error == "taken":
        username, age = "admin", 25  # Triggers UsernameTakenError
    elif args.error == "value":
        username, age = "alice", -5  # Triggers ValueError
    else:
        username, age = "alice", 25  # Successful path

    url = os.environ.get("RESONATE_URL", "http://localhost:8001")
    r = Resonate(url=url, retry_policy=Never())

    # Registering both tasks so RPC coordination works flawlessly
    r.register(foo)
    r.register(bar)

    try:
        id = f"error-handling-{time.time_ns()}"
        await r.run(id, foo, username, age, args.mode).result()

    finally:
        await r.stop()


if __name__ == "__main__":
    asyncio.run(main())
