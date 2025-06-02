from __future__ import annotations

import logging
import os
import threading
import traceback
import urllib.parse
from functools import wraps
from importlib.metadata import version
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)


def exit_on_exception[**P, R](func: Callable[P, R]) -> Callable[P, R]:
    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            body = f"""
An exception occurred in the resonate python sdk.

**Version**
```
{resonate_version()}
```

**Thread**
```
{threading.current_thread().name}
```

**Exception**
```
{e!r}
```

**Stacktrace**
```
{traceback.format_exc()}
```

**Additional context**
Please provide any additional context that might help us debug this issue.
"""

            format = """
Resonate encountered an unexpected exception and had to shut down.

📦 Version:     %s
🧵 Thread:      %s
❌ Exception:   %s
📄 Stacktrace:
─────────────────────────────────────────────────────────────────────
%s
─────────────────────────────────────────────────────────────────────

🔗 Please help us make resonate better by reporting this issue:
https://github.com/resonatehq/resonate-sdk-py/issues/new?body=%s
"""
            logger.critical(
                format,
                resonate_version(),
                threading.current_thread().name,
                repr(e),
                traceback.format_exc(),
                urllib.parse.quote(body),
            )

            # Exit the process with a non-zero exit code, this kills all
            # threads
            os._exit(1)

    return wrapper


def resonate_version() -> str:
    try:
        return version("resonate-sdk")
    except Exception:
        return "unknown"


def format_args_and_kwargs(args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
    parts = [repr(arg) for arg in args]
    parts += [f"{k}={v!r}" for k, v in kwargs.items()]
    return ", ".join(parts)


def truncate(s: str, n: int) -> str:
    if len(s) > n:
        return s[:n] + "..."
    return s
