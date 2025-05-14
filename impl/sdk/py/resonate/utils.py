from __future__ import annotations

import logging
import os
import traceback
import urllib.parse
from functools import wraps
from importlib.metadata import version
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable


def exit_on_exception[R, **P](component: str) -> Callable[[Callable[P, R]], Callable[P, R]]:
    def _exit_on_exception(func: Callable[P, R]) -> Callable[P, R]:
        logger = logging.getLogger(f"{__package__}.{component}")

        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            try:
                return func(*args, **kwargs)
            except Exception:
                try:
                    v = version("resonate-sdk")
                except Exception:
                    v = "unknown"

                logger.exception(
                    "An unexpected error happened.\n\nPlease report this issue so we can fix it as fast a possible:\n - https://github.com/resonatehq/resonate-sdk-py/issues/new?body=%s\n\n",
                    urllib.parse.quote(f"Resonate (version {v}) process exited with error:\n```bash\n{traceback.format_exc()}```"),
                )
                os._exit(1)

        return wrapper

    return _exit_on_exception
