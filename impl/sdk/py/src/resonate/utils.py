from __future__ import annotations

import hashlib
import os
import traceback
import urllib
import urllib.parse
from functools import wraps
from importlib.metadata import version
from typing import Any, Callable, TypeVar
from uuid import UUID

from resonate.logging import logger

F = TypeVar("F", bound=Callable[..., Any])

T = TypeVar("T")


def string_to_uuid(string: str) -> str:
    return UUID(bytes=hashlib.sha1(string.encode("utf-8")).digest()[:16]).hex[-4:]  # noqa: S324


def exit_on_exception(func: F) -> F:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
        try:
            return func(*args, **kwargs)
        except Exception:
            v = version("resonate-sdk")
            logger.exception(
                "An unexpected error happened.\n\nPlease report this issue so we can fix it as fast a possible:\n - https://github.com/resonatehq/resonate-sdk-py/issues/new?body=%s\n\n",  # noqa: E501
                urllib.parse.quote(
                    f"Resonate (version {v}) process exited with error:\n```bash\n{traceback.format_exc()}```"  # noqa: E501
                ),
            )
            os._exit(1)

    return wrapper  # type: ignore[return-value]
