"""Golden-file comparison.

The workflow the post describes: generate the output once, eyeball it, commit
it. From then on the committed file *is* the expected value, and a change to
the SDK's wire format or execution-tree shape shows up as a reviewable diff
rather than as a runtime failure against a live server.

Regenerate every golden with::

    RESONATE_UPDATE_GOLDEN=1 uv run pytest

then read the diff before committing it. That review step is the whole point --
an unread golden proves nothing.

This module has no opinion on *where* golden files live -- that differs per
suite (root ``tests/golden``, one day maybe ``packages/*/tests/golden``). Each
suite's ``conftest.py`` sets :data:`GOLDEN_DIR` once, near the top, before any
test runs::

    from pathlib import Path
    from resonate_testing import golden

    golden.GOLDEN_DIR = Path(__file__).parent / "golden"
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

#: Directory golden files are read from and written to. ``None`` until a
#: suite's ``conftest.py`` sets it -- see the module docstring.
GOLDEN_DIR: Path | None = None

_UPDATE_ENV = "RESONATE_UPDATE_GOLDEN"


def _require_golden_dir() -> Path:
    if GOLDEN_DIR is None:
        msg = (
            "resonate_testing.golden.GOLDEN_DIR is not set. Set it once, near "
            "the top of the suite's conftest.py, e.g.:\n\n"
            "    from pathlib import Path\n"
            "    from resonate_testing import golden\n"
            '    golden.GOLDEN_DIR = Path(__file__).parent / "golden"\n'
        )
        raise RuntimeError(msg)
    return GOLDEN_DIR


def golden_path(name: str) -> Path:
    """Absolute path of the golden file called ``name``."""
    return _require_golden_dir() / name


def assert_golden(name: str, actual: str) -> None:
    """Compare ``actual`` against the committed golden ``name``.

    Fails directly rather than returning a result, so a test reads as one line.
    A missing golden is written and then failed: the first run records the
    output, and the failure is the prompt to go and read it.
    """
    path = golden_path(name)
    normalized = actual if actual.endswith("\n") else actual + "\n"

    if os.environ.get(_UPDATE_ENV):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(normalized, encoding="utf-8", newline="\n")
        return

    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(normalized, encoding="utf-8", newline="\n")
        msg = (
            f"golden {name!r} did not exist and has been written to {path}. "
            f"Read it, confirm it is correct, and commit it."
        )
        raise AssertionError(msg)

    expected = path.read_text(encoding="utf-8")
    if expected != normalized:
        msg = (
            f"golden {name!r} mismatch.\n"
            f"--- expected ({path})\n{expected}\n"
            f"--- actual\n{normalized}\n"
            f"If the change is intended, re-run with {_UPDATE_ENV}=1 and "
            f"review the diff."
        )
        raise AssertionError(msg)
