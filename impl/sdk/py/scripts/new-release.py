"""New release."""

from __future__ import annotations

import pathlib
import webbrowser
from typing import Any
from urllib.parse import urlencode

import tomli


def project_project(cwd: pathlib.Path) -> dict[str, Any]:
    """Read `pyproject.toml` project."""
    pyproject = tomli.loads(
        cwd.joinpath(
            "pyproject.toml",
        ).read_text(encoding="utf-8"),
    )

    return pyproject["project"]


def main() -> None:
    """Prepare new release."""
    cwd = pathlib.Path().cwd()
    project = project_project(cwd=cwd)
    version = project["version"]
    source = project["urls"]["Source"]
    params = urlencode(
        query={
            "title": f"v{version}",
            "tag": f"v{version}",
        },
    )
    webbrowser.open_new_tab(url=f"{source}/releases/new?{params}")


if __name__ == "__main__":
    main()
