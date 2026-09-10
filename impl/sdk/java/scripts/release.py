"""New release."""

from __future__ import annotations

import pathlib
import re
import webbrowser
from urllib.parse import urlencode

SOURCE = "https://github.com/resonatehq/resonate-sdk-java"
VERSION_RE = re.compile(r'^version\s*=\s*"([^"]+)"', re.MULTILINE)


def read_version(cwd: pathlib.Path) -> str:
    """Read `version = "x.y.z"` from build.gradle.kts."""
    text = cwd.joinpath("build.gradle.kts").read_text(encoding="utf-8")
    match = VERSION_RE.search(text)
    if not match:
        raise RuntimeError("version not found in build.gradle.kts")
    return match.group(1)


def main() -> None:
    """Prepare new release."""
    version = read_version(cwd=pathlib.Path.cwd())
    params = urlencode(query={"title": f"v{version}", "tag": f"v{version}"})
    webbrowser.open_new_tab(url=f"{SOURCE}/releases/new?{params}")


if __name__ == "__main__":
    main()
