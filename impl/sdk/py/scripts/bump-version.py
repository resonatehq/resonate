"""Bump every package to one version, in lockstep.

Usage: ``bump-version.py <version>``

Sets the given version on the root package and every workspace member,
rewrites each member's exact pin on the root package, refreshes the
lockfile, and re-runs ``check-versions.py`` so a half-applied release
cannot slip through.
"""

from __future__ import annotations

import pathlib
import re
import subprocess
import sys
import tomllib


def run(cmd: list[str]) -> None:
    """Run a command, exiting with its return code on failure."""
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        sys.exit(result.returncode)


def main() -> None:
    """Apply one version across the workspace and verify the result."""
    if len(sys.argv) != 2:
        sys.stderr.write("usage: bump-version.py <version>\n")
        sys.exit(2)
    version = sys.argv[1]

    root_name = tomllib.loads(pathlib.Path("pyproject.toml").read_text())["project"][
        "name"
    ]

    # uv validates the version against PEP 440 and rewrites the TOML in place.
    run(["uv", "version", version, "--frozen"])

    # Tolerate -, _ and . interchangeably in the pin's name, as PEP 503 does.
    name = re.sub(r"[-_.]+", "[-_.]+", root_name)
    pin = re.compile(rf"({name})\s*==\s*[^\s;,\"']+")

    for member in sorted(pathlib.Path("packages").glob("*/pyproject.toml")):
        member_name = tomllib.loads(member.read_text())["project"]["name"]
        run(["uv", "version", version, "--frozen", "--package", member_name])

        text, count = pin.subn(rf"\g<1>=={version}", member.read_text())
        if count == 0:
            sys.stderr.write(f"{member}: no '{root_name}==<version>' pin to rewrite\n")
            sys.exit(1)
        member.write_text(text)

    run(["uv", "lock"])
    run([sys.executable, str(pathlib.Path(__file__).with_name("check-versions.py"))])
    sys.stdout.write(
        f"\nAll packages at {version}. Commit, then tag v{version} to release.\n"
    )


if __name__ == "__main__":
    main()
