"""Enforce lockstep versioning.

Every workspace member must carry the same version as the root package, pin
it exactly, match its ``requires-python``, and resolve it from the workspace
during development. With ``--tag v<version>`` (passed by CD from the release
tag), additionally require the root version to match the tag -- catching a
release cut without running ``just bump``.

This assumes a two-tier workspace: the root SDK plus members that each
depend directly on it. A future member that depends on another member (or
on nothing) needs these checks -- and bump-version.py's pin rewrite --
extended first.
"""

from __future__ import annotations

import pathlib
import re
import sys
import tomllib

_PIN = re.compile(r"([A-Za-z0-9._-]+)(?:\[[^]]*\])?\s*==\s*([^\s;]+)\s*(?:;.*)?$")


def _normalize(name: str) -> str:
    """Normalize a distribution name per PEP 503."""
    return re.sub(r"[-_.]+", "-", name).lower()


def main() -> None:
    """Check member versions, cross-pins, and sources against the root package."""
    root = tomllib.loads(pathlib.Path("pyproject.toml").read_text())["project"]
    root_name, root_version = root["name"], root["version"]

    errors = []

    args = sys.argv[1:]
    if args and args[0] == "--tag":
        expected = args[1].removeprefix("v")
        if root_version != expected:
            errors.append(f"pyproject.toml: version {root_version} != tag {expected}")

    for member in sorted(pathlib.Path("packages").glob("*/pyproject.toml")):
        data = tomllib.loads(member.read_text())
        project = data["project"]

        if project["version"] != root_version:
            errors.append(
                f"{member}: version {project['version']} != {root_name} {root_version}"
            )

        # An exact pin means the pair always installs together, so a member
        # claiming support for a Python the root has dropped is a lie in the
        # published metadata.
        if project.get("requires-python") != root.get("requires-python"):
            errors.append(
                f"{member}: requires-python {project.get('requires-python')!r} "
                f"!= {root_name} {root.get('requires-python')!r}"
            )

        pins = [_PIN.match(d.strip()) for d in project.get("dependencies", [])]
        if not any(
            m and _normalize(m[1]) == _normalize(root_name) and m[2] == root_version
            for m in pins
        ):
            errors.append(
                f"{member}: missing exact pin '{root_name}=={root_version}' in dependencies"
            )

        # Without a workspace source, uv quietly resolves the root package
        # from PyPI instead of the local tree, and the member's tests run
        # against already-released code.
        sources = data.get("tool", {}).get("uv", {}).get("sources", {})
        source = next(
            (v for k, v in sources.items() if _normalize(k) == _normalize(root_name)),
            None,
        )
        if not (isinstance(source, dict) and source.get("workspace") is True):
            errors.append(
                f"{member}: missing '{root_name} = {{ workspace = true }}' in [tool.uv.sources]"
            )

    for error in errors:
        sys.stderr.write(f"{error}\n")
    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
