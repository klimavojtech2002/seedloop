"""Release-channel guard: reconcile the git tag with the built version, then pick the PyPI index.

A tag-triggered publish workflow decides two things from two different inputs — the *version* (from
the built artifact) and the *channel* (which index to publish to). Left uncoupled, a release
can ship the wrong version or reach the wrong index: an rc published to production, or a final
published only to TestPyPI while the rehearsal looks green. This guard couples them. It fails unless
the tag names exactly the built version, then routes on the version's own pre-release status — the
single source of truth — instead of on how the tag happens to be spelled.

Both sides parse as PEP 440 versions, so cosmetic spellings compare equal (PyPI normalises too). A
pre-release (a/b/rc, or a .dev segment) goes to TestPyPI; a final or .post release goes to PyPI.

Usage:
    python scripts/release_guard.py <tag> <built-version>

Prints `channel=pypi` or `channel=testpypi` to stdout, and appends the same line to $GITHUB_OUTPUT
when set (so a workflow step can expose it as an output). Exits non-zero with a message when the tag
and the built version do not name the same release, or when either is not a valid PEP 440 version.
"""

from __future__ import annotations

import os
import sys

from packaging.version import InvalidVersion, Version


def resolve_channel(tag: str, built_version: str) -> str:
    """Return the target index (`"pypi"` or `"testpypi"`) for a tagged release.

    `tag` is the git ref name; a single leading `v` is stripped. Raises `ValueError` if the tag and
    the built version do not parse to the same PEP 440 version, or if either is not a valid version.
    """
    name = tag[1:] if tag.startswith("v") else tag
    try:
        tag_version = Version(name)
        built = Version(built_version)
    except InvalidVersion as exc:
        raise ValueError(f"not a PEP 440 version: {exc}") from exc
    if tag_version != built:
        raise ValueError(
            f"tag {tag!r} names version {name!r} but the build produced {built_version!r}; "
            "bump __version__ to match the tag, or tag the right commit, before releasing."
        )
    return "testpypi" if built.is_prerelease else "pypi"


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: release_guard.py <tag> <built-version>", file=sys.stderr)
        return 2
    tag, built_version = argv
    try:
        channel = resolve_channel(tag, built_version)
    except ValueError as exc:
        print(f"release guard: {exc}", file=sys.stderr)
        return 1
    line = f"channel={channel}"
    print(line)
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
