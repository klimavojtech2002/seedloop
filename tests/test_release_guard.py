"""The release guard is the one release-critical decision, so it is tested like everything else.

These drive the script as the workflow does — the command-line contract — so what is proven here is
exactly what runs in CI: the exit code (a tag/version mismatch must fail the release), the routing
(pre-releases to TestPyPI, finals to PyPI), and the GITHUB_OUTPUT line a workflow step reads.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "release_guard.py"


def _run(*args: str, github_output: Path | None = None) -> subprocess.CompletedProcess[str]:
    # Inherit the real environment (Windows needs SystemRoot/PATH to start Python) but control
    # GITHUB_OUTPUT ourselves, so a stray value from a CI runner cannot leak into these tests.
    env = dict(os.environ)
    env.pop("GITHUB_OUTPUT", None)
    if github_output is not None:
        env["GITHUB_OUTPUT"] = str(github_output)
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        capture_output=True,
        text=True,
        env=env,
        timeout=30,
    )


# tag, built version, expected channel — routing follows the version, not the tag spelling.
_ROUTES = [
    ("v1.2.3", "1.2.3", "pypi"),
    ("v1.2.3rc1", "1.2.3rc1", "testpypi"),
    ("v1.2.3a1", "1.2.3a1", "testpypi"),
    ("v1.2.3b1", "1.2.3b1", "testpypi"),
    ("v1.2.3.dev1", "1.2.3.dev1", "testpypi"),
    ("v1.2.3.post1", "1.2.3.post1", "pypi"),  # a .post is a real release, not a pre-release
    ("v1.2.3rc1.post1", "1.2.3rc1.post1", "testpypi"),  # a .post *of* an rc is still a pre-release
    ("v1!2.3", "1!2.3", "pypi"),  # an epoch is a final release
    ("v0.3.2rc1", "0.3.2rc1", "testpypi"),  # the planned dry-run tag
    ("1.2.3", "1.2.3", "pypi"),  # a missing leading `v` is tolerated
]


@pytest.mark.parametrize(("tag", "version", "channel"), _ROUTES)
def test_matched_tag_routes_by_prerelease_status(tag: str, version: str, channel: str) -> None:
    result = _run(tag, version)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == f"channel={channel}"


# tag, built version — the tag names a different release than the build produced.
_MISMATCHES = [
    ("v1.2.4", "1.2.3"),  # wrong number
    ("v1.2.3", "1.2.3rc1"),  # final tag on an rc build -> would push an rc to production
    ("v1.2.3rc1", "1.2.3"),  # rc tag on a final build -> would ship the release only to TestPyPI
    ("v1.2.3rc2", "1.2.3rc1"),  # wrong rc number
]


@pytest.mark.parametrize(("tag", "version"), _MISMATCHES)
def test_mismatch_fails_the_release(tag: str, version: str) -> None:
    result = _run(tag, version)
    assert result.returncode == 1
    assert result.stdout.strip() == ""  # a rejected release prints nothing routable to stdout
    assert version in result.stderr and tag in result.stderr


@pytest.mark.parametrize("bad", ["vfoo", "v1.2.x", "vlatest"])
def test_invalid_version_fails(bad: str) -> None:
    result = _run(bad, "1.2.3")
    assert result.returncode == 1
    assert "PEP 440" in result.stderr


def test_wrong_argument_count_is_a_usage_error() -> None:
    assert _run("v1.2.3").returncode == 2
    assert _run().returncode == 2


def test_channel_is_appended_to_github_output(tmp_path: Path) -> None:
    # GitHub accumulates every step's outputs in one file, so the guard must append, not overwrite:
    # pre-seed a prior step's line and assert it survives alongside ours.
    out = tmp_path / "gh_output"
    out.write_text("existing=kept\n", encoding="utf-8")
    result = _run("v1.2.3rc1", "1.2.3rc1", github_output=out)
    assert result.returncode == 0
    assert out.read_text(encoding="utf-8").splitlines() == ["existing=kept", "channel=testpypi"]
