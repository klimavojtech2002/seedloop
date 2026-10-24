"""The source distribution has the files its tests read, and none that should stay behind."""

from __future__ import annotations

import shutil
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
_COPIED_DIRS = ("src", "tests", "scripts", "docs", "examples")
_COPIED_FILES = (
    "pyproject.toml",
    "MANIFEST.in",
    "README.md",
    "LICENSE",
    "CHANGELOG.md",
    "CONTRIBUTING.md",
)
_SKIP = shutil.ignore_patterns("__pycache__", "*.pyc", "*.egg-info", "build")
# Files a working tree can hold that must never reach the archive.
_STRAYS = (
    "tests/__pycache__/stray.cpython-313.pyc",
    "scripts/__pycache__/stray.cpython-313.pyc",
    "docs/stray.pyc",
    "CLAUDE.md",
    ".dev/notes.md",
    ".private/notes.md",
    ".claude/settings.json",
)


def _files_below(folder: Path) -> set[str]:
    return {p.relative_to(folder.parent).as_posix() for p in folder.rglob("*") if p.is_file()}


@pytest.mark.meta
@pytest.mark.timeout(240)
def test_the_sdist_has_every_file_its_test_suite_reads_and_no_stray_ones(tmp_path: Path) -> None:
    # Build from a clean copy: a stale egg-info or build directory in the working tree must not
    # decide the result, and the stray files below stand in for whatever a working tree collects.
    tree = tmp_path / "tree"
    tree.mkdir()
    for name in _COPIED_DIRS:
        shutil.copytree(ROOT / name, tree / name, ignore=_SKIP)
    for name in _COPIED_FILES:
        shutil.copy2(ROOT / name, tree / name)
    for stray in _STRAYS:
        (tree / stray).parent.mkdir(parents=True, exist_ok=True)
        (tree / stray).write_text("not for the archive\n", encoding="utf-8")
    out = tmp_path / "dist"
    built = subprocess.run(
        [sys.executable, "-m", "build", "--sdist", "--outdir", str(out), str(tree)],
        capture_output=True,
        text=True,
        check=False,
        timeout=200,
    )
    assert built.returncode == 0, built.stderr[-800:]
    (archive,) = out.glob("*.tar.gz")
    with tarfile.open(archive) as tar:
        names = {name.split("/", 1)[1] for name in tar.getnames() if "/" in name}

    expected = set(_COPIED_FILES)
    for folder in ("tests", "scripts", "docs", "examples"):
        expected |= {n for n in _files_below(tree / folder) if n not in _STRAYS}
    missing = sorted(expected - names)
    assert not missing, f"missing from the sdist: {missing[:10]}"
    strays = sorted(n for n in names if n in _STRAYS or "__pycache__" in n or n.endswith(".pyc"))
    assert not strays, f"stray files in the sdist: {strays}"
    assert not any(n.startswith((".dev/", ".private/", ".claude/")) for n in names)
