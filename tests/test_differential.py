"""Differential test: generated programs must behave the same on the previous release.

The oracle is the previously released wheel (``tests/data/oracle_version.txt``), unpacked and
imported in a separate process, so it is a different implementation from the one under test, not
a copy of it running on new code. A program is a generated scenario (``_programs.py``); both
builds run it and print the outcome, the number of events and a digest of the full timeline. Any
difference is a behavioural change and must be listed in ``DECLARED`` with the reason, or it
fails.

Opt-in (``pytest -m differential``): it needs the released wheel, which is downloaded on first
use (or taken from ``SEEDLOOP_ORACLE_SRC``). If it cannot be obtained the tests are skipped
*with the reason*, never silently passed. ``SEEDLOOP_DIFF_PROGRAMS`` and ``SEEDLOOP_DIFF_START``
choose how many programs and from which id.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import NoReturn

import pytest

from _support import (
    DROPPED_SETTLE_STEP,
    REVERSE_CANCEL_ORDER,
    ROOT,
    Change,
    copy_src_with,
    run_script,
)

pytestmark = [pytest.mark.differential, pytest.mark.timeout(1800)]

CHILD = ROOT / "tests" / "_program_child.py"
ORACLE_VERSION = (
    (ROOT / "tests" / "data" / "oracle_version.txt").read_text(encoding="utf-8").strip()
)

# Differences that are intended, by program id: ``{12: "why this program legitimately differs"}``.
# Empty while the code under test is meant to behave exactly like the oracle.
DECLARED: dict[int, str] = {}

PROGRAMS = int(os.environ.get("SEEDLOOP_DIFF_PROGRAMS", "60"))
START = int(os.environ.get("SEEDLOOP_DIFF_START", "0"))


def _unavailable(reason: str) -> NoReturn:
    """The oracle cannot be had: skip locally, but fail where it is required (CI)."""
    if os.environ.get("SEEDLOOP_DIFF_REQUIRED"):
        pytest.fail(f"the oracle is required here but unavailable: {reason}")
    pytest.skip(reason)


def _oracle_src() -> Path:
    given = os.environ.get("SEEDLOOP_ORACLE_SRC")
    if given:
        return Path(given)
    cache = Path(tempfile.gettempdir()) / f"seedloop-oracle-{ORACLE_VERSION}"
    marker = cache / ".complete"
    if marker.exists() and marker.read_text(encoding="utf-8") == ORACLE_VERSION:
        return cache
    # Download and unpack beside the final directory, then rename: an interrupted run must not leave
    # a half-unpacked cache that later runs would trust.
    staging = Path(tempfile.mkdtemp(prefix=f"seedloop-oracle-{ORACLE_VERSION}-"))
    download = subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "download",
            f"seedloop=={ORACLE_VERSION}",
            "--no-deps",
            "--dest",
            str(staging / "wheel"),
            "--quiet",
        ],
        capture_output=True,
        text=True,
        timeout=180,
    )
    wheels = sorted((staging / "wheel").glob("*.whl")) if download.returncode == 0 else []
    if not wheels:
        shutil.rmtree(staging, ignore_errors=True)
        _unavailable(f"oracle seedloop=={ORACLE_VERSION} unavailable: {download.stderr[-300:]}")
    with zipfile.ZipFile(wheels[0]) as wheel:
        wheel.extractall(staging)
    (staging / ".complete").write_text(ORACLE_VERSION, encoding="utf-8")
    shutil.rmtree(cache, ignore_errors=True)  # only reached when the cache was missing or invalid
    try:
        staging.rename(cache)
    except OSError:  # another run won the race: use its copy
        shutil.rmtree(staging, ignore_errors=True)
    return cache


@pytest.fixture(scope="module")
def oracle() -> Iterator[Path]:
    src = _oracle_src()
    _run(src, 0)  # the oracle must run a program at all
    version = (
        subprocess.run(
            [
                sys.executable,
                "-c",
                "import seedloop; print(seedloop.__version__, seedloop.__file__)",
            ],
            capture_output=True,
            text=True,
            env={**os.environ, "PYTHONPATH": str(src)},
            timeout=60,
        )
        .stdout.strip()
        .split(maxsplit=1)
    )
    assert len(version) == 2, "the oracle could not report its version"
    assert version[0] == ORACLE_VERSION and str(src) in version[1], version
    yield src


def _run(src: Path, program: int) -> str:
    result = run_script(CHILD, str(program), env={"PYTHONPATH": str(src)}, timeout=20)
    lines = result.stdout.strip().splitlines()
    # A crashed or silent child is a failure of the harness, never a result to compare: two builds
    # that crash identically would otherwise look equal.
    assert result.returncode == 0 and lines, (
        f"program {program} did not run under {src}: exit {result.returncode}\n"
        f"{result.stderr[-400:]}"
    )
    return lines[-1]


def _differences(
    oracle_src: Path, candidate_src: Path, programs: range
) -> list[tuple[int, str, str]]:
    with ThreadPoolExecutor(max_workers=4) as pool:
        old = list(pool.map(lambda p: _run(oracle_src, p), programs))
        new = list(pool.map(lambda p: _run(candidate_src, p), programs))
    return [(p, o, n) for p, o, n in zip(programs, old, new, strict=True) if o != n]


def test_the_candidate_behaves_like_the_previous_release(oracle: Path) -> None:
    differences = _differences(oracle, ROOT / "src", range(START, START + PROGRAMS))
    unexpected = [d for d in differences if d[0] not in DECLARED]
    report = "\n".join(
        f"program {p}: oracle {o!r} vs candidate {n!r}" for p, o, n in unexpected[:5]
    )
    assert not unexpected, f"{len(unexpected)} of {PROGRAMS} programs differ:\n{report}"


@pytest.mark.parametrize(
    "defect", [DROPPED_SETTLE_STEP, REVERSE_CANCEL_ORDER], ids=lambda c: c.name
)
def test_the_harness_notices_a_planted_defect(defect: Change, tmp_path: Path) -> None:
    # Non-vacuity: the current tree with a defect planted (the extra settle step dropped, which
    # corpus, golden and the unit tests all missed; or teardown cancelling in another order) must
    # differ from the unmodified current tree on some generated program. Comparing against the
    # current tree, not the oracle, keeps this independent of any difference between the current
    # tree and the released one.
    candidate = copy_src_with(tmp_path, defect)
    for start in range(0, 300, 30):
        if _differences(ROOT / "src", candidate, range(start, start + 30)):
            return
    pytest.fail(f"no generated program noticed the planted defect ({defect.name}) in 300 tries")
