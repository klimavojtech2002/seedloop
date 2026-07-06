"""ADR-0011's intra-version contract, pinned: the canonical seed's timeline must not silently drift.

`scripts/pin_replay.py` checks a canonical scenario's timeline against a committed golden. Running
it
from the suite puts the pin on the CI matrix (3 OS x CPython 3.12-3.14) — the cross-platform proof
that a seeded timeline is bit-identical everywhere — and proves the check is non-vacuous: a changed
golden goes red, so a real drift could not pass unnoticed.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PIN = ROOT / "scripts" / "pin_replay.py"
GOLDEN = ROOT / "tests" / "data" / "replay_golden.txt"


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(PIN), *args],
        capture_output=True,
        text=True,
        cwd=ROOT,
        timeout=60,
    )


def test_canonical_timeline_matches_the_golden() -> None:
    # The real pin: on every supported OS/Python the canonical seed reproduces the committed run.
    result = _run()
    assert result.returncode == 0, result.stderr


def test_a_changed_timeline_is_caught(tmp_path: Path) -> None:
    # Non-vacuity: tamper one line of a copy of the golden; the pin must go red, byte-exact (LF).
    with open(GOLDEN, encoding="utf-8", newline="") as f:
        lines = f.read().splitlines()
    lines[-1] = "(999.0, 'tampered')"
    tampered = tmp_path / "golden.txt"
    with open(tampered, "w", encoding="utf-8", newline="") as f:
        f.write("\n".join(lines) + "\n")

    result = _run("--golden", str(tampered))
    assert result.returncode == 1
    assert "timeline-affecting change" in result.stderr
