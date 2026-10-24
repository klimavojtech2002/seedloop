"""The coverage check compares a combined report with per-file floors, and only lets floors rise.

The script is driven the way CI drives it, so what is proven here is what runs there: the exit
code (a file under its floor, a new file without a floor, or a floor for a file that is gone must
fail) and the messages a person reads.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "check_coverage.py"
FLOORS = ROOT / "scripts" / "coverage_floors.txt"


def _run(report: Path, floors: Path, *flags: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), str(report), str(floors), *flags],
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )


def _report(tmp_path: Path, percents: dict[str, float]) -> Path:
    path = tmp_path / "coverage.json"
    files = {name: {"summary": {"percent_covered": value}} for name, value in percents.items()}
    path.write_text(json.dumps({"meta": {}, "files": files, "totals": {}}), encoding="utf-8")
    return path


def _floors(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "floors.txt"
    path.write_text(text, encoding="utf-8")
    return path


def test_a_file_at_its_floor_passes(tmp_path: Path) -> None:
    result = _run(_report(tmp_path, {"src/a.py": 97.0}), _floors(tmp_path, "src/a.py 97\n"))
    assert result.returncode == 0, result.stdout + result.stderr


def test_a_fractional_percentage_counts_for_its_whole_part(tmp_path: Path) -> None:
    result = _run(_report(tmp_path, {"src/a.py": 96.875}), _floors(tmp_path, "src/a.py 96\n"))
    assert result.returncode == 0, result.stdout + result.stderr


def test_a_file_under_its_floor_fails_and_is_named(tmp_path: Path) -> None:
    result = _run(_report(tmp_path, {"src/a.py": 95.9}), _floors(tmp_path, "src/a.py 96\n"))
    assert result.returncode != 0
    assert "src/a.py" in result.stdout + result.stderr
    assert "95.9" in result.stdout + result.stderr
    assert "96" in result.stdout + result.stderr


def test_a_file_well_above_its_floor_passes_but_says_to_raise_it(tmp_path: Path) -> None:
    result = _run(_report(tmp_path, {"src/a.py": 99.4}), _floors(tmp_path, "src/a.py 95\n"))
    assert result.returncode == 0, result.stdout + result.stderr
    assert "raise" in result.stdout.lower()
    assert "src/a.py" in result.stdout


def test_a_file_just_above_its_floor_does_not_ask_for_a_raise(tmp_path: Path) -> None:
    result = _run(_report(tmp_path, {"src/a.py": 96.9}), _floors(tmp_path, "src/a.py 96\n"))
    assert result.returncode == 0
    assert "raise" not in result.stdout.lower()


def test_a_measured_file_without_a_floor_fails(tmp_path: Path) -> None:
    report = _report(tmp_path, {"src/a.py": 100.0, "src/new.py": 80.0})
    result = _run(report, _floors(tmp_path, "src/a.py 100\n"))
    assert result.returncode != 0
    assert "src/new.py" in result.stdout + result.stderr
    assert "no floor" in (result.stdout + result.stderr).lower()


def test_a_floor_for_a_file_that_is_not_measured_fails(tmp_path: Path) -> None:
    result = _run(
        _report(tmp_path, {"src/a.py": 100.0}), _floors(tmp_path, "src/a.py 100\nsrc/gone.py 90\n")
    )
    assert result.returncode != 0
    assert "src/gone.py" in result.stdout + result.stderr


def test_windows_separators_in_the_report_are_read_as_slashes(tmp_path: Path) -> None:
    result = _run(
        _report(tmp_path, {"src\\pkg\\a.py": 97.0}), _floors(tmp_path, "src/pkg/a.py 97\n")
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_comments_and_blank_lines_in_the_floors_are_ignored(tmp_path: Path) -> None:
    floors = _floors(tmp_path, "# a note\n\nsrc/a.py 90  # trailing note\n")
    result = _run(_report(tmp_path, {"src/a.py": 90.0}), floors)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("line", ["src/a.py", "src/a.py ninety", "src/a.py 101", "src/a.py 90 91"])
def test_a_malformed_floor_line_fails(tmp_path: Path, line: str) -> None:
    result = _run(_report(tmp_path, {"src/a.py": 90.0}), _floors(tmp_path, line + "\n"))
    assert result.returncode != 0
    assert "floors" in (result.stdout + result.stderr).lower()


def test_raising_the_floors_adds_new_files_and_never_lowers_one(tmp_path: Path) -> None:
    report = _report(tmp_path, {"src/a.py": 99.2, "src/b.py": 80.5, "src/c.py": 70.0})
    floors = _floors(tmp_path, "# header kept\nsrc/a.py 95\nsrc/b.py 90\n")
    result = _run(report, floors, "--raise")
    assert result.returncode == 0, result.stdout + result.stderr
    lines = floors.read_text(encoding="utf-8").splitlines()
    assert lines[0] == "# header kept"
    entries = dict(line.split() for line in lines if line and not line.startswith("#"))
    assert entries == {"src/a.py": "99", "src/b.py": "90", "src/c.py": "70"}  # b is not lowered


def test_the_committed_floors_name_exactly_the_files_of_the_package() -> None:
    names = {
        line.split()[0]
        for line in FLOORS.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    }
    package = {p.relative_to(ROOT).as_posix() for p in (ROOT / "src" / "seedloop").rglob("*.py")}
    assert names == package
