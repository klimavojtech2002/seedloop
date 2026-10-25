"""Compare a combined coverage report with the committed per-file floors.

    python scripts/check_coverage.py coverage.json scripts/coverage_floors.txt
    python scripts/check_coverage.py coverage.json scripts/coverage_floors.txt --raise

The report is the JSON that ``coverage json`` writes after the data of every CI cell has been
combined. Each source file has a floor, a whole percentage taken from a measured value (rounded
down). The check fails when

* a file is below its floor (coverage went down),
* a measured file has no floor (a new module has to be given one), or
* a floor names a file that is not measured (a module was removed or renamed).

A file well above its floor passes but is reported, so the floor gets raised. Floors only go up:
``--raise`` rewrites the floors file with the larger of the old floor and the measured value, and
adds new files. Lowering a floor is a deliberate edit of the text file, visible in review.

Coverage is a tripwire, not a quality measure: that is the mutation gate's job (ADR-0019). The
floors only make sure a deleted test or an untested new module cannot go unnoticed.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

# Points a file may sit above its floor before the check suggests raising it.
RAISE_MARGIN = 2.0


def read_floors(path: Path) -> tuple[list[str], dict[str, int]]:
    """The leading comment lines and the ``file floor`` entries of the floors file."""
    header: list[str] = []
    floors: dict[str, int] = {}
    for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw.split("#", 1)[0].strip()
        if not line:
            if raw.startswith("#") and not floors:
                header.append(raw)
            continue
        parts = line.split()
        if len(parts) != 2 or not parts[1].isdigit() or not 0 <= int(parts[1]) <= 100:
            raise SystemExit(f"{path}:{number}: floors must read '<file> <whole percent 0-100>'")
        floors[parts[0]] = int(parts[1])
    return header, floors


def read_report(path: Path) -> dict[str, float]:
    """The measured percentage of each file, with ``/`` as the separator."""
    data = json.loads(path.read_text(encoding="utf-8"))
    return {
        name.replace("\\", "/"): float(info["summary"]["percent_covered"])
        for name, info in data["files"].items()
    }


def check(report: dict[str, float], floors: dict[str, int]) -> tuple[list[str], list[str]]:
    """The errors (fail the check) and the notes (suggest raising a floor)."""
    errors: list[str] = []
    notes: list[str] = []
    for name in sorted(report):
        percent = report[name]
        if name not in floors:
            errors.append(f"{name}: measured at {percent:.1f}% but has no floor; add one")
        elif percent + 1e-9 < floors[name]:
            errors.append(f"{name}: {percent:.1f}% is below its floor of {floors[name]}%")
        elif percent >= floors[name] + RAISE_MARGIN:
            notes.append(
                f"{name}: {percent:.1f}% is well above its floor of {floors[name]}%; "
                f"raise it to {math.floor(percent)}"
            )
    for name in sorted(set(floors) - set(report)):
        errors.append(f"{name}: has a floor but is not measured; remove or fix the entry")
    return errors, notes


def raised(report: dict[str, float], floors: dict[str, int]) -> dict[str, int]:
    """The floors after a raise: never lower than before, new files at their measured value."""
    return {name: max(floors.get(name, 0), math.floor(report[name])) for name in sorted(report)}


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("report", type=Path)
    parser.add_argument("floors", type=Path)
    parser.add_argument("--raise", dest="raise_floors", action="store_true")
    args = parser.parse_args(argv)

    header, floors = read_floors(args.floors)
    report = read_report(args.report)

    if args.raise_floors:
        new = raised(report, floors)
        body = [f"{name} {value}" for name, value in new.items()]
        # LF on every platform: the file is committed, so a rewrite must not change its line endings
        text = "\n".join([*header, *body]) + "\n"
        args.floors.write_text(text, encoding="utf-8", newline="\n")
        print(f"floors written for {len(new)} files")
        return 0

    errors, notes = check(report, floors)
    for note in notes:
        print(f"note: {note}")
    for error in errors:
        print(f"error: {error}", file=sys.stderr)
    if errors:
        print(f"coverage check failed: {len(errors)} problem(s)", file=sys.stderr)
        return 1
    print(f"coverage check passed for {len(report)} files")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
