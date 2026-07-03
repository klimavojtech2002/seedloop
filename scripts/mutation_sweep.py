"""Mutation-testing gate: prove the tests catch broken behaviour, not just that they are green.

One small mutation at a time (comparison, boolean, arithmetic, and constant swaps) is applied to
every source module; the test suite then runs against it. A mutation no test catches is a SURVIVOR
— either a real coverage gap (write a test) or a genuinely equivalent mutant that changes no
observable behaviour (record it in the baseline with a reason). The gate fails when the survivor set
differs from the baseline, so a new gap cannot land silently and the baseline stays an honest,
reviewed list.

Usage:
    python scripts/mutation_sweep.py            # check survivors against the baseline
    python scripts/mutation_sweep.py --update   # rewrite the baseline (review the diff!)
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import libcst as cst

ROOT = Path(__file__).resolve().parent.parent
PKG = ROOT / "src" / "seedloop"
BASELINE = Path(__file__).resolve().parent / "mutation_baseline.txt"
# The gate targets the library. demos/ is a worked example validated end-to-end by its own tests
# (the Raft split-brain seed sweep), not held to per-branch mutation kills — mutating its internal
# election logic changes behaviour the outcome-level demo tests deliberately do not pin.
SOURCES = sorted(
    p for p in PKG.rglob("*.py") if "__pycache__" not in p.parts and "demos" not in p.parts
)

_COMPARISON = {
    cst.LessThan: cst.LessThanEqual,
    cst.LessThanEqual: cst.LessThan,
    cst.GreaterThan: cst.GreaterThanEqual,
    cst.GreaterThanEqual: cst.GreaterThan,
    cst.Equal: cst.NotEqual,
    cst.NotEqual: cst.Equal,
    cst.Is: cst.IsNot,
    cst.IsNot: cst.Is,
}
_BOOLEAN = {cst.And: cst.Or, cst.Or: cst.And}
_BINARY = {
    cst.Add: cst.Subtract,
    cst.Subtract: cst.Add,
    cst.Multiply: cst.Divide,
    cst.Divide: cst.Multiply,
}


class _Mutator(cst.CSTTransformer):
    """Applies exactly the mutation at ordinal ``target`` and records its description."""

    def __init__(self, target: int) -> None:
        self.target = target
        self.i = -1
        self.desc: str | None = None

    def _hit(self) -> bool:
        self.i += 1
        return self.i == self.target

    def leave_ComparisonTarget(
        self, node: cst.ComparisonTarget, updated: cst.ComparisonTarget
    ) -> cst.ComparisonTarget:
        op = type(updated.operator)
        if op in _COMPARISON and self._hit():
            self.desc = f"cmp {op.__name__}->{_COMPARISON[op].__name__}"
            return updated.with_changes(operator=_COMPARISON[op]())
        return updated

    def leave_BooleanOperation(
        self, node: cst.BooleanOperation, updated: cst.BooleanOperation
    ) -> cst.BooleanOperation:
        op = type(updated.operator)
        if op in _BOOLEAN and self._hit():
            self.desc = f"bool {op.__name__}->{_BOOLEAN[op].__name__}"
            return updated.with_changes(operator=_BOOLEAN[op]())
        return updated

    def leave_UnaryOperation(
        self, node: cst.UnaryOperation, updated: cst.UnaryOperation
    ) -> cst.BaseExpression:
        if isinstance(updated.operator, cst.Not) and self._hit():
            self.desc = "drop 'not'"
            return updated.expression
        return updated

    def leave_BinaryOperation(
        self, node: cst.BinaryOperation, updated: cst.BinaryOperation
    ) -> cst.BinaryOperation:
        op = type(updated.operator)
        if op in _BINARY and self._hit():
            self.desc = f"binop {op.__name__}->{_BINARY[op].__name__}"
            return updated.with_changes(operator=_BINARY[op]())  # type: ignore[abstract]
        return updated

    def leave_Integer(self, node: cst.Integer, updated: cst.Integer) -> cst.Integer:
        if updated.value in ("0", "1") and self._hit():
            new = "1" if updated.value == "0" else "0"
            self.desc = f"int {updated.value}->{new}"
            return updated.with_changes(value=new)
        return updated

    def leave_Name(self, node: cst.Name, updated: cst.Name) -> cst.Name:
        if updated.value in ("True", "False") and self._hit():
            new = "False" if updated.value == "True" else "True"
            self.desc = f"bool {updated.value}->{new}"
            return updated.with_changes(value=new)
        return updated


def _changed_line(original: str, mutated: str) -> int:
    for i, (a, b) in enumerate(zip(original.splitlines(), mutated.splitlines(), strict=False), 1):
        if a != b:
            return i
    return 0


def _read(path: Path) -> str:
    with open(path, encoding="utf-8", newline="") as f:  # newline="" keeps LF, no CRLF on Windows
        return f.read()


def _write(path: Path, text: str) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(text)


def _run_tests() -> bool:
    """True if the suite passed (mutant SURVIVED); False if a test failed (mutant caught)."""
    # Clear the package bytecode cache so a same-length mutation (e.g. `+= 1` -> `+= 0`) cannot be
    # masked by a stale .pyc whose (mtime, size) still matches the restored source.
    for cache in PKG.rglob("__pycache__"):
        shutil.rmtree(cache, ignore_errors=True)
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-x", "-p", "no:cacheprovider"],
        cwd=ROOT,
        capture_output=True,
        timeout=300,
    )
    return result.returncode == 0


def sweep() -> list[str]:
    survivors: list[str] = []
    total = 0
    for path in SOURCES:
        rel = path.relative_to(ROOT).as_posix()
        original = _read(path)
        module = cst.parse_module(original)
        count = _Mutator(target=-2)  # a target no node matches, to count the opportunities
        module.visit(count)
        for target in range(count.i + 1):
            mutator = _Mutator(target=target)
            mutated = module.visit(mutator).code
            if mutated == original:
                continue
            total += 1
            _write(path, mutated)
            try:
                survived = _run_tests()
            finally:
                _write(path, original)
            if survived:
                line = _changed_line(original, mutated)
                survivors.append(f"{rel}:{line} {mutator.desc}")
        print(f"{rel}: swept", flush=True)
    print(f"\n{total} mutants run, {len(survivors)} survivors", flush=True)
    return sorted(survivors)


def load_baseline() -> list[str]:
    if not BASELINE.exists():
        return []
    return sorted(
        line.split("#", 1)[0].strip()
        for line in BASELINE.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    )


def main() -> int:
    update = "--update" in sys.argv[1:]
    survivors = sweep()
    if update:
        header = (
            "# Equivalent mutants: each changes no observable behaviour; no test catches them.\n"
            "# Fails on any survivor not listed here — a new one is a real coverage gap.\n"
            "# Format: `path:line mutation  # why`; restore the reasons after regenerating.\n"
        )
        BASELINE.write_text(header + "\n".join(survivors) + "\n", encoding="utf-8")
        print(f"baseline written: {len(survivors)} entries -> {BASELINE}")
        return 0
    baseline = load_baseline()
    new = [s for s in survivors if s not in baseline]
    gone = [s for s in baseline if s not in survivors]
    if new:
        print("\nFAIL: new surviving mutants (unpinned behaviour — add a test or justify):")
        for s in new:
            print(f"  + {s}")
    if gone:
        print("\nFAIL: baselined mutants no longer survive (tighten the baseline with --update):")
        for s in gone:
            print(f"  - {s}")
    if new or gone:
        return 1
    print(f"OK: {len(survivors)} survivors, all in the reviewed baseline.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
