"""Mutation-testing gate: prove the tests catch broken behaviour, not just that they are green.

One small mutation at a time (comparison, boolean, arithmetic and constant swaps) is applied to
every source module; the test suite then runs against it. A mutation no test catches is a
SURVIVOR: either a real coverage gap (write a test) or a genuinely equivalent mutant that
changes no observable behaviour (record it in the baseline with a reason). The gate fails when
the survivor set differs from the baseline, so a new gap cannot land silently and the baseline
stays an honest, reviewed list.

Mutants are identified by a key that survives unrelated edits:

    src/seedloop/_net.py::Transport._send#2 cmp LessThan->LessThanEqual [3fa9c1d2]

``path::qualname#ordinal`` locates the n-th mutation opportunity inside one function (a property's
getter and setter share a name, so they share the sequence); the
bracket is a hash of the original source line, so an equivalence judged for one piece of code is
not silently carried over to different code. Mutations run in a throw-away copy of the source
(reached through PYTHONPATH), never in the working tree, so an interrupted run cannot leave the
repository mutated.

Usage:
    python scripts/mutation_sweep.py                  # check survivors against the baseline
    python scripts/mutation_sweep.py --shard 2/4      # run the 2nd of 4 shards (for CI matrices)
    python scripts/mutation_sweep.py --list           # print every mutant key and exit
    python scripts/mutation_sweep.py --update         # rewrite the baseline (unsharded only)
"""

from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import libcst as cst
import proctree

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_BASELINE = Path(__file__).resolve().parent / "mutation_baseline.txt"
# pytest exit codes that mean the harness itself is broken, not that a mutant was noticed: 3
# (internal error), 4 (usage error), 5 (nothing collected). Every other non-zero code (failed
# tests, a collection error from a mutation that breaks an import, an interruption, or a crash)
# is a mutant the suite noticed.
HARNESS_ERROR_CODES = (3, 4, 5)
# A fixed order keeps kill/survive results and run times comparable between sweeps.
TEST_ARGS = (
    "-q",
    "-x",
    "-p",
    "no:cacheprovider",
    "-p",
    "no:randomly",
    "-m",
    "not meta and not differential",
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


@dataclass(frozen=True)
class Project:
    """What is being mutated and how it is tested (parameterized so the harness can be tested)."""

    root: Path = ROOT
    package: str = "seedloop"
    exclude_dirs: tuple[str, ...] = ("demos",)
    test_args: tuple[str, ...] = TEST_ARGS
    timeout: float = 300.0

    @property
    def package_dir(self) -> Path:
        return self.root / "src" / self.package

    def sources(self) -> list[Path]:
        return sorted(
            p
            for p in self.package_dir.rglob("*.py")
            if "__pycache__" not in p.parts
            and not any(part in self.exclude_dirs for part in p.relative_to(self.package_dir).parts)
        )


@dataclass(frozen=True)
class Mutant:
    path: str  # posix path relative to the project root
    target: int  # index of the mutation opportunity within the file
    key: str


# --- finding and applying mutations ------------------------------------------------------------


class _Mutator(cst.CSTTransformer):
    """Applies the mutation at ordinal ``target`` (or none); records every opportunity it sees."""

    def __init__(self, target: int) -> None:
        self.target = target
        self.i = -1
        self.desc: str | None = None
        self.scope: list[str] = []
        self.sites: list[tuple[str, int, str]] = []  # (qualname, ordinal in qualname, description)
        self._per_scope: dict[str, int] = {}

    def visit_ClassDef(self, node: cst.ClassDef) -> bool:
        self.scope.append(node.name.value)
        return True

    def leave_ClassDef(self, node: cst.ClassDef, updated: cst.ClassDef) -> cst.ClassDef:
        self.scope.pop()
        return updated

    def visit_FunctionDef(self, node: cst.FunctionDef) -> bool:
        self.scope.append(node.name.value)
        return True

    def leave_FunctionDef(self, node: cst.FunctionDef, updated: cst.FunctionDef) -> cst.FunctionDef:
        self.scope.pop()
        return updated

    def _hit(self, description: str) -> bool:
        self.i += 1
        qualname = ".".join(self.scope) or "<module>"
        ordinal = self._per_scope.get(qualname, 0)
        self._per_scope[qualname] = ordinal + 1
        self.sites.append((qualname, ordinal, description))
        if self.i == self.target:
            self.desc = description
            return True
        return False

    def leave_ComparisonTarget(
        self, node: cst.ComparisonTarget, updated: cst.ComparisonTarget
    ) -> cst.ComparisonTarget:
        op = type(updated.operator)
        if op in _COMPARISON and self._hit(f"cmp {op.__name__}->{_COMPARISON[op].__name__}"):
            return updated.with_changes(operator=_COMPARISON[op]())
        return updated

    def leave_BooleanOperation(
        self, node: cst.BooleanOperation, updated: cst.BooleanOperation
    ) -> cst.BooleanOperation:
        op = type(updated.operator)
        if op in _BOOLEAN and self._hit(f"bool {op.__name__}->{_BOOLEAN[op].__name__}"):
            return updated.with_changes(operator=_BOOLEAN[op]())
        return updated

    def leave_UnaryOperation(
        self, node: cst.UnaryOperation, updated: cst.UnaryOperation
    ) -> cst.BaseExpression:
        if isinstance(updated.operator, cst.Not) and self._hit("drop 'not'"):
            return updated.expression
        return updated

    def leave_BinaryOperation(
        self, node: cst.BinaryOperation, updated: cst.BinaryOperation
    ) -> cst.BinaryOperation:
        op = type(updated.operator)
        if op in _BINARY and self._hit(f"binop {op.__name__}->{_BINARY[op].__name__}"):
            return updated.with_changes(operator=_BINARY[op]())  # type: ignore[abstract]
        return updated

    def leave_Integer(self, node: cst.Integer, updated: cst.Integer) -> cst.Integer:
        if updated.value in ("0", "1"):
            new = "1" if updated.value == "0" else "0"
            if self._hit(f"int {updated.value}->{new}"):
                return updated.with_changes(value=new)
        return updated

    def leave_Name(self, node: cst.Name, updated: cst.Name) -> cst.Name:
        if updated.value in ("True", "False"):
            new = "False" if updated.value == "True" else "True"
            if self._hit(f"bool {updated.value}->{new}"):
                return updated.with_changes(value=new)
        return updated


def _read(path: Path) -> str:
    with open(path, encoding="utf-8", newline="") as f:  # newline="" keeps LF/CRLF exactly
        return f.read()


def _write(path: Path, text: str) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(text)


def _changed_line(original: str, mutated: str) -> int:
    for i, (a, b) in enumerate(zip(original.splitlines(), mutated.splitlines(), strict=False), 1):
        if a != b:
            return i
    return 0


def _line_hash(text: str) -> str:
    return hashlib.sha1(text.strip().encode()).hexdigest()[:8]


def mutated_text(original: str, target: int) -> str:
    return cst.parse_module(original).visit(_Mutator(target)).code


def enumerate_mutants(project: Project) -> list[Mutant]:
    """Every mutant of every source file, in a stable order, with its stable key."""
    mutants: list[Mutant] = []
    for path in project.sources():
        rel = path.relative_to(project.root).as_posix()
        original = _read(path)
        module = cst.parse_module(original)
        counter = _Mutator(target=-2)  # matches nothing: just collects the opportunities
        module.visit(counter)
        lines = original.splitlines()
        for target, (qualname, ordinal, description) in enumerate(counter.sites):
            mutated = module.visit(_Mutator(target)).code
            if mutated == original:
                continue
            line = _changed_line(original, mutated)
            text = lines[line - 1] if 0 < line <= len(lines) else ""
            key = f"{rel}::{qualname}#{ordinal} {description} [{_line_hash(text)}]"
            mutants.append(Mutant(rel, target, key))
    keys = [m.key for m in mutants]
    if len(set(keys)) != len(keys):
        raise SystemExit("internal error: mutant keys are not unique")
    return mutants


# --- running the suite against one mutant ------------------------------------------------------


def _env(copy_src: Path) -> dict[str, str]:
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([str(copy_src), env.get("PYTHONPATH", "")]).rstrip(
        os.pathsep
    )
    env["PYTHONHASHSEED"] = "0"
    env["SEEDLOOP_MUTATION_COPY"] = str(copy_src)  # lets a test assert it imports the copy
    # A same-length mutation written within the same second as the previous one keeps the source
    # mtime and size that a cached .pyc is validated by, so a stale .pyc would silently run the
    # previous mutant's code. Never write bytecode here, and drop any before each run.
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return env


def _check_import_source(project: Project, copy_src: Path) -> None:
    """Fail fast if the tests would import the real tree instead of the mutated copy."""
    code = f"import {project.package}; print({project.package}.__file__)"
    try:
        probe = run_with_timeout(
            [sys.executable, "-c", code], timeout=60, env=_env(copy_src), cwd=project.root
        )
    except subprocess.TimeoutExpired:
        raise SystemExit("harness error: importing the package took longer than 60 s") from None
    imported = probe.stdout.decode(errors="replace").strip()
    if probe.returncode != 0 or str(copy_src) not in imported:
        raise SystemExit(
            f"harness error: the tests would import {imported!r}, not the copy\n"
            f"{probe.stderr.decode(errors='replace')}"
        )


def run_with_timeout(
    argv: Sequence[str], *, timeout: float, env: dict[str, str], cwd: Path
) -> subprocess.CompletedProcess[bytes]:
    """Like ``subprocess.run`` but a timeout or interruption kills the whole process tree."""
    result = proctree.run(argv, timeout=timeout, env=env, cwd=cwd, text=False)
    assert isinstance(result.stdout, bytes)
    return subprocess.CompletedProcess(list(argv), result.returncode, result.stdout, result.stderr)


def run_suite(project: Project, copy_src: Path) -> bool:
    """True if the suite passed (the mutant SURVIVED); False if it noticed (killed or timed out)."""
    for cache in copy_src.rglob("__pycache__"):
        shutil.rmtree(cache, ignore_errors=True)
    try:
        result = run_with_timeout(
            [sys.executable, "-m", "pytest", *project.test_args],
            timeout=project.timeout,
            env=_env(copy_src),
            cwd=project.root,
        )
    except subprocess.TimeoutExpired:
        return False  # a mutant that hangs the suite is noticed
    if result.returncode == 0:
        return True
    if result.returncode in HARNESS_ERROR_CODES:
        raise SystemExit(
            f"harness error: pytest exited {result.returncode}, which is not a test outcome\n"
            f"{result.stdout.decode(errors='replace')[-800:]}"
            f"{result.stderr.decode(errors='replace')[-800:]}"
        )
    return False


def sweep(project: Project, mutants: Sequence[Mutant]) -> list[str]:
    """Run each mutant in a temporary copy; return the keys of the survivors."""
    survivors: list[str] = []
    with tempfile.TemporaryDirectory(prefix="seedloop-mutants-") as tmp:
        copy_src = Path(tmp) / "src"
        shutil.copytree(
            project.root / "src",
            copy_src,
            ignore=shutil.ignore_patterns("__pycache__", "*.egg-info"),
        )
        _check_import_source(project, copy_src)
        # Control run: the unmutated copy must pass. If the suite is broken in this environment,
        # every mutant would look "killed" and the gate would be green for the wrong reason.
        if not run_suite(project, copy_src):
            raise SystemExit("harness error: the unmutated suite fails against the copy")
        by_path: dict[str, list[Mutant]] = {}
        for mutant in mutants:
            by_path.setdefault(mutant.path, []).append(mutant)
        for rel, items in by_path.items():
            original = _read(project.root / rel)
            target_file = copy_src / Path(rel).relative_to("src")
            for mutant in items:
                _write(target_file, mutated_text(original, mutant.target))
                try:
                    if run_suite(project, copy_src):
                        survivors.append(mutant.key)
                finally:
                    _write(target_file, original)
            print(f"{rel}: {len(items)} mutants run", flush=True)
    return survivors


# --- baseline ----------------------------------------------------------------------------------

_SEPARATOR = "  # "


def parse_baseline(text: str) -> dict[str, str]:
    """``{key: reason}`` from the baseline file (comments and blank lines are ignored)."""
    entries: dict[str, str] = {}
    for raw in text.splitlines():
        line = raw.rstrip()
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        key, _, reason = line.partition(_SEPARATOR)
        entries[key.strip()] = reason.strip()
    return entries


def format_baseline(survivors: Sequence[str], reasons: dict[str, str]) -> str:
    header = (
        "# Equivalent mutants: each changes no observable behaviour, so no test can (or should)\n"
        "# catch it. The gate fails on any survivor NOT listed here (a new one is a real coverage\n"
        "# gap, not a free pass), and on any entry that no longer exists or no longer survives.\n"
        "# Each line is `key  # why it is equivalent`; the key is\n"
        "# path::function#n description [hash of the original line].\n"
        "# Regenerate with `python scripts/mutation_sweep.py --update`, then write the reasons.\n"
    )
    body = "".join(
        f"{key}{_SEPARATOR}{reasons.get(key) or 'TODO: write why this mutant is equivalent'}\n"
        for key in sorted(survivors)
    )
    return header + body


def compare(
    survivors: Sequence[str], baseline: dict[str, str], shard_keys: set[str], all_keys: set[str]
) -> tuple[list[str], list[str], list[str]]:
    """(new survivors, baselined mutants that now die, baseline keys that no longer exist)."""
    new = sorted(set(survivors) - set(baseline))
    gone = sorted(k for k in baseline if k in shard_keys and k not in set(survivors))
    stale = sorted(k for k in baseline if k not in all_keys)
    return new, gone, stale


def shard_of(mutants: Sequence[Mutant], index: int, count: int) -> list[Mutant]:
    """Round-robin shard ``index`` (1-based) of ``count`` over the key-sorted mutant list."""
    ordered = sorted(mutants, key=lambda m: m.key)
    return [m for position, m in enumerate(ordered) if position % count == index - 1]


# --- command line ------------------------------------------------------------------------------


def _parse_shard(text: str) -> tuple[int, int]:
    index, _, count = text.partition("/")
    if not (index.isdigit() and count.isdigit()) or not 1 <= int(index) <= int(count):
        raise argparse.ArgumentTypeError("--shard must look like I/N with 1 <= I <= N")
    return int(index), int(count)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "--update", action="store_true", help="rewrite the baseline (not with --shard)"
    )
    parser.add_argument("--list", dest="list_only", action="store_true", help="print mutant keys")
    parser.add_argument("--shard", type=_parse_shard, help="run shard I of N (e.g. 2/4)")
    parser.add_argument("--baseline", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--timeout", type=float, default=300.0, help="seconds per mutant")
    args = parser.parse_args(argv)
    if args.update and args.shard:
        parser.error("--update needs the whole mutant set: run it without --shard")

    project = Project(timeout=args.timeout)
    mutants = enumerate_mutants(project)
    if args.list_only:
        print("\n".join(m.key for m in sorted(mutants, key=lambda m: m.key)))
        return 0

    chosen = shard_of(mutants, *args.shard) if args.shard else list(mutants)
    baseline_text = _read(args.baseline) if args.baseline.exists() else ""
    baseline = parse_baseline(baseline_text)
    print(f"{len(chosen)} of {len(mutants)} mutants to run", flush=True)
    survivors = sweep(project, chosen)
    print(f"\n{len(chosen)} mutants run, {len(survivors)} survivors", flush=True)

    if args.update:
        _write(args.baseline, format_baseline(survivors, baseline))
        print(f"baseline written: {len(survivors)} entries -> {args.baseline}")
        return 0

    new, gone, stale = compare(
        survivors, baseline, {m.key for m in chosen}, {m.key for m in mutants}
    )
    if new:
        print("\nFAIL: new surviving mutants (unpinned behaviour: add a test or justify):")
        for key in new:
            print(f"  + {key}")
    if gone:
        print("\nFAIL: baselined mutants no longer survive (tighten the baseline with --update):")
        for key in gone:
            print(f"  - {key}")
    if stale:
        print("\nFAIL: baseline entries that match no mutant (the code changed; re-review them):")
        for key in stale:
            print(f"  ? {key}")
    if new or gone or stale:
        return 1
    print(f"OK: {len(survivors)} survivors in this run, all in the reviewed baseline.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
