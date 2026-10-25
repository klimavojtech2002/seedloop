"""The mutation harness is part of the safety net, so it is tested like the code it guards.

A tiny throw-away project (one module, one test file) stands in for seedloop, so every property of
the harness is checked on a case small enough to reason about: stable keys, a partition into shards,
a hanging mutant counted as killed, the real tree left byte-identical, and stale baseline entries
reported. Needs ``libcst`` (the ``mutation`` extra); skipped where it is not installed.
"""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import pytest

from _support import pid_alive

pytest.importorskip("libcst")

# Slow (each case runs a real mini test suite) and about the harness itself: own CI job, and never
# part of the suite the mutation gate runs against each mutant.
pytestmark = pytest.mark.meta


def _load_script() -> Any:
    # The harness lives in scripts/ (not a package); load it by path so mypy treats it as untyped.
    scripts = Path(__file__).resolve().parent.parent / "scripts"
    path = scripts / "mutation_sweep.py"
    sys.path.insert(0, str(scripts))  # the harness imports its sibling helper by name
    spec = importlib.util.spec_from_file_location("mutation_sweep", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["mutation_sweep"] = module  # dataclasses look the module up by name
    spec.loader.exec_module(module)
    return module


ms: Any = _load_script()

MODULE = """\
LIMIT = 3


def over(x):
    return x > LIMIT


def spin(n):
    total = 0
    while n > 0:
        total = total + 1
        n = n - 1
    return total
"""

TESTS = """\
from mini import over, spin


def test_over():
    assert over(5)
    assert not over(1)


def test_spin():
    assert spin(3) == 3
"""


def make_project(tmp_path: Path, module: str = MODULE, timeout: float = 10.0) -> Any:
    package = tmp_path / "src" / "mini"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("from mini.core import over, spin\n", encoding="utf-8")
    (package / "core.py").write_text(module, encoding="utf-8")
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "test_mini.py").write_text(TESTS, encoding="utf-8")
    return ms.Project(
        root=tmp_path,
        package="mini",
        exclude_dirs=(),
        test_args=("-q", "-x", "-p", "no:cacheprovider", "-p", "no:randomly"),
        timeout=timeout,
    )


def keys(project: Any) -> list[str]:
    return [m.key for m in ms.enumerate_mutants(project)]


def test_keys_name_the_function_and_the_line_hash(tmp_path: Path) -> None:
    project = make_project(tmp_path)
    found = keys(project)
    assert any("core.py::over#0 cmp GreaterThan->GreaterThanEqual [" in k for k in found), found
    assert any("::spin#" in k for k in found)
    assert len(found) == len(set(found))


def test_keys_survive_an_unrelated_edit_above(tmp_path: Path) -> None:
    # The old line-number keys broke whenever anything above a baselined mutant changed.
    before = keys(make_project(tmp_path / "a"))
    after = keys(make_project(tmp_path / "b", module="# a new comment\n\n\n" + MODULE))
    assert [k.split("::", 1)[1] for k in before] == [k.split("::", 1)[1] for k in after]


def test_a_changed_line_changes_its_key(tmp_path: Path) -> None:
    before = keys(make_project(tmp_path / "a"))
    after = keys(make_project(tmp_path / "b", module=MODULE.replace("x > LIMIT", "x >  LIMIT + 0")))
    changed = set(before) ^ set(after)
    assert any("over" in k for k in changed)


@pytest.mark.parametrize("count", [1, 2, 3, 5])
def test_shards_partition_the_mutants(tmp_path: Path, count: int) -> None:
    mutants = ms.enumerate_mutants(make_project(tmp_path))
    shards = [ms.shard_of(mutants, i, count) for i in range(1, count + 1)]
    flat = [m.key for shard in shards for m in shard]
    assert sorted(flat) == sorted(m.key for m in mutants)
    assert len(flat) == len(set(flat))


@pytest.mark.parametrize("bad", ["0/3", "4/3", "x/3", "2", "2/0"])
def test_a_malformed_shard_is_rejected(bad: str) -> None:
    import argparse

    with pytest.raises(argparse.ArgumentTypeError):
        ms._parse_shard(bad)


@pytest.mark.timeout(120)
def test_the_suite_kills_a_real_change_and_the_survivor_is_reported(tmp_path: Path) -> None:
    project = make_project(tmp_path)
    chosen = [
        m
        for m in ms.enumerate_mutants(project)
        if "::over#" in m.key or ("::spin#0 " in m.key and "int 0->1" in m.key)
    ]
    assert len(chosen) == 2, [m.key for m in chosen]
    survivors = ms.sweep(project, chosen)
    # `x > LIMIT` -> `x >= LIMIT` survives: the tests never probe the boundary x == 3.
    assert [k for k in survivors if "::over#0 cmp GreaterThan->GreaterThanEqual" in k]
    # `total = 0` -> `total = 1` is caught by test_spin.
    assert not [k for k in survivors if "::spin#0 int 0->1" in k]


@pytest.mark.timeout(60)
def test_a_mutant_that_hangs_is_counted_as_killed(tmp_path: Path) -> None:
    # `n = n - 1` -> `n = n + 1` makes spin() loop forever: the timeout counts that as noticed.
    project = make_project(tmp_path, timeout=5.0)
    hang = next(
        m
        for m in ms.enumerate_mutants(project)
        if "spin" in m.key and "binop Subtract->Add" in m.key
    )
    assert ms.sweep(project, [hang]) == []


@pytest.mark.timeout(120)
def test_the_working_tree_is_never_touched(tmp_path: Path) -> None:
    project = make_project(tmp_path)
    before = {p: p.read_bytes() for p in project.root.rglob("*.py")}
    over_only = [m for m in ms.enumerate_mutants(project) if "::over#" in m.key]
    ms.sweep(project, over_only)
    assert {p: p.read_bytes() for p in project.root.rglob("*.py")} == before


def test_line_endings_are_preserved_in_the_mutated_text() -> None:
    crlf = "def f(x):\r\n    return x > 1\r\n"
    assert "\r\n" in ms.mutated_text(crlf, 0)


def test_a_stale_baseline_entry_is_reported(tmp_path: Path) -> None:
    mutants = ms.enumerate_mutants(make_project(tmp_path))
    all_keys = {m.key for m in mutants}
    baseline = {"src/mini/core.py::gone#0 cmp Equal->NotEqual [deadbeef]": "removed code"}
    new, gone, stale = ms.compare([], baseline, all_keys, all_keys)
    assert (new, gone) == ([], [])
    assert stale == list(baseline)


def test_compare_separates_new_gone_and_out_of_shard() -> None:
    baseline = {"a": "r", "b": "r"}
    # `a` still survives, `b` was baselined but now dies, `c` is a new survivor.
    new, gone, stale = ms.compare(["a", "c"], baseline, {"a", "b", "c"}, {"a", "b", "c"})
    assert (new, gone, stale) == (["c"], ["b"], [])
    # A baselined mutant outside this shard is neither "gone" nor "stale".
    new, gone, stale = ms.compare(["c"], baseline, {"c"}, {"a", "b", "c"})
    assert (new, gone, stale) == (["c"], [], [])


def test_the_baseline_file_round_trips_with_its_reasons() -> None:
    survivors = ["a.py::f#0 cmp Equal->NotEqual [aaaaaaaa]", "a.py::g#1 int 0->1 [bbbbbbbb]"]
    reasons = {survivors[0]: "boundary; both sides give the same result"}
    text = ms.format_baseline(survivors, reasons)
    parsed = ms.parse_baseline(text)
    assert parsed[survivors[0]] == reasons[survivors[0]]
    assert parsed[survivors[1]].startswith("TODO")  # a new entry must be given a reason by a human
    assert set(parsed) == set(survivors)


def test_update_is_refused_together_with_a_shard() -> None:
    # A baseline written from one shard would drop every entry that belongs to the others.
    with pytest.raises(SystemExit) as refused:
        ms.main(["--update", "--shard", "1/2"])
    assert refused.value.code == 2


def test_a_copy_that_is_not_imported_is_a_harness_error(tmp_path: Path) -> None:
    project = make_project(tmp_path)
    with pytest.raises(SystemExit, match="would import"):
        ms._check_import_source(project, tmp_path / "somewhere-else")


@pytest.mark.timeout(120)
def test_a_stale_bytecode_cache_cannot_mask_a_same_length_mutant(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Two mutants of the same file with the same length, written with the same mtime: a cached .pyc
    # (validated by mtime and size) would make the second run the first one's code. The first is
    # harmless (an unused constant), the second breaks spin(): it must be killed, not "survive".
    module = MODULE.replace("LIMIT = 3\n", "LIMIT = 3\nUNUSED = 0\n")
    project = make_project(tmp_path, module=module)
    mutants = ms.enumerate_mutants(project)
    harmless = next(m for m in mutants if "<module>#" in m.key and "int 0->1" in m.key)
    breaking = next(m for m in mutants if "::spin#0 int 0->1" in m.key)
    assert harmless.path == breaking.path

    real_write = ms._write

    def write_with_fixed_mtime(path: Path, text: str) -> None:
        real_write(path, text)
        os.utime(path, (1_700_000_000, 1_700_000_000))

    monkeypatch.setattr(ms, "_write", write_with_fixed_mtime)
    survivors = ms.sweep(project, [harmless, breaking])
    assert survivors == [harmless.key]


@pytest.mark.timeout(60)
def test_a_timeout_kills_the_whole_process_tree(tmp_path: Path) -> None:
    # The suite under test may have started children of its own; killing only the direct child
    # would leave them running (and burning CPU) after the sweep has moved on.
    pid_file = tmp_path / "grandchild.pid"
    code = (
        "import pathlib, subprocess, sys, time\n"
        "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(300)'])\n"
        f"pathlib.Path({str(pid_file)!r}).write_text(str(child.pid))\n"
        "time.sleep(300)\n"
    )
    with pytest.raises(subprocess.TimeoutExpired):
        ms.run_with_timeout(
            [sys.executable, "-c", code], timeout=10, env=dict(os.environ), cwd=tmp_path
        )
    assert pid_file.exists(), "the child never wrote the grandchild's pid (slow start?)"
    grandchild = int(pid_file.read_text())
    deadline = time.monotonic() + 15
    while pid_alive(grandchild) and time.monotonic() < deadline:
        time.sleep(0.1)
    assert not pid_alive(grandchild), "the grandchild outlived the timeout"


def _fake_run(code: int):  # type: ignore[no-untyped-def]
    def fake(argv, *, timeout, env, cwd):  # type: ignore[no-untyped-def]
        return subprocess.CompletedProcess(argv, code, b"out", b"err")

    return fake


@pytest.mark.parametrize(
    ("code", "survived"), [(0, True), (1, False), (2, False), (139, False), (-11, False)]
)
def test_exit_codes_map_to_survived_or_noticed(
    code: int, survived: bool, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = make_project(tmp_path)
    monkeypatch.setattr(ms, "run_with_timeout", _fake_run(code))
    assert ms.run_suite(project, tmp_path / "src") is survived  # a crash is a noticed mutant


@pytest.mark.parametrize("code", [3, 4, 5])
def test_pytest_harness_errors_abort_instead_of_counting_as_killed(
    code: int, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = make_project(tmp_path)
    monkeypatch.setattr(ms, "run_with_timeout", _fake_run(code))
    with pytest.raises(SystemExit, match="harness error"):
        ms.run_suite(project, tmp_path / "src")


@pytest.mark.timeout(60)
def test_a_suite_that_fails_unmutated_aborts_the_sweep(tmp_path: Path) -> None:
    # If the suite is broken in this environment every mutant would look "killed" and the gate
    # would pass for the wrong reason, so an unmutated control run has to pass first.
    project = make_project(tmp_path)
    (tmp_path / "tests" / "test_mini.py").write_text("def test_broken():\n    assert False\n")
    with pytest.raises(SystemExit, match="unmutated suite fails"):
        ms.sweep(project, ms.enumerate_mutants(project)[:1])


@pytest.mark.timeout(60)
def test_an_interrupted_sweep_leaves_the_tree_and_the_temp_directory_clean(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = make_project(tmp_path / "project")
    before = {p: p.read_bytes() for p in project.root.rglob("*.py")}
    pattern = "seedloop-mutants-*"
    temp = Path(tempfile.gettempdir())
    leftovers_before = set(temp.glob(pattern))
    calls = {"n": 0}

    def interrupted(_project: object, _copy: Path) -> bool:
        calls["n"] += 1
        if calls["n"] == 1:
            return True  # the control run passes
        raise KeyboardInterrupt  # Ctrl-C in the middle of the first mutant

    monkeypatch.setattr(ms, "run_suite", interrupted)
    with pytest.raises(KeyboardInterrupt):
        ms.sweep(project, ms.enumerate_mutants(project)[:2])
    assert {p: p.read_bytes() for p in project.root.rglob("*.py")} == before
    assert set(temp.glob(pattern)) == leftovers_before
