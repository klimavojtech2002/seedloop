"""The characterization corpus: pinned across commits, and proven able to notice a real change.

``scripts/pin_corpus.py`` runs many scenarios and compares them with the committed golden. The first
group of tests is the pin itself (it runs on the whole CI matrix). The second, marked ``meta``,
checks the pin is not vacuous: each named change to the library, applied to a throw-away copy of
``src/``, must make the corpus fail. A corpus that cannot fail proves nothing.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from _support import (
    DROPPED_SETTLE_STEP,
    REVERSE_CANCEL_ORDER,
    ROOT,
    Change,
    IsolatedTimeout,
    copy_src_with,
    run_isolated,
    run_script,
)

SCRIPT = ROOT / "scripts" / "pin_corpus.py"
GOLDEN = ROOT / "tests" / "data" / "corpus_golden.txt"


def test_matches_the_golden() -> None:
    result = run_script(SCRIPT)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("hash_seed", ["1", "4242", "random"])
def test_independent_of_the_hash_seed(hash_seed: str) -> None:
    # The golden was written under PYTHONHASHSEED=0 (the _support default); other seeds must agree.
    result = run_script(SCRIPT, env={"PYTHONHASHSEED": hash_seed})
    assert result.returncode == 0, result.stderr


def test_a_tampered_golden_is_caught(tmp_path: Path) -> None:
    with open(GOLDEN, encoding="utf-8", newline="") as f:
        lines = f.read().splitlines()
    index = next(i for i, line in enumerate(lines) if line.startswith("spawn_chain 0 "))
    lines[index] = "spawn_chain 0 ok 000000000000"
    tampered = tmp_path / "golden.txt"
    with open(tampered, "w", encoding="utf-8", newline="") as f:
        f.write("\n".join(lines) + "\n")

    result = run_script(SCRIPT, "--golden", str(tampered))
    assert result.returncode == 1
    assert "timeline-affecting change" in result.stderr
    assert "spawn_chain 0 ok" in result.stderr  # the diff names the scenario that moved


def test_a_missing_golden_is_an_error(tmp_path: Path) -> None:
    result = run_script(SCRIPT, "--golden", str(tmp_path / "absent.txt"))
    assert result.returncode == 1
    assert "--update" in result.stderr


def test_the_golden_covers_every_scenario() -> None:
    # Every scenario the script defines must be pinned (one added without --update would not be).
    text = GOLDEN.read_text(encoding="utf-8")
    result = run_isolated(
        "import sys; sys.path.insert(0, 'scripts'); import pin_corpus; "
        "print('\\n'.join(e.name for e in pin_corpus.entries()))"
    )
    assert result.returncode == 0, result.stderr
    names = result.stdout.split()
    assert names, "no scenarios found"
    for name in names:
        assert f"\n{name} 0 " in text, name


# --- the pin must be able to fail ---------------------------------------------------------------


CHANGES = [
    Change(
        "slow_link multiplier range shrinks",
        "_net.py",
        "self._faults.uniform(2.0, 50.0)",
        "self._faults.uniform(2.0, 25.0)",
    ),
    Change(
        "slow_link draws the factor before the window",
        "_net.py",
        "        factor = fault.factor if fault.factor is not None else "
        "self._faults.uniform(2.0, 50.0)\n"
        "        start = self._faults.uniform(0.0, seconds)\n",
        "        start = self._faults.uniform(0.0, seconds)\n"
        "        factor = fault.factor if fault.factor is not None else "
        "self._faults.uniform(2.0, 50.0)\n",
    ),
    Change(
        "a partition window may end before it starts",
        "_net.py",
        "        end = self._faults.uniform(start, seconds)\n"
        "        fault_id = self._next_fault_id\n"
        "        self._next_fault_id += 1\n"
        "        # The resolved",
        "        end = self._faults.uniform(0.0, seconds)\n"
        "        fault_id = self._next_fault_id\n"
        "        self._next_fault_id += 1\n"
        "        # The resolved",
    ),
    Change(
        "a pinned partition is ignored",
        "_net.py",
        "groups = [set(g) for g in fault.groups] if fault.groups else self._draw_bipartition()",
        "groups = self._draw_bipartition()",
    ),
    Change(
        "a crashed node's drop is checked after the partition",
        "_net.py",
        "        if src in self._crashed or dst in self._crashed:",
        "        if (src in self._crashed or dst in self._crashed) and self._reachable(src, dst):",
    ),
    Change(
        "reliable links lose their ordering clamp",
        "_net.py",
        "when = max(self._loop.time() + latency, self._reliable_clock.get(key, 0.0))",
        "when = self._loop.time() + latency",
    ),
    Change(
        "bipartition is biased",
        "_net.py",
        "self._faults.random() < 0.5",
        "self._faults.random() < 0.6",
    ),
    Change(
        "overlapping slow links stop compounding",
        "_net.py",
        "factor *= link_factor",
        "factor = link_factor",
    ),
    Change(
        "crash times are drawn from half the window",
        "_net.py",
        "else self._faults.uniform(0.0, seconds)\n        node = fault.node",
        "else self._faults.uniform(0.0, seconds / 2)\n        node = fault.node",
    ),
    REVERSE_CANCEL_ORDER,
    Change(
        "a deadline beats a predicate that holds in the same step",
        "_world.py",
        "            if holds:\n                done.set_result(None)\n"
        "            elif deadline_at is not None and self._loop.time() >= deadline_at:",
        "            if deadline_at is not None and self._loop.time() >= deadline_at:",
    ),
    Change(
        "faults are resolved in reverse order",
        "_world.py",
        "        for fault in faults:\n            self.net._commit_fault(fault, seconds)",
        "        for fault in list(faults)[::-1]:\n"
        "            self.net._commit_fault(fault, seconds)",
    ),
    Change(
        "the CSPRNG shim uses another stream",
        "_run.py",
        'substream(seed, "csprng")',
        'substream(seed, "csprng2")',
    ),
    DROPPED_SETTLE_STEP,
    Change(
        "timers due exactly now are not run",
        "_loop.py",
        "self._sl_timers[0][0] <= self._sl_time",
        "self._sl_timers[0][0] < self._sl_time",
        may_hang=True,
    ),
]


@pytest.mark.meta
@pytest.mark.timeout(90)
@pytest.mark.parametrize("change", CHANGES, ids=[c.name for c in CHANGES])
def test_a_semantic_change_is_caught(change: Change, tmp_path: Path) -> None:
    copy = copy_src_with(tmp_path, change)
    env = {"PYTHONPATH": str(copy)}

    # The child must really import the modified copy (an editable install must not win).
    probe = run_isolated("import seedloop; print(seedloop.__file__)", env=env)
    assert str(copy) in probe.stdout, probe.stdout

    try:
        result = run_script(SCRIPT, env=env, timeout=20)
    except IsolatedTimeout:
        assert change.may_hang, f"the corpus hung, which was not expected for: {change.name}"
        return  # for a change that stops timers firing, a hang is how it notices
    # It must fail because the pinned timeline moved, not because the script crashed.
    assert result.returncode == 1, (
        f"{change.name}: exit {result.returncode}\n{result.stderr[-500:]}"
    )
    assert "timeline-affecting change" in result.stderr, change.name


def test_replays_identically_within_one_process() -> None:
    # One process runs the whole corpus three times: as pinned, in reverse order, and after
    # allocating and freezing garbage. State shared between runs, or a dependence on allocation
    # history, would make the three differ. (Across processes is covered by the golden test.)
    code = """
import gc, sys
sys.path.insert(0, 'scripts')
import pin_corpus as corpus

first = corpus.serialize(corpus.entries())
second = corpus.serialize(list(reversed(corpus.entries())))
junk = [[i] for i in range(200_000)]
gc.freeze()
third = corpus.serialize(corpus.entries())
assert first == third, 'the corpus changed after allocation noise'
assert sorted(first.splitlines()) == sorted(second.splitlines()), 'order of runs mattered'
print('ok')
"""
    result = run_isolated(code, timeout=25)
    assert result.returncode == 0 and result.stdout.strip() == "ok", result.stderr[-600:]
