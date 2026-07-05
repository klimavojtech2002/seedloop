"""The optional Hypothesis integration: exploration + shrinking on top of deterministic runs.

The load-bearing claim is the boundary one — Hypothesis draws the seed and inputs *outside* the run,
so a run stays a pure function of its seed. That is proven here (a drawn seed replays identically,
even under the auditor), alongside the opt-in import contract and the reproduction-line ergonomics.
"""

from __future__ import annotations

import subprocess
import sys

import pytest

pytest.importorskip("hypothesis")

from collections.abc import Awaitable, Callable

from hypothesis import find, given, settings
from hypothesis import strategies as st

import seedloop
from seedloop._run import _run_one
from seedloop._world import World
from seedloop.hypothesis import given_seed, seeds

Scenario = Callable[[World], Awaitable[None]]


def test_import_without_the_extra_fails_with_guidance() -> None:
    # Block hypothesis in a fresh interpreter, then import the module: the failure must name the
    # extra, not surface a bare ModuleNotFoundError deep in a stack.
    code = "import sys; sys.modules['hypothesis'] = None; import seedloop.hypothesis"
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=30
    )
    assert result.returncode != 0
    assert "seedloop[hypothesis]" in result.stderr


def test_seeds_respects_bounds() -> None:
    # seeds() encodes the seed domain. `find` returns a strategy's minimal example, so it pins the
    # lower bound exactly: the default floor is 0 (not 1), and an explicit floor is honoured.
    assert find(seeds(), lambda s: True) == 0
    assert find(seeds(min_value=10, max_value=12), lambda s: True) == 10

    @given(s=seeds(min_value=10, max_value=12))
    @settings(max_examples=30, database=None, deadline=None)
    def prop(s: int) -> None:
        assert 10 <= s <= 12

    prop()


def test_seeds_draws_valid_world_seeds() -> None:
    drawn: list[int] = []

    @given_seed()
    @settings(max_examples=50, database=None, deadline=None)
    def prop(seed: int) -> None:
        assert isinstance(seed, int) and seed >= 0  # the documented, conventional domain
        World(seed)  # the runtime accepts every drawn seed
        drawn.append(seed)

    prop()
    assert len(drawn) >= 1  # the property actually ran


def _flaky(n: int) -> Scenario:
    # A scenario whose only failure mode is input-dependent: it fails once n reaches 3, so shrinking
    # has a clear minimal target and the failure is a pure function of the input (not the seed).
    async def scenario(world: World) -> None:
        world.record(("n", n))
        assert n < 3, "planted input-dependent failure"

    return scenario


def test_given_seed_finds_shrinks_and_reports() -> None:
    failing_inputs: list[int] = []

    @given_seed(n=st.integers(min_value=0, max_value=20))
    @settings(max_examples=200, database=None, deadline=None)
    def prop(seed: int, n: int) -> None:
        try:
            _run_one(_flaky(n), seed)
        except AssertionError:
            failing_inputs.append(n)
            raise

    with pytest.raises(AssertionError) as excinfo:
        prop()

    # Hypothesis shrank the input to its minimal failing value.
    assert min(failing_inputs) == 3
    # The reproduction line is attached to the reported failure and carries the *shrunk* seed (every
    # seed fails this input-only bug, so Hypothesis minimises it to 0).
    notes = getattr(excinfo.value, "__notes__", [])
    assert any("seedloop.replay" in note and "seed=0" in note for note in notes)
    # And the reported minimal case genuinely replays — the whole point of a DST report.
    with pytest.raises(AssertionError):
        seedloop.replay(_flaky(3), seed=0)


def test_reproduction_note_carries_the_actual_seed() -> None:
    # A seed-only failure that fires for seed >= 5 shrinks to the boundary 5, forcing a non-zero
    # reported seed — so the note must interpolate the real shrunk seed, not a hard-coded value.
    @given_seed()
    @settings(max_examples=200, database=None, deadline=None)
    def prop(seed: int) -> None:
        assert seed < 5, "planted seed-only failure"

    with pytest.raises(AssertionError) as excinfo:
        prop()
    notes = getattr(excinfo.value, "__notes__", [])
    assert any("seedloop.replay(scenario, seed=5)" in note for note in notes)


async def _rng_scenario(world: World) -> None:
    # Uses only world.rng (the seeded stream), so the run is fully determined by the seed.
    for _ in range(5):
        world.record(world.rng.random())


def test_drawn_seed_run_is_deterministic_even_under_the_auditor() -> None:
    @given_seed()
    @settings(max_examples=40, database=None, deadline=None)
    def prop(seed: int) -> None:
        first = _run_one(_rng_scenario, seed)
        second = _run_one(_rng_scenario, seed)
        assert first == second  # Hypothesis's entropy never entered the run
        # The auditor (real time, unseeded random, os.urandom, threads) finds no leak, and the
        # audited run matches the normal one — the boundary holds under exploration.
        assert _run_one(_rng_scenario, seed, audit=True) == first

    prop()


def test_settings_are_respected_through_given_seed() -> None:
    # given_seed must not swallow Hypothesis's @settings: cap examples and count the runs.
    runs = 0

    @given_seed(n=st.integers(0, 1000))
    @settings(max_examples=15, database=None, deadline=None)
    def prop(seed: int, n: int) -> None:
        nonlocal runs
        runs += 1

    prop()
    assert runs <= 15
