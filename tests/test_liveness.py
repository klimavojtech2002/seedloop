"""A run that cannot finish is reported, not waited on: the step limit and what it says.

Scenarios that would spin forever without the limit run in a child interpreter, so a regression
fails one test instead of hanging the job.
"""

from __future__ import annotations

import asyncio
import inspect
import math

import pytest

import seedloop
from _support import isolated_check, run_isolated
from seedloop._run import _run_one
from seedloop._world import World

_SPIN = """
async def scenario(world):
    while True:
        await asyncio.sleep(0)
"""


def test_a_spinning_scenario_raises_livelock_error() -> None:
    outcome = isolated_check(_SPIN, options="max_steps=1_000")
    assert outcome["type"] == "LivelockError"
    assert outcome["seed"] == 0
    assert "scenario" in outcome["message"]  # the running tasks are named


def test_livelock_error_is_a_seedloop_error() -> None:
    assert issubclass(seedloop.LivelockError, seedloop.SeedloopError)


def test_the_message_names_the_limit_and_the_virtual_time() -> None:
    outcome = isolated_check(
        """
async def scenario(world):
    await asyncio.sleep(2.5)
    while True:
        await asyncio.sleep(0)
""",
        options="max_steps=500",
    )
    assert outcome["type"] == "LivelockError"
    assert "500" in outcome["message"]
    assert "t=2.5" in outcome["message"]


def test_the_same_seed_reports_the_same_livelock() -> None:
    first = isolated_check(_SPIN, seeds="[3]", options="max_steps=800")
    second = isolated_check(_SPIN, seeds="[3]", options="max_steps=800")
    assert first["message"] == second["message"]


def test_a_poller_next_to_a_sleep_is_a_livelock_at_time_zero() -> None:
    # While a task keeps the ready queue non-empty the clock cannot advance, so the one-second
    # sleep never completes. A real loop would finish it; here it must be reported, not hang.
    outcome = isolated_check(
        """
flag = {"stop": False}

async def poller():
    while not flag["stop"]:
        await asyncio.sleep(0)

async def scenario(world):
    task = asyncio.ensure_future(poller())
    await asyncio.sleep(1.0)
    flag["stop"] = True
    await task
""",
        options="max_steps=2_000",
    )
    assert outcome["type"] == "LivelockError"
    assert "t=0.0" in outcome["message"]


def test_the_limit_is_exact() -> None:
    # A run that needs T steps passes with max_steps=T and fails with T-1: one threshold, no gap.
    async def scenario(world: World) -> None:
        for _ in range(5):
            await asyncio.sleep(0)

    outcomes = [
        seedloop.check(scenario, seeds=1, on_failure="return", max_steps=limit).error is None
        for limit in range(1, 40)
    ]
    first_pass = outcomes.index(True)
    assert first_pass > 0
    assert not any(outcomes[:first_pass])
    assert all(outcomes[first_pass:])


def test_replay_takes_the_same_limit() -> None:
    child = run_isolated(
        "import asyncio, seedloop\n"
        "async def scenario(world):\n"
        "    while True:\n"
        "        await asyncio.sleep(0)\n"
        "try:\n"
        "    seedloop.replay(scenario, seed=0, max_steps=1_000)\n"
        "except seedloop.LivelockError as error:\n"
        "    print(type(error).__name__)\n",
    )
    assert child.stdout.strip() == "LivelockError", child.stderr


def test_the_default_limit_is_ten_million() -> None:
    for function in (seedloop.check, seedloop.replay):
        assert inspect.signature(function).parameters["max_steps"].default == 10_000_000


def test_no_limit_is_allowed() -> None:
    async def scenario(world: World) -> None:
        for _ in range(20_000):
            await asyncio.sleep(0)

    assert seedloop.check(scenario, seeds=1, on_failure="return", max_steps=None).error is None


@pytest.mark.parametrize("bad", [0, -1, True, False, 1.5, "10", math.inf])
def test_a_bad_limit_is_a_usage_error(bad: object) -> None:
    async def scenario(world: World) -> None:
        pass

    # True would otherwise mean "one step" and fail the run with a LivelockError, which is also a
    # SeedloopError: the message shows the value was refused before anything ran.
    with pytest.raises(seedloop.SeedloopError, match="positive int or None") as caught:
        seedloop.check(scenario, seeds=1, max_steps=bad)  # type: ignore[arg-type]
    assert not isinstance(caught.value, seedloop.LivelockError)


def test_a_bad_limit_is_not_reported_as_a_failing_seed() -> None:
    async def scenario(world: World) -> None:
        pass

    with pytest.raises(seedloop.SeedloopError) as caught:
        seedloop.check(scenario, seeds=1, on_failure="return", max_steps=0)
    assert not any("failing seed" in note for note in getattr(caught.value, "__notes__", []))


def test_a_run_inside_the_limit_is_unchanged() -> None:
    async def scenario(world: World) -> None:
        await asyncio.sleep(1)
        world.record(("done", world.now()))

    assert list(_run_one(scenario, 0)) == [(1.0, ("done", 1.0))]


def test_the_limit_counts_loop_steps_exactly(monkeypatch: pytest.MonkeyPatch) -> None:
    # Count the steps independently of the limit, then check the boundary on both sides.
    from seedloop._loop import DeterministicLoop

    taken = 0
    real = DeterministicLoop._run_once

    def counting(self: DeterministicLoop) -> None:
        nonlocal taken
        taken += 1
        real(self)

    async def scenario(world: World) -> None:
        for _ in range(5):
            await asyncio.sleep(0)

    monkeypatch.setattr(DeterministicLoop, "_run_once", counting)
    seedloop.check(scenario, seeds=1, max_steps=None)
    needed = taken
    monkeypatch.undo()

    assert seedloop.check(scenario, seeds=1, on_failure="return", max_steps=needed).error is None
    failed = seedloop.check(scenario, seeds=1, on_failure="return", max_steps=needed - 1)
    assert isinstance(failed.error, seedloop.LivelockError)
