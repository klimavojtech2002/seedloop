"""Teardown ends: a task that will not stop is reported; work cleanup starts is finished.

Scenarios that would never finish tearing down run in a child interpreter, so a regression fails
one test instead of hanging the job.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import pytest

import seedloop
from _support import isolated_check, run_isolated
from seedloop._run import _run_one
from seedloop._world import World

_STUBBORN = """
class Stubborn:
    async def run(self):
        while True:
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.sleep(1)
"""


def test_a_task_that_ignores_cancellation_raises_teardown_error() -> None:
    outcome = isolated_check(
        _STUBBORN
        + """
async def scenario(world):
    world.start(Stubborn())
    await asyncio.sleep(0.1)
"""
    )
    assert outcome["type"] == "TeardownError"
    assert "Stubborn.run" in outcome["message"]


def test_teardown_error_is_a_seedloop_error() -> None:
    assert issubclass(seedloop.TeardownError, seedloop.SeedloopError)


def test_a_scenario_failure_stays_the_error_and_teardown_becomes_a_note() -> None:
    outcome = isolated_check(
        _STUBBORN
        + """
async def scenario(world):
    world.start(Stubborn())
    await asyncio.sleep(0.1)
    raise AssertionError("the real failure")
"""
    )
    assert outcome["type"] == "AssertionError"
    assert any("TeardownError" in note for note in outcome["notes"]), outcome["notes"]


def test_the_same_seed_reports_the_same_teardown_error() -> None:
    source = (
        _STUBBORN
        + """
async def scenario(world):
    world.start(Stubborn())
    await asyncio.sleep(0.1)
"""
    )
    first = isolated_check(source, seeds="[4]")
    second = isolated_check(source, seeds="[4]")
    assert first["message"] == second["message"]


def test_cancel_handlers_that_keep_spawning_are_stopped_by_the_round_limit() -> None:
    outcome = isolated_check(
        """
kept = []

async def child():
    try:
        await asyncio.sleep(10)
    except asyncio.CancelledError:
        kept.append(asyncio.ensure_future(child()))
        raise

async def scenario(world):
    kept.append(asyncio.ensure_future(child()))
    await asyncio.sleep(0.1)
"""
    )
    assert outcome["type"] == "TeardownError"


def test_a_task_started_by_a_cancel_handler_is_cancelled_too() -> None:
    spawned: list[asyncio.Task[None]] = []

    class Node:
        async def run(self) -> None:
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                spawned.append(asyncio.ensure_future(asyncio.sleep(10)))
                raise

    async def scenario(world: World) -> None:
        world.start(Node())
        await asyncio.sleep(0.1)

    _run_one(scenario, 0)
    assert len(spawned) == 1
    assert spawned[0].cancelled()


def test_a_check_registered_by_cleanup_does_not_cut_teardown_short() -> None:
    # Invariants describe the logical run, not cancellation cleanup: a check that cleanup adds,
    # even one that is false, must not end teardown before the cleanup has finished.
    log: list[str] = []

    class Node:
        def __init__(self, world: World) -> None:
            self.world = world

        async def run(self) -> None:
            try:
                await asyncio.sleep(100)
            except asyncio.CancelledError:
                self.world.always(lambda: False, name="added during teardown")
                await asyncio.sleep(1)
                log.append("cleaned")
                raise

    async def scenario(world: World) -> None:
        world.start(Node(world))
        await asyncio.sleep(1)

    _run_one(scenario, 0)
    assert log == ["cleaned"]


@pytest.mark.parametrize("where", ["cancel handler", "after cleanup"])
def test_loop_stop_during_teardown_does_not_fail_a_passing_scenario(where: str) -> None:
    log: list[str] = []

    class Node:
        async def run(self) -> None:
            try:
                await asyncio.sleep(100)
            except asyncio.CancelledError:
                if where == "cancel handler":
                    asyncio.get_running_loop().stop()
                await asyncio.sleep(1)
                if where == "after cleanup":
                    asyncio.get_running_loop().stop()
                log.append("cleaned")
                raise

    async def scenario(world: World) -> None:
        world.start(Node())
        await asyncio.sleep(1)

    _run_one(scenario, 0)
    assert log == ["cleaned"]


def test_a_runtime_error_that_is_not_a_stop_is_not_swallowed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Only the loop's own "stopped before the future completed" is retried; anything else that
    # escapes the loop while teardown drives it is a real error and must propagate.
    world = World(0)
    future: asyncio.Future[None] = world._loop.create_future()

    def broken(_: object) -> None:
        raise RuntimeError("something else")

    monkeypatch.setattr(world._loop, "run_until_complete", broken)
    with pytest.raises(RuntimeError, match="something else"):
        world._run_until(future)
    world._close()


def test_the_allowance_is_a_hundred_thousand_steps_plus_a_hundred_per_task() -> None:
    world = World(0)
    world._limit_teardown_steps(3)
    assert world._loop._sl_steps_left == 100_300
    world._limit_teardown_steps()
    assert world._loop._sl_steps_left == 100_000
    world._close()


def test_tasks_teardown_gave_up_on_are_not_logged_as_destroyed_pending() -> None:
    # The error already names them; asyncio's own "Task was destroyed but it is pending" on top
    # would be a second, noisier report of the same thing.
    child = run_isolated(
        "import asyncio, contextlib, gc, seedloop\n"
        "class Stubborn:\n"
        "    async def run(self):\n"
        "        while True:\n"
        "            with contextlib.suppress(asyncio.CancelledError):\n"
        "                await asyncio.sleep(1)\n"
        "async def scenario(world):\n"
        "    world.start(Stubborn())\n"
        "    await asyncio.sleep(0.1)\n"
        "result = seedloop.check(scenario, seeds=1, on_failure='return')\n"
        "del result\n"
        "gc.collect()\n"
        "print('done')\n"
    )
    assert child.stdout.strip() == "done", child.stderr
    assert "destroyed but it is pending" not in child.stderr


def test_a_scenario_that_ignores_cancellation_after_a_failure_is_still_bounded() -> None:
    # The step limit of the run must not carry over into the settling of the scenario task: with no
    # limit set, a main task that swallows cancellation would otherwise settle for ever.
    outcome = isolated_check(
        """
async def scenario(world):
    state = {"bad": False}
    world.always(lambda: not state["bad"], name="inv")

    async def flip():
        await asyncio.sleep(1)
        state["bad"] = True

    asyncio.ensure_future(flip())
    while True:
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.sleep(2)
""",
        options="max_steps=None",
    )
    assert outcome["type"] == "InvariantError"
    teardown_notes = [n for n in outcome["notes"] if "TeardownError" in n]
    assert len(teardown_notes) == 1, outcome["notes"]


def test_cancel_handlers_that_keep_spawning_get_exactly_one_hundred_rounds() -> None:
    outcome = isolated_check(
        """
cancelled = []

async def child():
    try:
        await asyncio.sleep(10)
    except asyncio.CancelledError:
        cancelled.append(1)
        asyncio.ensure_future(child())
        raise

async def scenario(world):
    asyncio.ensure_future(child())
    await asyncio.sleep(0.1)""",
        report="len(cancelled)",
    )
    assert outcome["type"] == "TeardownError"
    assert outcome["extra"] == 100  # one cancellation per round
    assert "child" in outcome["message"]  # the tasks still running are named


def test_a_round_with_many_tasks_gets_a_share_of_steps_per_task() -> None:
    # A thousand cancel handlers that clean up one after another (through a lock) take about
    # 150 000 steps in a single round: more than the flat allowance, within the per-task share.
    done: list[int] = []

    async def scenario(world: World) -> None:
        lock = asyncio.Lock()

        async def worker(index: int) -> None:
            try:
                await asyncio.sleep(100)
            except asyncio.CancelledError:
                async with lock:
                    for _ in range(150):
                        await asyncio.sleep(0)
                done.append(index)
                raise

        for index in range(1000):
            world.start(_Worker(worker, index))
        await asyncio.sleep(1)

    class _Worker:
        def __init__(self, work: Callable[[int], Awaitable[None]], index: int) -> None:
            self.work, self.index = work, index

        async def run(self) -> None:
            await self.work(self.index)

    _run_one(scenario, 0)
    assert len(done) == 1000


def test_tasks_a_deadlocked_teardown_leaves_behind_are_not_logged_as_destroyed_pending() -> None:
    child = run_isolated(
        "import asyncio, gc, seedloop\n"
        "class Stuck:\n"
        "    async def run(self):\n"
        "        try:\n"
        "            await asyncio.sleep(100)\n"
        "        except asyncio.CancelledError:\n"
        "            await asyncio.Event().wait()\n"
        "async def scenario(world):\n"
        "    world.start(Stuck())\n"
        "    await asyncio.sleep(1)\n"
        "result = seedloop.check(scenario, seeds=1, on_failure='return')\n"
        "print(type(result.error).__name__)\n"
        "del result\n"
        "gc.collect()\n"
    )
    assert child.stdout.split() == ["DeadlockError"], child.stderr
    assert "destroyed but it is pending" not in child.stderr


def test_tasks_the_round_limit_gave_up_on_are_not_logged_as_destroyed_pending() -> None:
    child = run_isolated(
        "import asyncio, gc, seedloop\n"
        "kept = []\n"
        "async def child():\n"
        "    try:\n"
        "        await asyncio.sleep(10)\n"
        "    except asyncio.CancelledError:\n"
        "        kept.append(asyncio.ensure_future(child()))\n"
        "        raise\n"
        "async def scenario(world):\n"
        "    kept.append(asyncio.ensure_future(child()))\n"
        "    await asyncio.sleep(0.1)\n"
        "result = seedloop.check(scenario, seeds=1, on_failure='return')\n"
        "print(type(result.error).__name__)\n"
        "del result\n"
        "kept.clear()\n"
        "gc.collect()\n"
    )
    assert child.stdout.split() == ["TeardownError"], child.stderr
    assert "destroyed but it is pending" not in child.stderr


def test_an_interrupt_during_teardown_keeps_asyncios_own_log_of_the_unfinished_task() -> None:
    # The interrupt does not name the tasks it left behind, so asyncio's line is all there is.
    child = run_isolated(
        "import asyncio, gc, seedloop\n"
        "class Node:\n"
        "    async def run(self):\n"
        "        try:\n"
        "            await asyncio.sleep(100)\n"
        "        except asyncio.CancelledError:\n"
        "            raise KeyboardInterrupt\n"
        "class Other:\n"
        "    async def run(self):\n"
        "        await asyncio.sleep(100)\n"
        "async def scenario(world):\n"
        "    world.start(Node(), Other())\n"
        "    await asyncio.sleep(1)\n"
        "try:\n"
        "    seedloop.check(scenario, seeds=1)\n"
        "except KeyboardInterrupt:\n"
        "    print('interrupted')\n"
        "gc.collect()\n"
    )
    assert child.stdout.split() == ["interrupted"], child.stderr
    assert "destroyed but it is pending" in child.stderr
