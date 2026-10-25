"""Settling the scenario task and tearing the rest down: order, deadlocks and cleanup that waits."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from _sim import spawn
from seedloop._run import _run_one
from seedloop._world import World
from seedloop.errors import DeadlockError, InvariantError


def _timeline(scenario: Any) -> tuple[str, list[Any]]:
    worlds: list[World] = []

    async def wrapped(world: World) -> None:
        worlds.append(world)
        await scenario(world)

    outcome = "ok"
    try:
        _run_one(wrapped, 0)
    except RuntimeError as exc:
        outcome = f"RuntimeError: {exc}"
    except InvariantError:
        outcome = "InvariantError"
    return outcome, [event[1] for event in worlds[0].timeline]  # type: ignore[index]


class _OrderedSet(list):  # type: ignore[type-arg]
    """A list that iterates in the order given, standing in for the set ``all_tasks`` returns."""


def test_cleanup_that_sleeps_survives_teardown() -> None:
    log: list[str] = []

    class Node:
        async def run(self) -> None:
            try:
                await asyncio.sleep(100)
            except asyncio.CancelledError:
                await asyncio.sleep(1)  # the stop state must not be sticky
                log.append("cleaned")
                raise

    async def scenario(world: World) -> None:
        world.start(Node())
        await asyncio.sleep(1)

    _run_one(scenario, 0)
    assert log == ["cleaned"]


def test_teardown_deadlock_still_raises_deadlock_error() -> None:
    class Stuck:
        async def run(self) -> None:
            try:
                await asyncio.sleep(100)
            except asyncio.CancelledError:
                await asyncio.get_running_loop().create_future()

    async def scenario(world: World) -> None:
        world.start(Stuck())
        await asyncio.sleep(0.1)

    with pytest.raises(DeadlockError):
        _run_one(scenario, 0)


def test_the_scenario_task_unwinds_before_the_nodes_are_cancelled() -> None:
    async def scenario(world: World) -> None:
        state = {"bad": False}
        world.always(lambda: not state["bad"], name="inv")

        class Node:
            async def run(self) -> None:
                try:
                    await asyncio.sleep(100)
                except asyncio.CancelledError:
                    world.record("node-cancelled")
                    raise

        async def flip() -> None:
            await asyncio.sleep(1)
            state["bad"] = True

        world.start(Node())
        spawn(flip())
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            await asyncio.sleep(0)
            world.record("unwind1")
            await asyncio.sleep(0)
            world.record("unwind2")
            raise

    assert _timeline(scenario) == ("InvariantError", ["unwind1", "unwind2", "node-cancelled"])


def test_a_shield_at_the_end_of_the_scenario_does_not_hold_the_run() -> None:
    log: list[str] = []

    async def scenario(world: World) -> None:
        async def slow() -> None:
            await asyncio.sleep(5)
            log.append("slow")

        spawn(asyncio.shield(slow()))  # type: ignore[arg-type]

    _run_one(scenario, 0)
    assert log == []  # the shielded work is cancelled with everything else, not awaited


def test_a_done_callback_that_schedules_work_on_the_scenario_task() -> None:
    seen: list[str] = []

    async def scenario(world: World) -> None:
        loop = asyncio.get_running_loop()
        task = asyncio.current_task()
        assert task is not None
        task.add_done_callback(lambda _t: loop.call_soon(lambda: seen.append("callback-work")))

    _run_one(scenario, 0)
    assert seen == ["callback-work"]


def test_cleanup_that_uses_a_timeout_survives_teardown() -> None:
    log: list[str] = []

    class Node:
        async def run(self) -> None:
            try:
                await asyncio.sleep(100)
            except asyncio.CancelledError:
                try:
                    async with asyncio.timeout(1):
                        await asyncio.sleep(5)
                except TimeoutError:
                    log.append("timed out")
                raise

    async def scenario(world: World) -> None:
        world.start(Node())
        await asyncio.sleep(1)

    _run_one(scenario, 0)
    assert log == ["timed out"]


def test_an_unstamped_task_is_cancelled_before_the_scenario_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A task built straight from asyncio.Task has no creation stamp; it sorts before every stamped
    # task, so teardown cancels it first and the order stays the same from run to run. The set
    # asyncio returns is handed over in the worst order (stamped scenario task first), so only the
    # sort key can put the unstamped tasks ahead.
    real_all_tasks = asyncio.all_tasks

    def worst_order(loop: asyncio.AbstractEventLoop | None = None) -> set[asyncio.Task[object]]:
        ordered = sorted(real_all_tasks(loop), key=lambda t: getattr(t, "_sl_seq", 10**9))
        return _OrderedSet(ordered)  # type: ignore[return-value]

    monkeypatch.setattr(asyncio, "all_tasks", worst_order)
    keep: list[asyncio.Future[None]] = []
    worlds: list[World] = []

    async def scenario(world: World) -> None:
        worlds.append(world)
        state = {"bad": False}
        world.always(lambda: not state["bad"], name="inv")
        loop = asyncio.get_running_loop()

        async def unstamped() -> None:
            try:
                await loop.create_future()
            except asyncio.CancelledError:
                world.record("unstamped-cancelled")
                raise

        for _ in range(3):
            keep.append(asyncio.Task(unstamped(), loop=loop))

        async def flip() -> None:
            await asyncio.sleep(1)
            state["bad"] = True

        spawn(flip())
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            world.record("main-cancelled-1")
            try:
                await loop.create_future()  # never completes: settling the scenario task deadlocks
            except asyncio.CancelledError:
                world.record("main-cancelled-2")
                raise

    code_events: list[object] = []
    with pytest.raises(InvariantError):
        _run_one(scenario, 0)
    code_events = [event[1] for event in worlds[0].timeline]  # type: ignore[index]
    assert code_events == ["main-cancelled-1", *["unstamped-cancelled"] * 3, "main-cancelled-2"]
