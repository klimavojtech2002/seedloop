"""A deadlock names who is stuck on what, in text that is the same on every run."""

from __future__ import annotations

import asyncio
import math
import re
from typing import Any

import pytest

import seedloop
from seedloop._run import _run_one
from seedloop._world import World


def _deadlock_message(scenario: Any, seed: int = 0) -> str:
    result = seedloop.check(scenario, seeds=[seed], on_failure="return")
    assert isinstance(result.error, seedloop.DeadlockError)
    return str(result.error)


def test_the_message_names_the_blocked_task_and_what_it_waits_on() -> None:
    async def wait_for_a_signal(event: asyncio.Event) -> None:
        await event.wait()

    async def scenario(world: World) -> None:
        await wait_for_a_signal(asyncio.Event())

    message = _deadlock_message(scenario)
    assert "scenario" in message
    assert "Future" in message  # the awaited future's type


def test_every_blocked_task_is_listed() -> None:
    async def scenario(world: World) -> None:
        event = asyncio.Event()

        async def stuck_node() -> None:
            await event.wait()

        asyncio.ensure_future(stuck_node())  # noqa: RUF006
        asyncio.ensure_future(stuck_node())  # noqa: RUF006
        await event.wait()

    message = _deadlock_message(scenario)
    assert message.count("stuck_node") == 2
    assert "scenario" in message


def test_at_most_twenty_tasks_are_listed_and_the_rest_are_counted() -> None:
    async def scenario(world: World) -> None:
        event = asyncio.Event()

        async def stuck_node() -> None:
            await event.wait()

        for _ in range(30):
            asyncio.ensure_future(stuck_node())  # noqa: RUF006
        await event.wait()

    message = _deadlock_message(scenario)
    assert message.count(" awaiting Future") == 20
    assert "and 11 more" in message  # 31 blocked tasks in all: the scenario and thirty nodes


def test_the_text_has_no_memory_addresses_and_is_the_same_every_time() -> None:
    async def scenario(world: World) -> None:
        await asyncio.Event().wait()

    first = _deadlock_message(scenario)
    second = _deadlock_message(scenario)
    assert first == second
    assert not re.search(r"0x[0-9a-fA-F]+", first)


def test_a_sleep_with_no_end_is_a_deadlock_not_a_pass() -> None:
    async def scenario(world: World) -> None:
        await asyncio.sleep(math.inf)
        world.record(("woke", world.now()))

    message = _deadlock_message(scenario)
    assert "scenario" in message


def test_a_sleep_with_no_end_next_to_a_finite_one_still_passes() -> None:
    async def scenario(world: World) -> None:
        asyncio.ensure_future(asyncio.sleep(math.inf))  # noqa: RUF006
        await asyncio.sleep(1)
        world.record(("done", world.now()))

    assert list(_run_one(scenario, 0)) == [(1.0, ("done", 1.0))]


def test_a_scenario_that_is_not_a_coroutine_is_a_seedloop_error() -> None:
    def not_a_coroutine(world: World) -> None:
        return None

    with pytest.raises(seedloop.SeedloopError):
        _run_one(not_a_coroutine, 0)  # type: ignore[arg-type]


def test_exactly_twenty_blocked_tasks_are_all_listed_without_a_remainder() -> None:
    async def scenario(world: World) -> None:
        event = asyncio.Event()

        async def stuck_node() -> None:
            await event.wait()

        for _ in range(19):
            asyncio.ensure_future(stuck_node())  # noqa: RUF006
        await event.wait()

    message = _deadlock_message(scenario)
    assert message.count(" awaiting Future") == 20
    assert "more" not in message


def test_a_task_the_loop_did_not_index_is_listed_first_as_unknown() -> None:
    async def scenario(world: World) -> None:
        loop = asyncio.get_running_loop()
        event = asyncio.Event()

        async def direct() -> None:
            await event.wait()

        asyncio.Task(direct(), loop=loop)
        await event.wait()

    message = _deadlock_message(scenario)
    listing = message.split("Blocked tasks: ", 1)[1]
    assert listing.startswith("#? "), listing
    assert "#0 " in listing


def test_tasks_the_loop_did_not_index_are_listed_in_a_fixed_order() -> None:
    # They have no creation index, and the set they come from is in memory-address order.
    async def scenario(world: World) -> None:
        loop = asyncio.get_running_loop()
        event = asyncio.Event()

        async def waiter() -> None:
            await event.wait()

        def named(name: str) -> object:
            waiter.__qualname__ = name
            return waiter()

        for name in ["f", "b", "e", "a", "d", "c"]:
            asyncio.Task(named(name), loop=loop)  # type: ignore[arg-type]
        await event.wait()

    listing = _deadlock_message(scenario).split("Blocked tasks: ", 1)[1]
    names = re.findall(r"#\? (\w)", listing)
    assert names == ["a", "b", "c", "d", "e", "f"]
