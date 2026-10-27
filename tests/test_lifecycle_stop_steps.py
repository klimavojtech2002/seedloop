"""The steps after the scenario returns: the two stop steps, and user code calling loop.stop()."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from _sim import spawn
from seedloop._run import _run_one
from seedloop._world import World
from seedloop.errors import InvariantError


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


def _spawn_chain(world: World, names: list[str]) -> None:
    # Each task records its name and starts the next one: the record shows how far the run got.
    async def link(index: int) -> None:
        world.record(names[index])
        if index + 1 < len(names):
            spawn(link(index + 1))

    spawn(link(0))


def test_a_child_spawned_in_the_last_step_runs_before_teardown() -> None:
    seen: list[str] = []

    async def scenario(world: World) -> None:
        async def child() -> None:
            seen.append("child")

        await asyncio.sleep(0)
        spawn(child())

    _run_one(scenario, 0)
    assert seen == ["child"]


def test_a_grandchild_runs_in_the_second_stop_step_but_not_a_third() -> None:
    seen: list[str] = []

    async def scenario(world: World) -> None:
        async def grandchild() -> None:
            seen.append("grandchild")

        async def child() -> None:
            seen.append("child")
            spawn(grandchild())

        spawn(child())

    _run_one(scenario, 0)
    assert seen == ["child", "grandchild"]

    seen.clear()

    async def too_deep(world: World) -> None:
        async def great() -> None:
            seen.append("great")

        async def grandchild() -> None:
            seen.append("grandchild")
            spawn(great())

        async def child() -> None:
            seen.append("child")
            spawn(grandchild())

        spawn(child())

    _run_one(too_deep, 0)
    assert seen == ["child", "grandchild"]  # the third generation is cancelled before it starts


@pytest.mark.parametrize(("steps", "fails"), [(1, True), (2, True), (3, False)])
def test_a_node_failing_soon_after_the_scenario_fails_the_seed(steps: int, fails: bool) -> None:
    # The run gives started nodes two more steps after the scenario returns; a node that fails in
    # the first or second of them fails the seed, a later failure is cancelled away.
    async def scenario(world: World) -> None:
        class Node:
            async def run(self) -> None:
                for _ in range(steps):
                    await asyncio.sleep(0)
                raise ValueError("late")

        world.start(Node())
        await asyncio.sleep(0)

    if fails:
        with pytest.raises(ValueError, match="late"):
            _run_one(scenario, 0)
    else:
        _run_one(scenario, 0)


def test_the_stop_steps_do_not_advance_the_clock() -> None:
    times: list[float] = []

    async def scenario(world: World) -> None:
        async def later() -> None:
            await asyncio.sleep(5)
            times.append(world.now())

        spawn(later())
        await asyncio.sleep(1)
        times.append(world.now())

    _run_one(scenario, 0)
    assert times == [1.0]  # the task sleeping until t=5 never wakes: no autojump after the scenario


def test_invariants_are_cleared_between_the_stop_steps() -> None:
    # A node that breaks the invariant while the run winds down (after the scenario returned) must
    # not fail the seed: invariants describe the logical run, not cancellation cleanup.
    async def scenario(world: World) -> None:
        state = {"bad": False}
        world.always(lambda: not state["bad"], name="inv")

        async def late() -> None:
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            state["bad"] = True

        spawn(late())

    _run_one(scenario, 0)


def test_a_stale_completion_callback_does_not_stop_the_teardown() -> None:
    # An invariant ends the run while the scenario task settles on a later step: asyncio's own
    # completion callback must not be left behind to stop the teardown early.
    async def scenario(world: World) -> None:
        state = {"bad": False}
        world.always(lambda: not state["bad"], name="inv")

        async def flip() -> None:
            await asyncio.sleep(1)
            state["bad"] = True

        spawn(flip())
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            raise

    with pytest.raises(InvariantError):
        _run_one(scenario, 0)


def test_loop_stop_in_the_finishing_step_still_gives_one_more_step() -> None:
    async def scenario(world: World) -> None:
        _spawn_chain(world, ["child", "grandchild"])
        asyncio.get_running_loop().stop()

    assert _timeline(scenario) == ("ok", ["child"])


def test_loop_stop_one_step_after_the_scenario_finished() -> None:
    async def scenario(world: World) -> None:
        _spawn_chain(world, ["child", "grandchild"])
        loop = asyncio.get_running_loop()
        task = asyncio.current_task()
        assert task is not None
        task.add_done_callback(lambda _t: loop.stop())

    assert _timeline(scenario) == ("ok", ["child", "grandchild"])


def test_loop_stop_two_steps_after_the_scenario_finished() -> None:
    async def scenario(world: World) -> None:
        _spawn_chain(world, ["child", "grandchild", "great"])
        loop = asyncio.get_running_loop()
        task = asyncio.current_task()
        assert task is not None
        task.add_done_callback(lambda _t: loop.call_soon(lambda: loop.call_soon(loop.stop)))

    # The stop lands while teardown is cancelling what is left. Like a stop during the unwinding of
    # the scenario task, it ends one pass of the loop, not the teardown, so the run still passes.
    outcome, events = _timeline(scenario)
    assert events == ["child", "grandchild"]
    assert outcome == "ok"


def test_loop_stop_before_the_scenario_finished_is_reported() -> None:
    async def scenario(world: World) -> None:
        asyncio.get_running_loop().stop()
        await asyncio.sleep(1)

    outcome, _ = _timeline(scenario)
    assert outcome == "RuntimeError: Event loop stopped before Future completed."


def test_loop_stop_in_a_cancel_handler_does_not_end_the_unwinding() -> None:
    async def scenario(world: World) -> None:
        state = {"bad": False}
        world.always(lambda: not state["bad"], name="inv")

        async def flip() -> None:
            await asyncio.sleep(1)
            state["bad"] = True

        spawn(flip())
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            asyncio.get_running_loop().stop()
            await asyncio.sleep(0)
            world.record("after-stop")
            raise

    assert _timeline(scenario) == ("InvariantError", ["after-stop"])


@pytest.mark.parametrize(
    ("stop", "steps", "fails"),
    [
        (False, 0, True),
        (False, 1, True),
        (False, 2, False),
        (False, 3, False),
        (True, 0, True),
        (True, 1, False),
        (True, 2, False),
    ],
)
def test_invariants_stop_counting_after_the_first_stop_step(
    stop: bool, steps: int, fails: bool
) -> None:
    # A node breaks an invariant `steps` loop steps after the scenario's last await. Until the first
    # stop step the invariant still counts; afterwards the run is winding down and it does not.
    # With a user loop.stop() in the finishing step the first stop step is the only extra one.
    async def scenario(world: World) -> None:
        state = {"bad": False}
        world.always(lambda: not state["bad"], name="inv")

        async def flip() -> None:
            for _ in range(steps):
                await asyncio.sleep(0)
            state["bad"] = True

        spawn(flip())
        await asyncio.sleep(0)
        if stop:
            asyncio.get_running_loop().stop()

    if fails:
        with pytest.raises(InvariantError):
            _run_one(scenario, 0)
    else:
        _run_one(scenario, 0)


def test_loop_stop_in_the_step_where_a_cancelled_scenario_ends() -> None:
    # The scenario task is settled on its own after a failure; its cancel handler stops the loop in
    # the very step it finishes in. The failure in flight is still the result.
    async def scenario(world: World) -> None:
        state = {"bad": False}
        world.always(lambda: not state["bad"], name="inv")

        async def flip() -> None:
            await asyncio.sleep(1)
            state["bad"] = True

        spawn(flip())
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            asyncio.get_running_loop().stop()
            raise

    with pytest.raises(InvariantError) as caught:
        _run_one(scenario, 0)
    # Settling went wrong silently if a note was attached.
    assert not getattr(caught.value, "__notes__", [])


def test_an_invariant_failing_in_a_stop_step_still_gives_the_next_step() -> None:
    # The scenario returns at once; the invariant breaks in the very first task step. The check
    # ends the run, and the step that follows (with the checks cleared) still runs once.
    async def scenario(world: World) -> None:
        state = {"bad": False}
        world.always(lambda: not state["bad"], name="inv")

        async def chain(index: int) -> None:
            world.record(f"c{index}")
            if index == 0:
                state["bad"] = True
            if index < 8:
                spawn(chain(index + 1))

        spawn(chain(0))

    worlds: list[World] = []

    async def wrapped(world: World) -> None:
        worlds.append(world)
        await scenario(world)

    with pytest.raises(InvariantError):
        _run_one(wrapped, 0)
    assert [event[1] for event in worlds[0].timeline] == ["c0", "c1"]  # type: ignore[index]


def test_sleeping_cleanup_works_after_a_check_ended_the_run_in_a_stop_step() -> None:
    # The extra step must not leave the loop in a stop state: teardown needs the clock to move.
    log: list[str] = []

    class Node:
        async def run(self) -> None:
            try:
                await asyncio.sleep(100)
            except asyncio.CancelledError:
                await asyncio.sleep(1)
                log.append("cleaned")
                raise

    async def scenario(world: World) -> None:
        state = {"bad": False}
        world.always(lambda: not state["bad"], name="inv")

        async def flip() -> None:
            state["bad"] = True

        world.start(Node())
        spawn(flip())

    with pytest.raises(InvariantError):
        _run_one(scenario, 0)
    assert log == ["cleaned"]


def test_a_check_failing_in_the_second_stop_step_ends_the_run() -> None:
    async def scenario(world: World) -> None:
        async def child() -> None:
            async def grandchild() -> None:
                world.always(lambda: False, name="late")

            spawn(grandchild())

        spawn(child())

    with pytest.raises(InvariantError):
        _run_one(scenario, 0)


def test_an_interrupt_in_the_second_stop_step_propagates() -> None:
    async def scenario(world: World) -> None:
        async def child() -> None:
            async def grandchild() -> None:
                raise SystemExit(3)

            spawn(grandchild())

        spawn(child())

    with pytest.raises(SystemExit):
        _run_one(scenario, 0)
