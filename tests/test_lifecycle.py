"""The life of one run: refusing misuse, never losing a failure, closing the loop on every path.

Tests that could hang on a regression run in a fresh interpreter under a time cap
(``run_isolated``), so a bug shows up as a clear failure and not as a stuck test run.
"""

from __future__ import annotations

import asyncio
import gc
import warnings
from collections.abc import Coroutine
from typing import Any

import pytest

import seedloop
from _support import run_isolated
from seedloop._loop import DeterministicLoop
from seedloop._run import _run_one
from seedloop._world import World
from seedloop.errors import DeadlockError, InvariantError

_KEPT: list[asyncio.Future[None]] = []


def _spawn(coro: Coroutine[Any, Any, None]) -> None:
    # A task nobody references can be collected mid-run; keep it for the life of the test process.
    _KEPT.append(asyncio.ensure_future(coro))


# --- misuse is refused, not reported as a failing seed ---


def test_check_inside_a_running_loop_is_refused_untagged() -> None:
    code = """
import asyncio, seedloop

async def noop(world):
    pass

async def outer():
    seedloop.check(noop, seeds=3)

try:
    asyncio.run(outer())
except BaseException as exc:
    print(type(exc).__name__, "|", exc, "|", getattr(exc, "__notes__", None))
"""
    out = run_isolated(code, timeout=10).stdout.strip()
    assert "cannot run inside a running event loop" in out, out
    assert "replay with" not in out  # misuse is not a failing seed
    assert "failing seed" not in out


def test_replay_inside_a_running_loop_is_refused() -> None:
    code = """
import asyncio, seedloop

async def noop(world):
    pass

async def outer():
    seedloop.replay(noop, seed=0)

try:
    asyncio.run(outer())
except BaseException as exc:
    print("refused:", exc)
"""
    assert "cannot run inside a running event loop" in run_isolated(code, timeout=10).stdout


def test_a_refused_run_closes_its_coroutine() -> None:
    code = """
import asyncio, gc, warnings, seedloop
from seedloop._world import World

async def noop(world):
    pass

async def outer():
    world = World(0)
    coro = noop(world)
    try:
        world._drive(coro)
    except seedloop.SeedloopError:
        pass
    print("coroutine-closed", coro.cr_frame is None)

with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    asyncio.run(outer())
    gc.collect()
print("warnings", [str(w.message) for w in caught])
"""
    out = run_isolated(code, timeout=10).stdout
    assert "coroutine-closed True" in out
    assert "never awaited" not in out


# --- a failure already in flight is never lost ---


def test_interrupt_from_the_main_cancel_handler_is_not_lost() -> None:
    # An invariant fails; the scenario task is cancelled and its cancel handler raises an interrupt
    # (Ctrl-C arriving while the run winds down). The interrupt must reach the caller.
    async def scenario(world: World) -> None:
        state = {"bad": False}
        world.always(lambda: not state["bad"], name="inv")

        async def flip() -> None:
            await asyncio.sleep(1)
            state["bad"] = True

        _spawn(flip())
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            raise KeyboardInterrupt from None

    with pytest.raises(KeyboardInterrupt):
        _run_one(scenario, 0)


def test_system_exit_in_a_cancel_handler_propagates() -> None:
    async def scenario(world: World) -> None:
        world.always(lambda: world.now() < 1, name="early")
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            raise SystemExit(3) from None

    with pytest.raises(SystemExit):
        _run_one(scenario, 0)


def test_interrupt_from_a_node_then_a_cancel_handler_that_waits_forever() -> None:
    # The interrupt must not be replaced by the DeadlockError of the wait that follows it.
    code = """
import asyncio, _thread, seedloop

async def scenario(world):
    async def node():
        await asyncio.sleep(1)
        raise KeyboardInterrupt

    class Node:
        async def run(self):
            await node()

    world.start(Node())
    try:
        await asyncio.sleep(10)
    except asyncio.CancelledError:
        await asyncio.get_running_loop().create_future()

try:
    seedloop.check(scenario, seeds=1)
except BaseException as exc:
    print("propagated:", type(exc).__name__)
"""
    assert "propagated: KeyboardInterrupt" in run_isolated(code, timeout=10).stdout


def test_a_later_settle_error_is_a_note_not_a_replacement() -> None:
    # The invariant fails first; then the scenario's cancel handler deadlocks. The run reports the
    # invariant, with the deadlock attached as a note. Run in a child process, because it hangs if
    # this regresses.
    code = """
import asyncio, logging
from seedloop._run import _run_one
from seedloop.errors import InvariantError

logging.getLogger("asyncio").disabled = True
keep = []

async def scenario(world):
    state = {"bad": False}
    world.always(lambda: not state["bad"], name="inv")

    async def flip():
        await asyncio.sleep(1)
        state["bad"] = True

    keep.append(asyncio.ensure_future(flip()))
    try:
        await asyncio.sleep(10)
    except asyncio.CancelledError:
        await asyncio.get_running_loop().create_future()

try:
    _run_one(scenario, 0)
except InvariantError as exc:
    print("InvariantError", getattr(exc, "__notes__", []))
"""
    out = run_isolated(code, timeout=15).stdout
    assert out.startswith("InvariantError"), out
    assert "DeadlockError" in out, out


# --- the two stop steps ---


def test_a_child_spawned_in_the_last_step_runs_before_teardown() -> None:
    seen: list[str] = []

    async def scenario(world: World) -> None:
        async def child() -> None:
            seen.append("child")

        await asyncio.sleep(0)
        _spawn(child())

    _run_one(scenario, 0)
    assert seen == ["child"]


def test_a_grandchild_runs_in_the_second_stop_step_but_not_a_third() -> None:
    seen: list[str] = []

    async def scenario(world: World) -> None:
        async def grandchild() -> None:
            seen.append("grandchild")

        async def child() -> None:
            seen.append("child")
            _spawn(grandchild())

        _spawn(child())

    _run_one(scenario, 0)
    assert seen == ["child", "grandchild"]

    seen.clear()

    async def too_deep(world: World) -> None:
        async def great() -> None:
            seen.append("great")

        async def grandchild() -> None:
            seen.append("grandchild")
            _spawn(great())

        async def child() -> None:
            seen.append("child")
            _spawn(grandchild())

        _spawn(child())

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

        _spawn(later())
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

        _spawn(late())

    _run_one(scenario, 0)


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


def test_a_stale_completion_callback_does_not_stop_the_teardown() -> None:
    # An invariant ends the run while the scenario task settles on a later step: asyncio's own
    # completion callback must not be left behind to stop the teardown early.
    async def scenario(world: World) -> None:
        state = {"bad": False}
        world.always(lambda: not state["bad"], name="inv")

        async def flip() -> None:
            await asyncio.sleep(1)
            state["bad"] = True

        _spawn(flip())
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            raise

    with pytest.raises(InvariantError):
        _run_one(scenario, 0)


# --- the loop is closed on every path ---


def test_the_loop_is_closed_after_a_synchronous_raise() -> None:
    loops: list[DeterministicLoop] = []

    def scenario(world: World) -> asyncio.Future[None]:
        loops.append(world._loop)
        raise ValueError("sync")

    with pytest.raises(ValueError, match="sync"):
        _run_one(scenario, 0)
    assert loops[0].is_closed()


def test_the_loop_is_closed_for_a_scenario_that_is_not_a_coroutine() -> None:
    loops: list[DeterministicLoop] = []

    def scenario(world: World) -> None:
        loops.append(world._loop)

    with pytest.raises(TypeError):
        _run_one(scenario, 0)  # type: ignore[arg-type]
    assert loops[0].is_closed()


def test_no_resource_warning_after_a_failing_scenario() -> None:
    def raises_sync(world: World) -> None:
        raise ValueError("sync")

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = seedloop.check(raises_sync, seeds=2, on_failure="return")  # type: ignore[arg-type]
        assert isinstance(result.error, ValueError)
        del result  # its traceback keeps the World alive
        gc.collect()
    assert [w for w in caught if issubclass(w.category, ResourceWarning)] == []


def test_a_refused_run_leaves_no_open_loop() -> None:
    code = """
import asyncio, gc, warnings
from seedloop._run import _run_one

async def noop(world):
    pass

async def outer():
    try:
        _run_one(noop, 0)
    except Exception as exc:
        print("refused", type(exc).__name__)

with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    asyncio.run(outer())
    gc.collect()
print("warnings", [str(w.message) for w in caught])
"""
    out = run_isolated(code, timeout=10).stdout
    assert "refused" in out
    assert "warnings []" in out, out


# --- a NaN timer is rejected ---


def test_nan_delay_is_rejected() -> None:
    async def scenario(world: World) -> None:
        asyncio.get_running_loop().call_later(float("nan"), lambda: None)

    with pytest.raises(ValueError, match="NaN"):
        _run_one(scenario, 0)


# --- usage errors under Hypothesis are not annotated as failing examples ---


def test_given_seed_does_not_annotate_a_usage_error() -> None:
    pytest.importorskip("hypothesis")
    from seedloop.errors import _UsageError
    from seedloop.hypothesis import given_seed

    @given_seed()
    def scenario_test(*, seed: int) -> None:
        raise _UsageError("misuse")

    with pytest.raises(_UsageError) as caught:
        scenario_test()
    notes = getattr(caught.value, "__notes__", [])
    assert not [n for n in notes if "seedloop.replay" in n], notes  # Hypothesis adds its own


# --- the scenario task settles on its own, and user code may call loop.stop() ---------------------


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
        _spawn(flip())
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            await asyncio.sleep(0)
            world.record("unwind1")
            await asyncio.sleep(0)
            world.record("unwind2")
            raise

    assert _timeline(scenario) == ("InvariantError", ["unwind1", "unwind2", "node-cancelled"])


def _spawn_chain(world: World, names: list[str]) -> None:
    # Each task records its name and starts the next one: the record shows how far the run got.
    async def link(index: int) -> None:
        world.record(names[index])
        if index + 1 < len(names):
            _spawn(link(index + 1))

    _spawn(link(0))


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

    outcome, events = _timeline(scenario)
    assert events == ["child", "grandchild"]
    assert outcome == "RuntimeError: Event loop stopped before Future completed."


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

        _spawn(flip())
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            asyncio.get_running_loop().stop()
            await asyncio.sleep(0)
            world.record("after-stop")
            raise

    assert _timeline(scenario) == ("InvariantError", ["after-stop"])


def test_the_scenario_error_wins_over_a_teardown_deadlock() -> None:
    # In a child process: the stuck task is destroyed pending at exit, which asyncio logs.
    code = """
import asyncio, logging
from seedloop._run import _run_one

logging.getLogger("asyncio").disabled = True

class Stuck:
    async def run(self):
        try:
            await asyncio.sleep(100)
        except asyncio.CancelledError:
            await asyncio.get_running_loop().create_future()

async def scenario(world):
    world.start(Stuck())
    await asyncio.sleep(0.1)
    raise ValueError("first")

try:
    _run_one(scenario, 0)
except ValueError as exc:
    print("ValueError", exc, getattr(exc, "__notes__", None))
"""
    out = run_isolated(code, timeout=10).stdout
    assert out.startswith("ValueError first"), out
    assert "DeadlockError" in out, out


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

        _spawn(flip())
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

        _spawn(flip())
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
                _spawn(chain(index + 1))

        _spawn(chain(0))

    worlds: list[World] = []

    async def wrapped(world: World) -> None:
        worlds.append(world)
        await scenario(world)

    with pytest.raises(InvariantError):
        _run_one(wrapped, 0)
    assert [event[1] for event in worlds[0].timeline] == ["c0", "c1"]  # type: ignore[index]


def test_a_failure_while_unwinding_carries_no_internal_note() -> None:
    async def scenario(world: World) -> None:
        state = {"bad": False}
        world.always(lambda: not state["bad"], name="inv")

        async def flip() -> None:
            await asyncio.sleep(1)
            state["bad"] = True

        _spawn(flip())
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            await asyncio.sleep(0)
            raise

    with pytest.raises(InvariantError) as caught:
        _run_one(scenario, 0)
    assert not getattr(caught.value, "__notes__", [])


def test_an_error_raised_by_the_cancel_handler_is_a_note_on_the_failure() -> None:
    async def scenario(world: World) -> None:
        state = {"bad": False}
        world.always(lambda: not state["bad"], name="inv")

        async def flip() -> None:
            await asyncio.sleep(1)
            state["bad"] = True

        _spawn(flip())
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            raise ValueError("while unwinding") from None

    with pytest.raises(InvariantError) as caught:
        _run_one(scenario, 0)
    notes = getattr(caught.value, "__notes__", [])
    assert any("ValueError: while unwinding" in note for note in notes), notes


def test_a_shield_at_the_end_of_the_scenario_does_not_hold_the_run() -> None:
    log: list[str] = []

    async def scenario(world: World) -> None:
        async def slow() -> None:
            await asyncio.sleep(5)
            log.append("slow")

        _spawn(asyncio.shield(slow()))  # type: ignore[arg-type]

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


def test_a_cancel_handler_that_swallows_the_cancellation_adds_no_note() -> None:
    async def scenario(world: World) -> None:
        state = {"bad": False}
        world.always(lambda: not state["bad"], name="inv")

        async def flip() -> None:
            await asyncio.sleep(1)
            state["bad"] = True

        _spawn(flip())
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            return  # the scenario ends normally instead of re-raising

    with pytest.raises(InvariantError) as caught:
        _run_one(scenario, 0)
    assert not getattr(caught.value, "__notes__", [])


def test_refusal_happens_before_the_seed_loop() -> None:
    code = """
import asyncio, seedloop

async def noop(world):
    pass

async def outer():
    try:
        seedloop.check(noop, seeds=0)
    except Exception as exc:
        print("refused", "running event loop" in str(exc))

asyncio.run(outer())
"""
    assert "refused True" in run_isolated(code, timeout=10).stdout


def test_a_scenario_that_calls_check_is_a_usage_error_not_a_failing_seed() -> None:
    # In a child process: it hangs if this regresses.
    code = """
import seedloop

async def inner(world):
    pass

async def outer(world):
    seedloop.check(inner, seeds=1)

try:
    seedloop.check(outer, seeds=3)
except seedloop.SeedloopError as exc:
    print("usage error:", "running event loop" in str(exc))
"""
    assert "usage error: True" in run_isolated(code, timeout=15).stdout


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
        _spawn(flip())

    with pytest.raises(InvariantError):
        _run_one(scenario, 0)
    assert log == ["cleaned"]


def test_an_unstamped_task_is_cancelled_before_the_scenario_task() -> None:
    # A task built straight from asyncio.Task has no creation stamp; it sorts before every stamped
    # task, so teardown cancels it first and the order stays the same from run to run.
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

        for _ in range(30):  # enough that the order is not an accident of set iteration
            keep.append(asyncio.Task(unstamped(), loop=loop))

        async def flip() -> None:
            await asyncio.sleep(1)
            state["bad"] = True

        _spawn(flip())
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
    assert code_events == ["main-cancelled-1", *["unstamped-cancelled"] * 30, "main-cancelled-2"]


def test_a_check_failing_in_the_second_stop_step_ends_the_run() -> None:
    async def scenario(world: World) -> None:
        async def child() -> None:
            async def grandchild() -> None:
                world.always(lambda: False, name="late")

            _spawn(grandchild())

        _spawn(child())

    with pytest.raises(InvariantError):
        _run_one(scenario, 0)


def test_an_interrupt_in_the_second_stop_step_propagates() -> None:
    async def scenario(world: World) -> None:
        async def child() -> None:
            async def grandchild() -> None:
                raise SystemExit(3)

            _spawn(grandchild())

        _spawn(child())

    with pytest.raises(SystemExit):
        _run_one(scenario, 0)


def test_an_error_whose_text_cannot_be_built_does_not_replace_the_failure() -> None:
    class Hostile(Exception):
        def __str__(self) -> str:
            raise RuntimeError("str boom")

    async def scenario(world: World) -> None:
        state = {"bad": False}
        world.always(lambda: not state["bad"], name="inv")

        async def flip() -> None:
            await asyncio.sleep(1)
            state["bad"] = True

        _spawn(flip())
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            raise Hostile from None

    with pytest.raises(InvariantError) as caught:
        _run_one(scenario, 0)
    assert any("Hostile" in note for note in getattr(caught.value, "__notes__", []))


def test_a_nested_check_is_never_returned_as_a_failing_seed() -> None:
    async def inner(world: World) -> None:
        pass

    async def outer(world: World) -> None:
        seedloop.check(inner, seeds=1)

    with pytest.raises(seedloop.SeedloopError, match="running event loop") as caught:
        seedloop.check(outer, seeds=3, on_failure="return")
    assert not [n for n in getattr(caught.value, "__notes__", []) if "replay" in n]


def _run_with_shared_hook_error(error: Exception, handler_raises: Exception) -> BaseException:
    """A step hook fails with ``error``; then the scenario's cancel handler raises another error."""

    async def scenario(world: World) -> None:
        state = {"fail": False}

        def hook() -> Exception | None:
            if state["fail"]:
                raise error
            return None

        world._loop._sl_after_step_hooks.append(hook)

        async def trigger() -> None:
            await asyncio.sleep(1)
            state["fail"] = True

        _spawn(trigger())
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            raise handler_raises from None

    with pytest.raises(Exception) as caught:
        _run_one(scenario, 0)
    return caught.value


def test_the_same_error_from_the_hook_and_the_cancel_handler_is_not_a_note_on_itself() -> None:
    shared = ValueError("shared")
    raised = _run_with_shared_hook_error(shared, shared)
    assert raised is shared
    assert not getattr(raised, "__notes__", [])


def test_a_failure_with_unusable_notes_is_kept_when_a_later_error_cannot_be_noted() -> None:
    first = ValueError("first")
    first.__notes__ = "not a list"  # type: ignore[assignment]
    raised = _run_with_shared_hook_error(first, KeyError("later"))
    assert raised is first


def test_a_failure_that_cannot_take_a_note_is_still_the_reported_failure() -> None:
    class NoteRefusingError(Exception):
        def add_note(self, note: str) -> None:
            raise AttributeError("this exception takes no notes")

    class Stuck:
        async def run(self) -> None:
            try:
                await asyncio.sleep(100)
            except asyncio.CancelledError:
                await asyncio.get_running_loop().create_future()  # teardown deadlocks here

    async def scenario(world: World) -> None:
        world.start(Stuck())
        await asyncio.sleep(1)
        raise NoteRefusingError(3)

    with pytest.raises(NoteRefusingError):
        _run_one(scenario, 0)


def test_a_teardown_error_is_noted_on_the_interrupt_that_ended_the_run() -> None:
    class Stuck:
        async def run(self) -> None:
            try:
                await asyncio.sleep(100)
            except asyncio.CancelledError:
                await asyncio.get_running_loop().create_future()  # teardown deadlocks here

    async def scenario(world: World) -> None:
        state = {"bad": False}
        world.always(lambda: not state["bad"], name="inv")
        world.start(Stuck())

        async def flip() -> None:
            await asyncio.sleep(1)
            state["bad"] = True

        _spawn(flip())
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            raise KeyboardInterrupt from None

    with pytest.raises(KeyboardInterrupt) as caught:
        _run_one(scenario, 0)
    assert any("DeadlockError" in n for n in getattr(caught.value, "__notes__", []))


def test_an_interrupt_in_teardown_keeps_the_failure_it_replaced_as_a_note() -> None:
    class Node:
        async def run(self) -> None:
            try:
                await asyncio.sleep(100)
            except asyncio.CancelledError:
                raise KeyboardInterrupt from None

    async def scenario(world: World) -> None:
        world.start(Node())
        await asyncio.sleep(1)
        raise ValueError("first")

    with pytest.raises(KeyboardInterrupt) as caught:
        _run_one(scenario, 0)
    assert any("ValueError: first" in n for n in getattr(caught.value, "__notes__", []))


def test_an_interrupt_in_teardown_with_no_earlier_failure_has_no_note() -> None:
    class Node:
        async def run(self) -> None:
            try:
                await asyncio.sleep(100)
            except asyncio.CancelledError:
                raise KeyboardInterrupt from None

    async def scenario(world: World) -> None:
        world.start(Node())
        await asyncio.sleep(1)

    with pytest.raises(KeyboardInterrupt) as caught:
        _run_one(scenario, 0)
    assert not getattr(caught.value, "__notes__", [])


def test_the_same_interrupt_in_flight_and_in_teardown_is_not_a_note_on_itself() -> None:
    shared = KeyboardInterrupt("shared")

    class Quitter:
        async def run(self) -> None:
            await asyncio.sleep(1)
            raise shared

    class Stubborn:
        async def run(self) -> None:
            try:
                await asyncio.sleep(100)
            except asyncio.CancelledError:
                raise shared from None

    async def scenario(world: World) -> None:
        world.start(Stubborn())
        world.start(Quitter())
        await asyncio.sleep(10)

    with pytest.raises(KeyboardInterrupt) as caught:
        _run_one(scenario, 0)
    assert caught.value is shared
    assert not getattr(shared, "__notes__", [])


def test_a_scenario_error_with_nothing_else_wrong_carries_no_notes() -> None:
    async def scenario(world: World) -> None:
        await asyncio.sleep(1)
        raise ValueError("plain")

    with pytest.raises(ValueError, match="plain") as caught:
        _run_one(scenario, 0)
    assert not getattr(caught.value, "__notes__", [])


def test_check_returns_a_failure_that_takes_no_notes() -> None:
    class NoNotes(Exception):
        def add_note(self, note: str) -> None:
            raise RuntimeError("no notes")

    async def scenario(world: World) -> None:
        raise NoNotes("plain")

    result = seedloop.check(scenario, seeds=2, on_failure="return")
    assert isinstance(result.error, NoNotes)
    assert result.failing_seed == 0


def test_a_check_added_while_settling_is_noted_once() -> None:
    class Slow:
        async def run(self) -> None:
            try:
                await asyncio.sleep(100)
            except asyncio.CancelledError:
                await asyncio.sleep(1)
                raise

    async def scenario(world: World) -> None:
        world.start(Slow())
        asyncio.get_running_loop().stop()  # the run ends before the scenario finishes
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            world.always(lambda: False, name="late")
            await asyncio.sleep(1)
            raise

    with pytest.raises(RuntimeError) as caught:
        _run_one(scenario, 0)
    notes = [n for n in getattr(caught.value, "__notes__", []) if "late" in n]
    assert len(notes) == 1, notes
