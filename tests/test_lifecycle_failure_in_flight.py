"""The first failure of a run is kept: later errors become notes, and interrupts are never lost."""

from __future__ import annotations

import asyncio

import pytest

import seedloop
from _sim import spawn
from _support import run_isolated
from seedloop._run import _run_one
from seedloop._world import World
from seedloop.errors import InvariantError


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

        spawn(trigger())
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            raise handler_raises from None

    with pytest.raises(Exception) as caught:
        _run_one(scenario, 0)
    return caught.value


def test_interrupt_from_the_main_cancel_handler_is_not_lost() -> None:
    # An invariant fails; the scenario task is cancelled and its cancel handler raises an interrupt
    # (Ctrl-C arriving while the run winds down). The interrupt must reach the caller.
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


def test_a_failure_while_unwinding_carries_no_internal_note() -> None:
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

        spawn(flip())
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            raise ValueError("while unwinding") from None

    with pytest.raises(InvariantError) as caught:
        _run_one(scenario, 0)
    notes = getattr(caught.value, "__notes__", [])
    assert any("ValueError: while unwinding" in note for note in notes), notes


def test_a_cancel_handler_that_swallows_the_cancellation_adds_no_note() -> None:
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
            return  # the scenario ends normally instead of re-raising

    with pytest.raises(InvariantError) as caught:
        _run_one(scenario, 0)
    assert not getattr(caught.value, "__notes__", [])


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

        spawn(flip())
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            raise Hostile from None

    with pytest.raises(InvariantError) as caught:
        _run_one(scenario, 0)
    assert any("Hostile" in note for note in getattr(caught.value, "__notes__", []))


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

        spawn(flip())
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
