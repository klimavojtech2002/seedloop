"""The error hierarchy is a contract: catching SeedloopError catches everything seedloop raises."""

from __future__ import annotations

import copy
import inspect
import pickle
from collections.abc import Callable

import pytest

from seedloop import errors
from seedloop.errors import (
    BoundaryError,
    DeadlockError,
    EntropyLeakError,
    InvariantError,
    SeedloopError,
)


@pytest.mark.parametrize("exc", [BoundaryError, DeadlockError])
def test_errors_subclass_seedloop_error(exc: type[Exception]) -> None:
    assert issubclass(exc, SeedloopError)


@pytest.mark.parametrize("exc", [BoundaryError, DeadlockError])
def test_seedloop_error_catches_subclasses(exc: type[SeedloopError]) -> None:
    with pytest.raises(SeedloopError):
        raise exc("boom")


# --- every error survives pickle and copy (process pools and parallel sweeps ship them) ---

# Constructor arguments for the classes whose __init__ takes more than a message.
_EXAMPLES: dict[type[SeedloopError], tuple[object, ...]] = {
    InvariantError: ("at-most-one-leader", 12.5),
    EntropyLeakError: ("time.time",),
}


def _error_classes() -> list[type[SeedloopError]]:
    return [
        cls
        for _, cls in inspect.getmembers(errors, inspect.isclass)
        if issubclass(cls, SeedloopError) and cls.__module__ == errors.__name__
    ]


def _build(cls: type[SeedloopError]) -> SeedloopError:
    if cls in _EXAMPLES:
        return cls(*_EXAMPLES[cls])
    own_init = cls.__init__ is not Exception.__init__ and "__init__" in vars(cls)
    assert not own_init, f"{cls.__name__} defines __init__: add an example to _EXAMPLES"
    return cls("boom")


def _pickled(protocol: int) -> Callable[[SeedloopError], SeedloopError]:
    def trip(error: SeedloopError) -> SeedloopError:
        clone: SeedloopError = pickle.loads(pickle.dumps(error, protocol=protocol))
        return clone

    return trip


def _round_trips() -> list[tuple[str, Callable[[SeedloopError], SeedloopError]]]:
    trips = [(f"pickle-{proto}", _pickled(proto)) for proto in range(pickle.HIGHEST_PROTOCOL + 1)]
    trips.append(("copy", copy.copy))
    trips.append(("deepcopy", copy.deepcopy))
    return trips


@pytest.mark.parametrize("cls", _error_classes(), ids=lambda c: c.__name__)
@pytest.mark.parametrize(
    "trip", _round_trips(), ids=lambda t: t[0] if isinstance(t, tuple) else str(t)
)
def test_every_error_round_trips_with_its_notes_and_attributes(
    cls: type[SeedloopError], trip: tuple[str, Callable[[SeedloopError], SeedloopError]]
) -> None:
    original = _build(cls)
    original.add_note("seedloop: failing seed=3 (replay with seedloop.replay)")
    clone = trip[1](original)
    assert type(clone) is type(original)
    assert clone.args == original.args
    assert str(clone) == str(original)
    assert clone.__dict__ == original.__dict__
    assert clone.__notes__ == original.__notes__
    assert clone is not original


def test_the_discovered_classes_include_the_ones_with_extra_arguments() -> None:
    found = set(_error_classes())
    assert {InvariantError, EntropyLeakError, DeadlockError, BoundaryError} <= found


def test_an_invariant_error_keeps_its_name_and_time_across_pickle() -> None:
    clone = pickle.loads(pickle.dumps(InvariantError("inv", 3.0)))
    assert (clone.name, clone.time) == ("inv", 3.0)


def test_an_entropy_leak_error_keeps_its_source_across_pickle() -> None:
    clone = pickle.loads(pickle.dumps(EntropyLeakError("os.urandom")))
    assert clone.source == "os.urandom"
