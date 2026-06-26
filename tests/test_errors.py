"""The error hierarchy is a contract: catching SeedloopError catches everything seedloop raises."""

from __future__ import annotations

import pytest

from seedloop.errors import BoundaryError, DeadlockError, SeedloopError


@pytest.mark.parametrize("exc", [BoundaryError, DeadlockError])
def test_errors_subclass_seedloop_error(exc: type[Exception]) -> None:
    assert issubclass(exc, SeedloopError)


@pytest.mark.parametrize("exc", [BoundaryError, DeadlockError])
def test_seedloop_error_catches_subclasses(exc: type[SeedloopError]) -> None:
    with pytest.raises(SeedloopError):
        raise exc("boom")
