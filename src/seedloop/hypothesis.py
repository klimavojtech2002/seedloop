"""Optional Hypothesis integration: drive seedloop scenarios from property-based exploration.

seedloop's core sweeps seeds linearly (``check``). Hypothesis adds structured input generation,
shrinking a failure to a minimal case, and an example database that re-tries past failures first.
This module borrows all of that (ADR-0004) as an *opt-in* extra so the core stays dependency-free
(ADR-0017): ``import seedloop.hypothesis`` needs ``pip install 'seedloop[hypothesis]'``.

Hypothesis generates the seed and any inputs *outside* the run; inside, only the seed drives
entropy, so a run stays a pure function of its seed and ``seedloop.replay`` reproduces any reported
case. Seeds themselves do not shrink to anything meaningful — a smaller integer is not a simpler bug
— so shrinking earns its keep on the *inputs*; the seed is there for exact reproduction.
"""

from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any

try:
    from hypothesis import given
    from hypothesis import strategies as st
except ImportError as exc:  # pragma: no cover - the bare-env path is covered by a subprocess test
    raise ImportError(
        "seedloop.hypothesis requires the optional Hypothesis extra: "
        "pip install 'seedloop[hypothesis]'"
    ) from exc

__all__ = ["given_seed", "seeds"]

_TestFn = Callable[..., None]


def seeds(*, min_value: int = 0, max_value: int | None = None) -> st.SearchStrategy[int]:
    """A Hypothesis strategy drawing a seedloop World seed.

    Seeds are non-negative by convention (``check`` sweeps ``0..N-1``), though ``World`` accepts any
    ``int``. The bounds let a test narrow the space; the default is every non-negative integer.
    """
    return st.integers(min_value=min_value, max_value=max_value)


def given_seed(**input_strategies: st.SearchStrategy[Any]) -> Callable[[_TestFn], _TestFn]:
    """Turn a seedloop scenario test into a Hypothesis property test.

    Sugar for ``@given(seed=seeds(), **input_strategies)``: the decorated function is called
    ``f(seed, **inputs)`` for each generated example. On a failing example a
    ``seedloop.replay(..., seed=S)`` reproduction line is attached to the error, so the minimal case
    is copy-pasteable; Hypothesis's own generation, shrinking, and reporting are untouched, and the
    seed in the note is the final shrunk one because Hypothesis re-runs the minimal example last.
    """

    def decorate(test: _TestFn) -> _TestFn:
        @functools.wraps(test)
        def annotated(*args: Any, seed: int, **inputs: Any) -> None:
            try:
                test(*args, seed=seed, **inputs)
            except Exception as exc:
                # Annotate a scenario failure and re-raise it unchanged. Only Exception is caught,
                # so KeyboardInterrupt/SystemExit still abort a long exploration (as in check).
                exc.add_note(f"seedloop: reproduce with seedloop.replay(scenario, seed={seed})")
                raise

        wrapped: _TestFn = given(seed=seeds(), **input_strategies)(annotated)
        return wrapped

    return decorate
