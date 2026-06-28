"""The non-determinism auditor: runtime tripwires for uncontrolled entropy (ADR-0008).

A run is a pure function of its seed only if every entropy source it touches is the World's seeded
one. The loop already rejects the I/O boundary (``run_in_executor``, real sockets, DNS) in every
mode. This adds an opt-in *audit mode* that closes the Python-level entropy sources the loop does
not see: real wall-clock time, the unseeded global ``random``, ``os.urandom``/``secrets``, and a
bare ``threading.Thread``. In audit mode each raises instead of running, so a leak is a loud,
reproducible failure on the seed that hit it — the boundary enforced, not just stated (scope.md).

The tripwires patch only module-level entry points, never ``random.Random`` itself, so the World's
seeded ``rng`` keeps working; they are pure raises that touch no entropy and leave a clean run's
timeline unchanged; and they are restored on exit even on error.

Like any monkeypatch (and the CSPRNG shim), a tripwire catches a call that looks the name up at call
time — ``time.monotonic()``, ``random.random()`` — but not a reference bound *before* audit started
(``from time import monotonic`` then ``monotonic()``). The common attribute-call form is caught; the
same C-level caveat as ``scope.md`` applies below Python.
"""

from __future__ import annotations

import os
import random
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

from seedloop.errors import BoundaryError, EntropyLeakError

# Real-time entry points (the loop owns virtual time via loop.time(), so any direct call is a leak).
_REAL_TIME = ("time", "monotonic", "perf_counter", "time_ns", "monotonic_ns", "perf_counter_ns")

# Every entropy-drawing module-level `random` function — the *complete* set on the global unseeded
# instance, not a subset, so a leak through any (e.g. expovariate for latency jitter) is caught.
# These are module functions; `random.Random` instances such as the seeded rng are untouched.
_RANDOM_FUNCS = (
    "random",
    "uniform",
    "triangular",
    "randint",
    "randrange",
    "choice",
    "choices",
    "shuffle",
    "sample",
    "getrandbits",
    "randbytes",
    "betavariate",
    "expovariate",
    "gammavariate",
    "gauss",
    "lognormvariate",
    "normalvariate",
    "vonmisesvariate",
    "paretovariate",
    "weibullvariate",
    "binomialvariate",  # 3.12+
)

# Each tripwire is (module, attribute, display name). hasattr-guarded so a name absent on a given
# interpreter is skipped rather than crashing the patcher. os.urandom and the random._urandom alias
# that secrets draws through are intercepted too.
_ENTROPY_SURFACES: list[tuple[Any, str, str]] = [
    *((time, name, f"time.{name}") for name in _REAL_TIME if hasattr(time, name)),
    (os, "urandom", "os.urandom"),
    (random, "_urandom", "secrets/os.urandom"),
    *((random, name, f"random.{name}") for name in _RANDOM_FUNCS if hasattr(random, name)),
]


def _entropy_tripwire(source: str) -> Callable[..., Any]:
    def tripwire(*_args: Any, **_kwargs: Any) -> Any:
        raise EntropyLeakError(source)

    return tripwire


def _thread_tripwire(*_args: Any, **_kwargs: Any) -> Any:
    raise BoundaryError(
        "threading.Thread (a real thread) cannot be made deterministic and is out of scope in a "
        "simulated run (see docs/scope.md)"
    )


@contextmanager
def audit_mode() -> Iterator[None]:
    """Trip on uncontrolled entropy for the duration of the context.

    Inside the context, real time, the unseeded global ``random``, ``os.urandom``/``secrets``, and
    ``threading.Thread.start`` raise (``EntropyLeakError`` for entropy, ``BoundaryError`` for the
    thread) instead of running. The World's seeded ``rng`` and virtual clock are unaffected. Use it
    via ``check(..., audit=True)`` / ``replay(..., audit=True)``, or directly to wrap your own run.
    All patches are restored on exit, even on error.
    """
    saved = [(mod, attr, getattr(mod, attr)) for mod, attr, _ in _ENTROPY_SURFACES]
    saved_thread_start = threading.Thread.start
    for mod, attr, name in _ENTROPY_SURFACES:
        setattr(mod, attr, _entropy_tripwire(name))
    threading.Thread.start = _thread_tripwire  # type: ignore[method-assign]
    try:
        yield
    finally:
        for mod, attr, original in saved:
            setattr(mod, attr, original)
        threading.Thread.start = saved_thread_start  # type: ignore[method-assign]
