"""The non-determinism auditor: runtime tripwires for uncontrolled entropy (ADR-0008).

A run is a pure function of its seed only if every entropy source it touches is the World's seeded
one. The loop already rejects the I/O boundary (``run_in_executor``, real sockets, DNS) in every
mode. This adds an opt-in *audit mode* that closes the Python-level entropy sources the loop does
not see: real clocks (wall, monotonic, perf-counter, and the process/thread CPU clocks) and
current-time calendar reads (``gmtime``/``localtime``/``ctime``/``asctime``/``strftime`` with no
explicit timestamp), the unseeded global ``random``, ``os.urandom``/``secrets``, and a bare
``threading.Thread``. In audit mode each raises instead of running, so a leak is a loud,
reproducible failure on the seed that hit it — the boundary enforced, not just stated (scope.md).

The tripwires patch only module-level entry points, never ``random.Random`` itself, so the World's
seeded ``rng`` keeps working; they are pure raises that touch no entropy and leave a clean run's
timeline unchanged; and they are restored on exit even on error.

Like any monkeypatch (and the CSPRNG shim), a tripwire catches a call that looks the name up at call
time — ``time.monotonic()``, ``random.random()`` — but not a reference bound *before* audit started
(``from time import monotonic`` then ``monotonic()``). The common attribute-call form is caught; the
same C-level caveat as ``scope.md`` applies below Python. In particular ``datetime.now()`` /
``utcnow()`` read the clock in C, below these ``time.*`` attributes, so they are not caught — keep
the real clock out of a run rather than relying on the tripwire to find every path to it.
"""

from __future__ import annotations

import os
import random
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from typing import Any

from seedloop._entropy import _install, exclusive_patching, restore_if_ours
from seedloop.errors import BoundaryError, EntropyLeakError

# Real-time entry points: clocks with no deterministic form (the loop owns virtual time via
# loop.time(), so any direct call is a leak). Wall (time), monotonic, perf_counter, the process/
# thread CPU clocks, and POSIX clock_gettime, with their _ns variants. hasattr-guarded below, so the
# POSIX-only names are simply skipped on platforms (e.g. Windows) that lack them.
_REAL_TIME = (
    "time",
    "time_ns",
    "monotonic",
    "monotonic_ns",
    "perf_counter",
    "perf_counter_ns",
    "process_time",
    "process_time_ns",
    "thread_time",
    "thread_time_ns",
    "clock_gettime",
    "clock_gettime_ns",
)

# Calendar helpers that read the *current* time only when called without an explicit timestamp:
# gmtime/localtime/ctime/asctime take the time at positional index 0, strftime at index 1. Given a
# timestamp they are pure conversions, so the tripwire fires on the now-reading form only.
_CURRENT_TIME_FUNCS = (
    ("gmtime", 0),
    ("localtime", 0),
    ("ctime", 0),
    ("asctime", 0),
    ("strftime", 1),
)

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


def _entropy_tripwire(source: str, original: Callable[..., Any], owner: int) -> Callable[..., Any]:
    # Trips only on the run's own thread; other threads (and the host's event loop) keep the real
    # function.
    def tripwire(*args: Any, **kwargs: Any) -> Any:
        if threading.get_ident() != owner:
            return original(*args, **kwargs)
        raise EntropyLeakError(source)

    return tripwire


def _current_time_tripwire(
    source: str, original: Callable[..., Any], ts_index: int, owner: int
) -> Callable[..., Any]:
    # Trip only when the call reads the current time — no timestamp at ``ts_index`` (or an explicit
    # None). With a timestamp the wrapped function is a pure conversion, so we delegate to it.
    def tripwire(*args: Any, **kwargs: Any) -> Any:
        if threading.get_ident() == owner and (len(args) <= ts_index or args[ts_index] is None):
            raise EntropyLeakError(source)
        return original(*args, **kwargs)

    return tripwire


def _thread_tripwire(original: Callable[..., Any], owner: int) -> Callable[..., Any]:
    def tripwire(*args: Any, **kwargs: Any) -> Any:
        if threading.get_ident() != owner:
            return original(*args, **kwargs)
        raise BoundaryError(
            "threading.Thread (a real thread) cannot be made deterministic and is out of scope in "
            "a simulated run (see docs/scope.md)"
        )

    return tripwire


@contextmanager
def audit_mode() -> Iterator[None]:
    """Trip on uncontrolled entropy for the duration of the context.

    Inside the context, real clocks (wall, monotonic, perf-counter, process/thread CPU time) and
    current-time calendar reads, the unseeded global ``random``, ``os.urandom``/``secrets``, and
    ``threading.Thread.start`` raise (``EntropyLeakError`` for entropy, ``BoundaryError`` for the
    thread) instead of running. Calendar helpers given an explicit timestamp stay pure and still
    work. The World's seeded ``rng`` and virtual clock are unaffected. Use it via
    ``check(..., audit=True)`` / ``replay(..., audit=True)``, or directly to wrap your own run.
    All patches are restored on exit, even on error. The tripwires act on the calling thread
    only: other threads, including the host's event loop when a check runs under
    ``asyncio.to_thread``, keep the real functions. Runs on different threads wait for each other.
    """
    owner = threading.get_ident()
    # Each restore is registered before its install, and the stack runs every restore even if one
    # is interrupted, so an interrupt between two steps cannot leave a name patched. A name someone
    # else replaced while the run was active keeps its replacement.
    with exclusive_patching(), ExitStack() as undo:
        for mod, attr, name in _ENTROPY_SURFACES:
            original = getattr(mod, attr)
            tripwire = _entropy_tripwire(name, original, owner)
            undo.callback(restore_if_ours, mod, attr, tripwire, original)
            _install(mod, attr, tripwire)
        for name, ts_index in _CURRENT_TIME_FUNCS:
            if hasattr(time, name):
                original = getattr(time, name)
                tripwire = _current_time_tripwire(f"time.{name}", original, ts_index, owner)
                undo.callback(restore_if_ours, time, name, tripwire, original)
                _install(time, name, tripwire)
        original_start = threading.Thread.start
        start_tripwire = _thread_tripwire(original_start, owner)
        undo.callback(restore_if_ours, threading.Thread, "start", start_tripwire, original_start)
        _install(threading.Thread, "start", start_tripwire)
        yield
