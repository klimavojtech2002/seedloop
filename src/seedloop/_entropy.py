"""Seeded entropy: per-component sub-streams, a CSPRNG shim, and a hash-seed launcher.

A run is a pure function of its seed, so every source of randomness must derive from it. The root
seed is split into independent named sub-streams (ADR-0009) so adding a draw in one component does
not perturb another's sequence. The CSPRNG shim routes ``os.urandom``/``secrets`` to a seeded
source for the duration of a run, and the launcher pins ``PYTHONHASHSEED`` before the interpreter
starts so set/dict iteration order is fixed (ADR-0010).

Verified against the interpreter during design: shimming ``os.urandom`` alone does *not* control
``secrets``/``random``, because ``random`` binds ``from os import urandom as _urandom`` at import —
so the shim patches ``random._urandom`` too; and two child processes launched with the same
``PYTHONHASHSEED`` hash identically while a different value differs.
"""

from __future__ import annotations

import hashlib
import os
import random
import sys
import threading
from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from typing import Any

_REEXEC_GUARD = "_SEEDLOOP_HASHSEED_REEXEC"

_patch_lock = threading.RLock()


@contextmanager
def exclusive_patching() -> Iterator[None]:
    """Serialize runs that install process-wide patches (nesting on one thread is fine).

    Two threads patching at once would save each other's wrappers as the "original" and leave the
    process patched for good. A second thread therefore waits for the first run to finish, so the
    outcome never depends on thread timing (ADR-0026).
    """
    with _patch_lock:
        yield


def _install(owner: Any, name: str, value: Any) -> None:
    setattr(owner, name, value)


def restore_if_ours(owner: Any, name: str, ours: Any, original: Any) -> None:
    """Put ``original`` back, but only if ``ours`` is still installed under ``name``.

    Code that replaced the function while a run was active keeps its replacement.
    """
    if getattr(owner, name) is ours:
        setattr(owner, name, original)


def substream(root_seed: int, label: str) -> random.Random:
    """Derive an independent, reproducible ``random.Random`` for a named component.

    The stream is a pure function of ``(root_seed, label)``. Derivation hashes the canonical text
    ``f"{root_seed}:{label}"`` with ``blake2b`` — never the builtin ``hash()``, which is randomized
    per process — so the same pair yields the same stream in every process, and any ``int`` seed
    works (negative, or larger than 64 bits).
    """
    digest = hashlib.blake2b(f"{root_seed}:{label}".encode(), digest_size=32).digest()
    return random.Random(int.from_bytes(digest, "big"))


@contextmanager
def csprng_shim(stream: random.Random) -> Iterator[None]:
    """Route ``os.urandom`` and ``secrets`` to ``stream`` for the duration of the context.

    Patches both ``os.urandom`` and the ``random._urandom`` alias that ``secrets`` draws through;
    restores both originals on exit, even on error. Only the calling thread is seeded: any other
    thread keeps the real source. Runs from different threads wait for each other (ADR-0026).
    """
    seeded = _seeded_urandom(stream)
    owner = threading.get_ident()
    with exclusive_patching():
        # Read the originals only once the lock is held, or a concurrent run's wrapper is "saved".
        orig_os = os.urandom
        orig_random = random._urandom  # type: ignore[attr-defined]  # private alias secrets uses

        def scoped_os(n: int) -> bytes:
            return seeded(n) if threading.get_ident() == owner else orig_os(n)

        def scoped_random(n: int) -> bytes:
            return seeded(n) if threading.get_ident() == owner else orig_random(n)

        # Each restore is registered before its install, and the stack runs every restore even if
        # one is interrupted, so an interrupt between two steps cannot leave a name patched.
        with ExitStack() as undo:
            undo.callback(restore_if_ours, os, "urandom", scoped_os, orig_os)
            undo.callback(restore_if_ours, random, "_urandom", scoped_random, orig_random)
            _install(os, "urandom", scoped_os)
            _install(random, "_urandom", scoped_random)
            yield


def _seeded_urandom(stream: random.Random) -> Callable[[int], bytes]:
    def seeded_urandom(n: int) -> bytes:
        return stream.getrandbits(n * 8).to_bytes(n, "big") if n else b""

    return seeded_urandom


def hash_seed_for(root_seed: int) -> int:
    """The ``PYTHONHASHSEED`` value (0..4294967295) a run pins, derived from its root seed."""
    digest = hashlib.blake2b(f"{root_seed}:hashseed".encode(), digest_size=4).digest()
    return int.from_bytes(digest, "big")


def ensure_hash_seed(root_seed: int) -> None:
    """Ensure the interpreter runs with the run's pinned ``PYTHONHASHSEED``.

    ``PYTHONHASHSEED`` is read once at interpreter start, so it cannot be set from inside a run;
    this re-runs the interpreter with the pinned value when needed. If already pinned, returns and
    the caller proceeds in-process. Otherwise it launches a pinned child running the same command
    and does not return — on POSIX by replacing the process (``execve``), on Windows (no true
    ``exec``) by spawning a child and exiting with its return code. A guard env var prevents
    infinite recursion.
    """
    target = str(hash_seed_for(root_seed))
    if os.environ.get(_REEXEC_GUARD) == target or os.environ.get("PYTHONHASHSEED") == target:
        return  # already pinned (our child, or started correctly); proceed in-process
    child_env = dict(os.environ, PYTHONHASHSEED=target, **{_REEXEC_GUARD: target})
    # sys.orig_argv is the full original command (including -c / -m and their payload), so the
    # child re-runs exactly what the parent ran; reconstructing from sys.argv would drop -c code.
    argv = [sys.executable, *sys.orig_argv[1:]]
    if os.name == "posix":
        os.execve(sys.executable, argv, child_env)
    else:
        # Windows has no in-place exec; spawn a pinned child and propagate its exit code.
        import subprocess

        completed = subprocess.run(argv, env=child_env)
        sys.exit(completed.returncode)
