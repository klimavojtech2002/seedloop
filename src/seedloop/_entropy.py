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
from collections.abc import Callable, Iterator
from contextlib import contextmanager

_REEXEC_GUARD = "_SEEDLOOP_HASHSEED_REEXEC"


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
    restores both originals on exit, even on error. Scoped to a single run; runs do not overlap in
    one process.
    """
    seeded = _seeded_urandom(stream)
    orig_os = os.urandom
    orig_random = random._urandom  # type: ignore[attr-defined]  # private alias secrets draws through
    os.urandom = seeded
    random._urandom = seeded  # type: ignore[attr-defined]
    try:
        yield
    finally:
        os.urandom = orig_os
        random._urandom = orig_random  # type: ignore[attr-defined]


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
