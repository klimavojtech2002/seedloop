"""seedloop — deterministic simulation testing for Python asyncio.

Write a scenario against a :class:`World`, then ``check`` it across many seeds; a failing seed is
the reproduction — ``replay`` it to debug. Phase 1 makes asyncio runs reproducible and instant; the
simulated network and fault injection follow.
"""

from seedloop._entropy import ensure_hash_seed
from seedloop._run import CheckResult, Scenario, check, replay
from seedloop._world import Node, World
from seedloop.errors import (
    BoundaryError,
    DeadlockError,
    EntropyLeakError,
    SeedloopError,
)

__all__ = [
    "BoundaryError",
    "CheckResult",
    "DeadlockError",
    "EntropyLeakError",
    "Node",
    "Scenario",
    "SeedloopError",
    "World",
    "check",
    "ensure_hash_seed",
    "replay",
]
__version__ = "0.0.0"
