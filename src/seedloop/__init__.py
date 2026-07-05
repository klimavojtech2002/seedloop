"""seedloop — deterministic simulation testing for Python asyncio.

Write a scenario against a :class:`World`, then ``check`` it across many seeds; a failing seed is
the reproduction — ``replay`` it to debug. The deterministic core (loop, virtual clock, seeded
entropy), the simulated network with fault injection (loss, duplication, partitions), the invariant
API, and the non-determinism auditor are in place; a worked Raft demo ships in ``seedloop.demos``.
"""

from seedloop._audit import audit_mode
from seedloop._entropy import ensure_hash_seed
from seedloop._net import Address, Endpoint, Message, Transport
from seedloop._run import CheckResult, Scenario, check, replay
from seedloop._world import Node, World
from seedloop.errors import (
    BoundaryError,
    DeadlockError,
    EntropyLeakError,
    InvariantError,
    SeedloopError,
)

__all__ = [
    "Address",
    "BoundaryError",
    "CheckResult",
    "DeadlockError",
    "Endpoint",
    "EntropyLeakError",
    "InvariantError",
    "Message",
    "Node",
    "Scenario",
    "SeedloopError",
    "Transport",
    "World",
    "audit_mode",
    "check",
    "ensure_hash_seed",
    "replay",
]
__version__ = "0.3.2"
