"""seedloop — deterministic simulation testing for Python asyncio.

Write a scenario against a :class:`World`, then ``check`` it across many seeds; a failing seed is
the reproduction — ``replay`` it to debug. The deterministic core (loop, virtual clock, seeded
entropy) and the simulated network with fault injection (loss, duplication, partitions) are in
place; the invariant API and the worked demo are next.
"""

from seedloop._entropy import ensure_hash_seed
from seedloop._net import Address, Endpoint, Message, Transport
from seedloop._run import CheckResult, Scenario, check, replay
from seedloop._world import Node, World
from seedloop.errors import (
    BoundaryError,
    DeadlockError,
    EntropyLeakError,
    SeedloopError,
)

__all__ = [
    "Address",
    "BoundaryError",
    "CheckResult",
    "DeadlockError",
    "Endpoint",
    "EntropyLeakError",
    "Message",
    "Node",
    "Scenario",
    "SeedloopError",
    "Transport",
    "World",
    "check",
    "ensure_hash_seed",
    "replay",
]
__version__ = "0.2.0"
