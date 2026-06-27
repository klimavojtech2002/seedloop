"""The simulated network: messages delivered as seeded timer events.

A message in flight is an ordinary timer on the loop's heap (``docs/network.md``). ``send`` draws a
latency from the seed's ``"net"`` sub-stream and schedules a delivery at ``now + latency``; ``recv``
blocks in virtual time until a message is queued. Reordering is emergent — two messages sent close
together draw independent latencies, so arrival order can differ from send order, reproducibly. No
real socket exists; the "network" is queues and timers.

This slice is the unreliable datagram channel only. Faults (drop, duplicate, partition) and the
opt-in reliable/ordered channel arrive in slice 0210.
"""

from __future__ import annotations

import asyncio
from collections import deque
from random import Random
from typing import Protocol, runtime_checkable

from seedloop._trace import Timeline
from seedloop.errors import SeedloopError

Address = int  # a node's address on the simulated network
Message = object  # an opaque payload; seedloop schedules and orders it, never inspects it

# Default per-message latency range, in virtual seconds. Wide enough that two near-simultaneous
# sends can reorder; the distribution becomes tunable in slice 0210.
_LAT_MIN = 0.001
_LAT_MAX = 0.020


@runtime_checkable
class Endpoint(Protocol):
    """A node's bound handle on the network."""

    address: Address

    async def send(self, dst: Address, msg: Message) -> None: ...
    async def recv(self) -> tuple[Address, Message]: ...


class Transport:
    """The simulated network behind ``world.net``."""

    def __init__(
        self, loop: asyncio.AbstractEventLoop, net_rng: Random, timeline: Timeline
    ) -> None:
        self._loop = loop
        self._net = net_rng
        self._timeline = timeline
        self._endpoints: dict[Address, _Endpoint] = {}
        self._next_mid = 0  # monotonic message id — the stable timeline identity, not Python id()

    def bind(self, address: Address, *, reliable: bool = False) -> Endpoint:
        """Give a node an endpoint at ``address``. Binding the same address twice is an error."""
        if reliable:
            raise NotImplementedError("the reliable, ordered channel arrives in slice 0210")
        if address in self._endpoints:
            raise SeedloopError(f"address {address} is already bound")
        endpoint = _Endpoint(self, address)
        self._endpoints[address] = endpoint
        return endpoint

    def _send(self, src: Address, dst: Address, msg: Message) -> None:
        mid = self._next_mid
        self._next_mid += 1
        self._timeline.record((self._loop.time(), "send", mid, src, dst))
        latency = self._net.uniform(_LAT_MIN, _LAT_MAX)
        self._loop.call_later(latency, self._deliver, mid, src, dst, msg)

    def _deliver(self, mid: int, src: Address, dst: Address, msg: Message) -> None:
        self._timeline.record((self._loop.time(), "deliver", mid, src, dst))
        endpoint = self._endpoints.get(dst)
        if endpoint is None:
            return  # datagram to an unbound address is dropped, like sending into the void
        endpoint._enqueue((src, msg))


class _Endpoint:
    """Concrete endpoint: a receive queue and an optional waiter for a blocked ``recv``."""

    def __init__(self, transport: Transport, address: Address) -> None:
        self.address = address
        self._transport = transport
        self._queue: deque[tuple[Address, Message]] = deque()
        self._waiter: asyncio.Future[None] | None = None

    async def send(self, dst: Address, msg: Message) -> None:
        # Schedules a delivery and returns immediately; it does not block on delivery.
        self._transport._send(self.address, dst, msg)

    async def recv(self) -> tuple[Address, Message]:
        while not self._queue:
            self._waiter = self._transport._loop.create_future()
            try:
                await self._waiter
            finally:
                self._waiter = None
        return self._queue.popleft()

    def _enqueue(self, item: tuple[Address, Message]) -> None:
        self._queue.append(item)
        if self._waiter is not None and not self._waiter.done():
            self._waiter.set_result(None)
