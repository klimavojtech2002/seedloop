"""The simulated network: messages delivered as seeded timer events, with faults.

A message in flight is an ordinary timer on the loop's heap (``docs/network.md``). ``send`` draws a
latency from the seed's ``"net"`` sub-stream and schedules a delivery at ``now + latency``; ``recv``
blocks in virtual time until a message is queued. Reordering is emergent — two messages sent close
together draw independent latencies, so arrival order can differ from send order, reproducibly.

Faults — loss, duplication, and partitions — are drawn from the seed's ``"faults"`` sub-stream
(independent of ``"net"``, so enabling a fault does not shift surviving messages' latencies). An
endpoint can opt into a reliable, ordered channel. No real socket exists; the "network" is queues
and timers.
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
# sends can reorder.
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
        self,
        loop: asyncio.AbstractEventLoop,
        net_rng: Random,
        faults_rng: Random,
        timeline: Timeline,
    ) -> None:
        self._loop = loop
        self._net = net_rng
        self._faults = faults_rng
        self._timeline = timeline
        self._endpoints: dict[Address, _Endpoint] = {}
        self._next_mid = 0  # monotonic message id — the stable timeline identity, not Python id()
        self._partition: list[set[Address]] | None = None  # groups; None means full connectivity
        self._reliable_clock: dict[
            tuple[Address, Address], float
        ] = {}  # per-link FIFO delivery time

    def bind(
        self,
        address: Address,
        *,
        reliable: bool = False,
        loss: float = 0.0,
        duplicate: float = 0.0,
    ) -> Endpoint:
        """Give a node an endpoint at ``address``.

        ``loss``/``duplicate`` are per-message probabilities on this endpoint's outgoing links;
        ``reliable=True`` gives no-loss, in-order delivery (and ignores loss/duplicate).
        Binding the same address twice is an error.
        """
        if address in self._endpoints:
            raise SeedloopError(f"address {address} is already bound")
        if not 0.0 <= loss <= 1.0:
            raise SeedloopError(f"loss must be a probability in [0, 1], got {loss}")
        if not 0.0 <= duplicate <= 1.0:
            raise SeedloopError(f"duplicate must be a probability in [0, 1], got {duplicate}")
        endpoint = _Endpoint(self, address, reliable=reliable, loss=loss, duplicate=duplicate)
        self._endpoints[address] = endpoint
        return endpoint

    def partition(self, *groups: set[Address]) -> None:
        """Split the network: nodes in different groups cannot reach each other until ``heal``.

        A node in no listed group stays connected to everyone (it is not partitioned away).
        """
        self._partition = [set(g) for g in groups]

    def heal(self) -> None:
        """Restore full connectivity."""
        self._partition = None

    def _reachable(self, src: Address, dst: Address) -> bool:
        if self._partition is None:
            return True
        gs = next((g for g in self._partition if src in g), None)
        gd = next((g for g in self._partition if dst in g), None)
        if gs is None or gd is None:
            return True  # an unpartitioned node reaches everyone
        return gs is gd

    def _send(self, endpoint: _Endpoint, dst: Address, msg: Message) -> None:
        src = endpoint.address
        mid = self._next_mid
        self._next_mid += 1
        self._timeline.record((self._loop.time(), "send", mid, src, dst))
        if endpoint._reliable:
            self._schedule_reliable(mid, src, dst, msg)
            return
        if endpoint._loss > 0.0 and self._faults.random() < endpoint._loss:
            self._timeline.record((self._loop.time(), "drop", mid, src, dst))
            return
        self._schedule_delivery(mid, src, dst, msg)
        if endpoint._duplicate > 0.0 and self._faults.random() < endpoint._duplicate:
            self._timeline.record((self._loop.time(), "duplicate", mid, src, dst))
            self._schedule_delivery(mid, src, dst, msg)

    def _schedule_delivery(self, mid: int, src: Address, dst: Address, msg: Message) -> None:
        latency = self._net.uniform(_LAT_MIN, _LAT_MAX)
        self._loop.call_later(latency, self._deliver, mid, src, dst, msg)

    def _schedule_reliable(self, mid: int, src: Address, dst: Address, msg: Message) -> None:
        # Non-decreasing delivery times per (src, dst); equal times fire in send order via the timer
        # (when, seq) tie-break — so a reliable link delivers in order, with no loss or duplication.
        latency = self._net.uniform(_LAT_MIN, _LAT_MAX)
        key = (src, dst)
        when = max(self._loop.time() + latency, self._reliable_clock.get(key, 0.0))
        self._reliable_clock[key] = when
        self._loop.call_at(when, self._deliver, mid, src, dst, msg)

    def _deliver(self, mid: int, src: Address, dst: Address, msg: Message) -> None:
        if not self._reachable(src, dst):
            # Reachability is evaluated when the delivery fires, not at send: a partition opened in
            # flight cuts the message; one that healed in time lets it through.
            self._timeline.record((self._loop.time(), "drop-partitioned", mid, src, dst))
            return
        self._timeline.record((self._loop.time(), "deliver", mid, src, dst))
        endpoint = self._endpoints.get(dst)
        if endpoint is None:
            return  # datagram to an unbound address is dropped, like sending into the void
        endpoint._enqueue((src, msg))


class _Endpoint:
    """Concrete endpoint: a receive queue, an optional waiter, and its outgoing-link policy."""

    def __init__(
        self,
        transport: Transport,
        address: Address,
        *,
        reliable: bool,
        loss: float,
        duplicate: float,
    ) -> None:
        self.address = address
        self._transport = transport
        self._reliable = reliable
        self._loss = loss
        self._duplicate = duplicate
        self._queue: deque[tuple[Address, Message]] = deque()
        self._waiter: asyncio.Future[None] | None = None

    async def send(self, dst: Address, msg: Message) -> None:
        # Schedules a delivery and returns immediately; it does not block on delivery.
        self._transport._send(self, dst, msg)

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
