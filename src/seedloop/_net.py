"""The simulated network: messages delivered as seeded timer events, with faults.

A message in flight is an ordinary timer on the loop's heap (``docs/network.md``). ``send`` draws a
latency from the seed's ``"net"`` sub-stream and schedules a delivery at ``now + latency``; ``recv``
blocks in virtual time until a message is queued. Reordering is emergent — two messages sent close
together draw independent latencies, so arrival order can differ from send order, reproducibly.

Every ``send`` draws exactly one latency from ``"net"``, before any fault is decided. Faults — loss,
duplication, and partitions — draw from the separate ``"faults"`` sub-stream, so a realized drop or
duplicate never shifts another message's latency: a dropped message still consumed its ``"net"``
draw, and a duplicate's extra delivery draws from ``"faults"``. An endpoint can opt into a reliable,
ordered channel. No real socket exists; the "network" is queues and timers.
"""

from __future__ import annotations

import asyncio
import math
from collections import deque
from random import Random
from typing import Protocol, runtime_checkable

from seedloop._faults import CrashFault, Fault, PartitionFault, SlowLinkFault
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
        # Seed-scheduled fault state (run_for(faults=[...]), ADR-0022). Kept separate from
        # _partition/heal() (the scenario-driven, single-slot form) so one fault's end never
        # clobbers another's still-active window — each scheduled fault gets its own id.
        self._fault_partitions: dict[int, list[set[Address]]] = {}
        self._fault_slow_links: dict[int, tuple[frozenset[Address], float]] = {}
        self._crashed: set[Address] = set()
        self._next_fault_id = 0

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

        A node in no listed group stays connected to everyone (it is not partitioned away). This
        acts immediately, at scenario-timing; see also ``world.partition`` (a different method,
        on ``World``), which returns a seed-timed ``Fault`` for ``run_for(faults=[...])`` instead.
        """
        self._partition = [set(g) for g in groups]

    def heal(self) -> None:
        """Restore full connectivity."""
        self._partition = None

    def _validate_fault(self, fault: Fault, seconds: float) -> None:
        """Reject a fault before any fault in this ``run_for`` call commits state (ADR-0022).

        ``run_for`` calls this for every fault in its list *before* committing any of them
        (:meth:`_commit_fault`) — checked here, up front, so an invalid fault later in the list
        can never leave an earlier one already committed (its ``"faults"`` draw consumed, a
        timeline event recorded, a timer armed) despite the call raising. Every check here is
        cheap and draws no entropy — pinned values are checked as given; unresolved ones are
        checked only for whether resolution is *possible* (enough bound addresses to choose
        from), never resolved.
        """
        if isinstance(fault, PartitionFault):
            if fault.groups:
                self._validate_groups(fault.groups)
            elif len(self._bound_addresses()) < 2:
                raise SeedloopError(
                    "partition() needs at least two bound addresses for the seed to split; "
                    f"only {len(self._bound_addresses())} bound"
                )
        elif isinstance(fault, SlowLinkFault):
            # A pinned a/b is re-checked for shape here (not just World.slow_link's own eager
            # check) because SlowLinkFault is a public dataclass a caller could construct
            # directly with anything — a non-address a/b would otherwise silently schedule a
            # fault that can never match a real (src, dst) pair, recorded on the timeline as if
            # it worked, same bypass reason as the a == b / factor checks below.
            if fault.a is not None and not isinstance(fault.a, int):
                raise SeedloopError(f"slow_link a must be an address, got {fault.a!r}")
            if fault.b is not None and not isinstance(fault.b, int):
                raise SeedloopError(f"slow_link b must be an address, got {fault.b!r}")
            if fault.a is None and fault.b is None:
                if len(self._bound_addresses()) < 2:
                    raise SeedloopError(
                        "slow_link() needs at least two bound addresses for the seed to pick "
                        f"a pair from; only {len(self._bound_addresses())} bound"
                    )
            elif fault.a is None:
                # Only the pinned side (b) needs excluding, not "two bound overall" — a bound
                # address equal to b is unusable, but b itself need not be bound at all.
                if not any(addr != fault.b for addr in self._bound_addresses()):
                    raise SeedloopError(
                        "slow_link() needs a bound address distinct from the pinned b for "
                        f"the seed to pick a from; only {self._bound_addresses()!r} bound"
                    )
            elif fault.b is None and not any(addr != fault.a for addr in self._bound_addresses()):
                raise SeedloopError(
                    "slow_link() needs a bound address distinct from the pinned a for the "
                    f"seed to pick b from; only {self._bound_addresses()!r} bound"
                )
            if fault.a is not None and fault.a == fault.b:
                # Re-validated here (World.slow_link already checks two pinned, equal
                # addresses) because Fault is a public dataclass a caller could construct
                # directly, bypassing that check — a == b would otherwise silently no-op
                # (frozenset((a, b)) collapses to one element, which never matches a real
                # (src, dst) pair) instead of being rejected.
                raise SeedloopError(
                    f"slow_link requires two distinct addresses, got a == b == {fault.a!r}"
                )
            if fault.factor is not None and (
                not math.isfinite(fault.factor) or fault.factor <= 1.0
            ):
                # Re-validated here (World.slow_link already checks a pinned factor) — same
                # bypass reason as above.
                raise SeedloopError(
                    f"slow_link factor must be finite and > 1.0, got {fault.factor!r}"
                )
        elif isinstance(fault, CrashFault):
            # Same bypass reason as slow_link's a/b check above: a non-address node would never
            # match a real bound address, so the crash would schedule and record on the timeline
            # without ever cutting anything real.
            if fault.node is not None and not isinstance(fault.node, int):
                raise SeedloopError(f"crash node must be an address, got {fault.node!r}")
            if fault.at is not None:
                if not math.isfinite(fault.at) or fault.at < 0:
                    # Re-validated here (World.crash already checks this) — same bypass reason.
                    raise SeedloopError(f"crash(at={fault.at}) must be finite and >= 0")
                if fault.at > seconds:
                    raise SeedloopError(
                        f"crash(at={fault.at}) exceeds this run_for's seconds={seconds}; at "
                        f"must be in [0, seconds]"
                    )
            if fault.node is None and not self._bound_addresses():
                raise SeedloopError(
                    "crash() needs at least one bound address for the seed to pick from"
                )
        else:
            raise SeedloopError(f"unrecognized fault handle: {fault!r}")

    @staticmethod
    def _validate_groups(groups: tuple[frozenset[Address], ...]) -> None:
        # PartitionFault.groups is the one fault field with no World.partition equivalent check
        # (its constructor just wraps the arguments) -- re-validated here because it is a public
        # dataclass a caller could construct directly with anything. A malformed, non-iterable
        # group would otherwise escape as a bare TypeError instead of SeedloopError; overlapping
        # groups would otherwise produce an incoherent cut (_cut matches only the first group
        # containing an address, so two addresses "in the same" listed group could still be cut
        # from each other).
        try:
            group_list = list(groups)
        except TypeError as exc:
            raise SeedloopError(f"partition() groups must be an iterable of groups: {exc}") from exc
        seen: set[Address] = set()
        for g in group_list:
            try:
                addrs = set(g)
            except TypeError as exc:
                raise SeedloopError(
                    f"partition() groups must be iterables of addresses: {exc}"
                ) from exc
            if not all(isinstance(addr, int) for addr in addrs):
                raise SeedloopError(f"partition() groups must contain only addresses, got {g!r}")
            if seen & addrs:
                raise SeedloopError(f"partition() groups must not overlap, got {groups!r}")
            seen |= addrs

    def _commit_fault(self, fault: Fault, seconds: float) -> None:
        """Resolve and schedule one already-validated fault (ADR-0022).

        Called once per fault, only after every fault in the same ``run_for(faults=[...])`` call
        has passed :meth:`_validate_fault`. Any field the caller left unset is drawn from the
        ``"faults"`` sub-stream, in the order the ``faults`` list is given — reordering the list
        changes what a seed resolves, since resolution shares one sequential stream across every
        fault in the call.
        """
        now = self._loop.time()
        if isinstance(fault, PartitionFault):
            self._commit_partition_fault(fault, now, seconds)
        elif isinstance(fault, SlowLinkFault):
            self._commit_slow_link_fault(fault, now, seconds)
        elif isinstance(fault, CrashFault):
            self._commit_crash_fault(fault, now, seconds)

    def _bound_addresses(self) -> list[Address]:
        return sorted(self._endpoints)  # stable order so a seed draw over it is reproducible

    def _draw_bipartition(self) -> list[set[Address]]:
        addrs = self._bound_addresses()  # _validate_fault already confirmed len(addrs) >= 2
        while True:  # re-drawn until both sides are non-empty, so the split is a genuine cut
            a: set[Address] = set()
            b: set[Address] = set()
            for addr in addrs:
                (a if self._faults.random() < 0.5 else b).add(addr)
            if a and b:
                return [a, b]

    def _commit_partition_fault(self, fault: PartitionFault, now: float, seconds: float) -> None:
        groups = [set(g) for g in fault.groups] if fault.groups else self._draw_bipartition()
        start = self._faults.uniform(0.0, seconds)
        end = self._faults.uniform(start, seconds)
        fault_id = self._next_fault_id
        self._next_fault_id += 1
        # The resolved groups are recorded (sorted, for a stable repr) so a partition-dependent
        # failure is diagnosable from the trace alone -- which nodes were cut, not just when.
        recorded_groups = tuple(tuple(sorted(g)) for g in groups)
        self._timeline.record(
            (now, "fault-partition-scheduled", fault_id, recorded_groups, now + start, now + end)
        )
        self._loop.call_at(now + start, self._begin_fault_partition, fault_id, groups)
        self._loop.call_at(now + end, self._end_fault_partition, fault_id)

    def _commit_slow_link_fault(self, fault: SlowLinkFault, now: float, seconds: float) -> None:
        # _validate_fault already confirmed a resolvable candidate exists for whichever side is
        # unresolved. Each unresolved side must exclude the OTHER side once it is known, not just
        # "some bound address" -- drawing `a` from every bound address while `b` is already
        # pinned could otherwise land on `a == b` (a silently inert fault: frozenset((a, b))
        # collapses to one element, which never matches a real (src, dst) pair).
        a, b = fault.a, fault.b
        if a is None and b is None:
            a = self._faults.choice(self._bound_addresses())
            b = self._faults.choice([addr for addr in self._bound_addresses() if addr != a])
        elif a is None:
            a = self._faults.choice([addr for addr in self._bound_addresses() if addr != b])
        elif b is None:
            b = self._faults.choice([addr for addr in self._bound_addresses() if addr != a])
        assert a is not None and b is not None  # every branch above resolves both
        factor = fault.factor if fault.factor is not None else self._faults.uniform(2.0, 50.0)
        start = self._faults.uniform(0.0, seconds)
        end = self._faults.uniform(start, seconds)
        fault_id = self._next_fault_id
        self._next_fault_id += 1
        pair = frozenset((a, b))
        self._timeline.record(
            (now, "fault-slow-link-scheduled", fault_id, a, b, factor, now + start, now + end)
        )
        # The multiplier is fixed once, at send time (_send reads _fault_slow_links then) --
        # unlike partition/crash reachability, which is re-evaluated at delivery time. A message
        # already in flight when this window opens is not slowed; one sent inside it stays
        # slowed even after the window has closed by the time it is delivered.
        self._loop.call_at(now + start, self._begin_slow_link, fault_id, pair, factor)
        self._loop.call_at(now + end, self._end_slow_link, fault_id)

    def _commit_crash_fault(self, fault: CrashFault, now: float, seconds: float) -> None:
        at = fault.at if fault.at is not None else self._faults.uniform(0.0, seconds)
        node = fault.node
        if node is None:  # _validate_fault already confirmed >= 1 bound address in this case
            node = self._faults.choice(self._bound_addresses())
        fault_id = self._next_fault_id
        self._next_fault_id += 1
        self._timeline.record((now, "fault-crash-scheduled", fault_id, node, now + at))
        self._loop.call_at(now + at, self._begin_crash, fault_id, node)

    def _begin_fault_partition(self, fault_id: int, groups: list[set[Address]]) -> None:
        self._fault_partitions[fault_id] = groups
        self._timeline.record((self._loop.time(), "fault-partition-begin", fault_id))

    def _end_fault_partition(self, fault_id: int) -> None:
        self._fault_partitions.pop(fault_id, None)
        self._timeline.record((self._loop.time(), "fault-partition-heal", fault_id))

    def _begin_slow_link(self, fault_id: int, pair: frozenset[Address], factor: float) -> None:
        self._fault_slow_links[fault_id] = (pair, factor)
        self._timeline.record((self._loop.time(), "fault-slow-link-begin", fault_id))

    def _end_slow_link(self, fault_id: int) -> None:
        self._fault_slow_links.pop(fault_id, None)
        self._timeline.record((self._loop.time(), "fault-slow-link-end", fault_id))

    def _begin_crash(self, fault_id: int, node: Address) -> None:
        self._crashed.add(node)
        self._timeline.record((self._loop.time(), "fault-crash", fault_id, node))

    def _reachable(self, src: Address, dst: Address) -> bool:
        # Cut by the scenario-driven partition (world.net.partition/heal) OR by any currently active
        # fault-scheduled partition (run_for(faults=[...]), ADR-0022) — either one blocks delivery,
        # independently of the other, so neither can silently restore what the other cut.
        if self._cut(self._partition, src, dst):
            return False
        return all(not self._cut(groups, src, dst) for groups in self._fault_partitions.values())

    @staticmethod
    def _cut(groups: list[set[Address]] | None, src: Address, dst: Address) -> bool:
        if groups is None:
            return False
        gs = next((g for g in groups if src in g), None)
        gd = next((g for g in groups if dst in g), None)
        if gs is None or gd is None:
            return False  # a node in no listed group reaches everyone
        return gs is not gd

    def _send(self, endpoint: _Endpoint, dst: Address, msg: Message) -> None:
        src = endpoint.address
        mid = self._next_mid
        self._next_mid += 1
        self._timeline.record((self._loop.time(), "send", mid, src, dst))
        # Draw the message's network latency once per send, before any fault decision, so the "net"
        # sub-stream advances by exactly one draw per send whatever a fault does. A realized drop or
        # duplicate must not shift other messages' latencies (docs/network.md); a dropped message
        # still consumes its draw, and a duplicate's extra delivery draws from "faults" below.
        latency = self._net.uniform(_LAT_MIN, _LAT_MAX) * self._slow_factor(src, dst)
        if endpoint._reliable:
            self._schedule_reliable(mid, src, dst, msg, latency)
            return
        if endpoint._loss > 0.0 and self._faults.random() < endpoint._loss:
            self._timeline.record((self._loop.time(), "drop", mid, src, dst))
            return
        self._loop.call_later(latency, self._deliver, mid, src, dst, msg)
        if endpoint._duplicate > 0.0 and self._faults.random() < endpoint._duplicate:
            # The duplicate is a fault artifact, so its extra delivery time is drawn from the
            # "faults" sub-stream, not "net" — enabling duplication does not perturb "net".
            self._timeline.record((self._loop.time(), "duplicate", mid, src, dst))
            dup_latency = self._faults.uniform(_LAT_MIN, _LAT_MAX) * self._slow_factor(src, dst)
            self._loop.call_later(dup_latency, self._deliver, mid, src, dst, msg)

    def _slow_factor(self, src: Address, dst: Address) -> float:
        # Every currently active slow_link fault (run_for(faults=[...]), ADR-0022) whose pair
        # matches {src, dst} multiplies latency; overlapping faults on the same pair compound.
        pair = frozenset((src, dst))
        factor = 1.0
        for link_pair, link_factor in self._fault_slow_links.values():
            if link_pair == pair:
                factor *= link_factor
        return factor

    def _schedule_reliable(
        self, mid: int, src: Address, dst: Address, msg: Message, latency: float
    ) -> None:
        # Non-decreasing delivery times per (src, dst); equal times fire in send order via the timer
        # (when, seq) tie-break — so a reliable link delivers in order, with no loss or duplication.
        key = (src, dst)
        when = max(self._loop.time() + latency, self._reliable_clock.get(key, 0.0))
        self._reliable_clock[key] = when
        self._loop.call_at(when, self._deliver, mid, src, dst, msg)

    def _deliver(self, mid: int, src: Address, dst: Address, msg: Message) -> None:
        if src in self._crashed or dst in self._crashed:
            # Checked ahead of reachability so a crashed node's drops carry their own reason,
            # distinct from a partition cut (ADR-0022). Both checks are evaluated here, at
            # delivery time, not at send — a message sent before `at` but delivered after it is
            # still cut. Unlike partition, crash is monotone: once true it can never flip back
            # false (no heal), so there is no "recovered in time" case to account for.
            self._timeline.record((self._loop.time(), "drop-crashed", mid, src, dst))
            return
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
        if self._waiter is not None:
            # One endpoint has one logical receiver; a second concurrent recv would orphan the
            # first's waiter. Fail loudly rather than corrupt delivery silently.
            raise SeedloopError("concurrent recv on one endpoint is not supported")
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
