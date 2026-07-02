# Simulated network and faults

The transport model behind `world.net` and the faults a run can inject. The decision to model discrete
unreliable messages rather than TCP streams is ADR-0006; the addressed message-port API is ADR-0007 and
its signatures are in [api.md](api.md). This document is the model that implementation is held to.

## Addressing

A node has an integer `Address` and one `Endpoint`, obtained by `world.net.bind(address)`. Messages are
addressed node-to-node; there is no port/socket abstraction below the address. An `Address` is unique
within a run; binding the same address twice is an error.

## Message lifecycle

A message passes through four points, all inside the World:

1. **send** — `endpoint.send(dst, msg)` draws a latency from the network's seed sub-stream and schedules
   a *delivery event* (a timer) at `now + latency`. It returns immediately; sending does not block on
   delivery.
2. **in flight** — the message is a pending timer. Faults that apply to it (drop, duplicate, the link's
   reachability) are evaluated against the network state *at the moment the delivery event fires*, not at
   send time — so a partition that heals before delivery lets the message through, and one that opens
   after send still cuts it.
3. **deliver** — the event appends `(src, msg)` to `dst`'s receive queue and wakes any `recv` blocked on
   it.
4. **recv** — `endpoint.recv()` returns the next `(src, msg)`, blocking in virtual time until one is
   queued.

Because every delivery is an ordinary timer on the loop's heap, message timing is scheduled by the same
deterministic mechanism as everything else (see [internals.md](internals.md)).

## What the seed controls (datagram default)

By default an endpoint's links are an **unreliable datagram channel**. From the network sub-stream the
seed decides, per message:

| Effect | How it is produced |
|--------|--------------------|
| **Latency** | A delay drawn from a configured distribution; the delivery event is scheduled at `now + delay`. |
| **Reordering** | Emergent, not separately injected: two messages sent close together draw independent latencies, so arrival order can differ from send order. |
| **Drop** | With the link's loss probability, no delivery event is scheduled (the message still draws its `net` latency, so dropping it does not shift other messages). |
| **Duplication** | With the link's duplication probability, a second delivery is scheduled — the original's latency drawn from `net`, the duplicate's from `faults`. |
| **Partition** | A reachability predicate on the link; while open, a delivery that fires is dropped. |

The distributions and probabilities are run parameters with documented defaults; pinning them forces a
regime, leaving them lets the seed explore. Nothing here reads wall-clock time or a real socket — the
"network" is queues and timers.

## Opt-in reliable, ordered delivery

`world.net.bind(address, reliable=True)` opts an endpoint's links into **no-loss, in-order** delivery,
for protocols that legitimately assume per-connection ordering (many that normally run over TCP). It is
a delivery *policy* over the same channel, not a TCP stack:

- No drop and no duplication on a reliable link.
- Delivery order matches send order per `(src, dst)` pair — implemented by giving the link a single
  FIFO delivery queue rather than independent per-message latencies.
- Latency still applies, and a partition still cuts the link while open (messages queue or fail per the
  policy settled in Phase 2).

What `reliable` does **not** add: byte-stream semantics — partial reads, coalescing, flow control,
backpressure, connection handshakes. Those are TCP fidelity, out of scope by ADR-0001/0006. A reliable
link delivers whole messages, reliably and in order; it does not pretend to be a socket.

## Faults

Every `send` draws exactly one latency from `"net"`, before any fault is decided; faults draw from the
separate `"faults"` sub-stream. So a realized drop or duplicate never shifts another message's latency —
a dropped message still consumed its `"net"` draw, and a duplicate's extra delivery draws from `"faults"`.
*Implemented:*

- **Loss / duplication** — `bind(address, loss=p, duplicate=q)` sets per-message probabilities on that
  endpoint's outgoing links. At send the seed decides: drop (schedule no delivery, recorded as `drop`) or
  duplicate (a second delivery whose latency is drawn from `"faults"`, recorded as `duplicate`, sharing
  the message's id). Default 0.0.
- **Partition** — `world.net.partition(*groups)` splits the network; nodes in different groups cannot
  reach each other until `world.net.heal()`. A node in no listed group stays connected to everyone.
  Reachability is checked *when a delivery fires* — a partition that opened in flight cuts the message
  (`drop-partitioned`); one that healed in time lets it through.
- **Reliable channel** — `bind(address, reliable=True)` opts into no-loss, no-duplication, in-order
  delivery per `(src, dst)` (a single non-decreasing delivery clock per link); latency still applies and
  a partition still cuts it. Not a byte stream.

The same seed injects the same loss, duplication, and (under a scenario's topology) the same partition
effects at the same virtual times every run — which is what makes a partition-dependent bug reproducible
(the DST payoff: `check` finds the failing seed, `replay` reproduces it).

*Design (deferred, ADR-0016):* seed-*scheduled* faults via `world.run_for(seconds, faults=[...])` where
the seed chooses partition timing, the fault-handle constructors (`partition()`/`slow_link()`/`crash()`),
and node crash. The scenario drives partition topology explicitly for now.

## What this model is not

It is not a TCP/IP simulation. There are no byte streams, no flow control, no connection lifecycle, no
routing or addressing below the node address. That fidelity is a multi-month subsystem and is the
rabbit hole ADR-0001 was drawn to avoid; modelling discrete unreliable messages is enough for the
protocols seedloop targets and keeps the determinism seam small and fully controlled.
