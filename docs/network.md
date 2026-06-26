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
| **Drop** | With the link's loss probability, no delivery event is scheduled. |
| **Duplication** | With the link's duplication probability, two delivery events are scheduled (independent latencies). |
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

Faults are seed-parameterized handles passed to `world.run_for(faults=[...])`; their constructors are in
[api.md](api.md). Each registers timers on the seed's `"faults"` sub-stream that toggle network or node
state:

- **partition(\*groups)** — splits nodes into groups; cross-group links become unreachable at the
  partition's start and reachable again when it heals. Unparameterized, the seed picks the split and the
  open/heal times.
- **slow_link(a, b, factor)** — multiplies the `a↔b` link's latency for a window; a large factor is a
  near-stall short of a full cut.
- **crash(node, at)** — sets a node's stopped flag at virtual time `at`; its `run()` stops making
  progress. Restart-vs-stop semantics are settled in Phase 2 against the Raft demo's needs.

Because the schedule is a pure function of the seed, the same seed injects the same faults at the same
virtual times every run — which is what makes a partition-dependent bug reproducible (the Phase 2
payoff).

## What this model is not

It is not a TCP/IP simulation. There are no byte streams, no flow control, no connection lifecycle, no
routing or addressing below the node address. That fidelity is a multi-month subsystem and is the
rabbit hole ADR-0001 was drawn to avoid; modelling discrete unreliable messages is enough for the
protocols seedloop targets and keeps the determinism seam small and fully controlled.
