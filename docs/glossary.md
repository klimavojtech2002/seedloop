# Glossary

One name per concept, used consistently across the docs and the code. If a term here and a term in the
code ever drift, the code is wrong.

- **Seed** — the integer that is a run's entire identity. The loop, clock, RNG, network, and fault
  schedule are all derived from it; the same seed reproduces the same run (within a major version).
- **Run** — one execution of a scenario for one seed. A pure function of the seed.
- **World** — the object holding everything for one run: the loop, virtual clock, seeded RNG, network
  port, and fault schedule. User code receives it and never constructs it.
- **Scenario** — the user's async function `scenario(world) -> None` that builds nodes, states
  invariants, and advances time. The unit `check`/`replay` execute.
- **Deterministic event loop** — seedloop's custom `asyncio` loop, attached via `loop_factory`, whose
  scheduling is deterministic (faithful `call_soon` FIFO) and which never touches a real socket or clock;
  the seed drives I/O delivery timing, not callback order (ADR-0012).
- **Virtual clock** — simulated monotonic time returned by `loop.time()`; it advances by autojump, not
  by waiting.
- **Autojump** — when every task is blocked, time moves straight to the next scheduled timer instead of
  sleeping, so a long `sleep` resolves instantly.
- **Ready queue** — the `deque` of callbacks ready to run, drained in `call_soon` registration order
  (FIFO), preserved faithfully from `asyncio` (ADR-0012).
- **Timer / timer heap** — the `heapq` of scheduled callbacks keyed by `(virtual deadline, sequence)`;
  the sequence is the deterministic tie-break for equal deadlines.
- **Transport / network port** — the sans-I/O interface (`Transport`, `Endpoint`) user code sends and
  receives messages through; seedloop supplies the simulated implementation.
- **Endpoint** — a node's bound handle on the network, identified by its `Address`, with `send`/`recv`.
- **Address** — a node's integer identity on the simulated network.
- **Message** — an opaque payload sent between endpoints; seedloop schedules and orders it but never
  inspects it.
- **Datagram channel** — the default link: unreliable, unordered messages (drop/reorder/duplicate/delay
  decided by the seed).
- **Reliable channel** — the opt-in link policy: no-loss, in-order whole-message delivery; not a byte
  stream.
- **Fault** — an injected disturbance (partition, slow link, crash), seed-parameterized, applied during
  `run_for`.
- **Fault schedule** — the seed-derived set of faults and their virtual times; chaos made reproducible.
- **Partition** — a network split where cross-group messages are cut until it heals.
- **Sub-stream** — one of the independent per-component PRNG streams split from the root seed, so
  components do not perturb each other (ADR-0009).
- **Tripwire** — an interception point that raises `EntropyLeakError` in audit mode when an uncontrolled
  entropy source is touched (ADR-0008).
- **Timeline / trace** — the append-only record of a run's events; equality of two timelines for one
  seed is the determinism proof. In Phase 1 each entry is `(virtual_time, event)` from `world.record`;
  the automatic per-event `(virtual_time, kind, ids…)` schema arrives with the Phase-2 recorder.
- **Replay** — re-running a recorded seed to reproduce its timeline exactly.
- **Boundary** — the line between what is controlled (single-threaded `asyncio` logic against the
  transport) and what is not (real threads, sockets, `uvloop`); crossing it raises `BoundaryError`.
- **sans-I/O** — the pattern where protocol logic talks to an abstract transport instead of doing I/O
  itself, so a simulated transport can be substituted in tests and a real one in production.
- **DST** — deterministic simulation testing: exploring many seeded timelines to surface rare
  concurrency bugs and replay them from the seed.
