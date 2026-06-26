# seedloop

Deterministic simulation testing for Python. Run your concurrent async logic through thousands of
controlled, reproducible timelines — varying message timing and delivery order, injecting network
faults, partitions, and delays — to surface the rare concurrency bug that shows up once in a million
runs, and replay it exactly from a seed.

It brings the FoundationDB / TigerBeetle / Antithesis style of reliability testing — until now living
only in Rust, C++, and Java — to Python's `asyncio`, as a `pip`-installable library.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## The problem

Concurrency bugs are the worst bugs. A protocol or state machine works in every test, then once a
week in CI a test fails, and nobody can reproduce it — because the failure depended on an exact
interleaving of events, a message arriving late, a partition healing at the wrong moment. You cannot
fix what you cannot reproduce, so these bugs are patched by guesswork and survive for years.

Deterministic simulation testing (DST) inverts this. It takes total control of every source of
nondeterminism — scheduling order, time, randomness, the network — and drives them all from a single
seed. The same seed produces the same timeline, so the same bug, every time. You explore thousands of
seeds to hunt for failures, and when one is found, the seed *is* the reproduction: replay it and the
bug happens again, deterministically, every run.

This is how FoundationDB reached its reliability record. It exists as a polished library in Rust
(`madsim`, `turmoil`). In Python — where a great deal of distributed and protocol code is written — it
does not exist at all. `seedloop` is that library.

## What you do with it

You write your protocol or algorithm against an abstract transport (the
[sans-I/O](https://sans-io.readthedocs.io/) style), and `seedloop` runs it inside a deterministic
world it fully controls. A test looks like this (illustrative; the API is specified in
[docs/api.md](docs/api.md) and not implemented yet):

```python
import seedloop

async def scenario(world: seedloop.World) -> None:
    # Spin up your nodes; they send messages through the simulated network.
    nodes = [RaftNode(addr, world.net) for addr in range(5)]
    world.start(*nodes)

    # State the invariant that must hold at every step, not just at the end.
    world.always(lambda: at_most_one_leader(nodes), name="at-most-one-leader")

    # Inject chaos the seed decides the details of.
    await world.run_for(seconds=10, faults=[world.partition(), world.slow_link()])

# Hunt across 10,000 seeded timelines; on failure, print the seed.
seedloop.check(scenario, seeds=10_000)
# A failing run prints:  seed=4823  → replay with seedloop.replay(scenario, seed=4823)
```

`seedloop.replay(scenario, seed=4823)` re-runs that exact timeline, deterministically, as many times
as you need to debug it. The full API is in [docs/api.md](docs/api.md).

## What it does

- A **deterministic event loop** that makes `asyncio` task scheduling reproducible and drives the I/O
  seam — where nondeterminism actually enters — from the seed.
- A **virtual clock** — `sleep` and timeouts advance simulated time instantly; no run is slower for
  testing a 10-second scenario.
- **Seeded randomness** everywhere, so a run is a pure function of its seed.
- A **simulated network** with seeded latency, reordering, message loss, and partitions.
- **Fault injection** driven by the seed, so chaos is reproducible rather than random.
- **Seed replay** — the whole point: any failure reduces to a single integer you can replay forever.

## Scope — what it tests, and what it deliberately does not

The honesty in this section is the point. `seedloop` makes your async *logic* deterministic; it does
not make your *infrastructure* deterministic, and it does not pretend to. The full boundary, and the engineering reasons behind it, are in
[docs/scope.md](docs/scope.md). In short:

- **It is for** pure-Python async code that talks to an abstract transport: consensus (Raft/Paxos),
  replication, gossip, CRDTs, custom wire protocols, schedulers, retry/backoff/circuit-breaker logic,
  rate limiters — code where the *logic* holds the concurrency bugs.
- **It is not for** I/O-heavy applications bound to real drivers. Real threads, `multiprocessing`,
  `uvloop`, and C-extension drivers (`asyncpg`, `grpcio`) are explicitly out of scope, because their
  scheduling cannot be controlled from Python — the same wall that stops deterministic testing in Go.
  `seedloop` tests your algorithm, not your database driver.

Choosing this boundary deliberately — rather than promising determinism it cannot deliver — is what
keeps the guarantee real.

## Status

Design stage. The documentation is the specification — the architecture and the determinism boundary
are designed first. The packaging and CI scaffold is in place; the deterministic core is built phase by
phase, each useful on its own — see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Why it exists

There is no `pip`-installable deterministic simulation testing framework for Python `asyncio` — the
capability lives in Rust (`madsim`, `turmoil`), C++ (FoundationDB), Java (OpenDST), and behind a
commercial hypervisor (Antithesis), but not in Python. Meanwhile the discipline is rising fast among
serious engineers (Antithesis raised a $105M round led by Jane Street to standardize DST; AWS has
codified deterministic and formal methods as standing practice). As one of its proponents puts it:
*writing code is no longer the bottleneck — making sure it does the right thing is.* `seedloop` is a
tool for exactly that, in the language that lacked it.

## Documentation

The design is specified before the code:

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — how `asyncio` is made deterministic, and the phased build.
- [docs/api.md](docs/api.md) — the public API: `World`, `check`/`replay`, the transport, faults.
- [docs/internals.md](docs/internals.md) — the loop, virtual clock, entropy control, network and fault scheduling.
- [docs/network.md](docs/network.md) — the simulated transport and fault model.
- [docs/scope.md](docs/scope.md) — the determinism boundary: what is controlled and what is not.
- [docs/testing.md](docs/testing.md) — how determinism is proven by replay.
- [docs/decisions.md](docs/decisions.md) — the decision records (ADRs).
- [docs/glossary.md](docs/glossary.md) — the vocabulary.

## License

MIT — see [LICENSE](LICENSE).
