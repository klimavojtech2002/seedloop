# seedloop

Deterministic simulation testing for Python `asyncio`: run concurrent code through thousands of
seeded, reproducible timelines — varying message timing, injecting network faults and partitions —
to find the concurrency bug that shows up once in a million runs, then replay it exactly from a seed.

This is the FoundationDB / TigerBeetle / Antithesis style of reliability testing. `seedloop` brings it
to Python's `asyncio` as a `pip`-installable library.

[![CI](https://github.com/klimavojtech2002/seedloop/actions/workflows/ci.yml/badge.svg)](https://github.com/klimavojtech2002/seedloop/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/seedloop)](https://pypi.org/project/seedloop/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## The problem

Concurrency bugs depend on an exact interleaving of events — a message arriving late, a partition
healing at the wrong moment. A test suite that can't reproduce the interleaving can't reproduce the
bug, so it gets patched by guesswork and survives for years.

Deterministic simulation testing (DST) controls every source of nondeterminism — scheduling, time,
randomness, the network — from a single seed. Same seed, same timeline, same bug, every time. Sweep
thousands of seeds to find a failure; the seed that found it is the reproduction.

## Usage

```
pip install seedloop
```

Write your protocol against an abstract transport ([sans-I/O](https://sans-io.readthedocs.io/)), and
`seedloop` runs it inside a world it fully controls. The example below is
[`examples/quickstart.py`](examples/quickstart.py) and runs as written: a client retries a request
after 30 ms, the simulated network can take up to 40 ms for a request and its acknowledgement
together, so on some seeds the client retries while the acknowledgement is still on its way, and a
server with no memory of what it has handled applies the request twice.

```python
from __future__ import annotations

import asyncio

import seedloop


class Server:
    def __init__(self, world: seedloop.World, *, remember: bool) -> None:
        self.endpoint = world.net.bind(1)
        self.remember = remember
        self.applied: list[int] = []  # request ids, in the order they were applied

    async def run(self) -> None:
        while True:
            sender, request_id = await self.endpoint.recv()
            assert isinstance(request_id, int)
            if not (self.remember and request_id in self.applied):
                self.applied.append(request_id)
            await self.endpoint.send(sender, request_id)  # acknowledge


class Client:
    def __init__(self, world: seedloop.World) -> None:
        self.endpoint = world.net.bind(0)

    async def run(self) -> None:
        while True:
            await self.endpoint.send(1, 7)
            try:
                await asyncio.wait_for(self.endpoint.recv(), timeout=0.03)
            except TimeoutError:
                continue  # no acknowledgement in time: send the request again
            return


def make_scenario(*, remember: bool) -> seedloop.Scenario:
    async def scenario(world: seedloop.World) -> None:
        server, client = Server(world, remember=remember), Client(world)
        world.start(server, client)
        world.always(
            lambda: len(server.applied) == len(set(server.applied)), name="applied-at-most-once"
        )
        await asyncio.sleep(1)

    return scenario


if __name__ == "__main__":
    broken = seedloop.check(make_scenario(remember=False), seeds=200, on_failure="return")
    print(f"server without memory: {broken.error!r} at seed={broken.failing_seed}")
    assert broken.failing_seed is not None
    try:
        seedloop.replay(make_scenario(remember=False), seed=broken.failing_seed)
    except seedloop.InvariantError:
        print(f"replay of seed={broken.failing_seed}: the same failure, every time")
    fixed = seedloop.check(make_scenario(remember=True), seeds=200, on_failure="return")
    print(
        f"server that remembers: {fixed.checked} seeds checked, failing seed: {fixed.failing_seed}"
    )
```

```
$ python examples/quickstart.py
server without memory: InvariantError("invariant 'applied-at-most-once' violated at t=0.045590860354622995") at seed=9
replay of seed=9: the same failure, every time
server that remembers: 200 seeds checked, failing seed: None
```

`seedloop.replay(scenario, seed=...)` re-runs that exact timeline as many times as needed to debug
it. Full API, including the seed-scheduled fault handles (`world.partition()`/`slow_link()`/`crash()`
passed to `world.run_for`), in [docs/api.md](docs/api.md).

## Worked proof: a Raft split-brain, found and replayed

A small Raft leader-election demo ships with a labelled flaw — a node that skips the
single-vote-per-term rule. A seed sweep finds the timing where two nodes win the same term
(split-brain) and replays it; the corrected election passes the same sweep clean.

```
$ python -m seedloop.demos.raft
buggy election: split-brain found at seed=7
  replay reproduces it: invariant 'at-most-one-leader-per-term' violated at t=0.229...
correct election (single-vote rule enforced): no violation over the same 200 seeds
```

Election only — log replication, persistence, and membership changes are out of scope. Code in
[`src/seedloop/demos/raft.py`](src/seedloop/demos/raft.py).

## What it does

- A deterministic event loop that makes `asyncio` scheduling reproducible.
- A virtual clock — `sleep` and timeouts advance instantly.
- Seeded randomness everywhere, so a run is a pure function of its seed.
- A simulated network with seeded latency, reordering, loss, and partitions.
- Fault injection driven by the seed, so chaos is reproducible.
- `world.always(...)` — a continuous safety-invariant check.
- A non-determinism auditor (`audit=True`) that turns any uncontrolled entropy source into a
  reproducible failure.

## Scope

`seedloop` makes async *logic* deterministic; it does not make *infrastructure* deterministic, and
says so rather than pretending otherwise. Full boundary in [docs/scope.md](docs/scope.md).

- **For:** pure-Python async code against an abstract transport — consensus, replication, gossip,
  CRDTs, wire protocols, retry/backoff logic, rate limiters.
- **Not for:** real threads, `multiprocessing`, `uvloop`, C-extension drivers (`asyncpg`, `grpcio`) —
  their scheduling can't be controlled from Python, the same wall that blocks DST in Go.

## Design

`seedloop` subclasses `asyncio.BaseEventLoop` and replaces only the I/O-poll seam
(`_run_once`'s `select()`), rather than reimplementing scheduling from scratch: asyncio's own
`call_soon` FIFO ready queue and Task/Future machinery are already deterministic, so the loop
inherits them and only cuts the one seam that isn't (ADR-0013 in
[docs/decisions.md](docs/decisions.md)). The rest of the design log — every decision, each with
what was considered and rejected — is there too.

## Status

`seedloop 0.4.1` is live on PyPI: the deterministic core, simulated network with fault injection,
`world.always` invariants, the non-determinism auditor, `run_for`/`run_until`, seed-scheduled fault
handles (`partition()`/`slow_link()`/`crash()`), the optional Hypothesis integration, and the Raft
demo. The 4 gates (lint, format, types, tests) run green on Linux/macOS/Windows × Python
3.12–3.14. A mutation-sweep gate runs on Windows, generated programs are compared with the previous
release in every CI run, and the built source distribution runs its own test suite.

One limitation is disclosed rather than hidden: a task started with `world.start()` can violate an
`always()` invariant, uncaught, in a narrow one-scheduling-step window right after the scenario
coroutine returns and before teardown cancels it. Present since `v0.3.2`. Full detail in the
"Planned / deferred" section of
[docs/decisions.md](docs/decisions.md#planned--deferred-decisions).

Full API in [docs/api.md](docs/api.md), design in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Documentation

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — how `asyncio` is made deterministic.
- [docs/api.md](docs/api.md) — the public API: `World`, `check`/`replay`, the transport, faults.
- [docs/internals.md](docs/internals.md) — the loop, virtual clock, entropy control, network and fault scheduling.
- [docs/network.md](docs/network.md) — the simulated transport and fault model.
- [docs/scope.md](docs/scope.md) — the determinism boundary: what is controlled and what is not.
- [docs/testing.md](docs/testing.md) — how determinism is proven by replay.
- [docs/decisions.md](docs/decisions.md) — the decision records (ADRs).
- [docs/glossary.md](docs/glossary.md) — the vocabulary.

## License

MIT — see [LICENSE](LICENSE).

[![klimavojtech2002/seedloop, explained in a one-minute video](https://gitdiagram.com/api/video/file?username=klimavojtech2002&repo=seedloop&format=poster)](https://gitdiagram.com/klimavojtech2002/seedloop/video)
