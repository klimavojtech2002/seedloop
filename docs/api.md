# Public API

The surface a user writes against. This is the design target for Phases 1–3; no code exists yet, so
treat every signature as the committed *intent* the build is held to, not as shipped behaviour. The
shape follows the boundary in [scope.md](scope.md) and the decisions in [decisions.md](decisions.md):
user code is sans-I/O, talks to an addressed message port, and a run is a pure function of its seed.

Names use the working package name `seedloop` (ADR naming is still open); nothing here hard-codes the
name where a rename would be costly.

## The shape of a test

```python
import seedloop

async def scenario(world: seedloop.World) -> None:
    # Build your nodes; each binds an address on the simulated network.
    nodes = [RaftNode(addr, world.net) for addr in range(5)]
    world.start(*nodes)

    # An invariant that must hold at every step, not just at the end.
    world.always(lambda: at_most_one_leader(nodes), name="at-most-one-leader")

    # Advance virtual time under faults the seed parameterizes.
    await world.run_for(seconds=10, faults=[world.partition(), world.slow_link()])

# Hunt across many seeded timelines; on failure, report the seed.
result = seedloop.check(scenario, seeds=10_000)
# A failure raises with:  seed=4823  → seedloop.replay(scenario, seed=4823)
```

`replay` re-runs that one seed, identically, as often as a debugger needs:

```python
seedloop.replay(scenario, seed=4823)
```

## Top-level functions

```python
Scenario: TypeAlias = Callable[[World], Awaitable[None]]

def check(
    scenario: Scenario,
    *,
    seeds: int | Iterable[int] = 1000,
    on_failure: Literal["raise", "return"] = "raise",
) -> CheckResult: ...

def replay(scenario: Scenario, *, seed: int) -> None: ...
```

- **`check`** runs `scenario` once per seed. `seeds=N` runs seeds `0..N-1`; an iterable runs exactly
  those seeds. Each run is built from its seed alone (loop, clock, RNG, network, faults). The first seed
  whose run raises — an `assert`, an `InvariantError`, or any exception from user code — is the failure.
  With `on_failure="raise"` (default) `check` re-raises it, tagged with the seed; with `"return"` it
  stops and returns the result for programmatic use. Phase 3 routes seed generation and shrinking through
  Hypothesis (ADR-0004), so the reported seed is a *minimal* failing case where shrinking applies.
- **`replay`** rebuilds the exact World for one seed and runs it once. Same seed → same timeline → same
  outcome, within a major version (ADR-0011).

```python
@dataclass(frozen=True)
class CheckResult:
    checked: int                 # how many seeds ran
    failing_seed: int | None     # first failing seed, or None if all passed
    error: BaseException | None  # the exception that seed raised, or None
```

## `World`

Everything for one run, all derived from the seed. A user never constructs a `World`; `check`/`replay`
build it and pass it to the scenario.

```python
class World:
    seed: int                    # this run's identity
    net: Transport               # the simulated network port (see below)
    rng: random.Random           # seeded RNG for user code — use this, never the global random

    def now(self) -> float: ...                       # current virtual time, seconds
    def start(self, *nodes: Node) -> None: ...        # schedule each node's run() coroutine
    def always(self, predicate: Callable[[], bool], *, name: str) -> None: ...

    async def run_for(self, *, seconds: float, faults: Sequence[Fault] = ()) -> None: ...
    async def run_until(self, predicate: Callable[[], bool], *, deadline: float | None = None) -> None: ...

    # Fault constructors — seed-parameterized handles passed to run_for(...).
    def partition(self, *groups: Collection[Address]) -> Fault: ...
    def slow_link(self, a: Address | None = None, b: Address | None = None, *, factor: float | None = None) -> Fault: ...
    def crash(self, node: Address | None = None, *, at: float | None = None) -> Fault: ...
```

- **`rng`** is the user's entropy. Calling the global `random`, `os.urandom`, or `secrets` inside a run
  is an entropy leak; the auditor (ADR-0008) turns it into an `EntropyLeakError` rather than letting it
  pass.
- **`now`** reads the virtual clock; it advances instantly via autojump (ADR-0005), so a 10-second
  scenario runs in milliseconds.
- **`always`** registers an invariant checked after every scheduling step. The first step where
  `predicate()` is false raises `InvariantError(name)` — which is what `check` catches and ties to the
  seed. Invariants are how a *continuous* property ("never two leaders") is enforced, versus an `assert`
  at the end that only checks the final state.
- **`run_for`** advances virtual time by `seconds`, applying `faults`. A fault left unparameterized
  (`world.partition()` with no groups, `slow_link()` with no endpoints) lets the **seed** decide its
  details — which nodes, when, how long — so chaos is reproducible, not random.
- **`run_until`** advances until `predicate()` holds or the optional virtual `deadline` passes (a
  deadline miss raises `TimeoutError`); useful for "run until the cluster converges".

## The network port

The sans-I/O seam. User code sends and receives typed messages through an **addressed endpoint**; it
never touches a socket. seedloop supplies the deterministic implementation of `Transport`; a production
user could supply a real one against the same interface (ADR-0007).

```python
Address: TypeAlias = int        # a node's address on the simulated network
Message: TypeAlias = object     # an opaque payload; seedloop never inspects it

class Endpoint(Protocol):
    address: Address
    async def send(self, dst: Address, msg: Message) -> None: ...
    async def recv(self) -> tuple[Address, Message]: ...   # (src, msg), blocks until one arrives

class Transport(Protocol):
    def bind(self, address: Address, *, reliable: bool = False) -> Endpoint: ...
```

- **`bind`** gives a node its endpoint. `reliable=False` (default) is an unreliable datagram channel:
  the seed may delay, reorder, duplicate, or drop messages, and a partition can cut delivery entirely
  (ADR-0006). `reliable=True` opts that endpoint's links into ordered, no-loss delivery for protocols
  that assume per-connection ordering — still not a byte stream.
- **`send`** enqueues a message for `dst`; it returns once enqueued, not on delivery (delivery is a
  later scheduled event whose timing the seed owns). **`recv`** yields the next message for this
  endpoint, blocking in virtual time until one is scheduled to arrive.
- **`Message` is opaque.** seedloop schedules and orders messages but never reads them, so the
  *content's* determinism is the user's responsibility (don't put an unordered `set` on the wire and
  rely on its iteration order); the *delivery's* determinism is the World's. Messages are delivered by
  reference, not copied — treat a sent message as immutable, since mutating it after `send` would change
  what a later delivery sees.

A **`Node`** is just user code: any object with `async def run(self) -> None`. `world.start(*nodes)`
schedules each `run()` as a task. There is no required base class.

```python
class Node(Protocol):
    async def run(self) -> None: ...
```

## Faults

`Fault` is an opaque handle produced by the `world.partition/slow_link/crash` constructors and consumed
by `run_for`. The constructors are seed-parameterized: pin the arguments to force a specific fault, or
leave them out to let the seed choose within the run.

```python
class Fault(Protocol): ...      # no user-facing members; pass to run_for(faults=[...])
```

- **`partition(*groups)`** splits the network so messages cross group boundaries only after the
  partition heals. `partition(a, b)` splits the two given groups; `partition()` lets the seed pick a
  split and its timing.
- **`slow_link(a, b, *, factor)`** multiplies latency on the `a↔b` link (or a seed-chosen link); a large
  `factor` is a near-stall short of a full partition. `factor=None` (default) lets the seed choose the
  multiplier; pin it to force a regime.
- **`crash(node, *, at)`** stops a node at virtual time `at` (or a seed-chosen time). Recovery semantics
  (clean stop vs. restart) are settled in Phase 2.

## Errors

```python
class SeedloopError(Exception): ...           # base for everything seedloop raises
class InvariantError(SeedloopError): ...       # an always(...) invariant was violated
class DeadlockError(SeedloopError): ...         # the run is quiescent with tasks still awaiting
class BoundaryError(SeedloopError): ...         # out-of-boundary use inside a run
class EntropyLeakError(BoundaryError): ...      # an uncontrolled entropy source was touched (audit mode)
```

- **`InvariantError`** carries the invariant `name` and the violating step; it is the typical failure
  `check` reports.
- **`DeadlockError`** is raised when no task can make progress and nothing is scheduled to wake one — a
  deadlock in the simulated world. seedloop raises it instead of hanging (as a real program would), so
  the deadlock surfaces as that seed's failure.
- **`BoundaryError`** is raised when a run reaches for something outside the boundary — a real thread,
  `run_in_executor`, a subprocess, a real socket, `uvloop` — rather than letting it run nondeterminist
  ically (ADR-0002).
- **`EntropyLeakError`** is the auditor's tripwire (ADR-0008): an uncontrolled `os.urandom`, `secrets`,
  real time, or unseeded `random` call surfaces as a loud, reproducible failure on the seed that hit it.

## Replay stability

Within a **major version**, a recorded seed reproduces the same timeline and outcome — this is the
contract `replay` rests on, and it is covered by replay-equivalence tests (see
[testing.md](testing.md)). **Across major versions it is not guaranteed:** a change to scheduling, the
fault model, or entropy derivation can move what a seed produces, and the changelog calls out any such
change. The hierarchical seed-splitting of ADR-0009 is what keeps a seed stable under ordinary
intra-version refactors. Practical consequence: a seed you keep in a regression test is valid for the
major version you found it on; re-pin it after a major upgrade.
