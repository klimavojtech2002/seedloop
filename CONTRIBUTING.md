# Contributing

seedloop is a built, tested library (the planned roadmap is complete through v0.3.0); the documentation
under [docs/](docs/) is the design reference, and [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) describes
how a run works end to end. This file is how to work on the code.

## Requirements

- CPython 3.12 or newer. seedloop attaches its loop through `asyncio`'s `loop_factory`, which those
  versions support.
- A single-threaded target. seedloop tests `asyncio` logic against an abstract transport; real threads,
  `multiprocessing`, real sockets, and `uvloop` are out of scope by design (see [docs/scope.md](docs/scope.md)).

## Setup

```bash
python -m venv .venv
. .venv/bin/activate           # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
```

## The gates

Every change must pass all four locally before it is proposed, and CI runs the same on every push and
pull request across Linux, Windows, and macOS:

```bash
ruff check .            # lint
ruff format --check .   # formatting
mypy .                  # types — the public API is fully typed
pytest -q               # tests, including replay-equivalence and boundary tests
pytest -q -k <name>     # a single test while iterating
```

A change is not done until every gate is green and its output has been read. New logic ships with tests,
including the edge and adversarial cases; anything claiming determinism ships with a replay test that
proves *same seed → same timeline* (see [docs/testing.md](docs/testing.md)).

A separate CI job runs a mutation gate — it breaks the library one change at a time and fails if a test
does not catch it, so "the tests pass" means "a regression would fail a test" (ADR-0019):

```bash
pip install -e ".[dev,mutation]"
python scripts/mutation_sweep.py            # check survivors against the reviewed baseline
python scripts/mutation_sweep.py --update   # regenerate the baseline, then restore the reasons
```

A new survivor is a real coverage gap: add a test, or record it in `scripts/mutation_baseline.txt` with
the reason it is equivalent.

## Layout

```
src/seedloop/      the package
tests/             the test suite (deterministic and seeded)
docs/              the specification: architecture, scope, API, internals, network, testing, decisions
```

## Style

- Fully type-hinted; `mypy` clean.
- Raise specific exceptions; never swallow, no bare `except`.
- Comments and docs explain *why*, not *what* — plain, terse English. Match the voice of the existing
  `docs/`.
- Non-obvious choices get a short decision record in [docs/decisions.md](docs/decisions.md).
