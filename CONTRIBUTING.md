# Contributing

seedloop is a built, tested library. The documentation under [docs/](docs/) is the design reference, and
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) describes how a run works end to end. This file is how to
work on the code.

## Requirements

- CPython 3.12 or newer, tested on 3.12 to 3.14. seedloop's loop builds on private parts of `asyncio`,
  and only those versions are checked on every change.
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
python scripts/mutation_sweep.py --shard 2/4  # one shard of the mutants (CI runs four in parallel)
python scripts/mutation_sweep.py --list     # print every mutant key
python scripts/mutation_sweep.py --update   # regenerate the baseline, then write the reasons
```

Mutations run in a temporary copy of `src/`, never in your working tree. Each mutant has a stable key,
`path::function#n description [hash of the original line]`, so a baseline entry survives edits
elsewhere in the file and in other functions. It is reported as stale when the line it describes
changes, or when a new mutation site is added earlier in the same function (which renumbers the
later ones). A mutant that makes the suite hang counts as caught, and the whole process tree of a
hung run is killed.

A new survivor is a real coverage gap: add a test, or record it in `scripts/mutation_baseline.txt` with
the reason it is equivalent.

Three more checks guard the guards. The corpus runs as part of plain `pytest`; the other two are slower
or need the network, so plain `pytest` skips them and CI runs them in their own jobs:

```bash
python scripts/pin_corpus.py                # the characterization corpus (also part of `pytest`)
python scripts/pin_corpus.py --update       # re-pin after a deliberate, changelogged behaviour change
pytest -m meta                              # each named change to a copy of src/ must fail the corpus;
                                            # the mutation harness is tested on a toy project
pytest -m differential                      # generated programs must behave like the previous release
```

The corpus pins well over a hundred scenarios, most at one to three seeds, instead of one scenario at
one seed. Each change listed in `tests/test_corpus.py` is proven to make it fail. The differential
test compares generated programs against the previously released wheel in a separate process; a difference is a behaviour
change and has to be declared in `tests/test_differential.py`. Tests that could hang run through
`tests/_support.py::run_isolated`, which gives them a hard time limit in a fresh interpreter.

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
