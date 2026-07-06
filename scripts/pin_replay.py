"""Replay-stability pin: a golden timeline that guards the ADR-0011 intra-version contract.

ADR-0011 promises "same seed -> same timeline within a major version". The in-process replay tests
prove a seed is stable within one run; they cannot catch a refactor that silently shifts a timeline
versus a previous commit. This pins one canonical scenario's timeline to a committed golden fixture:
any timeline-affecting change (scheduling, the virtual clock, entropy derivation, or fault-draw
order) fails the check, forcing a conscious CHANGELOG note or a deliberate re-pin across a major.

The canonical scenario exercises the whole determinism surface at a fixed seed: concurrency and
scheduling, the virtual clock and autojump, the user RNG, and the seeded network with loss,
duplication, and a partition window. Its timeline is serialized one event per line (full-precision
``repr``), so a diff is human-readable and a float drift cannot hide.

Usage:
    python scripts/pin_replay.py           # check the canonical timeline against the golden
    python scripts/pin_replay.py --update  # rewrite the golden (review the diff!)
    python scripts/pin_replay.py --golden PATH  # check a specific fixture (used by the test)
"""

from __future__ import annotations

import asyncio
import difflib
import sys
from collections.abc import Sequence
from pathlib import Path

from seedloop._run import _run_one
from seedloop._world import World

SEED = 20260706
GOLDEN = Path(__file__).resolve().parent.parent / "tests" / "data" / "replay_golden.txt"


async def canonical_scenario(world: World) -> None:
    """One representative run: scheduling, clock, RNG, and the seeded network with faults."""
    a = world.net.bind(1, loss=0.3, duplicate=0.25)
    b = world.net.bind(2, loss=0.3, duplicate=0.25)
    endpoints = {1: a, 2: b}

    async def worker(tag: str, src: int, dst: int) -> None:
        endpoint = endpoints[src]
        for i in range(6):
            world.record((tag, world.rng.random(), world.rng.randint(1, 1000)))
            await endpoint.send(dst, (tag, i))
            await asyncio.sleep(0.3)

    async def partitioner() -> None:
        await asyncio.sleep(0.5)
        world.net.partition({1}, {2})  # sends during the window record drop-partitioned
        await asyncio.sleep(0.6)
        world.net.heal()  # later sends can deliver again — the full partition lifecycle is pinned

    await asyncio.gather(worker("x", 1, 2), worker("y", 2, 1), partitioner())
    await asyncio.sleep(2.5)  # let post-heal deliveries fire before teardown


def serialize(timeline: Sequence[object]) -> str:
    """One event per line, full-precision repr — byte-exact and diffable."""
    return "".join(f"{event!r}\n" for event in timeline)


def _read(path: Path) -> str:
    with open(path, encoding="utf-8", newline="") as f:  # newline="" keeps LF, no CRLF on Windows
        return f.read()


def _write(path: Path, text: str) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(text)


def main(argv: list[str]) -> int:
    update = "--update" in argv
    golden_path = GOLDEN
    if "--golden" in argv:
        idx = argv.index("--golden")
        if idx + 1 >= len(argv):
            print("--golden requires a path", file=sys.stderr)
            return 2
        golden_path = Path(argv[idx + 1])

    current = serialize(_run_one(canonical_scenario, SEED))

    if update:
        _write(golden_path, current)
        print(f"golden written: {current.count('\n')} events -> {golden_path}")
        return 0

    if not golden_path.exists():
        print(f"no golden at {golden_path}; generate it with --update", file=sys.stderr)
        return 1

    expected = _read(golden_path)
    if current == expected:
        print(f"OK: canonical timeline matches the golden ({current.count('\n')} events).")
        return 0

    print("FAIL: timeline differs from the golden (a timeline-affecting change).", file=sys.stderr)
    for line in difflib.unified_diff(
        expected.splitlines(), current.splitlines(), "golden", "current", lineterm=""
    ):
        print(line, file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
