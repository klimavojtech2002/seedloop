"""Run one generated program under whichever seedloop is on PYTHONPATH; print one result line.

Used by ``test_differential.py`` (via ``python tests/_program_child.py <id>``) so the same
program can be run under the repository's source and under a previously released build, in
separate processes. The line is ``<outcome> <events> <digest>``: the outcome is ``ok`` or the
exception type name, and the digest covers the whole timeline.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import sys

from _programs import scenario_for
from seedloop._run import _run_one
from seedloop._world import World


def main(program_id: int) -> str:
    logging.getLogger("asyncio").disabled = True  # asyncio's own log lines are not under comparison
    worlds: list[World] = []

    async def scenario(world: World) -> None:
        worlds.append(world)
        await scenario_for(program_id, world)

    outcome = "ok"
    try:
        _run_one(scenario, program_id % 5)
    except (Exception, asyncio.CancelledError) as exc:  # a cancelled scenario is an outcome too
        outcome = type(exc).__name__
    timeline = worlds[0].timeline
    text = "".join(f"{event!r}\n" for event in timeline)
    return f"{outcome} {len(timeline)} {hashlib.sha1(text.encode()).hexdigest()[:16]}"


if __name__ == "__main__":
    print(main(int(sys.argv[1])))
