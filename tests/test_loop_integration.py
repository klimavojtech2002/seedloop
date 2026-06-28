"""Integration tests: the loop attaches via loop_factory, and cancellation works."""

from __future__ import annotations

import asyncio

from seedloop._loop import DeterministicLoop

_EXPECTED = [("a", 0), ("b", 0), ("a", 1), ("b", 1), ("a", 2), ("b", 2)]


def _assert_seedloop_running() -> None:
    # Proves loop_factory actually attached OUR loop. The stock asyncio loop is also FIFO and
    # produces the same interleaving, so without this check the order assertions below would pass
    # on the default loop too — a falsely-green "attaches" test.
    assert isinstance(asyncio.get_running_loop(), DeterministicLoop)


async def _interleave() -> list[tuple[str, int]]:
    _assert_seedloop_running()
    events: list[tuple[str, int]] = []

    async def worker(name: str, n: int) -> None:
        for i in range(n):
            events.append((name, i))
            await asyncio.sleep(0)

    await asyncio.gather(worker("a", 3), worker("b", 3))
    return events


def test_attaches_via_asyncio_run_loop_factory() -> None:
    # The documented integration path: user code runs unchanged under asyncio.run.
    first = asyncio.run(_interleave(), loop_factory=DeterministicLoop)
    second = asyncio.run(_interleave(), loop_factory=DeterministicLoop)
    assert first == second == _EXPECTED


def test_attaches_via_asyncio_runner() -> None:
    with asyncio.Runner(loop_factory=DeterministicLoop) as runner:
        result = runner.run(_interleave())
    assert result == _EXPECTED


def test_cancellation_propagates_cleanly() -> None:
    async def scenario() -> str:
        _assert_seedloop_running()

        async def child() -> str:
            try:
                await asyncio.Future()  # blocks; will be cancelled
            except asyncio.CancelledError:
                return "cancelled"
            return "not-cancelled"

        task = asyncio.ensure_future(child())
        await asyncio.sleep(0)  # let the child reach its await
        task.cancel()
        return await task

    assert asyncio.run(scenario(), loop_factory=DeterministicLoop) == "cancelled"


def test_wait_for_around_fast_body_returns() -> None:
    # wait_for schedules a timeout timer; the body completes first, the timer is cancelled.
    async def scenario() -> int:
        _assert_seedloop_running()

        async def fast() -> int:
            await asyncio.sleep(0)
            return 7

        return await asyncio.wait_for(fast(), timeout=5)

    assert asyncio.run(scenario(), loop_factory=DeterministicLoop) == 7
