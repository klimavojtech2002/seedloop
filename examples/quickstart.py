"""Find a retry bug with a seed sweep, then replay it.

A client sends a request and retries if no acknowledgement arrives within 30 ms. The simulated
network delays each message by 1 to 20 ms, so the request and its acknowledgement can take up to
40 ms together. On some seeds the acknowledgement is still in flight when the client retries, and a
server that does not remember what it has handled applies the request twice.

Run it:  ``python examples/quickstart.py``
"""

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
