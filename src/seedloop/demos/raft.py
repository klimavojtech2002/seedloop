"""A small Raft leader election, run under seedloop — the worked proof.

This is election only (terms, ``RequestVote``, majority, heartbeats); log replication, persistence,
and membership changes are out of scope. It exists to demonstrate one thing end to end: seedloop
finds a real class of consensus bug and replays it from a seed.

The bug is a deliberate, labelled toggle, not a claimed discovery in canonical Raft. With
``buggy=True`` a node omits the single-vote-per-term rule, so it can grant a vote to two candidates
in the same term; in a three-node cluster that lets both reach a majority and become leader in one
term — split-brain, the exact failure the majority rule exists to prevent. With ``buggy=False`` the
rule is enforced and the same seed sweep finds no violation. That two-sided result is the proof: the
violation is the toggled flaw, not an accident of the harness.

Run it:  ``python -m seedloop.demos.raft``
"""

from __future__ import annotations

import asyncio
import sys
from typing import cast

import seedloop
from seedloop import World

FOLLOWER, CANDIDATE, LEADER = "follower", "candidate", "leader"

# Election timeouts are drawn from world.rng (so the seed owns the race); the leader's heartbeat is
# faster than any election timeout, so a stable leader suppresses new elections.
_ELECTION_MIN, _ELECTION_MAX = 0.15, 0.30
_HEARTBEAT = 0.05


class RaftNode:
    """One node's election logic, sans-I/O against ``world.net``."""

    def __init__(self, world: World, addr: int, peers: list[int], *, buggy: bool) -> None:
        self._world = world
        self._ep = world.net.bind(addr)
        self.addr = addr
        self._peers = peers
        self._all = len(peers) + 1
        self._buggy = buggy
        self.term = 0
        self.role = FOLLOWER
        self._voted_for: int | None = None
        self._votes: set[int] = set()

    def _election_timeout(self) -> float:
        return self._world.rng.uniform(_ELECTION_MIN, _ELECTION_MAX)

    async def _broadcast(self, msg: object) -> None:
        for p in self._peers:
            await self._ep.send(p, msg)

    def _adopt_newer_term(self, term: int) -> None:
        # Only a strictly higher term resets the vote — a node votes at most once per term, so
        # stepping down within the same term must NOT clear who it already voted for.
        if term > self.term:
            self.term = term
            self.role = FOLLOWER
            self._voted_for = None
            self._votes = set()

    async def run(self) -> None:
        while True:
            timeout = _HEARTBEAT if self.role == LEADER else self._election_timeout()
            try:
                src, msg = await asyncio.wait_for(self._ep.recv(), timeout=timeout)
            except TimeoutError:
                if self.role == LEADER:
                    await self._broadcast(("heartbeat", self.term, self.addr))
                else:
                    await self._begin_election()
                continue
            await self._handle(src, msg)

    async def _begin_election(self) -> None:
        self.term += 1
        self.role = CANDIDATE
        self._voted_for = self.addr
        self._votes = {self.addr}  # vote for self
        await self._broadcast(("request_vote", self.term, self.addr))

    async def _handle(self, src: int, msg: object) -> None:
        fields = cast("tuple[object, ...]", msg)
        kind = fields[0]
        if kind == "request_vote":
            await self._on_request_vote(src, cast("int", fields[1]))
        elif kind == "vote":
            await self._on_vote(
                cast("int", fields[1]), cast("int", fields[2]), cast("bool", fields[3])
            )
        elif kind == "heartbeat":
            self._on_heartbeat(cast("int", fields[1]))

    async def _on_request_vote(self, src: int, term: int) -> None:
        self._adopt_newer_term(term)
        grant = False
        # The bug: the correct rule grants at most one vote per term (`_voted_for` guard); the buggy
        # path drops the guard, so a node can vote for two candidates in one term.
        if (
            term == self.term
            and self.role != LEADER
            and (self._buggy or self._voted_for in (None, src))
        ):
            grant = True
            self._voted_for = src
        await self._ep.send(src, ("vote", self.term, self.addr, grant))

    async def _on_vote(self, term: int, voter: int, granted: bool) -> None:
        if term == self.term and self.role == CANDIDATE and granted:
            self._votes.add(voter)  # distinct voters; a majority of them elects
            if len(self._votes) > self._all // 2:  # a majority elects
                self.role = LEADER
                await self._broadcast(("heartbeat", self.term, self.addr))

    def _on_heartbeat(self, term: int) -> None:
        self._adopt_newer_term(term)
        if term == self.term and self.role != FOLLOWER:
            self.role = FOLLOWER  # a leader exists this term; step down but keep our vote


def leaders_by_term(nodes: list[RaftNode]) -> dict[int, set[int]]:
    """Map each term to the set of nodes that currently believe they lead it."""
    out: dict[int, set[int]] = {}
    for node in nodes:
        if node.role == LEADER:
            out.setdefault(node.term, set()).add(node.addr)
    return out


def at_most_one_leader_per_term(nodes: list[RaftNode]) -> bool:
    """Raft's election-safety property: no term ever has two leaders."""
    return all(len(leaders) <= 1 for leaders in leaders_by_term(nodes).values())


def election_scenario(*, buggy: bool, nodes: int = 3, seconds: float = 3.0) -> seedloop.Scenario:
    """A scenario that runs a cluster and asserts election safety throughout."""

    async def scenario(world: World) -> None:
        addrs = list(range(nodes))
        cluster = [RaftNode(world, a, [p for p in addrs if p != a], buggy=buggy) for a in addrs]
        for node in cluster:
            world.start(node)
        world.always(
            lambda: at_most_one_leader_per_term(cluster), name="at-most-one-leader-per-term"
        )
        await asyncio.sleep(seconds)  # let elections run; the invariant is checked each step

    return scenario


def find_split_brain(seeds: int = 200) -> int | None:
    """Sweep the buggy election for a seed that violates election safety; None if none found."""
    result = seedloop.check(election_scenario(buggy=True), seeds=seeds, on_failure="return")
    return result.failing_seed


def main() -> None:
    print("seedloop Raft election demo - hunting for split-brain\n")
    seed = find_split_brain()
    if seed is None:
        print("no split-brain found in the swept seeds (try more seeds)")
        sys.exit(1)  # the proof did not reproduce — fail loudly (CI runs this)
    print(f"buggy election: split-brain found at seed={seed}")
    print(f"  reproduce it:  seedloop.replay(election_scenario(buggy=True), seed={seed})")
    try:
        seedloop.replay(election_scenario(buggy=True), seed=seed)
    except seedloop.InvariantError as exc:
        print(f"  replay reproduces it: {exc}")
    clean = seedloop.check(election_scenario(buggy=False), seeds=200, on_failure="return")
    verdict = (
        "no violation" if clean.failing_seed is None else f"FAILED at seed={clean.failing_seed}"
    )
    print(f"\ncorrect election (single-vote rule enforced): {verdict} over the same 200 seeds")
    print("-> the violation is the toggled flaw, not the harness.")


if __name__ == "__main__":
    main()
