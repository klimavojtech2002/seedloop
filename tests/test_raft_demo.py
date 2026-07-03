"""The Raft election demo: a split-brain bug found and replayed; the fix passes the sweep."""

from __future__ import annotations

import contextlib

import seedloop
from seedloop._entropy import csprng_shim, substream
from seedloop._world import World
from seedloop.demos.raft import (
    RaftNode,
    at_most_one_leader_per_term,
    election_scenario,
    find_split_brain,
    leaders_by_term,
)

_SEEDS = 200  # the sweep budget the demo uses


def _capture_timeline(scenario: seedloop.Scenario, seed: int) -> tuple[object, ...]:
    # Capture a run's timeline even when it fails with an InvariantError, to check replay.
    world = World(seed)
    with csprng_shim(substream(seed, "csprng")), contextlib.suppress(seedloop.InvariantError):
        world._drive(scenario(world))
    return world.timeline


def test_buggy_election_has_split_brain_found_by_the_sweep() -> None:
    result = seedloop.check(election_scenario(buggy=True), seeds=_SEEDS, on_failure="return")
    assert result.failing_seed is not None
    assert isinstance(result.error, seedloop.InvariantError)
    assert result.error.name == "at-most-one-leader-per-term"


def test_failing_seed_replays_identically() -> None:
    seed = find_split_brain(seeds=_SEEDS)
    assert seed is not None
    first = _capture_timeline(election_scenario(buggy=True), seed)
    for _ in range(10):
        assert _capture_timeline(election_scenario(buggy=True), seed) == first


def test_correct_election_has_no_split_brain_over_the_same_range() -> None:
    # The control: with the single-vote rule enforced, the same sweep finds no violation — so the
    # buggy run's split-brain is the toggled flaw, not an artefact of the harness.
    result = seedloop.check(election_scenario(buggy=False), seeds=_SEEDS, on_failure="return")
    assert result.failing_seed is None


def _node(addr: int, role: str, term: int) -> RaftNode:
    world = World(0)
    node = RaftNode(world, addr, [], buggy=False)
    node.role = role
    node.term = term
    return node


def test_invariant_helpers() -> None:
    from seedloop.demos.raft import CANDIDATE, FOLLOWER, LEADER

    assert at_most_one_leader_per_term([_node(0, FOLLOWER, 1), _node(1, CANDIDATE, 1)])  # no leader
    assert at_most_one_leader_per_term([_node(0, LEADER, 1), _node(1, FOLLOWER, 1)])  # one leader
    assert at_most_one_leader_per_term([_node(0, LEADER, 1), _node(1, LEADER, 2)])  # diff terms ok
    two_in_one = [_node(0, LEADER, 5), _node(1, LEADER, 5)]
    assert not at_most_one_leader_per_term(two_in_one)  # split-brain in one term
    assert leaders_by_term(two_in_one) == {5: {0, 1}}


def test_majority_threshold_is_strict() -> None:
    # In a five-node cluster a candidate needs three votes (a majority), not two — guards the
    # majority rule independently of the three-node demo (where 1 vs 2 happens to coincide).
    from seedloop.demos.raft import CANDIDATE, LEADER

    roles: list[str] = []

    async def scenario(world: World) -> None:
        node = RaftNode(world, 0, [1, 2, 3, 4], buggy=False)
        node.role = CANDIDATE
        node.term = 1
        node._votes = {0}  # self-vote
        await node._on_vote(1, 1, granted=True)  # two votes
        roles.append(node.role)
        await node._on_vote(1, 2, granted=True)  # three votes — a majority of five
        roles.append(node.role)

    seedloop.replay(scenario, seed=0)
    assert roles == [CANDIDATE, LEADER]  # leader only once a majority is reached


def test_election_timeouts_are_seeded() -> None:
    # The race is a function of the seed: two runs draw identical election timeouts.
    def draws(seed: int) -> list[float]:
        world = World(seed)
        node = RaftNode(world, 0, [1, 2], buggy=False)
        return [node._election_timeout() for _ in range(5)]

    assert draws(7) == draws(7)
    assert draws(7) != draws(8)


def test_election_advances_the_term_each_round() -> None:
    # A candidate strictly increases its term on each election. The election-safety invariant is
    # direction-agnostic, so on its own it leaves the term-increment direction unpinned.
    async def scenario(world: World) -> None:
        node = RaftNode(world, 0, [1, 2], buggy=False)
        terms = [node.term]
        await node._begin_election()
        terms.append(node.term)
        await node._begin_election()
        terms.append(node.term)
        assert terms == [0, 1, 2]  # strictly advancing from 0

    seedloop.replay(scenario, seed=1)
