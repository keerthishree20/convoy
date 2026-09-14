"""Leader election: who may win, who may vote, and when anyone starts an election."""

from __future__ import annotations

import random

import pytest

from convoy import AppendEntries, Cluster, MemoryStorage, Node, RequestVote, Role, VoteReply

from .conftest import drain, elect, make_node


def test_a_cluster_elects_exactly_one_leader():
    cluster = Cluster(5, seed=1)
    leader = cluster.wait_for_leader()
    assert [n.id for n in cluster.live_nodes() if n.role is Role.LEADER] == [leader.id]
    assert all(n.leader_id == leader.id for n in cluster.live_nodes())


def test_a_single_node_cluster_leads_itself_and_commits_alone():
    cluster = Cluster(1, seed=0)
    leader = cluster.wait_for_leader()
    _, index, _ = cluster.propose({"op": "put", "key": "a", "value": "1"})
    cluster.tick()
    assert leader.commit_index == index
    assert cluster.machines["n1"].kv.data == {"a": "1"}


@pytest.mark.parametrize("seed", range(20))
def test_a_new_leader_emerges_after_the_leader_crashes(seed):
    cluster = Cluster(5, seed=seed)
    first = cluster.wait_for_leader()
    cluster.crash(first.id)
    second = cluster.wait_for_leader(max_ticks=200)
    assert second.id != first.id
    assert second.term > first.term


def test_a_minority_partition_cannot_elect_anyone():
    cluster = Cluster(5, seed=3)
    leader = cluster.wait_for_leader()
    minority = [nid for nid in cluster.ids if nid != leader.id][:2]
    majority = [nid for nid in cluster.ids if nid not in minority]
    cluster.partition(minority, majority)
    cluster.run(300)
    for nid in minority:
        assert cluster.node(nid).role is not Role.LEADER
    assert cluster.node(leader.id).role is Role.LEADER


def test_an_isolated_old_leader_steps_down_when_it_hears_a_newer_term():
    cluster = Cluster(3, seed=4)
    old = cluster.wait_for_leader()
    others = [nid for nid in cluster.ids if nid != old.id]
    cluster.partition([old.id], others)
    new = cluster.wait_for_leader(max_ticks=300)
    assert new.id != old.id
    assert old.role is Role.LEADER  # it cannot know yet
    cluster.heal()
    cluster.run_until(lambda: old.role is Role.FOLLOWER, max_ticks=100)
    # The newer term arrives first, often in a reply; the name of the leader
    # that holds it arrives with that leader's next heartbeat.
    cluster.run_until(lambda: old.leader_id == new.id, max_ticks=100)


# ---- the voting rule, one node at a time -----------------------------------


def test_a_voter_grants_one_vote_per_term():
    voter = make_node("n1", term=1)
    voter.step(RequestVote(2, "n2", "n1", 0, 0))
    voter.step(RequestVote(2, "n3", "n1", 0, 0))
    replies = drain(voter)
    assert [r.granted for r in replies] == [True, False]
    assert voter.voted_for == "n2"


def test_a_repeated_request_from_the_same_candidate_is_granted_again():
    """A lost reply gets the request retried. Refusing would cost the candidate a vote it has."""
    voter = make_node("n1", term=1)
    voter.step(RequestVote(2, "n2", "n1", 0, 0))
    voter.step(RequestVote(2, "n2", "n1", 0, 0))
    assert [r.granted for r in drain(voter)] == [True, True]


def test_a_vote_is_refused_to_a_candidate_with_an_older_last_term_even_if_its_log_is_longer():
    voter = make_node("n1", term=3, log=[(1, "a"), (3, "b")])
    voter.step(RequestVote(4, "n2", "n1", last_log_index=5, last_log_term=2))
    assert drain(voter)[0].granted is False


def test_a_vote_is_granted_to_a_candidate_with_a_newer_last_term_even_if_its_log_is_shorter():
    voter = make_node("n1", term=3, log=[(1, "a"), (1, "b"), (1, "c")])
    voter.step(RequestVote(4, "n2", "n1", last_log_index=1, last_log_term=2))
    assert drain(voter)[0].granted is True


def test_with_equal_last_terms_the_longer_log_wins():
    voter = make_node("n1", term=2, log=[(2, "a"), (2, "b")])
    voter.step(RequestVote(3, "n2", "n1", last_log_index=1, last_log_term=2))
    assert drain(voter)[0].granted is False
    voter.step(RequestVote(3, "n3", "n1", last_log_index=2, last_log_term=2))
    assert drain(voter)[0].granted is True


def test_a_request_from_an_older_term_is_refused_and_told_the_current_term():
    voter = make_node("n1", term=5)
    voter.step(RequestVote(4, "n2", "n1", 0, 0))
    (reply,) = drain(voter)
    assert reply.granted is False and reply.term == 5


def test_a_refused_vote_does_not_reset_the_election_timer():
    """Otherwise a candidate that can never win keeps everyone else from standing."""
    voter = make_node("n1", term=3, log=[(3, "x")])
    ticks_to_timeout = voter._timeout
    for _ in range(ticks_to_timeout - 1):
        voter.tick()
        voter.step(RequestVote(voter.term, "n2", "n1", 0, 0))
    voter.tick()
    assert voter.role is Role.CANDIDATE


def test_a_granted_vote_resets_the_election_timer():
    voter = make_node("n1", term=1)
    for _ in range(voter._timeout - 1):
        voter.tick()
    voter.step(RequestVote(2, "n2", "n1", 0, 0))
    voter.tick()
    assert voter.role is Role.FOLLOWER


def test_votes_from_an_earlier_term_do_not_count():
    candidate = make_node("n1", peers=("n1", "n2", "n3", "n4", "n5"))
    while candidate.role is not Role.CANDIDATE:
        candidate.tick()
    first_term = candidate.term
    for _ in range(25):
        candidate.tick()  # time out again, into a new term
    assert candidate.term > first_term
    candidate.step(VoteReply(first_term, "n2", "n1", True))
    candidate.step(VoteReply(first_term, "n3", "n1", True))
    assert candidate.role is Role.CANDIDATE


def test_a_candidate_that_hears_from_the_leader_of_its_term_stands_down():
    candidate = make_node("n1")
    while candidate.role is not Role.CANDIDATE:
        candidate.tick()
    candidate.step(AppendEntries(candidate.term, "n2", "n1", 0, 0, (), 0))
    assert candidate.role is Role.FOLLOWER and candidate.leader_id == "n2"


def test_a_new_leader_appends_a_no_op_in_its_own_term():
    node = make_node("n1", log=[(1, "old")])
    elect(node)
    assert node.log[-1].term == node.term and node.log[-1].command is None


def test_heartbeats_must_be_faster_than_elections():
    with pytest.raises(ValueError):
        Node("n1", ["n2"], MemoryStorage(), random.Random(0), election_ticks=(3, 6), heartbeat_ticks=3)
