"""Log replication and commitment, including the bugs nearly every Raft has once."""

from __future__ import annotations

import pytest

from convoy import AppendEntries, AppendReply, Cluster, Entry, NotLeader, Role

from .conftest import drain, elect, make_node


def put(i: int) -> dict:
    return {"op": "put", "key": f"k{i}", "value": str(i)}


def test_writes_reach_every_replica_in_order():
    cluster = Cluster(5, seed=1)
    cluster.wait_for_leader()
    for i in range(30):
        cluster.propose(put(i))
    cluster.run(40)
    logs = [n.log for n in cluster.live_nodes()]
    assert all(log == logs[0] for log in logs)
    for machine in cluster.machines.values():
        assert machine.kv.data == {f"k{i}": str(i) for i in range(30)}


def test_only_the_leader_accepts_proposals_and_says_who_leads():
    cluster = Cluster(3, seed=2)
    leader = cluster.wait_for_leader()
    follower = next(n for n in cluster.live_nodes() if n is not leader)
    with pytest.raises(NotLeader) as info:
        follower.propose(put(1))
    assert info.value.leader_id == leader.id


def test_a_leader_cut_off_from_the_majority_commits_nothing():
    cluster = Cluster(5, seed=3)
    leader = cluster.wait_for_leader()
    cluster.run(10)
    before = leader.commit_index
    others = [nid for nid in cluster.ids if nid != leader.id]
    cluster.partition([leader.id, others[0]], others[1:])
    for i in range(10):
        leader.propose(put(i))
    cluster.run(200)
    assert leader.commit_index == before


def test_writes_to_a_deposed_leader_are_replaced_by_the_new_leaders_log():
    cluster = Cluster(5, seed=4)
    old = cluster.wait_for_leader()
    cluster.run(10)
    others = [nid for nid in cluster.ids if nid != old.id]
    cluster.partition([old.id], others)
    for i in range(5):
        old.propose({"op": "put", "key": "lost", "value": str(i)})
    new = cluster.wait_for_leader(max_ticks=300)
    cluster.propose({"op": "put", "key": "kept", "value": "yes"})
    cluster.run(20)
    cluster.heal()
    cluster.run(100)
    assert old.log == new.log
    for machine in cluster.machines.values():
        assert "lost" not in machine.kv.data and machine.kv.data["kept"] == "yes"


def test_a_follower_that_was_down_catches_up_on_everything_it_missed():
    cluster = Cluster(3, seed=5, max_batch=4)
    leader = cluster.wait_for_leader()
    straggler = next(nid for nid in cluster.ids if nid != leader.id)
    cluster.crash(straggler)
    for i in range(100):
        cluster.propose(put(i))
        cluster.tick()
    cluster.restart(straggler)
    cluster.run_until(lambda: cluster.node(straggler).last_applied == leader.commit_index, max_ticks=500)
    assert cluster.machines[straggler].kv.data == cluster.machines[leader.id].kv.data


# ---- the follower's side, one message at a time -----------------------------


def test_a_gap_in_the_log_is_refused_with_a_hint_to_retry_from_the_end():
    follower = make_node("n2", term=1, log=[(1, "a")])
    follower.step(AppendEntries(1, "n1", "n2", prev_index=5, prev_term=1, entries=(), commit=0))
    (reply,) = drain(follower)
    assert reply.success is False and reply.match_index == 2


def test_a_conflicting_term_hint_skips_the_whole_term():
    follower = make_node("n2", term=3, log=[(1, "a"), (2, "b"), (2, "c"), (2, "d")])
    follower.step(AppendEntries(3, "n1", "n2", prev_index=4, prev_term=3, entries=(), commit=0))
    (reply,) = drain(follower)
    assert reply.success is False and reply.match_index == 2


def test_a_conflicting_tail_is_replaced():
    follower = make_node("n2", term=2, log=[(1, "a"), (1, "stale"), (1, "stale")])
    follower.step(AppendEntries(3, "n1", "n2", 1, 1, (Entry(3, "new"),), commit=0))
    assert follower.log == [Entry(1, "a"), Entry(3, "new")]
    assert drain(follower)[0].match_index == 2


def test_a_delayed_duplicate_does_not_truncate_entries_that_arrived_after_it():
    """The bug that silently loses committed writes.

    The leader sends entries 1-2, then 1-4. The second arrives first. When the
    stale first one lands, the follower already holds 1-2 with matching terms
    and must keep 3-4, which the leader may already have counted as committed.
    """
    follower = make_node("n2", term=1)
    newer = AppendEntries(1, "n1", "n2", 0, 0, tuple(Entry(1, i) for i in range(4)), commit=0)
    older = AppendEntries(1, "n1", "n2", 0, 0, tuple(Entry(1, i) for i in range(2)), commit=0)
    follower.step(newer)
    follower.step(older)
    assert follower.last_index == 4
    replies = drain(follower)
    assert [r.match_index for r in replies] == [4, 2]


def test_commit_index_stops_at_the_last_entry_the_message_vouched_for():
    """Past that point the follower's log may be a stale tail from an old term."""
    follower = make_node("n2", term=1, log=[(1, "a"), (1, "stale"), (1, "stale")])
    follower.step(AppendEntries(2, "n1", "n2", 1, 1, (), commit=3))
    assert follower.commit_index == 1


def test_commit_index_never_moves_backwards():
    follower = make_node("n2", term=1, log=[(1, "a"), (1, "b")])
    follower.step(AppendEntries(1, "n1", "n2", 2, 1, (), commit=2))
    follower.step(AppendEntries(1, "n1", "n2", 1, 1, (), commit=1))
    assert follower.commit_index == 2


def test_a_message_from_an_old_leader_is_refused_without_touching_the_log():
    follower = make_node("n2", term=5, log=[(5, "a")])
    follower.step(AppendEntries(4, "n1", "n2", 0, 0, (Entry(4, "old"),), commit=1))
    assert follower.log == [Entry(5, "a")] and follower.commit_index == 0
    assert drain(follower)[0].success is False


# ---- the leader's side ------------------------------------------------------


def test_figure_8_a_leader_does_not_commit_an_old_term_entry_by_counting_replicas():
    """Figure 8 of the Raft paper, from the point of view of the leader in term 4.

    Index 2 holds an entry from term 2 that this leader has now copied to a
    majority. It is still not safe to call it committed: a node whose last entry
    is from term 3 can win an election and overwrite it. Only once an entry from
    the leader's own term reaches a majority does everything before it commit.
    """
    leader = make_node("s1", peers=("s1", "s2", "s3", "s4", "s5"), term=3, log=[(1, "x"), (2, "y")])
    elect(leader)  # term 4, appends a no-op at index 3
    assert leader.term == 4

    for peer in ("s2", "s3"):
        leader.step(AppendReply(4, peer, "s1", True, 2))
    assert leader.commit_index == 0, "index 2 is on a majority but is not from term 4"

    for peer in ("s2", "s3"):
        leader.step(AppendReply(4, peer, "s1", True, 3))
    assert leader.commit_index == 3


def test_stale_replies_do_not_move_progress_backwards():
    leader = make_node("n1", term=1, log=[(1, "a")] * 5)
    elect(leader)
    leader.step(AppendReply(leader.term, "n2", "n1", True, 6))
    leader.step(AppendReply(leader.term, "n2", "n1", True, 3))
    assert leader.match_index["n2"] == 6 and leader.next_index["n2"] == 7


def test_a_rejection_backs_off_to_the_hint_but_never_below_what_is_known_to_match():
    leader = make_node("n1", term=1, log=[(1, "a")] * 10)
    elect(leader)
    leader.step(AppendReply(leader.term, "n2", "n1", True, 4))
    leader.next_index["n2"] = 9  # as if later sends had raced ahead
    drain(leader)
    leader.step(AppendReply(leader.term, "n2", "n1", False, 1))
    assert leader.next_index["n2"] == 5
    (retry,) = drain(leader)
    assert retry.prev_index == 4


def test_a_leader_that_sees_a_newer_term_in_a_reply_steps_down():
    leader = make_node("n1", term=1)
    elect(leader)
    leader.step(AppendReply(leader.term + 3, "n2", "n1", False, 0))
    assert leader.role is Role.FOLLOWER and leader.term == 5 and leader.voted_for is None
