"""Stable storage: what survives a crash, and what a torn write turns into."""

from __future__ import annotations

import random

from convoy import Entry, FileStorage, Node, Role

from .conftest import elect


def test_term_vote_and_log_survive_reopening(tmp_path):
    s = FileStorage(tmp_path)
    s.save_term_vote(7, "n2")
    s.append([Entry(1, {"op": "put", "key": "a", "value": "1"}), Entry(7, None)])
    s.close()

    s = FileStorage(tmp_path)
    assert (s.term, s.voted_for) == (7, "n2")
    assert s.log == [Entry(1, {"op": "put", "key": "a", "value": "1"}), Entry(7, None)]


def test_truncation_is_durable_and_appends_continue_after_it(tmp_path):
    s = FileStorage(tmp_path)
    s.append([Entry(1, i) for i in range(5)])
    s.truncate(3)
    s.append([Entry(2, "x")])
    s.close()
    assert FileStorage(tmp_path).log == [Entry(1, 0), Entry(1, 1), Entry(2, "x")]


def test_truncating_past_the_end_is_a_no_op(tmp_path):
    s = FileStorage(tmp_path)
    s.append([Entry(1, "a")])
    s.truncate(5)
    assert s.log == [Entry(1, "a")]


def test_a_torn_final_record_is_cut_away_on_open(tmp_path):
    s = FileStorage(tmp_path)
    s.append([Entry(1, "kept"), Entry(1, "also kept")])
    s.append([Entry(1, "torn")])
    s.close()
    log_file = tmp_path / "log"
    data = log_file.read_bytes()
    log_file.write_bytes(data[:-3])

    s = FileStorage(tmp_path)
    assert s.log == [Entry(1, "kept"), Entry(1, "also kept")]
    assert s.recovered_torn_bytes > 0
    s.append([Entry(2, "after")])
    s.close()
    assert FileStorage(tmp_path).log[-1] == Entry(2, "after")


def test_a_corrupted_record_ends_the_log_there(tmp_path):
    s = FileStorage(tmp_path)
    s.append([Entry(1, "a"), Entry(1, "b"), Entry(1, "c")])
    s.close()
    log_file = tmp_path / "log"
    data = bytearray(log_file.read_bytes())
    data[-5] ^= 0xFF
    log_file.write_bytes(bytes(data))
    assert FileStorage(tmp_path).log == [Entry(1, "a"), Entry(1, "b")]


def test_a_half_written_metadata_file_never_replaces_the_old_one(tmp_path):
    s = FileStorage(tmp_path)
    s.save_term_vote(3, "n1")
    (tmp_path / "meta.tmp").write_text('{"term": 9, "vot')  # a crash mid-write
    s.close()
    assert (FileStorage(tmp_path).term, FileStorage(tmp_path).voted_for) == (3, "n1")


def test_a_restarted_node_remembers_its_vote_and_will_not_vote_twice(tmp_path):
    from convoy import RequestVote

    node = Node("n1", ["n1", "n2", "n3"], FileStorage(tmp_path, fsync=False), random.Random(0))
    node.step(RequestVote(4, "n2", "n1", 0, 0))
    assert node.outbox[-1].granted
    node.storage.close()

    reborn = Node("n1", ["n1", "n2", "n3"], FileStorage(tmp_path, fsync=False), random.Random(1))
    assert reborn.term == 4 and reborn.role is Role.FOLLOWER
    reborn.step(RequestVote(4, "n3", "n1", 0, 0))
    assert reborn.outbox[-1].granted is False


def test_a_restarted_leader_comes_back_as_a_follower_with_its_log(tmp_path):
    node = Node("n1", ["n1", "n2", "n3"], FileStorage(tmp_path, fsync=False), random.Random(0))
    elect(node)
    node.propose({"op": "put", "key": "a", "value": "1"})
    term, log = node.term, list(node.log)
    node.storage.close()

    reborn = Node("n1", ["n1", "n2", "n3"], FileStorage(tmp_path, fsync=False), random.Random(0))
    assert reborn.role is Role.FOLLOWER
    assert reborn.term == term and reborn.log == log
    assert reborn.commit_index == 0  # volatile: relearned from the next leader
