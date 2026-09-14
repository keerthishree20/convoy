from __future__ import annotations

import random

import pytest

from convoy import Entry, MemoryStorage, Node


def make_node(node_id: str = "n1", peers=("n1", "n2", "n3"), *, term: int = 0, log=(), voted_for=None, seed: int = 0) -> Node:
    """A single node with a hand-built past, for testing one rule at a time."""
    storage = MemoryStorage()
    storage.term = term
    storage.voted_for = voted_for
    storage.log = [Entry(t, c) for t, c in log]
    return Node(node_id, list(peers), storage, random.Random(seed))


def drain(node: Node) -> list:
    out = list(node.outbox)
    node.outbox.clear()
    return out


def elect(node: Node) -> None:
    """Make `node` leader by timing it out and handing it every vote."""
    from convoy import VoteReply

    while node.role.value != "candidate":
        node.tick()
    for peer in node.peers:
        node.step(VoteReply(node.term, peer, node.id, True))
    assert node.role.value == "leader"
    drain(node)


@pytest.fixture
def node_factory():
    return make_node
