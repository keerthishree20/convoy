"""Real processes, real sockets, real SIGKILL.

The simulator proves the algorithm. These prove the runtime around it: that a
node killed without warning comes back with its log, that clients find the new
leader on their own, and that no write a client was told succeeded is ever
lost or applied twice.
"""

from __future__ import annotations

import threading
import time

import pytest

from convoy.client import ConvoyClient, Unavailable
from convoy.local import LocalCluster


@pytest.fixture
def cluster(tmp_path):
    with LocalCluster(3, tmp_path / "data") as c:
        c.wait_for_leader()
        yield c


def replica_values(cluster: LocalCluster, keys: list[str], timeout: float = 15.0) -> dict[str, list]:
    """Each node's own applied values for `keys`, once every node has applied the same index.

    Read with the unreplicated /local endpoint, so each replica's state machine
    is inspected directly rather than through the leader.
    """
    client = ConvoyClient([], timeout=1)
    deadline = time.monotonic() + timeout
    try:
        while True:
            values = {}
            for nid, peer in cluster.peers.items():
                answers = [client._request(peer.http_address, "GET", f"/local/{key}", None)[1] for key in keys]
                values[nid] = answers
            applied = {a.get("applied") for answers in values.values() for a in answers}
            if len(applied) == 1 and None not in applied:
                return {nid: [a["value"] for a in answers] for nid, answers in values.items()}
            if time.monotonic() > deadline:
                raise TimeoutError(f"replicas never converged: {values}")
            time.sleep(0.1)
    finally:
        client.close()


def test_a_write_is_readable_through_any_node(cluster):
    client = ConvoyClient(cluster.http_addresses)
    client.put("greeting", "hello")
    for address in reversed(cluster.http_addresses):
        assert ConvoyClient([address] + cluster.http_addresses).get("greeting") == "hello"


def test_no_acknowledged_write_is_lost_or_doubled_when_the_leader_is_killed(cluster):
    acknowledged: list[str] = []
    lock = threading.Lock()
    stop = threading.Event()
    errors: list[BaseException] = []

    def writer(worker: int) -> None:
        client = ConvoyClient(cluster.http_addresses, give_up_after=20)
        n = 0
        try:
            while not stop.is_set():
                token = f"{worker}.{n};"
                client.append(f"log{worker % 2}", token)
                with lock:
                    acknowledged.append(token)
                n += 1
        except BaseException as exc:  # surfaced below
            errors.append(exc)
        finally:
            client.close()

    threads = [threading.Thread(target=writer, args=(w,)) for w in range(6)]
    for t in threads:
        t.start()

    for _ in range(2):
        time.sleep(1.0)
        leader = cluster.wait_for_leader()
        cluster.kill(leader)
        cluster.wait_for_leader(exclude={leader})
        time.sleep(0.5)
        cluster.start(leader)

    time.sleep(1.0)
    stop.set()
    for t in threads:
        t.join(timeout=30)
    assert not errors, errors
    assert len(acknowledged) > 50

    cluster.wait_for_leader()
    stored = replica_values(cluster, ["log0", "log1"])
    reference = next(iter(stored.values()))
    assert all(values == reference for values in stored.values()), "replicas diverged"
    tokens = [t + ";" for t in "".join(v or "" for v in reference).split(";") if t]
    assert len(tokens) == len(set(tokens)), "a write was applied twice"
    assert set(acknowledged) <= set(tokens), "an acknowledged write is missing"


def test_the_whole_cluster_restarts_with_its_data(cluster):
    client = ConvoyClient(cluster.http_addresses)
    for i in range(20):
        client.put(f"k{i}", str(i))
    client.close()
    cluster.stop()
    cluster.start()
    cluster.wait_for_leader()
    client = ConvoyClient(cluster.http_addresses)
    assert [client.get(f"k{i}") for i in range(20)] == [str(i) for i in range(20)]


def test_writes_continue_with_one_node_down_and_stop_with_two(cluster):
    client = ConvoyClient(cluster.http_addresses, give_up_after=3)
    leader = cluster.wait_for_leader()
    follower = next(nid for nid in cluster.peers if nid != leader)
    cluster.kill(follower)
    client.put("one-down", "fine")

    cluster.kill(leader)
    with pytest.raises(Unavailable):
        client.put("two-down", "impossible")

    cluster.start(follower)
    cluster.start(leader)
    cluster.wait_for_leader()
    patient = ConvoyClient(cluster.http_addresses, give_up_after=15)
    assert patient.get("one-down") == "fine"
