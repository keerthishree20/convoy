"""Real processes on this machine: write throughput, and the write outage when the leader dies.

Throughput is acknowledged writes per second from N concurrent clients, each
sending one write at a time. Every acknowledgement means the entry is fsynced
on a majority and applied on the leader.

Failover kills the leader with SIGKILL while clients write, and reports the
longest gap between two acknowledgements around each kill. That gap is what a
client actually experiences: failure detection (the election timeout), the
election, and clients finding the new leader.

Everything runs on one machine over loopback. There is no network latency
between nodes, and all nodes share one disk, so fsyncs from different nodes
contend with each other. Treat the numbers as this laptop, not as a server.

    python -m bench.failover --throughput --failover
"""

from __future__ import annotations

import argparse
import statistics
import tempfile
import threading
import time

from convoy.client import ConvoyClient
from convoy.local import LocalCluster


def run_clients(cluster: LocalCluster, clients: int, seconds: float, on_ack=None) -> int:
    stop = threading.Event()
    counts = [0] * clients

    def work(i: int) -> None:
        client = ConvoyClient(cluster.http_addresses, give_up_after=30)
        n = 0
        while not stop.is_set():
            client.put(f"c{i}-{n % 100}", "x" * 100)
            n += 1
            counts[i] += 1
            if on_ack:
                on_ack(time.monotonic())
        client.close()

    threads = [threading.Thread(target=work, args=(i,)) for i in range(clients)]
    for t in threads:
        t.start()
    time.sleep(seconds)
    stop.set()
    for t in threads:
        t.join()
    return sum(counts)


def throughput(fsync: bool, seconds: float) -> None:
    for size in (3, 5):
        for clients in (1, 8, 32):
            with tempfile.TemporaryDirectory() as tmp, LocalCluster(size, tmp, fsync=fsync) as cluster:
                cluster.wait_for_leader()
                run_clients(cluster, clients, 1.0)  # warm up connections
                done = run_clients(cluster, clients, seconds)
                print(f"| {size} | {'on' if fsync else 'off'} | {clients} | {done / seconds:,.0f} |", flush=True)


def failover(kills: int, clients: int) -> None:
    gaps = []
    with tempfile.TemporaryDirectory() as tmp, LocalCluster(3, tmp) as cluster:
        cluster.wait_for_leader()
        acks: list[float] = []
        lock = threading.Lock()

        def record(t: float) -> None:
            with lock:
                acks.append(t)

        kill_times = []

        def killer() -> None:
            time.sleep(2.0)
            for _ in range(kills):
                leader = cluster.wait_for_leader()
                kill_times.append(time.monotonic())
                cluster.kill(leader)
                cluster.wait_for_leader(exclude={leader})
                time.sleep(1.0)
                cluster.start(leader)
                time.sleep(2.0)

        k = threading.Thread(target=killer)
        k.start()
        run_clients(cluster, clients, 2.0 + kills * 3.5 + 1.0, on_ack=record)
        k.join()

    acks.sort()
    for killed_at in kill_times:
        before = max((a for a in acks if a <= killed_at), default=killed_at)
        after = min((a for a in acks if a > killed_at), default=None)
        if after is not None:
            gaps.append((after - before) * 1000)
    print(f"{kills} leader kills, {clients} clients, 10 ms ticks, election timeout 150-300 ms, heartbeat 50 ms")
    print("| p50 outage | mean | max |")
    print("|---|---|---|")
    print(f"| {statistics.median(gaps):.0f} ms | {statistics.mean(gaps):.0f} ms | {max(gaps):.0f} ms |")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--throughput", action="store_true")
    parser.add_argument("--failover", action="store_true")
    parser.add_argument("--seconds", type=float, default=5.0)
    parser.add_argument("--kills", type=int, default=20)
    args = parser.parse_args()

    if args.throughput:
        print("| nodes | fsync | clients | writes/s |")
        print("|---|---|---|---|")
        throughput(True, args.seconds)
        throughput(False, args.seconds)
    if args.failover:
        failover(args.kills, clients=8)


if __name__ == "__main__":
    main()
