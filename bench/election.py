"""How long a cluster is leaderless after its leader crashes, in the simulator.

Measured in ticks, the simulator's only unit of time. With the defaults the
election timeout is 10-20 ticks and the heartbeat 3, so the result reads as
multiples of a heartbeat interval. The process benchmark in `bench.failover`
measures the same thing in milliseconds over real sockets.

    python -m bench.election --seeds 2000
"""

from __future__ import annotations

import argparse
import statistics

from convoy.sim import Cluster, NetworkFaults


def measure(seed: int, size: int, faults: NetworkFaults) -> tuple[int, int]:
    """(ticks until a stable new leader, elections started) after killing the leader."""
    cluster = Cluster(size, seed, faults=faults, check=False)
    leader = cluster.wait_for_leader()
    cluster.run(30)
    term_before = leader.term
    cluster.crash(leader.id)
    ticks = cluster.run_until(lambda: (l := cluster._stable_leader()) is not None and l.id != leader.id, max_ticks=5000)
    new = cluster.leader()
    return ticks, new.term - term_before


def percentile(values: list[int], p: float) -> int:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(p / 100 * len(ordered)))]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seeds", type=int, default=2000)
    args = parser.parse_args()

    print("| cluster | network | p50 ticks | p99 ticks | max | mean terms to elect |")
    print("|---|---|---|---|---|---|")
    for size in (3, 5):
        for label, faults in (
            ("clean, 1-3 tick delay", NetworkFaults()),
            ("10% loss, 1-6 tick delay", NetworkFaults(drop=0.10, max_delay=6)),
        ):
            results = [measure(seed, size, faults) for seed in range(args.seeds)]
            ticks = [t for t, _ in results]
            terms = [e for _, e in results]
            print(
                f"| {size} | {label} | {percentile(ticks, 50)} | {percentile(ticks, 99)} | {max(ticks)} "
                f"| {statistics.mean(terms):.2f} |"
            )


if __name__ == "__main__":
    main()
