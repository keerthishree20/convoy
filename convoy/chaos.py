"""Seeded chaos scenarios: random faults, safety checked throughout, liveness checked at the end.

One scenario, from a single seed:

1. **Turbulence.** For a fixed number of ticks, clients send writes while nodes
   crash and restart, the network partitions and heals, and messages are
   dropped, duplicated and delayed. The safety checker runs after every step.
2. **Calm.** Every fault is lifted. The cluster must elect a leader and commit
   a fresh write within a bounded number of ticks, or the scenario fails on
   liveness.
3. **Retries.** Clients resend every write they never saw committed, with the
   same sequence number. This is exactly the ambiguous case a real client
   faces, and the state machine must not apply any write twice.
4. **Audit.** Every replica's key-value data must be identical, and every write
   must appear exactly once.

    python -m convoy.chaos --seeds 2000
    python -m convoy.chaos --replay 1234
"""

from __future__ import annotations

import argparse
import random
import sys
import time
from dataclasses import dataclass

from .sim import Cluster, NetworkFaults


@dataclass
class ScenarioResult:
    seed: int
    ticks: int
    writes: int
    committed_during_chaos: int
    leaders: int
    recovery_ticks: int
    messages: int


class ScenarioFailure(AssertionError):
    pass


def run_scenario(seed: int, *, size: int = 5, ticks: int = 1500, keys: int = 4) -> ScenarioResult:
    plan = random.Random(f"plan:{seed}")
    faults = NetworkFaults(
        drop=plan.uniform(0.0, 0.15),
        duplicate=plan.uniform(0.0, 0.1),
        min_delay=1,
        max_delay=plan.randint(2, 8),
    )
    # Small batches keep lagging followers lagging, so a new leader's catch-up
    # traffic spans several messages. Several classic bugs only show when a
    # leader dies between two of them.
    cluster = Cluster(size, seed, faults=faults, max_batch=plan.choice([1, 2, 4, 16, 64]))
    write_rate = plan.uniform(0.1, 0.5)
    fault_rate = plan.uniform(0.005, 0.05)

    writes: dict[int, dict] = {}  # seq -> command

    def send(seq: int) -> None:
        cluster.propose(writes[seq])

    try:
        for _ in range(ticks):
            if plan.random() < write_rate:
                seq = len(writes) + 1
                key = f"k{plan.randrange(keys)}"
                # One client per write. The session table remembers only each
                # client's newest sequence number, which is correct for a real
                # client that waits for one request before sending the next,
                # and wrong for one client firing hundreds concurrently.
                writes[seq] = {"op": "append", "key": key, "value": f"{seq};", "client": f"c{seq}", "seq": 1}
                send(seq)
            if plan.random() < fault_rate:
                _inject_fault(cluster, plan)
            cluster.tick()

        committed_during_chaos = max((n.commit_index for n in cluster.live_nodes()), default=0)

        # Calm.
        cluster.faults = NetworkFaults()
        cluster.heal()
        for nid in cluster.ids:
            cluster.restart(nid)
        start = cluster.now
        cluster.wait_for_leader(max_ticks=2000)
        recovery_ticks = cluster.now - start

        # Retries, the way a real client behaves: resend anything the current
        # leader does not hold, and keep doing so through leader changes. Even
        # with the network calm a restarted node can time out before hearing
        # the new leader and replace it, discarding its unreplicated tail.
        writes[len(writes) + 1] = {"op": "put", "key": "marker", "value": str(seed), "client": "marker", "seq": 1}
        deadline = cluster.now + 4000
        while True:
            leader = cluster.wait_for_leader(max_ticks=max(1, deadline - cluster.now))
            held = {e.command["client"] for e in leader.log if e.command is not None}
            committed = {e.command["client"] for e in leader.log[: leader.commit_index] if e.command is not None}
            if len(committed) == len(writes) and all(
                n.last_applied == leader.commit_index for n in cluster.live_nodes()
            ):
                break
            for seq, command in writes.items():
                if command["client"] not in held:
                    send(seq)
            cluster.run(20)
            if cluster.now > deadline:
                raise TimeoutError(f"{len(writes) - len(committed)} writes still uncommitted 4000 ticks after faults stopped")

        _audit(cluster, writes, keys)
    except (AssertionError, TimeoutError) as exc:
        raise ScenarioFailure(f"seed {seed}: {exc}\n{cluster.dump()}") from exc

    return ScenarioResult(
        seed=seed,
        ticks=cluster.now,
        writes=len(writes),
        committed_during_chaos=committed_during_chaos,
        leaders=len(cluster.checker.leaders),
        recovery_ticks=recovery_ticks,
        messages=cluster.stats["sent"],
    )


def _inject_fault(cluster: Cluster, plan: random.Random) -> None:
    up = [nid for nid in cluster.ids if cluster.is_up(nid)]
    down = [nid for nid in cluster.ids if not cluster.is_up(nid)]
    choice = plan.random()
    leader = cluster.leader()
    if choice < 0.15 and leader is not None:
        # Leaders are where the interesting state is: half-replicated entries
        # and followers mid-catch-up. A uniform choice would rarely pick one.
        cluster.crash(leader.id)
    elif choice < 0.3 and up:
        cluster.crash(plan.choice(up))
    elif choice < 0.55 and down:
        cluster.restart(plan.choice(down))
    elif choice < 0.8:
        shuffled = cluster.ids[:]
        plan.shuffle(shuffled)
        cut = plan.randint(1, len(shuffled) - 1)
        cluster.partition(shuffled[:cut], shuffled[cut:])
    else:
        cluster.heal()


def _audit(cluster: Cluster, writes: dict[int, dict], keys: int) -> None:
    machines = [cluster.machines[nid] for nid in cluster.ids]
    reference = machines[0].kv.data
    for machine in machines[1:]:
        if machine.kv.data != reference:
            raise AssertionError(f"replicas diverged: {machine.node.id} data differs from {cluster.ids[0]}")

    seen: dict[int, int] = {len(writes): 1} if reference.get("marker") is not None else {}
    for key in (f"k{i}" for i in range(keys)):
        for part in filter(None, reference.get(key, "").split(";")):
            seq = int(part)
            seen[seq] = seen.get(seq, 0) + 1
            if writes[seq]["key"] != key:
                raise AssertionError(f"write {seq} landed under {key}, sent to {writes[seq]['key']}")
    doubled = sorted(s for s, count in seen.items() if count > 1)
    if doubled:
        raise AssertionError(f"writes applied more than once: {doubled[:10]}")
    missing = sorted(set(writes) - set(seen))
    if missing:
        raise AssertionError(f"writes lost after every client retried: {missing[:10]}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--seeds", type=int, default=500, help="number of scenarios to run")
    parser.add_argument("--start", type=int, default=0, help="first seed")
    parser.add_argument("--replay", type=int, help="run one seed and print its event trace")
    parser.add_argument("--size", type=int, default=5)
    parser.add_argument("--ticks", type=int, default=1500)
    args = parser.parse_args(argv)

    if args.replay is not None:
        try:
            result = run_scenario(args.replay, size=args.size, ticks=args.ticks)
        except ScenarioFailure as exc:
            print(exc)
            return 1
        print(result)
        return 0

    began = time.perf_counter()
    totals = {"writes": 0, "leaders": 0, "messages": 0}
    worst_recovery = 0
    for seed in range(args.start, args.start + args.seeds):
        try:
            result = run_scenario(seed, size=args.size, ticks=args.ticks)
        except ScenarioFailure as exc:
            print(exc)
            print(f"\nreplay with: python -m convoy.chaos --replay {seed} --size {args.size} --ticks {args.ticks}")
            return 1
        totals["writes"] += result.writes
        totals["leaders"] += result.leaders
        totals["messages"] += result.messages
        worst_recovery = max(worst_recovery, result.recovery_ticks)
        if (seed - args.start + 1) % 100 == 0:
            print(f"  {seed - args.start + 1} scenarios passed", flush=True)

    elapsed = time.perf_counter() - began
    print(
        f"{args.seeds} scenarios passed in {elapsed:.1f}s: {totals['writes']} writes, "
        f"{totals['leaders']} elections won, {totals['messages']} messages, "
        f"slowest recovery {worst_recovery} ticks"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
