"""A whole cluster in one process, on a network that misbehaves on purpose.

Time is a tick counter. Messages are delivered after a random delay, so they
arrive out of order. Some are dropped, some are delivered twice, partitions cut
the cluster into groups that cannot hear each other, and nodes crash and come
back with only what their storage kept.

Every random choice comes from one seeded generator, and every node's timeouts
from a generator derived from the same seed. The same seed therefore produces
the same run, message for message, which is what turns "the chaos test failed
once" into a bug that can be stepped through.
"""

from __future__ import annotations

import heapq
import random
from collections import deque
from dataclasses import dataclass, field

from .checker import SafetyChecker
from .kv import KVStateMachine
from .messages import Message
from .node import Node, NotLeader, Role
from .storage import MemoryStorage, Storage


@dataclass
class NetworkFaults:
    drop: float = 0.0
    duplicate: float = 0.0
    min_delay: int = 1
    max_delay: int = 3


@dataclass
class Machine:
    """A node's hardware: its disk outlives crashes, the process on it does not."""

    storage: Storage
    node: Node | None = None
    kv: KVStateMachine = field(default_factory=KVStateMachine)
    incarnation: int = 0
    applied: list = field(default_factory=list)  # (index, entry) in apply order, this incarnation


class Cluster:
    def __init__(
        self,
        size: int = 3,
        seed: int = 0,
        *,
        faults: NetworkFaults | None = None,
        election_ticks: tuple[int, int] = (10, 20),
        heartbeat_ticks: int = 3,
        max_batch: int = 64,
        check: bool = True,
        trace: int = 200,
    ) -> None:
        self.seed = seed
        self.rng = random.Random(seed)
        self.faults = faults or NetworkFaults()
        self.election_ticks = election_ticks
        self.heartbeat_ticks = heartbeat_ticks
        self.max_batch = max_batch
        self.ids = [f"n{i}" for i in range(1, size + 1)]
        self.machines = {nid: Machine(MemoryStorage()) for nid in self.ids}
        self.now = 0
        self._queue: list[tuple[int, int, Message]] = []
        self._seq = 0
        self._groups: dict[str, int] | None = None
        self.checker = SafetyChecker() if check else None
        self.trace: deque[str] = deque(maxlen=trace)
        self.stats = {"sent": 0, "delivered": 0, "dropped": 0, "duplicated": 0}
        for nid in self.ids:
            self._boot(nid)

    # ---- nodes ------------------------------------------------------------

    def _boot(self, nid: str) -> None:
        machine = self.machines[nid]
        machine.incarnation += 1
        node_rng = random.Random(f"{self.seed}:{nid}:{machine.incarnation}")
        machine.node = Node(
            nid,
            self.ids,
            machine.storage,
            node_rng,
            election_ticks=self.election_ticks,
            heartbeat_ticks=self.heartbeat_ticks,
            max_batch=self.max_batch,
        )
        machine.kv = KVStateMachine()
        machine.applied = []

    def node(self, nid: str) -> Node:
        node = self.machines[nid].node
        if node is None:
            raise KeyError(f"{nid} is down")
        return node

    def live_nodes(self) -> list[Node]:
        return [m.node for m in self.machines.values() if m.node is not None]

    def crash(self, nid: str) -> None:
        machine = self.machines[nid]
        if machine.node is None:
            return
        self._log(f"crash {nid}")
        if self.checker:
            self.checker.forget(machine.node)
        machine.node = None

    def restart(self, nid: str) -> None:
        if self.machines[nid].node is not None:
            return
        self._log(f"restart {nid}")
        self._boot(nid)

    def is_up(self, nid: str) -> bool:
        return self.machines[nid].node is not None

    def leader(self) -> Node | None:
        """The live leader with the highest term, if any. Stale leaders can coexist."""
        leaders = [n for n in self.live_nodes() if n.role is Role.LEADER]
        return max(leaders, key=lambda n: n.term) if leaders else None

    # ---- network ----------------------------------------------------------

    def partition(self, *groups: list[str]) -> None:
        """Split into groups that can only talk among themselves. Unlisted nodes are isolated."""
        self._groups = {}
        for g, members in enumerate(groups):
            for nid in members:
                self._groups[nid] = g
        self._log(f"partition {[sorted(g) for g in groups]}")

    def heal(self) -> None:
        if self._groups is not None:
            self._log("heal")
        self._groups = None

    def _connected(self, a: str, b: str) -> bool:
        if self._groups is None:
            return True
        ga, gb = self._groups.get(a), self._groups.get(b)
        return ga is not None and ga == gb

    def _collect(self, node: Node) -> None:
        f = self.faults
        for message in node.outbox:
            self.stats["sent"] += 1
            if f.drop and self.rng.random() < f.drop:
                self.stats["dropped"] += 1
                continue
            copies = 2 if f.duplicate and self.rng.random() < f.duplicate else 1
            self.stats["duplicated"] += copies - 1
            for _ in range(copies):
                at = self.now + self.rng.randint(f.min_delay, f.max_delay)
                self._seq += 1
                heapq.heappush(self._queue, (at, self._seq, message))
        node.outbox.clear()

    # ---- time -------------------------------------------------------------

    def tick(self) -> None:
        self.now += 1
        for machine in self.machines.values():
            if machine.node is not None:
                before = machine.node.role
                machine.node.tick()
                self._after_step(machine, before)

        while self._queue and self._queue[0][0] <= self.now:
            _, _, message = heapq.heappop(self._queue)
            machine = self.machines[message.dst]
            # Checked at delivery: a message in flight when a partition starts
            # is lost, just as one queued in a switch would be.
            if machine.node is None or not self._connected(message.src, message.dst):
                self.stats["dropped"] += 1
                continue
            self.stats["delivered"] += 1
            before = machine.node.role
            machine.node.step(message)
            self._after_step(machine, before)

        if self.checker:
            self.checker.check(self.live_nodes(), logs=True)

    def _after_step(self, machine: Machine, role_before: Role) -> None:
        node = machine.node
        if node.role is not role_before:
            self._log(f"{node.id} {role_before.value} -> {node.role.value} term {node.term}")
        self._collect(node)
        for index, entry in node.take_committed():
            machine.kv.apply(index, entry.command)
            machine.applied.append((index, entry))
        if self.checker:
            self.checker.check(self.live_nodes(), logs=False)

    def run(self, ticks: int) -> None:
        for _ in range(ticks):
            self.tick()

    def run_until(self, predicate, max_ticks: int = 1000) -> int:
        """Tick until `predicate()` holds. Returns ticks taken; raises if it never does."""
        start = self.now
        while not predicate():
            if self.now - start >= max_ticks:
                raise TimeoutError(f"condition not met within {max_ticks} ticks (seed {self.seed})\n" + self.dump())
            self.tick()
        return self.now - start

    def wait_for_leader(self, max_ticks: int = 1000) -> Node:
        self.run_until(lambda: self._stable_leader() is not None, max_ticks)
        return self._stable_leader()

    def _stable_leader(self) -> Node | None:
        """A leader that a majority of live, connected nodes follow in its term."""
        leader = self.leader()
        if leader is None:
            return None
        followers = sum(
            1
            for n in self.live_nodes()
            if n.term == leader.term and n.leader_id == leader.id and self._connected(n.id, leader.id)
        )
        return leader if followers * 2 > len(self.ids) else None

    # ---- clients ----------------------------------------------------------

    def propose(self, command) -> tuple[str, int, int] | None:
        """Hand `command` to whichever node currently believes it leads. None if nobody does."""
        leader = self.leader()
        if leader is None:
            return None
        try:
            index, term = leader.propose(command)
        except NotLeader:
            return None
        self._after_step(self.machines[leader.id], Role.LEADER)
        return leader.id, index, term

    def committed_on_all(self, index: int) -> bool:
        return all(n.commit_index >= index for n in self.live_nodes())

    # ---- debugging --------------------------------------------------------

    def _log(self, event: str) -> None:
        self.trace.append(f"t={self.now} {event}")

    def dump(self) -> str:
        lines = [f"seed={self.seed} now={self.now}"]
        for nid, machine in self.machines.items():
            if machine.node is None:
                lines.append(f"  {nid}: down (log {len(machine.storage.log)})")
            else:
                s = machine.node.status()
                lines.append(
                    f"  {nid}: {s['role']:9} term={s['term']} leader={s['leader']} "
                    f"last={s['last_index']} commit={s['commit_index']}"
                )
        lines.append("recent events:")
        lines.extend(f"  {e}" for e in self.trace)
        return "\n".join(lines)
