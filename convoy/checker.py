"""Raft's safety properties, checked against a live cluster.

The simulator calls `check()` after every message it delivers and every tick.
Each property here is one of the five in Figure 3 of the paper, restated as
something a program can observe:

- **Election Safety.** At most one leader per term, ever. Recorded across the
  whole run, so a second leader in an old term is caught even after the first
  one is long gone.
- **Log Matching.** If two logs hold an entry with the same index and term, the
  logs are identical up to that index.
- **Leader Completeness.** A leader holds every entry committed before it.
- **State Machine Safety.** Once any node commits an entry at an index, no node
  ever commits a different entry there. Also: a node's commit index never moves
  backwards while it stays up.
- **Leader Append-Only** is enforced inside the node, which raises rather than
  truncate an entry it has committed.
"""

from __future__ import annotations

from .messages import Entry
from .node import Node, Role


class InvariantViolation(AssertionError):
    pass


class SafetyChecker:
    def __init__(self) -> None:
        self.leaders: dict[int, str] = {}
        self.committed: dict[int, Entry] = {}
        self._commit_seen: dict[int, int] = {}  # id(node) -> commit index last observed
        self._leader_checked: dict[int, int] = {}  # id(node) -> term already checked
        self.checks = 0

    def check(self, nodes: list[Node], *, logs: bool = True) -> None:
        """Run every check. `logs=False` skips the pairwise log comparison, the
        only one whose cost grows with log length; the simulator runs it once
        per tick rather than after every message."""
        self.checks += 1
        for node in nodes:
            self._check_commit(node)
        for node in nodes:
            if node.role is Role.LEADER:
                self._check_leader(node)
        if logs:
            self._check_log_matching(nodes)

    def _check_leader(self, node: Node) -> None:
        holder = self.leaders.setdefault(node.term, node.id)
        if holder != node.id:
            raise InvariantViolation(f"election safety: {holder} and {node.id} both led term {node.term}")
        # Completeness is a property of the moment a leader takes office: it
        # must hold everything committed in earlier terms. A leader that is
        # later deposed without noticing can legitimately miss newer commits.
        if self._leader_checked.get(id(node)) == node.term:
            return
        self._leader_checked[id(node)] = node.term
        log = node.log
        for index, entry in self.committed.items():
            if index > len(log) or log[index - 1] != entry:
                raise InvariantViolation(
                    f"leader completeness: leader {node.id} of term {node.term} lacks committed entry {index}"
                )

    def _check_commit(self, node: Node) -> None:
        seen = self._commit_seen.get(id(node), 0)
        if node.commit_index < seen:
            raise InvariantViolation(f"{node.id} commit index went backwards: {seen} -> {node.commit_index}")
        if node.commit_index > node.last_index:
            raise InvariantViolation(f"{node.id} committed past the end of its log")
        for index in range(seen + 1, node.commit_index + 1):
            entry = node.log[index - 1]
            first = self.committed.setdefault(index, entry)
            if first != entry:
                raise InvariantViolation(
                    f"state machine safety: index {index} committed as {first} and later as {entry} on {node.id}"
                )
        self._commit_seen[id(node)] = node.commit_index

    def _check_log_matching(self, nodes: list[Node]) -> None:
        for i, a in enumerate(nodes):
            for b in nodes[i + 1 :]:
                la, lb = a.log, b.log
                n = min(len(la), len(lb))
                # The highest index where both hold the same term. Everything
                # up to it must then be identical.
                k = n
                while k > 0 and la[k - 1].term != lb[k - 1].term:
                    k -= 1
                if k and la[:k] != lb[:k]:
                    first = next(j for j in range(k) if la[j] != lb[j]) + 1
                    raise InvariantViolation(
                        f"log matching: {a.id} and {b.id} agree on term at index {k} "
                        f"but differ at index {first}"
                    )

    def forget(self, node: Node) -> None:
        """A crashed node's volatile commit index is gone; its successor starts from zero."""
        self._commit_seen.pop(id(node), None)
        self._leader_checked.pop(id(node), None)
