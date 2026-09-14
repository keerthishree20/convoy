"""A Raft node as a deterministic state machine.

The node does no I/O of its own. A driver calls `tick()` to advance logical
time, `step(message)` to deliver a message, and `propose(command)` to ask a
leader to replicate something. Each call may append to `outbox`, which the
driver empties and delivers however it likes. Randomness comes only from the
`random.Random` the driver passes in.

That shape is the whole reason the chaos suite is worth anything: given a seed,
every run is identical, so a failure found in scenario 3,817 replays exactly.

Section references are to Figure 2 of "In Search of an Understandable Consensus
Algorithm" (Ongaro and Ousterhout, 2014).
"""

from __future__ import annotations

import random
from enum import Enum

from .messages import AppendEntries, AppendReply, Entry, Message, RequestVote, VoteReply
from .storage import Storage


class Role(str, Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class NotLeader(Exception):
    def __init__(self, leader_id: str | None) -> None:
        super().__init__(f"not the leader; last known leader is {leader_id!r}")
        self.leader_id = leader_id


class SafetyViolation(AssertionError):
    """Raised when the node is asked to do something Raft guarantees never happens."""


class Node:
    def __init__(
        self,
        node_id: str,
        peers: list[str],
        storage: Storage,
        rng: random.Random,
        *,
        election_ticks: tuple[int, int] = (10, 20),
        heartbeat_ticks: int = 3,
        max_batch: int = 64,
    ) -> None:
        if heartbeat_ticks >= election_ticks[0]:
            raise ValueError("heartbeats must be more frequent than the shortest election timeout")
        self.id = node_id
        self.peers = [p for p in peers if p != node_id]
        self.storage = storage
        self.rng = rng
        self.election_ticks = election_ticks
        self.heartbeat_ticks = heartbeat_ticks
        self.max_batch = max_batch

        # Volatile state. All of it is rebuilt after a restart.
        self.role = Role.FOLLOWER
        self.leader_id: str | None = None
        self.commit_index = 0
        self.last_applied = 0
        self.votes: set[str] = set()
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}
        self.outbox: list[Message] = []

        self._elapsed = 0
        self._timeout = 0
        self._reset_election_timer()

    # Persistent state lives in storage and is read through it, so there is no
    # second copy to fall out of step.

    @property
    def term(self) -> int:
        return self.storage.term

    @property
    def voted_for(self) -> str | None:
        return self.storage.voted_for

    @property
    def log(self) -> list[Entry]:
        return self.storage.log

    @property
    def last_index(self) -> int:
        return len(self.storage.log)

    def term_at(self, index: int) -> int:
        return self.storage.log[index - 1].term if index > 0 else 0

    @property
    def cluster_size(self) -> int:
        return len(self.peers) + 1

    def _is_majority(self, count: int) -> bool:
        return count * 2 > self.cluster_size

    # ---- driver interface -------------------------------------------------

    def tick(self) -> None:
        self._elapsed += 1
        if self.role is Role.LEADER:
            if self._elapsed >= self.heartbeat_ticks:
                self._elapsed = 0
                self._broadcast_append()
        elif self._elapsed >= self._timeout:
            self._start_election()

    def step(self, message: Message) -> None:
        if message.dst != self.id:
            return
        # Rules for all servers: a newer term demotes whoever sees it.
        if message.term > self.term:
            self._become_follower(message.term, leader_id=None)

        if isinstance(message, RequestVote):
            self._on_request_vote(message)
        elif isinstance(message, VoteReply):
            self._on_vote_reply(message)
        elif isinstance(message, AppendEntries):
            self._on_append_entries(message)
        elif isinstance(message, AppendReply):
            self._on_append_reply(message)

    def propose(self, command) -> tuple[int, int]:
        """Append `command` to the leader's log. Returns (index, term).

        Being appended is not being committed. The caller learns the outcome
        when an entry at that index is applied: if its term matches, this
        command made it; if not, leadership changed and it was overwritten.
        """
        return self.propose_many([command])[0]

    def propose_many(self, commands: list) -> list[tuple[int, int]]:
        """Append several commands with one storage write and one round of sends.

        On a real disk every append is an fsync, so a server that gathers the
        requests waiting at one moment and proposes them together pays for one
        sync instead of one per request. This is group commit.
        """
        if self.role is not Role.LEADER:
            raise NotLeader(self.leader_id)
        first = self.last_index + 1
        self.storage.append([Entry(self.term, c) for c in commands])
        if self.cluster_size == 1:
            self._advance_leader_commit()
        else:
            self._broadcast_append()
        return [(first + i, self.term) for i in range(len(commands))]

    def take_committed(self) -> list[tuple[int, Entry]]:
        """Entries committed since the last call, in order, for the state machine."""
        out = []
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            out.append((self.last_applied, self.log[self.last_applied - 1]))
        return out

    # ---- role transitions -------------------------------------------------

    def _reset_election_timer(self) -> None:
        self._elapsed = 0
        self._timeout = self.rng.randint(*self.election_ticks)

    def _become_follower(self, term: int, leader_id: str | None) -> None:
        if term > self.term:
            self.storage.save_term_vote(term, None)
        self.role = Role.FOLLOWER
        self.leader_id = leader_id
        self.votes = set()

    def _start_election(self) -> None:
        self.storage.save_term_vote(self.term + 1, self.id)
        self.role = Role.CANDIDATE
        self.leader_id = None
        self.votes = {self.id}
        self._reset_election_timer()
        if self._is_majority(len(self.votes)):
            self._become_leader()
            return
        for peer in self.peers:
            self.outbox.append(
                RequestVote(self.term, self.id, peer, self.last_index, self.term_at(self.last_index))
            )

    def _become_leader(self) -> None:
        self.role = Role.LEADER
        self.leader_id = self.id
        self.next_index = {p: self.last_index + 1 for p in self.peers}
        self.match_index = {p: 0 for p in self.peers}
        # A leader may only count replicas of entries from its own term (the
        # Figure 8 rule), so without a fresh entry, older entries that already
        # sit on a majority could wait indefinitely for a client write. The
        # no-op gives it one immediately.
        self.storage.append([Entry(self.term, None)])
        self._elapsed = 0
        if self.cluster_size == 1:
            self._advance_leader_commit()
        else:
            self._broadcast_append()

    # ---- RequestVote ------------------------------------------------------

    def _on_request_vote(self, m: RequestVote) -> None:
        granted = False
        if m.term == self.term and self.voted_for in (None, m.src):
            # Up to date means a later last term, or the same last term and a
            # log at least as long. Comparing lengths alone would let a node
            # with a long stale log beat one holding newer committed entries.
            my_last_term = self.term_at(self.last_index)
            up_to_date = (m.last_log_term, m.last_log_index) >= (my_last_term, self.last_index)
            if up_to_date:
                granted = True
                self.storage.save_term_vote(self.term, m.src)
                # Only a granted vote resets the timer. Resetting on every
                # request lets a candidate that cannot win keep everyone else
                # from ever timing out.
                self._reset_election_timer()
        self.outbox.append(VoteReply(self.term, self.id, m.src, granted))

    def _on_vote_reply(self, m: VoteReply) -> None:
        if self.role is not Role.CANDIDATE or m.term != self.term or not m.granted:
            return
        self.votes.add(m.src)
        if self._is_majority(len(self.votes)):
            self._become_leader()

    # ---- AppendEntries: follower side -------------------------------------

    def _on_append_entries(self, m: AppendEntries) -> None:
        if m.term < self.term:
            self.outbox.append(AppendReply(self.term, self.id, m.src, False, 0))
            return

        if self.role is Role.LEADER:
            raise SafetyViolation(f"{self.id} is leader of term {self.term} and heard from leader {m.src}")
        # A current-term AppendEntries can only come from the one leader of this
        # term. That makes it the only message, besides granting a vote, that
        # proves a live leader and so resets the election timer.
        self.role = Role.FOLLOWER
        self.leader_id = m.src
        self.votes = set()
        self._reset_election_timer()

        if m.prev_index > self.last_index:
            self.outbox.append(AppendReply(self.term, self.id, m.src, False, self.last_index + 1))
            return
        if m.prev_index > 0 and self.term_at(m.prev_index) != m.prev_term:
            # Skip back past every entry of the conflicting term at once.
            conflict_term = self.term_at(m.prev_index)
            hint = m.prev_index
            while hint > 1 and self.term_at(hint - 1) == conflict_term:
                hint -= 1
            self.outbox.append(AppendReply(self.term, self.id, m.src, False, hint))
            return

        # Walk the entries. Only a real disagreement truncates. An entry we
        # already hold with the same term is identical by Log Matching, and a
        # delayed or duplicated message whose entries are all a prefix of our
        # log must leave the tail behind them alone: that tail may hold entries
        # a newer message from this same leader delivered, already counted
        # towards a commit.
        new_entries: list[Entry] = []
        for offset, entry in enumerate(m.entries):
            index = m.prev_index + 1 + offset
            if index <= self.last_index:
                if self.term_at(index) == entry.term:
                    continue
                if index <= self.commit_index:
                    raise SafetyViolation(
                        f"{self.id} asked to overwrite committed entry {index} "
                        f"(term {self.term_at(index)} -> {entry.term})"
                    )
                self.storage.truncate(index)
            new_entries = list(m.entries[offset:])
            break
        self.storage.append(new_entries)

        last_new = m.prev_index + len(m.entries)
        if m.commit > self.commit_index:
            # Bounded by the last entry this message vouched for, not by the end
            # of our log: anything past last_new may be a stale tail.
            self.commit_index = max(self.commit_index, min(m.commit, last_new))
        self.outbox.append(AppendReply(self.term, self.id, m.src, True, last_new))

    # ---- AppendEntries: leader side ---------------------------------------

    def _send_append(self, peer: str) -> None:
        prev = self.next_index[peer] - 1
        entries = tuple(self.log[prev : prev + self.max_batch])
        self.outbox.append(
            AppendEntries(self.term, self.id, peer, prev, self.term_at(prev), entries, self.commit_index)
        )

    def _broadcast_append(self) -> None:
        for peer in self.peers:
            self._send_append(peer)

    def _on_append_reply(self, m: AppendReply) -> None:
        if self.role is not Role.LEADER or m.term != self.term:
            return
        peer = m.src
        if m.success:
            # max(): replies arrive out of order, and an old one must not move
            # progress backwards.
            if m.match_index <= self.match_index[peer]:
                # A stale or duplicate reply. Sending more on it would start a
                # second stream of requests to this peer alongside the one the
                # newer reply already continued, and those streams multiply.
                return
            self.match_index[peer] = m.match_index
            self.next_index[peer] = max(self.next_index[peer], m.match_index + 1)
            self._advance_leader_commit()
            if self.next_index[peer] <= self.last_index:
                self._send_append(peer)
        else:
            retry = max(self.match_index[peer] + 1, min(m.match_index, self.next_index[peer] - 1))
            if retry != self.next_index[peer]:
                self.next_index[peer] = max(1, retry)
                self._send_append(peer)

    def _advance_leader_commit(self) -> None:
        for n in range(self.last_index, self.commit_index, -1):
            # Figure 8: counting replicas only proves commitment for an entry of
            # the leader's own term. An older entry on a majority can still be
            # overwritten by a leader that never saw it; it becomes committed
            # indirectly, once a current-term entry after it is.
            if self.term_at(n) != self.term:
                break
            replicas = 1 + sum(1 for p in self.peers if self.match_index[p] >= n)
            if self._is_majority(replicas):
                self.commit_index = n
                return

    def status(self) -> dict:
        return {
            "id": self.id,
            "role": self.role.value,
            "term": self.term,
            "leader": self.leader_id,
            "last_index": self.last_index,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
        }
