# Convoy — Complete Project Guide

A complete guide from zero to a working, tested Raft cluster. Covers every feature, every design
decision and the reason behind it, with the real code. It is self-contained: you can paste it into
any AI chat and ask questions about the project without sharing the repository.

**Repository:** https://github.com/keerthishree20/convoy
**All projects:** https://github.com/keerthishree20

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Tech Stack & Why](#2-tech-stack--why)
3. [Project Setup from Scratch](#3-project-setup-from-scratch)
4. [Raft in Plain Words](#4-raft-in-plain-words)
5. [Project Structure](#5-project-structure)
6. [Messages & Data Types](#6-messages--data-types)
7. [The Node: A State Machine With No I/O](#7-the-node-a-state-machine-with-no-io)
8. [Leader Election](#8-leader-election)
9. [Log Replication](#9-log-replication)
10. [Commitment & the Figure 8 Rule](#10-commitment--the-figure-8-rule)
11. [Storage & Durability](#11-storage--durability)
12. [The Deterministic Simulator](#12-the-deterministic-simulator)
13. [The Safety Checker](#13-the-safety-checker)
14. [Chaos Scenarios](#14-chaos-scenarios)
15. [Planted Bugs (Mutation Testing)](#15-planted-bugs-mutation-testing)
16. [Key-Value Store & Exactly-Once Writes](#16-key-value-store--exactly-once-writes)
17. [The Network Server](#17-the-network-server)
18. [Group Commit](#18-group-commit)
19. [The Client](#19-the-client)
20. [Command Line & Local Cluster](#20-command-line--local-cluster)
21. [HTTP API](#21-http-api)
22. [Testing](#22-testing)
23. [Benchmarks & Results](#23-benchmarks--results)
24. [Bugs Found Along the Way](#24-bugs-found-along-the-way)
25. [Deliberately Not Built](#25-deliberately-not-built)
26. [Troubleshooting](#26-troubleshooting)
27. [Complete Feature Summary](#27-complete-feature-summary)

---

## 1. Project Overview

Convoy is an implementation of the **Raft consensus algorithm** in pure Python. A group of servers
(a *cluster*) agrees on one ordered list of commands, even when servers crash, restart, or lose
messages. Every server applies those commands in the same order to its own copy of a key-value store,
so they all end up holding the same data.

It has three parts:

| Part | What it does |
|---|---|
| **The Raft node** | Elects a leader, replicates a log, decides when an entry is committed |
| **The simulator** | Runs a whole cluster in one process on a network that drops, delays, duplicates and partitions messages, and checks Raft's safety rules after every message |
| **The key-value store** | Runs as real operating-system processes, talking TCP to each other and HTTP to clients |

The question the project is built to answer is not "does it work?" but **"how do you know it is
correct?"** Most of the code is testing machinery: a safety checker, seeded chaos scenarios, and five
deliberately broken copies of the node that the tests must catch.

**Status:** complete. 82 tests pass on Python 3.10 and 3.12. 3,000 chaos scenarios pass on 5 nodes
and 3,000 on 3 nodes.

---

## 2. Tech Stack & Why

| Technology | Role | Why We Chose It |
|---|---|---|
| **Python 3.10+** | Language | Readable enough that the Raft rules can be checked line by line against the paper |
| **Standard library only** | Runtime | No dependencies to install or break. The point is the algorithm, not a framework |
| **asyncio** | Real server | One event loop per node, so the Raft state is never touched from two places at once |
| **TCP + newline JSON** | Peer transport | Simple to read in a packet dump, and Raft already tolerates lost messages |
| **HTTP/1.1 (hand-written)** | Client API | Any tool, even `curl`, can talk to the cluster |
| **pytest** | Tests | Parametrised tests make it easy to run hundreds of seeded scenarios |
| **GitHub Actions** | CI | Runs the suite on Python 3.10, 3.11 and 3.12 on every push |

---

## 3. Project Setup from Scratch

### Step 1: Get the code
```bash
git clone https://github.com/keerthishree20/convoy.git
cd convoy
```

### Step 2: Create the environment
On the author's machine `python3` is Python 3.6, which is too old, so the Makefile calls
`python3.12` directly.
```bash
make install     # creates .venv and installs pytest (Convoy itself needs nothing)
```

### Step 3: Run the tests
```bash
make test        # all 82 tests, about 90 seconds
```

### Step 4: Start a real cluster
```bash
make up                                            # terminal 1: three nodes, Ctrl-C to stop
.venv/bin/python -m convoy.cli put greeting hello  # terminal 2
.venv/bin/python -m convoy.cli get greeting
.venv/bin/python -m convoy.cli status
```

`make help` lists every target.

---

## 4. Raft in Plain Words

**The problem.** Several servers must agree on the same list of commands in the same order, even if
some crash or messages go missing.

**Terms.** Time is split into numbered *terms*. Each term has at most one leader. Every message
carries its sender's term. A server that sees a higher term immediately becomes a follower.

**Election.** A follower that hears nothing from a leader for a random timeout becomes a
*candidate*: it increases the term, votes for itself, and asks the others for votes. Each server
votes at most once per term, and only for a candidate whose log is at least as up to date as its own.
A majority of votes makes a leader.

**Replication.** The leader adds client commands to its log and sends them to followers in
`AppendEntries` messages. Each message names the entry just before the new ones. A follower that does
not hold that exact entry refuses, and the leader backs up and tries earlier. This keeps every log a
copy of the leader's.

**Commitment.** An entry is *committed* once it is stored on a majority of servers. Committed entries
are applied to the key-value store and never change. One subtle exception, the Figure 8 rule, is
explained in section 10.

**Persistence.** A server must save its term, its vote and its log to disk before answering anyone.

The rules come from Figure 2 of *In Search of an Understandable Consensus Algorithm* by Diego Ongaro
and John Ousterhout, 2014.

---

## 5. Project Structure

```
convoy/
  node.py       Raft itself: election, replication, commitment. No I/O at all
  messages.py   the four RPC message types, and their JSON form for the network
  storage.py    term, vote and log: MemoryStorage (simulator) and FileStorage (disk)
  checker.py    SafetyChecker: raises the moment any Raft safety rule breaks
  sim.py        Cluster: a whole cluster in one process on a faulty network
  chaos.py      run_scenario(seed): faults, recovery, retries, audit
  kv.py         KVStateMachine: the replicated key-value store
  server.py     ConvoyServer: one node as a real process
  client.py     ConvoyClient: finds the leader and retries safely
  local.py      LocalCluster: starts and kills real node processes
  cli.py        convoy serve | up | put | get | delete | status
tests/
  mutants.py            five broken copies of node.py the suite must catch
  test_election.py      voting rules, timers, partitions
  test_replication.py   conflicts, duplicates, commit bounds, Figure 8
  test_storage.py       durability, torn writes, corruption
  test_kv.py            state machine and exactly-once writes
  test_chaos.py         seeded scenarios, determinism, planted bugs
  test_processes.py     real processes, SIGKILL, full restarts
bench/
  election.py   leaderless time after a crash, simulator
  failover.py   throughput and outage on real processes
Makefile  pyproject.toml  requirements-dev.txt  .github/workflows/tests.yml
```

---

## 6. Messages & Data Types

All four Raft messages live in `convoy/messages.py` as frozen dataclasses.

```python
class Entry(NamedTuple):
    term: int
    command: Any          # anything JSON can hold; None is a new leader's no-op

@dataclass(frozen=True, slots=True)
class RequestVote:
    term: int
    src: str
    dst: str
    last_log_index: int
    last_log_term: int

@dataclass(frozen=True, slots=True)
class VoteReply:
    term: int
    src: str
    dst: str
    granted: bool

@dataclass(frozen=True, slots=True)
class AppendEntries:
    term: int
    src: str
    dst: str
    prev_index: int
    prev_term: int
    entries: tuple[Entry, ...]
    commit: int

@dataclass(frozen=True, slots=True)
class AppendReply:
    term: int
    src: str
    dst: str
    success: bool
    match_index: int
```

### Why is `Entry` a NamedTuple?
Tuples compare in C, so checking whether two logs are identical is a fast list comparison. The
safety checker does this constantly.

### What `match_index` means
- **On success:** the last index the follower now shares with the leader, `prev_index + len(entries)`.
  Not the end of the follower's log, which may still hold stale entries.
- **On failure:** a hint for where the leader should retry, so a follower that is far behind is found
  in a few round trips instead of one per missing entry.

`to_wire()` and `from_wire()` turn messages into plain dictionaries for JSON on the network.

---

## 7. The Node: A State Machine With No I/O

`convoy/node.py` is Raft as a pure state machine. It has **no clock, no sockets and no threads**.
A driver calls four methods:

| Method | Purpose |
|---|---|
| `tick()` | advance logical time by one step |
| `step(message)` | deliver one message |
| `propose(command)` | ask a leader to replicate a command. Raises `NotLeader` otherwise |
| `take_committed()` | return entries committed since the last call, to apply to the store |

Outgoing messages collect in `node.outbox`, and the driver delivers them however it likes.

```python
def tick(self) -> None:
    self._elapsed += 1
    if self.role is Role.LEADER:
        if self._elapsed >= self.heartbeat_ticks:
            self._elapsed = 0
            self._broadcast_append()
    elif self._elapsed >= self._timeout:
        self._start_election()
```

### Why no I/O?
Two drivers use the exact same node:
- the **simulator**, with a fake clock and a fake network,
- the **server**, with a real 10 ms clock and real TCP.

Because the node's only randomness comes from a `random.Random` the driver passes in, a simulated
run with the same seed is identical message for message. A failure found in chaos scenario 3,817
replays exactly, every time. Adding determinism to an asyncio Raft afterwards would mean rewriting it,
so this was decided before any code was written.

### Persistent state lives in storage
`term`, `voted_for` and `log` are properties that read straight from the storage object, so there is
never a second copy that could fall out of step.

---

## 8. Leader Election

### Starting an election
A follower whose timer runs out becomes a candidate:

```python
def _start_election(self) -> None:
    self.storage.save_term_vote(self.term + 1, self.id)   # new term, vote for self, saved first
    self.role = Role.CANDIDATE
    self.leader_id = None
    self.votes = {self.id}
    self._reset_election_timer()
    if self._is_majority(len(self.votes)):                # a 1-node cluster wins at once
        self._become_leader()
        return
    for peer in self.peers:
        self.outbox.append(
            RequestVote(self.term, self.id, peer, self.last_index, self.term_at(self.last_index))
        )
```

The timeout is random, between 10 and 20 ticks by default, so candidates rarely collide.

### Granting a vote
```python
def _on_request_vote(self, m: RequestVote) -> None:
    granted = False
    if m.term == self.term and self.voted_for in (None, m.src):
        my_last_term = self.term_at(self.last_index)
        up_to_date = (m.last_log_term, m.last_log_index) >= (my_last_term, self.last_index)
        if up_to_date:
            granted = True
            self.storage.save_term_vote(self.term, m.src)
            self._reset_election_timer()
    self.outbox.append(VoteReply(self.term, self.id, m.src, granted))
```

### Why compare the last term first, then length?
Comparing log length alone would let a node with a long **stale** log beat one holding newer
committed entries. That node could then win and overwrite committed data.

### Why only a *granted* vote resets the timer
If every vote request reset the timer, a candidate that can never win would keep everyone else from
ever timing out, and the cluster would never elect a leader.

### Becoming leader
```python
def _become_leader(self) -> None:
    self.role = Role.LEADER
    self.leader_id = self.id
    self.next_index = {p: self.last_index + 1 for p in self.peers}
    self.match_index = {p: 0 for p in self.peers}
    self.storage.append([Entry(self.term, None)])   # the no-op, see section 10
    self._elapsed = 0
    self._broadcast_append()
```

---

## 9. Log Replication

### The follower's side
When a follower receives `AppendEntries`, it:
1. **Rejects an older term** and tells the sender the current term.
2. **Accepts the sender as leader** for this term and resets its election timer. A current-term
   `AppendEntries` is the only message, besides granting a vote, that proves a live leader.
3. **Checks the previous entry.** If it does not hold `prev_index` with `prev_term`, it refuses and
   sends a hint: either "my log ends here", or "skip back past every entry of this conflicting term".
4. **Walks the new entries**, truncating only at a real conflict:

```python
for offset, entry in enumerate(m.entries):
    index = m.prev_index + 1 + offset
    if index <= self.last_index:
        if self.term_at(index) == entry.term:
            continue                               # already have it, identical
        if index <= self.commit_index:
            raise SafetyViolation(...)             # must never overwrite a committed entry
        self.storage.truncate(index)               # a real disagreement
    new_entries = list(m.entries[offset:])
    break
self.storage.append(new_entries)
```

5. **Advances its commit index**, but only up to the last entry this message vouched for:

```python
last_new = m.prev_index + len(m.entries)
if m.commit > self.commit_index:
    self.commit_index = max(self.commit_index, min(m.commit, last_new))
```

### Why "only a real conflict truncates"
Messages arrive late or twice. If a delayed old message made the follower cut its log, it could
delete entries a newer message from the same leader already delivered and that the leader already
counted towards a commit. That silently loses committed writes.

### Why the commit stops at `last_new`
Anything past `last_new` may be a stale tail from an old term that this leader never confirmed.

### The leader's side
- Each follower has a `next_index` (what to send next) and a `match_index` (what is known to match).
- On **success**, `match_index` rises and the leader sends more if the follower is still behind.
- On **failure**, `next_index` jumps to the hint, never below what is already known to match.
- A **stale reply**, one that does not raise `match_index`, sends nothing (see section 24).
- Heartbeats every 3 ticks also carry any missing entries, up to 64 per message.

---

## 10. Commitment & the Figure 8 Rule

```python
def _advance_leader_commit(self) -> None:
    for n in range(self.last_index, self.commit_index, -1):
        if self.term_at(n) != self.term:
            break                     # only entries from the leader's own term count
        replicas = 1 + sum(1 for p in self.peers if self.match_index[p] >= n)
        if self._is_majority(replicas):
            self.commit_index = n
            return
```

### What is Figure 8?
A leader may **not** mark an entry committed just because it sits on a majority, if that entry is
from an earlier term. A different server whose last entry has a newer term could still win an
election and overwrite it. The entry only becomes safe once an entry from the leader's own term, after
it, reaches a majority. Committing that newer entry commits everything before it.

### Why the no-op entry
Without it, older entries could wait forever for a client write before the new leader could commit
anything. The empty no-op, appended the moment a leader is elected, gives it a current-term entry to
commit immediately.

---

## 11. Storage & Durability

`convoy/storage.py` has one interface and two implementations.

| Method | Purpose |
|---|---|
| `save_term_vote(term, voted_for)` | persist term and vote |
| `append(entries)` | add entries to the end of the log |
| `truncate(from_index)` | remove the log from an index onwards |

### MemoryStorage
The "disk" of a simulated machine. When the simulator crashes a node, it keeps this object and hands
it to the restarted node, so only persisted state survives.

### FileStorage
A directory holding two files:
```
data/n1/
  meta.json    {"term": 7, "voted_for": "n2"}
  log          append-only records
```

- **`meta.json`** is rewritten whole: write a temporary file, fsync it, rename it over the old one,
  fsync the directory. A crash leaves the old version or the new one, never half of each.
- **`log`** records are `crc32 | length | JSON`. On open, the file is scanned and cut at the first
  record whose checksum fails. That record was never acknowledged, because nothing is acknowledged
  before the fsync that would complete it.

### Why save before replying?
If a server forgets its vote after a crash, it can vote twice in one term and allow two leaders.
If it forgets a log entry the leader counted, a committed entry can vanish.

---

## 12. The Deterministic Simulator

`convoy/sim.py` runs a whole cluster in one process.

```python
from convoy import Cluster, NetworkFaults

cluster = Cluster(5, seed=42, faults=NetworkFaults(drop=0.1, duplicate=0.05, max_delay=6))
leader = cluster.wait_for_leader()
cluster.propose({"op": "put", "key": "a", "value": "1"})
cluster.run(50)
```

| Method | Effect |
|---|---|
| `tick()` | advance time, deliver due messages, run the safety checker |
| `crash(id)`, `restart(id)` | kill a node and bring it back with only its storage |
| `partition([...], [...])`, `heal()` | split the network into groups and join it again |
| `wait_for_leader()`, `run_until(predicate)` | drive tests |
| `dump()` | every node's state plus the recent event trace |

Messages are delivered after a random delay, so they arrive out of order. Some are dropped, some
duplicated. A message in flight when a partition starts is lost at delivery. Every random choice
comes from one seeded generator, so the same seed gives the same run.

---

## 13. The Safety Checker

`convoy/checker.py` watches the live cluster and raises `InvariantViolation` the moment Raft's
guarantees break:

| Property | Checked as |
|---|---|
| Election Safety | no two nodes ever lead the same term, across the whole run |
| Log Matching | two logs that hold the same term at an index are identical up to it |
| Leader Completeness | a node taking office holds every entry committed before it |
| State Machine Safety | no index is ever committed with two different entries, and a commit index never moves backwards |
| Leader Append-Only | the node itself refuses to truncate an entry it has committed |

The cheap checks run after every delivered message. The pairwise log comparison, which costs more as
logs grow, runs once per tick.

---

## 14. Chaos Scenarios

`convoy/chaos.py` turns one seed into one complete scenario:

1. **Turbulence.** Clients write while nodes crash and restart, the network partitions and heals,
   0-15% of messages are lost and 0-10% duplicated. Leaders are crashed deliberately more often than
   a random pick would, and batch sizes vary from 1 to 64 so lagging followers stay lagging.
2. **Calm.** All faults stop. A leader must emerge and commit within a time limit, or the scenario
   fails on liveness.
3. **Retries.** Every client resends every write the current leader does not hold, with the same
   client id and sequence number, through any further leader changes.
4. **Audit.** Every replica's data must be identical, and every write must appear exactly once.

```bash
make chaos SEEDS=5000
make replay SEED=1234 SIZE=5    # one scenario, with node states and the event trace
```

---

## 15. Planted Bugs (Mutation Testing)

A test suite that never fails proves nothing on its own. `tests/mutants.py` rewrites the node's
source to reintroduce one classic Raft bug at a time, and also disables the node's own guard against
truncating committed entries, so detection has to come from the checker and the audit.

| Planted bug | First caught (3 nodes, 800 ticks) | Caught by |
|---|---|---|
| leader commits an old-term entry by counting replicas (Figure 8) | seed 342 | leader completeness |
| follower truncates on a delayed duplicate | seed 0 | liveness, and mostly commit-past-end in 58 of the first 60 seeds |
| vote granted by log length, ignoring last term | seed 0 | leader completeness |
| more than one vote per term | seed 2 | election safety |
| follower commit index runs past what the leader vouched for | seed 5 | state machine safety |

The suite fails the build if any mutant survives.

### The Figure 8 lesson
The first version of the chaos plan ran 600 scenarios against the Figure 8 mutant and never caught
it. The likely reason: a new leader's no-op went out in the same message as the older entries, so the
unsafe commit had no window. Crashing leaders more often and shrinking batches to one entry made the
mutant fail. The case also has its own scripted unit test, from the paper's diagram.

---

## 16. Key-Value Store & Exactly-Once Writes

`convoy/kv.py` is the replicated state machine. Commands:

| op | example |
|---|---|
| `put` | `{"op": "put", "key": "a", "value": "1"}` |
| `get` | `{"op": "get", "key": "a"}` |
| `delete` | `{"op": "delete", "key": "a"}` |
| `append` | `{"op": "append", "key": "log", "value": "x;"}` |
| `cas` | `{"op": "cas", "key": "a", "expect": "1", "value": "2"}` |

### Why exactly-once needs extra work
Raft makes every server apply the same commands in order. It does not stop a client submitting the
same command twice. A client whose request timed out cannot know whether it committed, so it retries,
and a retried `append` would land twice.

```python
def apply(self, index: int, command: dict | None) -> Any:
    ...
    client, seq = command.get("client"), command.get("seq")
    if client is not None:
        last = self.sessions.get(client)
        if last is not None and seq <= last[0]:
            return last[1] if seq == last[0] else {"ok": False, "error": "stale sequence number"}
    result = self._execute(command)
    if client is not None:
        self.sessions[client] = (seq, result)
    return result
```

Each write carries a client id and sequence number. A repeat is answered from memory instead of
being executed again. Because the session table is itself built by applying the log, every replica
agrees on it.

---

## 17. The Network Server

`convoy/server.py` wraps one node as a real process.

- **Clock:** a task calls `node.tick()` every 10 ms. Election timeout 150-300 ms, heartbeat 50 ms.
- **Peers:** one outgoing TCP connection per peer, carrying newline-delimited JSON. A broken
  connection drops its queue and reconnects; Raft tolerates the lost messages.
- **Clients:** a small hand-written HTTP/1.1 server.

After every call into the node, the server sends its outbox, applies newly committed entries, and
answers the clients waiting on them:

```python
for index, entry in self.node.take_committed():
    result = self.kv.apply(index, entry.command)
    waiting = self._pending.pop(index, None)
    if waiting is not None:
        term, future = waiting
        if term == entry.term:
            future.set_result(result)          # our command committed
        else:
            future.set_exception(LeadershipLost())   # a different entry won this index
```

### Why check the term?
A leader that loses leadership may have its uncommitted entries overwritten by the next leader. If a
different entry lands at the index a client was waiting on, that client's command did not commit
there. The client is told to retry, which is safe because of exactly-once writes.

**Reads go through the log too**, so they are linearizable, at the cost of one replication round each.

---

## 18. Group Commit

Every write must be fsynced on a majority before it is acknowledged, and fsync is slow. So the server
collects every request waiting at the same moment and proposes them together, with one fsync:

```python
async def _proposal_loop(self) -> None:
    while True:
        await self._proposal_wakeup.wait()
        self._proposal_wakeup.clear()
        await asyncio.sleep(0)          # let requests arriving in this instant join the batch
        batch, self._proposals = self._proposals, []
        placed = self.node.propose_many([c for c, _ in batch])
        ...
```

With 32 concurrent clients this gives roughly ten times the throughput of one client (section 23).

---

## 19. The Client

`convoy/client.py`:

```python
from convoy.client import ConvoyClient

client = ConvoyClient(["127.0.0.1:7002", "127.0.0.1:7004", "127.0.0.1:7006"])
client.put("greeting", "hello")
client.append("log", "a;")
client.cas("greeting", expect="hello", value="hi")
client.get("greeting")
```

- Tries the last known leader first.
- Follows a `421` answer to the leader's address.
- Retries errors, timeouts and leader changes **with the same sequence number**, so a retry is safe.
- One request at a time per client object. That is what makes "highest sequence seen" enough on the
  server side.

---

## 20. Command Line & Local Cluster

```
convoy serve --id n1 --cluster n1=127.0.0.1:7001:7002,n2=...,n3=... --data ./data/n1
convoy up --size 3                 three local nodes in the foreground
convoy put <key> <value>           convoy get <key>      convoy delete <key>
convoy status
```

The cluster string is `id=host:raft_port:http_port`. `convoy up` uses ports 7001 to 7006 and stops
every node on Ctrl-C or SIGTERM.

`convoy/local.py` provides `LocalCluster`, which starts real node processes and kills them with
SIGKILL. Tests and benchmarks use it. A SIGKILL is a real crash: no cleanup code runs.

---

## 21. HTTP API

Every node serves the same routes. Non-leaders answer `421` with the leader's address.

| Method | Route | Purpose |
|---|---|---|
| GET | `/status` | role, term, leader, last index, commit index |
| PUT | `/kv/<key>` | body is the value |
| GET | `/kv/<key>` | linearizable read, through the log |
| DELETE | `/kv/<key>` | delete |
| POST | `/command` | any JSON command, including `client` and `seq` for exactly-once |
| GET | `/local/<key>` | this node's own applied value, no consensus. Possibly stale; for audits |

Status codes: `200` done, `421` not the leader, `503` leadership changed (retry), `504` not committed
in time (retry).

---

## 22. Testing

| File | What it proves |
|---|---|
| `test_election.py` | one vote per term, up-to-date rule, timer resets, minority partitions, stale leaders stepping down |
| `test_replication.py` | conflicts, delayed duplicates, commit bounds, the Figure 8 case |
| `test_storage.py` | durability, torn writes, corrupted records, a restarted node's vote |
| `test_kv.py` | state machine operations and exactly-once writes |
| `test_chaos.py` | hundreds of seeded scenarios, determinism, every planted bug caught |
| `test_processes.py` | real processes: SIGKILL the leader under load, full restart, losing a majority |

```bash
make test            # everything
make test-fast       # without the process tests
make test-processes  # only the process tests
```

The process test "no acknowledged write is lost or doubled when the leader is killed" was confirmed
to fail when deduplication is switched off, so it really checks what it claims.

---

## 23. Benchmarks & Results

All numbers come from one laptop. The process numbers are three or five processes on the same
machine over loopback, sharing one disk: no network latency, and fsyncs from different nodes compete.

### Correctness
| | |
|---|---|
| chaos scenarios, 5 nodes, 1,500 ticks each | 3,000 of 3,000 passed |
| chaos scenarios, 3 nodes | 3,000 of 3,000 passed |
| writes audited across those scenarios | about 2.7 million |
| planted bugs caught | 5 of 5 |

### Leaderless time after the leader crashes (simulator, ticks)
Election timeout 10-20 ticks, heartbeat 3 ticks, 2,000 seeds per row.

| nodes | network | p50 | p99 | max | terms to elect |
|---|---|---|---|---|---|
| 3 | 1-3 tick delay | 21 | 88 | 132 | 1.62 |
| 3 | 10% loss, 1-6 tick delay | 37 | 162 | 257 | 2.55 |
| 5 | 1-3 tick delay | 19 | 60 | 86 | 1.36 |
| 5 | 10% loss, 1-6 tick delay | 32 | 144 | 265 | 2.31 |

Three-node clusters take more terms because the two survivors often time out together and split
the vote.

### Write outage when the leader is SIGKILLed (real processes)
Three nodes, eight clients, 20 kills, election timeout 150-300 ms: **p50 325 ms, mean 343 ms,
max 544 ms**. The floor is the election timeout itself.

### Write throughput (real processes, writes per second)
| nodes | fsync | 1 client | 8 clients | 32 clients |
|---|---|---|---|---|
| 3 | on | 87 | 257 | 932 |
| 5 | on | 79 | 271 | 784 |
| 3 | off | 2,023 | 3,426 | 4,452 |
| 5 | off | 1,560 | 2,477 | 2,914 |

With fsync on, each write waits for disk syncs, which is the real cost of durability. Rows with fsync
off are **not durable** and must never be quoted as real throughput.

```bash
make bench-election
make bench-failover
```

---

## 24. Bugs Found Along the Way

- **Replication streams that multiplied.** The leader first sent more entries on every successful
  reply. Stale and duplicated replies each started another stream to the same follower: about 110
  messages per tick in a five-node cluster. Continuing only on a reply that raises `match_index` cut
  it to about 7.5.
- **A test client that could not retry.** The chaos harness first used one client id for hundreds of
  concurrent writes. The state machine keeps only the newest sequence per client, so retries of older
  writes were refused. Real clients send one request at a time, so the harness now does too.
- **Leader changes during calm.** A node restarted at the end of the chaos phase could win an
  election and discard the previous leader's unreplicated writes. That is correct Raft; the harness
  was wrong to send each retry only once.

---

## 25. Deliberately Not Built

| Feature | Why not |
|---|---|
| Snapshots and log compaction | the log grows forever and a restarted node replays it; adding it touches every index calculation |
| Membership changes | joint consensus is where implementations most often break; a half-working version is worse than none |
| Pre-vote and check-quorum | a rejoining node can depose a healthy leader once; costs availability, never safety |
| ReadIndex or leases | reads go through the log instead |

---

## 26. Troubleshooting

### `python3` fails with a syntax error
The system `python3` is 3.6. Use `make install`, then run everything with `.venv/bin/python`.

### `Address already in use` from `make up`
A previous cluster still holds ports 7001-7006:
```bash
pkill -f "convoy.cli serve"
```

### The client says no leader answered
Fewer than a majority of nodes are running. With three nodes, two must be up.

### A chaos seed fails
Run `make replay SEED=<n> SIZE=<size>`. The dump shows each node's role, term, last index and commit
index, plus the recent events. The same seed always reproduces the same run.

### Where are the node logs?
`make up` writes `n1.log` to `n3.log` in the data directory. Role changes are logged.

---

## 27. Complete Feature Summary

### All Features Built

| # | Feature | Type | Key Files |
|---|---|---|---|
| 1 | Leader election | Core | `node.py` |
| 2 | Log replication with conflict hints | Core | `node.py` |
| 3 | Figure 8-safe commitment | Core | `node.py` |
| 4 | Crash-safe storage with checksums | Storage | `storage.py` |
| 5 | Deterministic simulator | Testing | `sim.py` |
| 6 | Safety checker | Testing | `checker.py` |
| 7 | Seeded chaos scenarios with replay | Testing | `chaos.py` |
| 8 | Mutation testing with 5 planted bugs | Testing | `tests/mutants.py` |
| 9 | Replicated key-value store | App | `kv.py` |
| 10 | Exactly-once writes | App | `kv.py`, `client.py` |
| 11 | Real network server | Runtime | `server.py` |
| 12 | Group commit | Runtime | `server.py`, `node.py` |
| 13 | Leader-following retrying client | Runtime | `client.py` |
| 14 | Local cluster runner and CLI | Tooling | `local.py`, `cli.py` |
| 15 | Election and failover benchmarks | Tooling | `bench/` |

### Data Flow Architecture

```
Client (ConvoyClient or curl)
  └── HTTP PUT /kv/key ──► any node ──► 421 if not leader ──► leader

Leader (server.py)
  ├── request queued ──► proposal loop batches requests ──► node.propose_many()
  ├── storage.append() + fsync
  └── AppendEntries ──► followers (TCP, JSON lines)

Follower
  ├── consistency check ──► append + fsync ──► AppendReply
  └── commit index advances from the leader's heartbeat

Leader
  ├── majority match_index ──► commit (own-term entries only)
  ├── take_committed() ──► KVStateMachine.apply()
  └── answer the waiting client (or LeadershipLost ──► client retries same seq)

Simulator (sim.py) replaces the clock, TCP and fsync with seeded fakes,
and SafetyChecker inspects every node after every step.
```

### Tech Stack at a Glance

```
Language:   Python 3.10+ (standard library only)
Core:       Raft node as a pure state machine
Runtime:    asyncio, TCP with JSON lines, hand-written HTTP/1.1
Storage:    append-only log with CRC32, atomic metadata, fsync
Testing:    pytest, deterministic simulator, safety checker, chaos, mutation testing
CI:         GitHub Actions on Python 3.10, 3.11, 3.12
```
