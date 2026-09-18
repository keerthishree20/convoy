# Convoy — Complete Project Guide

## Table of Contents
1. [What is Convoy?](#what-is-convoy)
2. [Quick Start](#quick-start)
3. [Raft in Five Minutes](#raft-in-five-minutes)
4. [Architecture](#architecture)
5. [Code Walkthrough](#code-walkthrough)
6. [Using the Cluster](#using-the-cluster)
7. [Testing Strategy](#testing-strategy)
8. [Benchmarks](#benchmarks)
9. [Extending Convoy](#extending-convoy)
10. [Troubleshooting](#troubleshooting)

---

## What is Convoy?

Convoy is an implementation of the Raft consensus algorithm in pure Python, with no runtime
dependencies. It has three parts:

- **A Raft node** that elects leaders, replicates a log and decides when entries are committed.
- **A deterministic simulator** that runs a whole cluster in one process on a hostile network, and
  checks Raft's safety properties after every message.
- **A replicated key-value store** that runs as real operating system processes, talking TCP to each
  other and HTTP to clients.

The question the project is built to answer is not "does it work?" but "how do you know it is
correct?" Most of the code is test machinery: a safety checker, seeded chaos scenarios, and a set of
deliberately broken copies of the node that the tests must catch.

---

## Quick Start

Requires Python 3.10 or newer. On this machine `python3` is 3.6, so the Makefile uses `python3.12`.

```bash
cd convoy
make install        # creates .venv and installs pytest
make test           # the whole suite, about a minute and a half
```

Run a local three-node cluster and talk to it:

```bash
make up                                           # terminal 1, Ctrl-C to stop
.venv/bin/python -m convoy.cli put greeting hello # terminal 2
.venv/bin/python -m convoy.cli get greeting
.venv/bin/python -m convoy.cli status
```

`make help` lists every target.

---

## Raft in Five Minutes

A cluster of servers must agree on one ordered log of commands, even when servers crash and
messages are lost. Every server applies the log in order to its own copy of the state machine (here,
a key-value map), so they all end up with the same data.

**Terms.** Time is split into numbered terms. Each term has at most one leader. Every message
carries its sender's term, and a server that sees a higher term immediately steps down to follower.

**Election.** A follower that hears nothing from a leader for a random timeout becomes a candidate,
increments the term, votes for itself and asks everyone else for votes. A server grants at most one
vote per term, and only to a candidate whose log is at least as up to date as its own: a later last
term wins, and on equal last terms the longer log wins. A majority of votes makes a leader.

**Replication.** The leader appends client commands to its log and sends them to followers with
`AppendEntries`. Each message names the entry just before the new ones (`prev_index`,
`prev_term`). A follower that does not hold that exact entry refuses, and the leader backs up and
tries again. This is what keeps every log a prefix-copy of the leader's.

**Commitment.** An entry is committed once it is stored on a majority, with one subtle exception
(Figure 8 of the paper): a leader may only count replicas for entries from its own term. Older
entries become committed indirectly, when a newer entry after them does. That is why a new leader
appends an empty no-op entry the moment it is elected.

**Persistence.** A server must save its term, its vote and its log to disk before answering anyone.
Forgetting a vote lets a server vote twice in one term. Forgetting a log entry can lose a committed
write.

The rules come from Figure 2 of "In Search of an Understandable Consensus Algorithm" by Diego
Ongaro and John Ousterhout, 2014.

---

## Architecture

```
                 ┌──────────────────────────────┐
                 │   convoy/node.py             │
                 │   Raft as a pure state       │
                 │   machine: tick / step /     │
                 │   propose -> outbox          │
                 └──────┬───────────────┬───────┘
                        │ same code     │
          ┌─────────────▼──────┐   ┌────▼──────────────────────┐
          │ convoy/sim.py      │   │ convoy/server.py          │
          │ fake clock (ticks) │   │ asyncio loop, 10 ms tick  │
          │ fake network:      │   │ TCP to peers (JSON lines) │
          │ delay, drop, dup,  │   │ HTTP to clients           │
          │ partition, crash   │   │ FileStorage with fsync    │
          │ MemoryStorage      │   └────┬──────────────────────┘
          │ + checker.py       │        │
          └────────────────────┘   ┌────▼──────────────────────┐
                                   │ convoy/client.py          │
                                   │ finds leader, retries     │
                                   └───────────────────────────┘
```

The central design decision: **the node does no I/O**. It has no clock, no sockets and no threads.
A driver calls:

- `tick()` to advance logical time,
- `step(message)` to deliver a message,
- `propose(command)` to submit a command to a leader,

and then empties `node.outbox` and delivers those messages however it likes. The only randomness is
a `random.Random` the driver passes in. Given the same seed, a simulated run is identical message for
message, which is what makes a chaos failure replayable.

---

## Code Walkthrough

### `convoy/messages.py`
The four Raft RPCs as frozen dataclasses: `RequestVote`, `VoteReply`, `AppendEntries`,
`AppendReply`. A log `Entry` is a `NamedTuple(term, command)`, so comparing two logs is a fast list
comparison. `to_wire()` and `from_wire()` convert messages to and from JSON-ready dicts for the real
network.

In `AppendReply`, `match_index` means two different things. On success it is the last index the
follower now shares with the leader (`prev_index + len(entries)`). On failure it is a hint for where
the leader should retry.

### `convoy/storage.py`
The persistent state: `term`, `voted_for` and `log`.

- `MemoryStorage` is the "disk" of a simulated machine. The simulator keeps the object when a node
  crashes and hands it to the restarted node.
- `FileStorage` is the real thing. `meta.json` holds the term and vote and is rewritten atomically:
  write a temp file, fsync, rename, fsync the directory. The `log` file is append-only records of
  CRC32, length and JSON payload. On open it is scanned, and it is cut at the first record whose
  checksum fails, which is how a torn write from a crash is discarded.

### `convoy/node.py`
Raft itself, about 350 lines.

| method | what it does |
|---|---|
| `tick()` | Followers and candidates count towards their election timeout. Leaders send heartbeats every `heartbeat_ticks`. |
| `step(message)` | Steps down on a higher term, then dispatches to one of the four handlers. |
| `propose` / `propose_many` | Leader only. Appends commands with one storage write and broadcasts them. Raises `NotLeader` otherwise. |
| `take_committed()` | Returns entries committed since the last call, in order, for the state machine to apply. |
| `_start_election()` | Increments the term, votes for itself, sends `RequestVote` to every peer. |
| `_become_leader()` | Resets `next_index` and `match_index` and appends the no-op entry. |
| `_on_request_vote()` | One vote per term, only for an up-to-date log. A granted vote resets the election timer. A refused one does not. |
| `_on_append_entries()` | The consistency check, the conflict hint, and the careful truncation rule described below. |
| `_on_append_reply()` | Advances `match_index`, or backs off `next_index` using the hint. Ignores stale replies. |
| `_advance_leader_commit()` | Commits the highest index on a majority, but only if it is from the current term. |

Three rules in this file are easy to get wrong, and each has a comment and a test:

1. **Only a real conflict truncates.** A delayed duplicate `AppendEntries` whose entries the follower
   already holds must not cut off the entries after them.
2. **A follower's commit index stops at the last entry the message vouched for**, not the end of its
   own log, which may hold a stale tail.
3. **A stale reply never starts a new send.** Early on, resending on every reply made request
   streams multiply: about 110 messages per tick in a five-node cluster, against about 7.5 after
   the fix.

### `convoy/checker.py`
`SafetyChecker` watches a live cluster and raises `InvariantViolation` on:

- two leaders in one term (Election Safety),
- two logs that share a term at an index but differ before it (Log Matching),
- a new leader missing a committed entry (Leader Completeness),
- an index committed with two different entries, or a commit index moving backwards (State Machine
  Safety).

### `convoy/sim.py`
`Cluster(size, seed, faults=NetworkFaults(...))` builds nodes `n1..nN` on a simulated network.

- `tick()` advances time, delivers due messages and runs the checker.
- `crash(id)` and `restart(id)` kill a node and bring it back with only its storage.
- `partition([...], [...])` and `heal()` cut and restore connectivity.
- `wait_for_leader()`, `run_until(predicate)` and `propose(command)` drive tests.
- `dump()` prints every node's state and the recent event trace.

### `convoy/chaos.py`
`run_scenario(seed)` turns one seed into one full scenario: turbulence with random faults, calm,
client retries, then an audit that every replica is identical and every write appears exactly once.
Run it from the command line with `--seeds N` or `--replay SEED`.

### `convoy/kv.py`
`KVStateMachine` supports `put`, `get`, `delete`, `append` and `cas`. Commands that carry `client`
and `seq` are deduplicated: the state machine remembers each client's latest sequence number and
result, and answers a repeat from memory instead of running it twice.

### `convoy/server.py`
`ConvoyServer` wraps one node as a process.

- A tick loop calls `node.tick()` every 10 ms.
- One outgoing TCP connection per peer carries newline-delimited JSON. A broken connection drops its
  queue and reconnects, which Raft tolerates.
- A small HTTP/1.1 server takes client requests. A write waits until the entry at its index is
  applied. If a different entry lands there, leadership changed and the client gets a 503 and
  retries.
- Requests waiting at the same moment are proposed together with one fsync. This is group commit.

### `convoy/client.py`
`ConvoyClient(addresses)` tries the last known leader first, follows the `421` redirect to the real
leader, and retries on errors with the same sequence number. One request at a time per client.

### `convoy/local.py` and `convoy/cli.py`
`LocalCluster` starts and SIGKILLs real node processes for tests and benchmarks. `cli.py` provides
`convoy serve`, `up`, `put`, `get`, `delete` and `status`.

---

## Using the Cluster

### HTTP API
Every node serves the same API. Non-leaders answer `421` with the leader's address.

| method and path | meaning |
|---|---|
| `GET /status` | role, term, leader, last index, commit index |
| `PUT /kv/<key>` | body is the value |
| `GET /kv/<key>` | linearizable read, goes through the log |
| `DELETE /kv/<key>` | delete |
| `POST /command` | any JSON command, including `client` and `seq` for exactly-once |
| `GET /local/<key>` | this node's own applied value, no consensus. Possibly stale. For audits only. |

### Python client
```python
from convoy.client import ConvoyClient

client = ConvoyClient(["127.0.0.1:7002", "127.0.0.1:7004", "127.0.0.1:7006"])
client.put("greeting", "hello")
client.append("log", "a;")
client.cas("greeting", expect="hello", value="hi")
client.get("greeting")
```

### Running nodes by hand
```bash
python -m convoy.cli serve --id n1 \
  --cluster n1=127.0.0.1:7001:7002,n2=127.0.0.1:7003:7004,n3=127.0.0.1:7005:7006 \
  --data ./convoy-data/n1
```
The cluster string is `id=host:raft_port:http_port`, comma separated. Start one process per id.

---

## Testing Strategy

| file | what it proves |
|---|---|
| `tests/test_election.py` | voting rules, timer resets, minority partitions, stale leaders stepping down |
| `tests/test_replication.py` | conflict handling, duplicate messages, commit bounds, the Figure 8 case |
| `tests/test_storage.py` | durability, torn writes, corrupted records, a restarted node's vote |
| `tests/test_kv.py` | state machine ops and exactly-once writes |
| `tests/test_chaos.py` | hundreds of seeded scenarios, determinism, and the planted bugs |
| `tests/test_processes.py` | real processes: SIGKILL the leader, restart the cluster, lose a majority |

**Planted bugs.** `tests/mutants.py` rewrites the node's source to reintroduce one classic Raft bug
at a time, and the suite fails if the chaos scenarios do not catch it. The README lists the five bugs
and the scenario that first catches each.

**Long runs and replay.**
```bash
make chaos SEEDS=5000
make replay SEED=1234 SIZE=5
```
A failure prints its seed. Replaying it prints the node states and the event trace leading up to the
violation.

---

## Benchmarks

```bash
make bench-election   # leaderless time after a crash, simulator ticks
make bench-failover   # real processes: throughput and write outage on leader kill
```

The results and their caveats are in the README. Two caveats matter most when quoting them:

- All process numbers come from one laptop over loopback, with every node sharing one disk.
- Rows measured with fsync off are not durable and say nothing about a real deployment.

---

## Extending Convoy

These are left out on purpose. The README explains why.

- **Snapshots and log compaction.** Add an `InstallSnapshot` RPC, and teach storage to drop a log
  prefix. Every index calculation in `node.py` then needs an offset.
- **Membership changes.** Joint consensus is where implementations most often break. Add it only
  with new chaos scenarios that change membership mid-run.
- **Pre-vote and check-quorum.** Would stop a rejoining node from deposing a healthy leader. Affects
  availability, not safety.
- **ReadIndex or leases.** Would make reads cheaper than a full replication round.

When changing `node.py`, add a mutant to `tests/mutants.py` for any rule you touch, so the suite
proves it can catch that rule breaking.

---

## Troubleshooting

### `python3` fails with a syntax error
The system `python3` is 3.6. Use `make install`, which builds the venv from `python3.12`, then run
everything through `.venv/bin/python`.

### `Address already in use` from `make up`
A previous cluster is still running on ports 7001 to 7006.
```bash
pkill -f "convoy.cli serve"
```

### The client says no leader answered
Fewer than a majority of nodes are up. With three nodes, two must be running.

### A chaos seed fails
Replay it with `make replay SEED=<n> SIZE=<size>`. The dump shows each node's role, term, last index
and commit index, plus the recent events. Failures are deterministic, so the same seed always
reproduces the same trace.

### Node logs
`make up` writes one log per node, `n1.log` to `n3.log`, in the data directory. Role changes are
logged, which is usually enough to see an election happen.
