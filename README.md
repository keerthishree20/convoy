# Convoy

Raft consensus in Python, with a deterministic chaos simulator that checks
Raft's safety properties after every message, and a replicated key-value store
that runs as real processes. No dependencies.

```bash
make install
make test          # unit tests, chaos scenarios, planted bugs, real processes
make up            # three local nodes
.venv/bin/python -m convoy.cli put greeting hello
.venv/bin/python -m convoy.cli get greeting
```

The hard part of Raft is not writing it. It is knowing that it is right. Most
of this repository exists to answer that question.

---

## The design

### A node that does no I/O

`convoy/node.py` is Raft as a pure state machine. A driver calls `tick()` to
advance logical time, `step(message)` to deliver a message, and `propose()` to
submit a command. The node appends outgoing messages to an outbox and writes
its term, vote and log to a storage object before anything that depends on
them leaves. It has no clock, no sockets and no threads, and its only
randomness is a `random.Random` the driver hands it.

Two drivers use the same node, unchanged:

- **The simulator** (`convoy/sim.py`) runs a whole cluster in one process on a
  network that delays, reorders, drops and duplicates messages, partitions the
  cluster, and crashes and restarts nodes. Every random choice comes from one
  seed, so every run replays exactly.
- **The server** (`convoy/server.py`) puts the node on an asyncio event loop
  with a 10 ms tick, TCP between peers and HTTP for clients.

This split was decided before any code was written. Adding determinism to an
asyncio Raft afterwards means rewriting it.

### Checking safety, not just outcomes

`convoy/checker.py` watches the live cluster and raises the moment any of
Raft's guarantees breaks:

| property | checked as |
|---|---|
| Election Safety | no two nodes ever lead the same term, across the whole run |
| Log Matching | two logs that hold the same term at an index are identical up to it |
| Leader Completeness | a node taking office holds every entry committed before it |
| State Machine Safety | no index is ever committed with two different entries |
| Leader Append-Only | the node itself refuses to truncate an entry it has committed |

The cheap checks run after every delivered message. The pairwise log
comparison, which grows with log length, runs once per tick.

### Chaos scenarios

`convoy/chaos.py` turns one seed into one scenario:

1. **Turbulence.** Clients write while nodes crash and restart, the network
   partitions and heals, and 0-15% of messages are lost and 0-10% duplicated.
   Leaders are crashed deliberately more often than a uniform pick would,
   because leaders hold the half-replicated state where bugs live. Replication
   batch sizes vary from 1 to 64 entries so lagging followers stay lagging.
2. **Calm.** All faults stop. A leader must emerge and commit within a bound,
   or the scenario fails on liveness.
3. **Retries.** Every client resends every write the current leader does not
   hold, with the same client id and sequence number, through any further
   leader changes. Some of those writes did commit the first time, which is
   exactly the ambiguity a real client faces.
4. **Audit.** Every replica's data must be identical, and every write must
   appear exactly once.

```bash
make chaos SEEDS=5000
make replay SEED=1234        # one scenario, with its event trace
```

### Does the chaos suite find real bugs?

A test suite that never fails proves nothing on its own. `tests/mutants.py`
re-introduces one classic Raft bug at a time by editing the node's source, and
also removes the node's own guard against truncating committed entries, so
detection has to come from the checker and the audit. The suite fails the build
if any mutant survives.

Scenarios run on three nodes for 800 ticks, seeds counted from 0.

| planted bug | first caught | caught by |
|---|---|---|
| leader commits an earlier term's entry by counting replicas (Figure 8) | seed 342 | leader completeness |
| follower truncates on a delayed duplicate AppendEntries | seed 0 | liveness at seed 0; in 58 of the first 60 seeds, mostly as a commit index past the end of a log |
| vote granted by log length, ignoring last term | seed 0 | leader completeness |
| more than one vote per term | seed 2 | election safety |
| follower commit index runs past the entries the leader vouched for | seed 5 | state machine safety |

Figure 8 was the instructive one. The first version of the chaos plan ran 600
scenarios against that mutant and never caught it. The likely reason is that a
new leader's no-op entry went out in the same message as the older entries, so
the unsafe commit had no window. Crashing leaders more often and shrinking batch
sizes to as little as one entry made the mutant fail. The Figure 8 case also
has its own scripted unit test, taken from the paper's diagram.

### The replicated key-value store

Commands are `put`, `get`, `delete`, `append` and `cas`. Every write carries a
client id and sequence number. The state machine remembers each client's
latest sequence and its result, and answers a repeat from memory. That is what
makes a retry after a timeout safe, and the process tests show it matters:
with deduplication disabled, the leader-kill test reports a doubled write on
every run.

Reads go through the log, so they are linearizable. They also cost a full
replication round each. ReadIndex or leader leases would avoid that and are
not implemented.

### On disk

```
data/n1/
  meta.json    {"term": 7, "voted_for": "n2"}    rewritten whole: tmp, fsync, rename, fsync dir
  log          append-only records, cut back with ftruncate on conflict
```

A log record is a CRC32, a length, and a JSON payload. On open, the log is
scanned and cut at the first record whose checksum fails. That record was
never acknowledged, because nothing is acknowledged before the fsync that would
have completed it.

---

## Results

All numbers are from one laptop. The simulator numbers are in ticks. The
process numbers are three or five processes on the same machine over loopback,
sharing one disk, so there is no network latency and fsyncs from different
nodes contend with each other.

### Correctness

| | |
|---|---|
| chaos scenarios, 5 nodes, 1,500 ticks each | 3,000 of 3,000 passed |
| chaos scenarios, 3 nodes, 1,500 ticks each | 3,000 of 3,000 passed |
| writes audited across those scenarios | 2.7 million |
| planted bugs caught | 5 of 5 |
| tests, Python 3.10 and 3.12 | 82 passing |

### Leaderless time after the leader crashes, simulator

Election timeout 10-20 ticks, heartbeat 3 ticks, 2,000 seeds per row.

| nodes | network | p50 | p99 | max | terms to elect, mean |
|---|---|---|---|---|---|
| 3 | 1-3 tick delay | 21 | 88 | 132 | 1.62 |
| 3 | 10% loss, 1-6 tick delay | 37 | 162 | 257 | 2.55 |
| 5 | 1-3 tick delay | 19 | 60 | 86 | 1.36 |
| 5 | 10% loss, 1-6 tick delay | 32 | 144 | 265 | 2.31 |

Three-node clusters take more terms to elect than five-node ones. With one node
dead, only two remain, both must vote for the same candidate, and when they
time out close together each votes for itself. The extra terms in the last
column are those split votes. A wider timeout range would reduce them, at the
cost of slower failure detection.

### Write outage when the leader is SIGKILLed, real processes

Three nodes, eight clients writing, 20 kills. Election timeout 150-300 ms,
heartbeat 50 ms. The outage is the gap between the last write acknowledged
before the kill and the first after it.

| p50 | mean | max |
|---|---|---|
| 325 ms | 343 ms | 544 ms |

The floor is the election timeout itself: followers cannot tell a dead leader
from a slow one until it expires.

### Write throughput, real processes

Acknowledged writes per second over a five-second window, after a one-second
warm-up. Each client sends one 100-byte `put` at a time. In the fsync rows an
acknowledgement means the entry is fsynced on a majority and applied on the
leader. In the no-fsync rows it means only that it reached the page cache of a
majority.

| nodes | fsync | 1 client | 8 clients | 32 clients |
|---|---|---|---|---|
| 3 | on | 87 | 257 | 932 |
| 5 | on | 79 | 271 | 784 |
| 3 | off | 2,023 | 3,426 | 4,452 |
| 5 | off | 1,560 | 2,477 | 2,914 |

With fsync on, one client gets under 100 writes a second, because every write
waits for fsyncs on the leader and a follower. That is the cost of durability
on this disk, not a defect. More clients help because the server proposes all
requests waiting at one moment as a single batch with a single fsync, which is
group commit. Without fsync the limit is Python and JSON on one event loop, and
the numbers mean nothing for durability: a power cut would lose acknowledged
writes.

---

## Deliberately not implemented

- **Log compaction and snapshots.** The log grows forever, and a restarted node
  replays it from the start. InstallSnapshot is a well-defined addition, but
  it touches every index calculation in the node.
- **Membership changes.** The cluster is fixed at start. Joint consensus is
  where Raft implementations most often break, and a half-working version
  would be worse than none.
- **Pre-vote and check-quorum.** A node rejoining after a partition bumps the
  term and deposes a healthy leader once. That costs availability, never
  safety, and the chaos suite accepts it.
- **ReadIndex and leases.** Reads go through the log instead.

---

## Found along the way

- **Replication streams that multiplied.** The leader first sent more entries
  on every successful reply. Stale and duplicated replies each started another
  request stream to the same follower, and streams accumulated: 73,000
  messages for 660 ticks of a five-node cluster. Continuing only on a reply
  that advances the follower's match index cut that to 15,000 for 2,000 ticks.
- **A test client that could not retry.** The chaos harness first used one
  client id for hundreds of concurrent writes. The state machine keeps only the
  newest sequence per client, so retries of older writes were refused and the
  audit reported them lost. Real clients send one request at a time, so the
  harness now does too.
- **Leader changes during calm.** A node restarted at the end of the chaos
  phase could time out before hearing the new leader, win, and discard the
  previous leader's unreplicated writes. That is correct Raft. The harness was
  wrong to send each retry once, and now retries until the writes commit.

---

## Layout

```
convoy/
  node.py      Raft: election, replication, commitment (no I/O)
  messages.py  the four RPCs, and their wire form
  storage.py   term, vote and log; in memory or on disk
  checker.py   safety properties, checked on a live cluster
  sim.py       deterministic cluster on a hostile network
  chaos.py     seeded scenarios with an end-to-end audit
  kv.py        key-value state machine with exactly-once writes
  server.py    one node as a process: TCP to peers, HTTP to clients
  client.py    finds the leader, follows redirects, retries safely
  local.py     starts and kills local clusters
  cli.py       convoy serve | up | put | get | delete | status
tests/
  mutants.py   the planted bugs
bench/
  election.py  leaderless time, simulator
  failover.py  throughput and outage, real processes
```

The rules follow Figure 2 of "In Search of an Understandable Consensus
Algorithm" by Diego Ongaro and John Ousterhout, 2014.
