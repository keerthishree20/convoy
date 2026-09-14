"""One Convoy node as a real process: Raft over TCP, the key-value API over HTTP.

The node itself is the same deterministic `Node` the simulator drives. This
module only replaces the simulator's fake clock and fake network:

- **Clock.** A task calls `tick()` every `tick_ms` milliseconds.
- **Peers.** One TCP connection per peer, carrying newline-delimited JSON
  messages. Raft already tolerates lost messages, so a broken connection just
  drops what was queued on it and reconnects in the background.
- **Clients.** A small HTTP/1.1 server. A write is proposed, and the response
  waits until the entry at that index is applied. If a different entry lands
  there, leadership changed underneath the request and the client is told to
  retry, which is safe because every command carries a client id and sequence
  number the state machine deduplicates on.

Everything runs on one event loop, so the node is never touched from two
places at once. Storage fsyncs block that loop. That is deliberate and it is
the throughput limit: a follower must not acknowledge an entry before it is on
disk, and requests that queue up meanwhile are proposed together (group commit).

Reads go through the log too. That makes them linearizable at the cost of a
round of replication per read; ReadIndex or leader leases would avoid it, and
neither is implemented.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
from dataclasses import dataclass
from urllib.parse import unquote

from .kv import KVStateMachine
from .messages import from_wire, to_wire
from .node import Node, NotLeader, Role
from .storage import FileStorage

log = logging.getLogger("convoy")


@dataclass(frozen=True)
class Peer:
    id: str
    host: str
    raft_port: int
    http_port: int

    @property
    def http_address(self) -> str:
        return f"{self.host}:{self.http_port}"


def parse_cluster(spec: str) -> dict[str, Peer]:
    """`n1=127.0.0.1:7001:8001,n2=...` as id=host:raft_port:http_port."""
    peers = {}
    for item in filter(None, spec.split(",")):
        node_id, address = item.split("=")
        host, raft_port, http_port = address.rsplit(":", 2)
        peers[node_id] = Peer(node_id, host, int(raft_port), int(http_port))
    return peers


class LeadershipLost(Exception):
    pass


class ConvoyServer:
    def __init__(
        self,
        node_id: str,
        cluster: dict[str, Peer],
        data_dir: str,
        *,
        tick_ms: float = 10,
        election_ticks: tuple[int, int] = (15, 30),
        heartbeat_ticks: int = 5,
        fsync: bool = True,
        request_timeout: float = 5.0,
    ) -> None:
        self.me = cluster[node_id]
        self.cluster = cluster
        self.tick_ms = tick_ms
        self.request_timeout = request_timeout
        self.storage = FileStorage(data_dir, fsync=fsync)
        self.node = Node(
            node_id,
            list(cluster),
            self.storage,
            random.Random(),
            election_ticks=election_ticks,
            heartbeat_ticks=heartbeat_ticks,
        )
        self.kv = KVStateMachine()
        self._pending: dict[int, tuple[int, asyncio.Future]] = {}
        self._proposals: list[tuple[dict, asyncio.Future]] = []
        self._proposal_wakeup = asyncio.Event()
        self._outgoing: dict[str, asyncio.Queue] = {p: asyncio.Queue(maxsize=10_000) for p in self.node.peers}
        self._tasks: list[asyncio.Task] = []
        self._servers: list[asyncio.base_events.Server] = []
        self._role = self.node.role

    # ---- lifecycle --------------------------------------------------------

    async def start(self) -> None:
        self._servers.append(await asyncio.start_server(self._serve_peer, self.me.host, self.me.raft_port))
        self._servers.append(await asyncio.start_server(self._serve_http, self.me.host, self.me.http_port))
        self._tasks.append(asyncio.create_task(self._tick_loop()))
        self._tasks.append(asyncio.create_task(self._proposal_loop()))
        for peer in self.node.peers:
            self._tasks.append(asyncio.create_task(self._peer_sender(self.cluster[peer])))
        log.info("%s up: raft %s, http %s", self.me.id, self.me.raft_port, self.me.http_port)

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        for server in self._servers:
            server.close()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self.storage.close()

    # ---- driving the node -------------------------------------------------

    def _after_node_call(self) -> None:
        for message in self.node.outbox:
            queue = self._outgoing.get(message.dst)
            if queue is not None and not queue.full():
                queue.put_nowait(message)
        self.node.outbox.clear()

        for index, entry in self.node.take_committed():
            result = self.kv.apply(index, entry.command)
            waiting = self._pending.pop(index, None)
            if waiting is not None:
                term, future = waiting
                if future.done():
                    continue
                if term == entry.term:
                    future.set_result(result)
                else:
                    future.set_exception(LeadershipLost())

        if self.node.role is not self._role:
            log.info("%s is now %s in term %d", self.me.id, self.node.role.value, self.node.term)
            if self._role is Role.LEADER:
                # The entries may still commit under the next leader. The client
                # cannot know, so it retries with the same sequence number.
                for _, future in self._pending.values():
                    if not future.done():
                        future.set_exception(LeadershipLost())
                self._pending.clear()
            self._role = self.node.role

    async def _tick_loop(self) -> None:
        loop = asyncio.get_running_loop()
        interval = self.tick_ms / 1000
        next_at = loop.time()
        while True:
            next_at += interval
            await asyncio.sleep(max(0.0, next_at - loop.time()))
            self.node.tick()
            self._after_node_call()

    async def _proposal_loop(self) -> None:
        while True:
            await self._proposal_wakeup.wait()
            self._proposal_wakeup.clear()
            await asyncio.sleep(0)  # let requests arriving in this instant join the batch
            batch, self._proposals = self._proposals, []
            batch = [(c, f) for c, f in batch if not f.done()]
            if not batch:
                continue
            try:
                placed = self.node.propose_many([c for c, _ in batch])
            except NotLeader as exc:
                for _, future in batch:
                    future.set_exception(exc)
                continue
            for (index, term), (_, future) in zip(placed, batch):
                self._pending[index] = (term, future)
            self._after_node_call()

    async def submit(self, command: dict):
        if self.node.role is not Role.LEADER:
            raise NotLeader(self.node.leader_id)
        future = asyncio.get_running_loop().create_future()
        self._proposals.append((command, future))
        self._proposal_wakeup.set()
        return await asyncio.wait_for(future, self.request_timeout)

    # ---- peer transport ---------------------------------------------------

    async def _peer_sender(self, peer: Peer) -> None:
        queue = self._outgoing[peer.id]
        while True:
            try:
                _, writer = await asyncio.open_connection(peer.host, peer.raft_port)
            except OSError:
                await asyncio.sleep(0.05)
                self._drain(queue)
                continue
            try:
                while True:
                    message = await queue.get()
                    chunks = [json.dumps(to_wire(message), separators=(",", ":")).encode() + b"\n"]
                    while not queue.empty():
                        chunks.append(json.dumps(to_wire(queue.get_nowait()), separators=(",", ":")).encode() + b"\n")
                    writer.write(b"".join(chunks))
                    await writer.drain()
            except (OSError, ConnectionError):
                pass
            finally:
                writer.close()

    @staticmethod
    def _drain(queue: asyncio.Queue) -> None:
        # A peer that is down gets nothing queued for it. Stale messages would
        # arrive in a burst on reconnect and the next heartbeat covers them.
        while not queue.empty():
            queue.get_nowait()

    async def _serve_peer(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while line := await reader.readline():
                self.node.step(from_wire(json.loads(line)))
                self._after_node_call()
        except (OSError, ConnectionError, ValueError):
            pass
        finally:
            writer.close()

    # ---- HTTP -------------------------------------------------------------

    async def _serve_http(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                request_line = await reader.readline()
                if not request_line:
                    return
                method, target, _ = request_line.decode("latin-1").split(" ", 2)
                headers = {}
                while (line := await reader.readline()) not in (b"\r\n", b"\n", b""):
                    name, _, value = line.decode("latin-1").partition(":")
                    headers[name.strip().lower()] = value.strip()
                body = await reader.readexactly(int(headers.get("content-length", 0)))
                status, payload = await self._route(method, target, body)
                data = json.dumps(payload).encode()
                writer.write(
                    f"HTTP/1.1 {status}\r\nContent-Type: application/json\r\n"
                    f"Content-Length: {len(data)}\r\n\r\n".encode() + data
                )
                await writer.drain()
        except (OSError, ConnectionError, ValueError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()

    async def _route(self, method: str, target: str, body: bytes) -> tuple[str, dict]:
        path = target.split("?", 1)[0]
        if method == "GET" and path == "/status":
            status = self.node.status()
            status["keys"] = len(self.kv.data)
            return "200 OK", status
        if method == "GET" and path.startswith("/local/"):
            # This node's own applied value, with no consensus round. Possibly
            # stale; for auditing replicas, never for application reads.
            key = unquote(path[len("/local/") :])
            return "200 OK", {"value": self.kv.data.get(key), "applied": self.kv.applied_index}

        if method == "POST" and path == "/command":
            try:
                command = json.loads(body)
            except ValueError:
                return "400 Bad Request", {"error": "body must be a JSON command"}
        elif path.startswith("/kv/"):
            key = unquote(path[len("/kv/") :])
            if method == "GET":
                command = {"op": "get", "key": key}
            elif method == "PUT":
                command = {"op": "put", "key": key, "value": body.decode()}
            elif method == "DELETE":
                command = {"op": "delete", "key": key}
            else:
                return "405 Method Not Allowed", {"error": method}
        else:
            return "404 Not Found", {"error": path}

        try:
            return "200 OK", await self.submit(command)
        except NotLeader as exc:
            leader = self.cluster.get(exc.leader_id) if exc.leader_id else None
            return "421 Misdirected Request", {
                "error": "not the leader",
                "leader": exc.leader_id,
                "leader_http": leader.http_address if leader else None,
            }
        except LeadershipLost:
            return "503 Service Unavailable", {"error": "leadership changed; retry with the same sequence number"}
        except asyncio.TimeoutError:
            return "504 Gateway Timeout", {"error": "not committed in time; retry with the same sequence number"}


async def serve(node_id: str, cluster: dict[str, Peer], data_dir: str, **options) -> None:
    server = ConvoyServer(node_id, cluster, data_dir, **options)
    await server.start()
    try:
        await asyncio.Event().wait()
    finally:
        await server.stop()
