"""Run a cluster of real Convoy processes on this machine.

Used by the process tests, the failover benchmark and `convoy up`. Each node is
a separate operating system process, so killing one with SIGKILL is a real
crash: no cleanup runs, and whatever was not fsynced is gone.
"""

from __future__ import annotations

import os
import pathlib
import signal
import socket
import subprocess
import sys
import time

from .client import ConvoyClient
from .server import Peer


def free_ports(count: int) -> list[int]:
    sockets = []
    try:
        for _ in range(count):
            s = socket.socket()
            s.bind(("127.0.0.1", 0))
            sockets.append(s)
        return [s.getsockname()[1] for s in sockets]
    finally:
        for s in sockets:
            s.close()


class LocalCluster:
    def __init__(self, size: int, data_root: str | os.PathLike, *, fsync: bool = True, base_port: int | None = None, log_dir=None):
        self.size = size
        self.data_root = pathlib.Path(data_root)
        self.fsync = fsync
        self.log_dir = pathlib.Path(log_dir) if log_dir else self.data_root
        ports = list(range(base_port, base_port + 2 * size)) if base_port else free_ports(2 * size)
        self.peers = {
            f"n{i + 1}": Peer(f"n{i + 1}", "127.0.0.1", ports[2 * i], ports[2 * i + 1]) for i in range(size)
        }
        self.spec = ",".join(f"{p.id}={p.host}:{p.raft_port}:{p.http_port}" for p in self.peers.values())
        self.processes: dict[str, subprocess.Popen] = {}

    @property
    def http_addresses(self) -> list[str]:
        return [p.http_address for p in self.peers.values()]

    def start(self, node_id: str | None = None) -> None:
        for nid in [node_id] if node_id else list(self.peers):
            if nid in self.processes and self.processes[nid].poll() is None:
                continue
            self.log_dir.mkdir(parents=True, exist_ok=True)
            out = open(self.log_dir / f"{nid}.log", "ab")
            command = [sys.executable, "-m", "convoy.cli", "serve", "--id", nid, "--cluster", self.spec,
                       "--data", str(self.data_root / nid)]
            if not self.fsync:
                command.append("--no-fsync")
            # Make `-m convoy.cli` importable from any working directory, installed or not.
            env = dict(os.environ)
            package_parent = str(pathlib.Path(__file__).resolve().parents[1])
            env["PYTHONPATH"] = os.pathsep.join(filter(None, [package_parent, env.get("PYTHONPATH")]))
            self.processes[nid] = subprocess.Popen(command, stdout=out, stderr=subprocess.STDOUT, env=env)

    def kill(self, node_id: str) -> None:
        process = self.processes.get(node_id)
        if process and process.poll() is None:
            process.send_signal(signal.SIGKILL)
            process.wait()

    def stop(self) -> None:
        for nid in list(self.processes):
            self.kill(nid)

    def status(self) -> dict[str, dict | None]:
        client = ConvoyClient([], timeout=0.5)
        try:
            return {nid: client.status(p.http_address) if self.processes.get(nid) and self.processes[nid].poll() is None else None
                    for nid, p in self.peers.items()}
        finally:
            client.close()

    def wait_for_leader(self, timeout: float = 15.0, exclude: set[str] = frozenset()) -> str:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            statuses = {nid: s for nid, s in self.status().items() if s and nid not in exclude}
            leaders = [nid for nid, s in statuses.items() if s["role"] == "leader"]
            if leaders:
                leader = max(leaders, key=lambda nid: statuses[nid]["term"])
                term = statuses[leader]["term"]
                agreeing = sum(1 for s in statuses.values() if s["leader"] == leader and s["term"] == term)
                if agreeing * 2 > self.size:
                    return leader
            time.sleep(0.05)
        raise TimeoutError(f"no leader within {timeout}s: {self.status()}")

    def __enter__(self) -> "LocalCluster":
        self.start()
        return self

    def __exit__(self, *exc) -> None:
        self.stop()
