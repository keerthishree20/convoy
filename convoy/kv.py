"""The replicated state machine: a key-value map with exactly-once client writes.

Raft makes every node apply the same commands in the same order. It does not
stop a client from submitting the same command twice. A client whose request
timed out cannot know whether the leader crashed before or after committing it,
so it retries, and a retried `append` would otherwise land twice.

Each command carries a client id and a sequence number. The state machine
remembers the highest sequence applied per client and the result it produced,
and answers a repeat from memory instead of executing it again. Because that
table is itself built by applying the log, every replica agrees on it.
"""

from __future__ import annotations

from typing import Any


class KVStateMachine:
    def __init__(self) -> None:
        self.data: dict[str, str] = {}
        self.sessions: dict[str, tuple[int, Any]] = {}
        self.applied_index = 0

    def apply(self, index: int, command: dict | None) -> Any:
        if index != self.applied_index + 1:
            raise ValueError(f"applied out of order: expected {self.applied_index + 1}, got {index}")
        self.applied_index = index
        if command is None:
            return None

        client, seq = command.get("client"), command.get("seq")
        if client is not None:
            last = self.sessions.get(client)
            if last is not None and seq <= last[0]:
                return last[1] if seq == last[0] else {"ok": False, "error": "stale sequence number"}

        result = self._execute(command)
        if client is not None:
            self.sessions[client] = (seq, result)
        return result

    def _execute(self, command: dict) -> Any:
        op, key = command["op"], command.get("key")
        if op == "put":
            previous = self.data.get(key)
            self.data[key] = command["value"]
            return {"ok": True, "previous": previous}
        if op == "append":
            self.data[key] = self.data.get(key, "") + command["value"]
            return {"ok": True, "value": self.data[key]}
        if op == "delete":
            return {"ok": True, "existed": self.data.pop(key, None) is not None}
        if op == "get":
            return {"ok": True, "value": self.data.get(key)}
        if op == "cas":
            if self.data.get(key) != command["expect"]:
                return {"ok": False, "value": self.data.get(key)}
            self.data[key] = command["value"]
            return {"ok": True}
        return {"ok": False, "error": f"unknown op {op!r}"}
