"""A client that finds the leader, follows redirects and retries safely.

Every write carries this client's id and a sequence number that only advances
once the previous request got a definite answer. Retrying the same request
after a timeout, a dropped connection or a leader change is therefore always
safe: if the first attempt did commit, the state machine replays its result
instead of executing it again.

One request at a time per client object. That is what makes a single
"highest sequence seen" per client enough on the server side.
"""

from __future__ import annotations

import http.client
import json
import time
import uuid
from urllib.parse import quote


class Unavailable(Exception):
    pass


class ConvoyClient:
    def __init__(self, addresses: list[str], *, timeout: float = 2.0, give_up_after: float = 30.0, client_id: str | None = None):
        self.addresses = list(addresses)
        self.timeout = timeout
        self.give_up_after = give_up_after
        self.client_id = client_id or uuid.uuid4().hex[:12]
        self.seq = 0
        self._leader: str | None = None
        self._conns: dict[str, http.client.HTTPConnection] = {}
        self.retries = 0

    def put(self, key: str, value: str) -> dict:
        return self.command({"op": "put", "key": key, "value": value})

    def append(self, key: str, value: str) -> dict:
        return self.command({"op": "append", "key": key, "value": value})

    def delete(self, key: str) -> dict:
        return self.command({"op": "delete", "key": key})

    def cas(self, key: str, expect: str | None, value: str) -> dict:
        return self.command({"op": "cas", "key": key, "expect": expect, "value": value})

    def get(self, key: str) -> str | None:
        return self.command({"op": "get", "key": key}, write=False)["value"]

    def command(self, command: dict, *, write: bool = True) -> dict:
        if write:
            self.seq += 1
            command = {**command, "client": self.client_id, "seq": self.seq}
        body = json.dumps(command).encode()
        deadline = time.monotonic() + self.give_up_after
        candidates = self._order()
        while True:
            address = candidates.pop(0) if candidates else None
            if address is None:
                if time.monotonic() > deadline:
                    raise Unavailable(f"no leader answered within {self.give_up_after}s")
                time.sleep(0.05)
                candidates = self._order()
                continue
            status, payload = self._post(address, "/command", body)
            if status == 200:
                self._leader = address
                return payload
            self.retries += 1
            if status == 421 and payload.get("leader_http") and payload["leader_http"] != address:
                candidates.insert(0, payload["leader_http"])
            if time.monotonic() > deadline:
                raise Unavailable(f"gave up after {self.give_up_after}s: last answer {status} {payload}")

    def status(self, address: str) -> dict | None:
        status, payload = self._request(address, "GET", "/status", None)
        return payload if status == 200 else None

    def _order(self) -> list[str]:
        others = [a for a in self.addresses if a != self._leader]
        return ([self._leader] if self._leader else []) + others

    def _post(self, address: str, path: str, body: bytes) -> tuple[int, dict]:
        return self._request(address, "POST", path, body)

    def _request(self, address: str, method: str, path: str, body: bytes | None) -> tuple[int, dict]:
        for attempt in range(2):  # once more on a fresh connection if a kept-alive one went stale
            conn = self._conns.get(address)
            if conn is None:
                host, port = address.rsplit(":", 1)
                conn = self._conns[address] = http.client.HTTPConnection(host, int(port), timeout=self.timeout)
            try:
                conn.request(method, path, body=body, headers={"Content-Type": "application/json"})
                response = conn.getresponse()
                return response.status, json.loads(response.read() or b"{}")
            except (OSError, http.client.HTTPException, ValueError):
                conn.close()
                self._conns.pop(address, None)
                if attempt == 1:
                    return 0, {"error": "connection failed"}
        return 0, {}

    def close(self) -> None:
        for conn in self._conns.values():
            conn.close()
        self._conns.clear()


def quote_key(key: str) -> str:
    return quote(key, safe="")
