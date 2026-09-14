"""What a node must remember across a crash: its term, its vote, and its log.

Raft's safety argument assumes all three are on stable storage before the node
answers anyone. Forget a vote and a node can vote twice in one term. Forget a
log entry the leader counted towards a majority and a committed entry can
vanish. The node therefore calls into storage and only afterwards emits the
messages that depend on it.

Two implementations share one interface. `MemoryStorage` is the disk of a
simulated machine: the simulator keeps the object when it "crashes" a node and
hands it to the restarted one. `FileStorage` is the real thing.
"""

from __future__ import annotations

import json
import os
import pathlib
import struct
import zlib

from .messages import Entry


class Storage:
    """The interface. Log indexes are 1-based, as in the paper."""

    term: int
    voted_for: str | None
    log: list[Entry]

    def save_term_vote(self, term: int, voted_for: str | None) -> None:
        raise NotImplementedError

    def append(self, entries: list[Entry]) -> None:
        raise NotImplementedError

    def truncate(self, from_index: int) -> None:
        """Remove every entry at `from_index` and after."""
        raise NotImplementedError

    def close(self) -> None:
        pass


class MemoryStorage(Storage):
    def __init__(self) -> None:
        self.term = 0
        self.voted_for = None
        self.log = []

    def save_term_vote(self, term: int, voted_for: str | None) -> None:
        self.term = term
        self.voted_for = voted_for

    def append(self, entries: list[Entry]) -> None:
        self.log.extend(entries)

    def truncate(self, from_index: int) -> None:
        del self.log[from_index - 1 :]


# A log record: crc32 of everything after it, payload length, then JSON.
#   ┌──────────┬──────────┬──────────────────────────┐
#   │ crc32  4 │ len    4 │ {"t": term, "c": command} │
#   └──────────┴──────────┴──────────────────────────┘
_HEADER = struct.Struct(">II")


class FileStorage(Storage):
    """A directory holding `meta.json` and an append-only `log` file.

    The metadata is small and rewritten whole: write a temporary file, fsync
    it, rename it over the old one, fsync the directory. A crash leaves either
    the old version or the new one, never half of each.

    The log only ever grows at the end or is cut back with `ftruncate`, so the
    only damage a crash can do is a torn final record. Opening scans the file,
    stops at the first record whose checksum does not match, and cuts the file
    there. That record was never acknowledged, because nothing is acknowledged
    before the fsync that would have completed it.
    """

    def __init__(self, directory: str | os.PathLike, *, fsync: bool = True) -> None:
        self.directory = pathlib.Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.fsync = fsync
        self._meta_path = self.directory / "meta.json"
        self._log_path = self.directory / "log"

        self.term, self.voted_for = 0, None
        if self._meta_path.exists():
            meta = json.loads(self._meta_path.read_text())
            self.term, self.voted_for = meta["term"], meta["voted_for"]

        self.log = []
        self._offsets: list[int] = []  # byte offset where each entry's record starts
        self.recovered_torn_bytes = 0
        self._file = open(self._log_path, "a+b")
        self._load_log()

    def _load_log(self) -> None:
        self._file.seek(0)
        data = self._file.read()
        pos = 0
        while pos + _HEADER.size <= len(data):
            crc, length = _HEADER.unpack_from(data, pos)
            start, end = pos + _HEADER.size, pos + _HEADER.size + length
            if end > len(data):
                break
            payload = data[start:end]
            if zlib.crc32(struct.pack(">I", length) + payload) != crc:
                break
            record = json.loads(payload)
            self._offsets.append(pos)
            self.log.append(Entry(record["t"], record["c"]))
            pos = end
        if pos != len(data):
            self.recovered_torn_bytes = len(data) - pos
            self._file.truncate(pos)
            self._sync_file()

    def _sync_file(self) -> None:
        self._file.flush()
        if self.fsync:
            os.fsync(self._file.fileno())

    def save_term_vote(self, term: int, voted_for: str | None) -> None:
        if (term, voted_for) == (self.term, self.voted_for):
            return
        tmp = self._meta_path.with_suffix(".tmp")
        with open(tmp, "w") as f:
            json.dump({"term": term, "voted_for": voted_for}, f)
            f.flush()
            if self.fsync:
                os.fsync(f.fileno())
        os.replace(tmp, self._meta_path)
        if self.fsync:
            dir_fd = os.open(self.directory, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        self.term, self.voted_for = term, voted_for

    def append(self, entries: list[Entry]) -> None:
        if not entries:
            return
        self._file.seek(0, os.SEEK_END)
        pos = self._file.tell()
        chunks = []
        for entry in entries:
            payload = json.dumps({"t": entry.term, "c": entry.command}, separators=(",", ":")).encode()
            length = struct.pack(">I", len(payload))
            chunks.append(struct.pack(">I", zlib.crc32(length + payload)) + length + payload)
            self._offsets.append(pos)
            pos += len(chunks[-1])
        self._file.write(b"".join(chunks))
        self._sync_file()
        self.log.extend(entries)

    def truncate(self, from_index: int) -> None:
        if from_index > len(self.log):
            return
        self._file.truncate(self._offsets[from_index - 1])
        self._sync_file()
        del self.log[from_index - 1 :]
        del self._offsets[from_index - 1 :]

    def close(self) -> None:
        self._file.close()
