"""The four RPCs of Raft, as plain values.

Nothing here knows about sockets. The node consumes these and produces these,
and a driver (the simulator or the network runtime) decides how they travel.
Every message carries its sender's term, because the first thing any receiver
does is compare terms.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, NamedTuple, Union


class Entry(NamedTuple):
    """One log entry. A tuple, so comparing two logs is a C-level list compare.

    `command` is whatever the state machine understands, and must survive a JSON
    round trip. `None` is the no-op a new leader appends to commit its
    predecessors' entries.
    """

    term: int
    command: Any


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
    """The answer to one AppendEntries.

    On success, `match_index` is the last index the follower now holds that is
    known to agree with the sender: `prev_index + len(entries)`, and not the end
    of the follower's log, which may still carry a stale tail.

    On failure, `match_index` is a hint for where the leader should retry, so a
    follower that is far behind is found in a handful of round trips rather
    than one per missing entry.
    """

    term: int
    src: str
    dst: str
    success: bool
    match_index: int


Message = Union[RequestVote, VoteReply, AppendEntries, AppendReply]

_TYPES = {cls.__name__: cls for cls in (RequestVote, VoteReply, AppendEntries, AppendReply)}


def to_wire(message: Message) -> dict:
    body = asdict(message)
    body["type"] = type(message).__name__
    if isinstance(message, AppendEntries):
        body["entries"] = [[e.term, e.command] for e in message.entries]
    return body


def from_wire(body: dict) -> Message:
    body = dict(body)
    cls = _TYPES[body.pop("type")]
    if cls is AppendEntries:
        body["entries"] = tuple(Entry(t, c) for t, c in body["entries"])
    return cls(**body)
