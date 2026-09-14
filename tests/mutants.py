"""Deliberately broken copies of the node, one classic Raft bug each.

A chaos suite that never fails proves nothing on its own: it might simply not
be looking. Each mutant here re-introduces a real mistake by editing the node's
source text, and the suite is expected to catch every one of them.

Each mutant also disables the node's own guard against truncating a committed
entry, so that detection has to come from the independent checker and the
client audit, not from the code under test policing itself.
"""

from __future__ import annotations

import inspect
import types

import convoy.node

_GUARD = ("if index <= self.commit_index:", "if False:")

MUTANTS: dict[str, tuple[str, list[tuple[str, str]]]] = {
    "figure-8": (
        "a leader commits an entry from an earlier term by counting replicas",
        [("if self.term_at(n) != self.term:\n                break", "if False:\n                break")],
    ),
    "truncate-on-duplicate": (
        "a follower truncates at the first entry it already holds, even when terms match",
        [
            (
                "if self.term_at(index) == entry.term:\n                    continue",
                "pass",
            )
        ],
    ),
    "vote-by-length": (
        "a voter compares log lengths and ignores the last term",
        [("(m.last_log_term, m.last_log_index) >= (my_last_term, self.last_index)", "m.last_log_index >= self.last_index")],
    ),
    "double-vote": (
        "a voter grants every candidate in a term, not just the first",
        [("self.voted_for in (None, m.src)", "True")],
    ),
    "commit-past-last-new": (
        "a follower's commit index runs to the end of its log, over a stale tail",
        [("min(m.commit, last_new)", "min(m.commit, self.last_index)")],
    ),
}


def build(name: str) -> type:
    """The Node class with mutant `name` applied."""
    _, edits = MUTANTS[name]
    source = inspect.getsource(convoy.node)
    for old, new in edits + [_GUARD]:
        if old not in source:
            raise ValueError(f"mutant {name}: {old!r} no longer appears in node.py")
        source = source.replace(old, new)
    module = types.ModuleType(f"convoy.node_mutant_{name.replace('-', '_')}")
    module.__dict__["__package__"] = "convoy"
    exec(compile(source, f"<mutant {name}>", "exec"), module.__dict__)
    # Share the real module's types, so `role is Role.LEADER` in the simulator
    # and checker still recognises a mutant's leader.
    for shared in ("Role", "NotLeader", "SafetyViolation"):
        setattr(module, shared, getattr(convoy.node, shared))
    return module.Node
