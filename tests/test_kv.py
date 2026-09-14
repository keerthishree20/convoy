"""The key-value state machine, and exactly-once writes for retrying clients."""

from __future__ import annotations

import pytest

from convoy import KVStateMachine


def applier():
    kv = KVStateMachine()
    counter = iter(range(1, 10_000))
    return kv, lambda command: kv.apply(next(counter), command)


def test_put_get_delete():
    kv, apply = applier()
    assert apply({"op": "put", "key": "a", "value": "1"}) == {"ok": True, "previous": None}
    assert apply({"op": "get", "key": "a"}) == {"ok": True, "value": "1"}
    assert apply({"op": "delete", "key": "a"}) == {"ok": True, "existed": True}
    assert apply({"op": "delete", "key": "a"}) == {"ok": True, "existed": False}
    assert kv.data == {}


def test_compare_and_set_only_writes_on_a_match():
    kv, apply = applier()
    apply({"op": "put", "key": "a", "value": "1"})
    assert apply({"op": "cas", "key": "a", "expect": "2", "value": "3"}) == {"ok": False, "value": "1"}
    assert apply({"op": "cas", "key": "a", "expect": "1", "value": "3"}) == {"ok": True}
    assert kv.data["a"] == "3"


def test_no_ops_change_nothing_but_still_advance_the_applied_index():
    kv, apply = applier()
    assert apply(None) is None
    assert kv.applied_index == 1 and kv.data == {}


def test_a_retried_write_is_answered_from_memory_not_applied_twice():
    kv, apply = applier()
    command = {"op": "append", "key": "a", "value": "x", "client": "c1", "seq": 1}
    first = apply(command)
    again = apply(dict(command))
    assert first == again == {"ok": True, "value": "x"}
    assert kv.data["a"] == "x"


def test_an_older_sequence_number_is_refused():
    kv, apply = applier()
    apply({"op": "put", "key": "a", "value": "new", "client": "c1", "seq": 5})
    result = apply({"op": "put", "key": "a", "value": "old", "client": "c1", "seq": 4})
    assert result["ok"] is False and kv.data["a"] == "new"


def test_clients_are_deduplicated_independently():
    kv, apply = applier()
    apply({"op": "append", "key": "a", "value": "1", "client": "c1", "seq": 1})
    apply({"op": "append", "key": "a", "value": "2", "client": "c2", "seq": 1})
    assert kv.data["a"] == "12"


def test_entries_must_be_applied_in_order():
    kv = KVStateMachine()
    with pytest.raises(ValueError):
        kv.apply(2, None)
