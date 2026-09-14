"""Randomised fault injection, and proof that it finds the bugs it claims to.

`make chaos` runs thousands of scenarios. The suite runs a few hundred, enough
to catch a regression in the common paths, in under a minute or two.
"""

from __future__ import annotations

import pytest

import convoy.sim
from convoy.chaos import ScenarioFailure, run_scenario
from convoy.checker import InvariantViolation, SafetyChecker
from convoy.sim import Cluster

from .mutants import MUTANTS, build
from .conftest import make_node


@pytest.mark.parametrize("size", [3, 5])
def test_chaos_scenarios_hold_every_invariant(size):
    for seed in range(150):
        run_scenario(seed, size=size, ticks=1000)


def test_the_same_seed_replays_the_same_run():
    assert run_scenario(99) == run_scenario(99)

    a, b = Cluster(5, seed=7), Cluster(5, seed=7)
    for cluster in (a, b):
        cluster.run(200)
    assert list(a.trace) == list(b.trace)
    assert [n.status() for n in a.live_nodes()] == [n.status() for n in b.live_nodes()]


@pytest.mark.parametrize("name", sorted(MUTANTS))
def test_the_chaos_suite_catches_a_planted_bug(name, monkeypatch):
    monkeypatch.setattr(convoy.sim, "Node", build(name))
    for size in (3, 5):
        for seed in range(1500):
            try:
                run_scenario(seed, size=size, ticks=800)
            except ScenarioFailure:
                return
    pytest.fail(f"mutant {name!r} ({MUTANTS[name][0]}) survived 3000 scenarios")


# ---- the checker catches what it says it does -------------------------------


def test_the_checker_rejects_two_leaders_in_one_term():
    a, b = make_node("n1", term=2), make_node("n2", term=2)
    a.role = b.role = a.role.__class__.LEADER
    with pytest.raises(InvariantViolation, match="election safety"):
        SafetyChecker().check([a, b])


def test_the_checker_rejects_logs_that_agree_on_a_term_but_not_before_it():
    a = make_node("n1", log=[(1, "x"), (2, "y")])
    b = make_node("n2", log=[(1, "DIFFERENT"), (2, "y")])
    with pytest.raises(InvariantViolation, match="log matching"):
        SafetyChecker().check([a, b])


def test_the_checker_rejects_a_changed_committed_entry():
    checker = SafetyChecker()
    a = make_node("n1", log=[(1, "x")])
    a.commit_index = 1
    checker.check([a])
    b = make_node("n2", log=[(2, "y")])
    b.commit_index = 1
    with pytest.raises(InvariantViolation, match="state machine safety"):
        checker.check([a, b], logs=False)
