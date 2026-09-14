# Convoy. `make install && make test`

PY      ?= .venv/bin/python
VENV_PY ?= python3.12          # NOT python3: that is 3.6 on some machines
SEEDS   ?= 5000

.PHONY: help install test test-fast test-processes chaos replay bench bench-election bench-failover up clean

help:
	@grep -E '^[a-z-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-16s %s\n", $$1, $$2}'

install: ## create .venv and install pytest (Convoy itself has no dependencies)
	$(VENV_PY) -m venv .venv
	$(PY) -m pip install -q --upgrade pip
	$(PY) -m pip install -q -r requirements-dev.txt

test: ## the whole suite: unit, chaos, planted bugs, real processes
	$(PY) -m pytest -q

test-fast: ## everything except the tests that start processes
	$(PY) -m pytest -q --ignore=tests/test_processes.py

test-processes: ## only the tests that start and kill real nodes
	$(PY) -m pytest -q tests/test_processes.py

chaos: ## SEEDS scenarios on 5 nodes, then on 3
	$(PY) -m convoy.chaos --seeds $(SEEDS) --size 5
	$(PY) -m convoy.chaos --seeds $(SEEDS) --size 3

replay: ## one scenario with its event trace: make replay SEED=1234 SIZE=5
	$(PY) -m convoy.chaos --replay $(SEED) --size $(or $(SIZE),5)

bench: bench-election bench-failover ## every benchmark

bench-election: ## leaderless time after a crash, in simulator ticks
	$(PY) -m bench.election --seeds 2000

bench-failover: ## real processes: throughput with and without fsync, outage on leader kill
	$(PY) -m bench.failover --throughput --failover --seconds 5 --kills 20

up: ## three local nodes on ports 7001-7006; try `convoy put a 1` in another shell
	$(PY) -m convoy.cli up --size 3

clean:
	rm -rf .venv .pytest_cache convoy-data **/__pycache__ *.egg-info
