# Meridian Semiconductor, Fab 3 — six cosmonic:kafka workloads on Cosmonic Desktop.
# `make up` is the whole bring-up; the rest are the demo beats.
SHELL := /bin/bash
.DEFAULT_GOAL := help

.PHONY: help up down purge build validate start stop shift-change poison drift excursion calm status \
        refuse naive robust broker-restart rollout-st03 probe lint

help: ## this list
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[1m%-16s\033[0m %s\n", $$1, $$2}'

up: ## bring everything up and validate it (scripts/run.sh; flags via RUN_FLAGS="--naive --no-build …")
	@scripts/run.sh $(RUN_FLAGS)
down: ## stop the workloads and the broker (keeps the broker volume)
	@scripts/down.sh
purge: ## stop everything and remove the broker volume
	@scripts/down.sh --purge
build: ## cargo build every workload (no deploy)
	@for w in workloads/*/; do echo "== $$w"; (cd $$w && cargo build --target wasm32-wasip2 --release) || exit 1; done
validate: ## produced == consumed + dlq, every group's lag, the read_committed duplicate check
	@scripts/validate.sh

start: ## beat 1: the quiet-Tuesday loop (idempotent)
	@scripts/sim.sh start
stop: ## stop the loop
	@scripts/sim.sh stop
shift-change: ## beat 3: 100x lots + 2x mold telemetry for 60 s, then baseline by itself
	@scripts/sim.sh shift-change
poison: ## beat 4: one NaN thickness → Err(Permanent) → dieattach.dlq
	@scripts/sim.sh poison
drift: ## beat 5: bonder 17 sags 18% for 90 s → NSOP → seek/replay
	@scripts/sim.sh drift
excursion: ## beat 5: 20x inspection jobs for 120 s → ST-04 instances climb toward 12
	@scripts/sim.sh excursion
calm: ## back to baseline early
	@scripts/sim.sh calm
status: ## the simulator's last heartbeat
	@scripts/sim.sh status

refuse: ## beat 2: a host-only key refused by name; a grant missing its DLQ fails the bind permanently
	@scripts/beats.sh refuse
naive: ## beat 4: swap ST-02 for the build that panics on the poison record (after `make up RUN_FLAGS=--naive` once)
	@scripts/beats.sh naive
robust: ## beat 4: swap the Permanent build back in
	@scripts/beats.sh robust
broker-restart: ## beat 6: restart the broker mid-batch and watch the line recover
	@scripts/beats.sh broker-restart
rollout-st03: ## beat 7: re-apply ST-03 with NSOP_THRESHOLD_PCT=$(PCT) (default 12)
	@scripts/beats.sh rollout-st03 $(PCT)
probe: ## one probe record through ST-02 (proves the pipeline is live)
	@scripts/beats.sh probe

lint: ## shellcheck scripts/*.sh
	@shellcheck -x scripts/*.sh
