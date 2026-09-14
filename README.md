# kafka-factory-fab-demo — the Fab 3 assembly line

Meridian Semiconductor, Fab 3 — a fictional back-end line (six stations) whose
data path is six `cosmonic:kafka@0.5.0` WebAssembly workloads running in
**Cosmonic Desktop** against a local **Redpanda**, fed by a simulator workload
that plays scripted sensor records onto the topics, and watched through a
live line dashboard. One command brings it all up and validates it.

The story, the station → template mapping, the dashboard layout and the demo
beats are in **[docs/design.html](docs/design.html)**; the presenter's
script — what to say, run and point at, beat by beat — is
**[docs/DEMO-SCRIPT.md](docs/DEMO-SCRIPT.md)**. The build brief is
[PLAN.md](PLAN.md); every place the build deviates from it is in
[docs/DECISIONS.md](docs/DECISIONS.md); what the build taught us about the
templates, the plugin, the daemon and the skill is in
[docs/LEARNINGS.md](docs/LEARNINGS.md).

| Station | Template | Topics | Pattern in one line |
|---|---|---|---|
| ST-01 Wafer probe intake | `rust-kafka-http-producer` | HTTP → `probe.lots` | `POST /lot` → one `send_batch`, one line per wafer; bad schema is a 400 before anything is produced |
| ST-02 Die attach | `rust-kafka-handler-consumer` + producer | `dieattach.readings` → `dieattach.flags`, DLQ `dieattach.dlq` | spec checks per record; malformed → `Err(Permanent)`; `Ok(Some(offset))` keeps the work done |
| ST-03 Wire bond | `rust-kafka-pull-service` | `wirebond.raw` → `wirebond.metrics`, own DLQ | 5-s windows per bonder, explicit commit, rebalances answered, NSOP → `seek` and replay |
| ST-04 AOI & X-ray | handler, `handler.batch.size: 1` | `inspect.jobs` → `inspect.results` | one job per call, 200–400 ms of real CPU; the 0→32 pool is the story |
| ST-05 Mold & cure | handler, `handler.batch.size: 1000` | `mold.telemetry` → `mold.profile` | one batch = one window; cure-profile conformance per press (stream-processor stand-in) |
| ST-06 Final test | `rust-kafka-transactional` | `finaltest.bins` → `lot.disposition` + `inventory.moves` | exactly-once: outputs + offsets in one transaction; `replicas: 1` |
| ST-06 twin | `rust-kafka-pull-service` | → `lot.disposition.twin` + `inventory.moves.twin` | the same, at-least-once, so a replay shows duplicates next to the zero |
| factory-simulator | `rust-kafka-pull-service` shape | `sim.control` → every input topic | plays static scenario frames on a 1-s tick, forever; driven by one Kafka topic |
| line-dashboard | handler + `wasi:http` | `line.metrics` (+ DLQs, ledgers) → `http://fab-line-dashboard.localhost:8200/` | folds the one instrumentation path into six panels; `/validate` for the scripts |

## Bring it up

You need: **Cosmonic Desktop** (0.5.28+, running), **Docker** (or Podman),
**Rust** with the `wasm32-wasip2` target, `wasm-tools >= 1.250`,
`wasm-component-ld >= 0.5.27` (`cargo install wasm-tools wasm-component-ld`),
`wash`, `python3` with PyYAML (or ruby), `shellcheck` for `make lint`.
`make up` checks all of it first and prints an install hint for anything
missing.

```sh
git clone https://github.com/cosmonic-labs/kafka-factory-fab-demo
cd kafka-factory-fab-demo
make up            # ≈ 2 min the first time (cargo), ≈ 1 min after
```

`make up` runs [scripts/run.sh](scripts/run.sh): preflight → Desktop reachable
→ Kafka plugin present → Redpanda up (compose) → 21 topics created and
verified → build, `wasm-tools` checks and publish of every workload →
manifests validated → applied in order → the simulator loop started → one
real lot posted through ST-01 → `scripts/validate.sh` (produced == consumed +
dlq per station, every group's lag from `rpk group describe`, no duplicate
unit ids in `lot.disposition` read `read_committed`). It ends with the
dashboard URL and the demo beats. Every step prints PASS/FAIL with the
sentence the failing tool gave. It is idempotent; rerun it any time.

Flags (`make up RUN_FLAGS="…"`): `--no-build` applies the committed
`manifests/`, which pin the prebuilt images on
`ghcr.io/cosmonic-labs/kafka-factory-fab-demo/fab-*` (no Rust toolchain
needed — the fastest way to run the demo), `--naive` swaps ST-02 for the build
that panics on the poison record, `--no-sim` leaves the loop stopped,
`--use-kafka-yaml` puts the broker in Desktop's `kafka.yaml` default instead
of the manifests (needs a daemon restart), `--purge` starts from an empty
broker.

Open **http://fab-line-dashboard.localhost:8200/** — six panels in floor order, a
summary strip, a faults ledger, polling `/api/state` every 2 s.

Two more screens come with the broker: **Redpanda Console** at
http://localhost:8090/ (topics, a message browser that shows the DLQ
records' origin headers, consumer groups with lag per partition), and — after
`make metrics` — **Grafana** at http://localhost:3000/d/fab3-line (records/s
produced and fetched per topic, lag per group, broker latency, bytes/s; plus
Redpanda's own broker dashboard at `/d/fab3-redpanda`). Prometheus scrapes
the broker's `/public_metrics` every 5 s.

Every workload is named `fab-<station>` and labeled
`app.kubernetes.io/part-of: fab-factory` (plus `app.kubernetes.io/name`,
`app.kubernetes.io/version` and `fab.meridian.example/station`), so in
Desktop's Workloads grid a search for `fab-` or `fab-factory` shows just the
line, and `station=st02` finds one station.

## The demo (about 12 minutes; docs/design.html §5)

| Time | Beat | Command | What to watch |
|---|---|---|---|
| 00:00 | Quiet Tuesday | `make start` (already running after `make up`) | every panel's numbers move; `make validate` reads lag 0 everywhere |
| 01:30 | The binding is the whole client config | `make refuse` | a manifest with `plugin.library.paths` is refused **by name**; a grant that misses its DLQ fails the bind in the plugin's own words |
| 03:00 | Shift change | `make shift-change` | 100× lots and 2× mold telemetry for 60 s, then baseline by itself; ST-01's latency does not move |
| 05:00 | Poison pill, two ways | `make poison` · then `make naive; make poison; make robust` | one NaN thickness → `Ok(Some(offset))` partial resume → `Err(Permanent)` → `dieattach.dlq` with `x-dlq-original-*` headers. The naive build traps five times, then the host dead-letters it with *its* reason |
| 06:30 | Bond excursion → inspection backlog | `make drift` · `make excursion` | bonder 17 lights up (−16 %), ST-03 seeks the partition back 60 s and replays; ST-04's live instances climb toward 12 (its partitions) and fall back |
| 09:00 | Restart the broker mid-batch | `make broker-restart` · `make crash-st06` | the silent gap, every group back in ~50 s, a probe record proves it; then both ST-06 workers restarted mid-batch: the twin ledger gains duplicates, `lot.disposition` gains none |
| 11:00 | Rolling update | `make rollout-st03` | ST-03 re-applied with `NSOP_THRESHOLD_PCT=12`; the stable `consumer.group.id` resumes where it stopped; `make validate` still balances |

`make status` shows the simulator's heartbeat; `make stop` / `make start`
stop and resume the loop; `make down` removes the workloads and stops the
broker (volume kept); `make purge` removes the volume too.

## What is live and what is mocked

| Thing | On Desktop today |
|---|---|
| Six stations, simulator, dashboard, DLQs, transactions, twin ledger, lag | **Live** |
| Handler instances 0→32 per replica (`instance` heartbeats) | **Live** — counted from heartbeats every 10 s of record time, not from the daemon API |
| Replica window min/max (ST-04, ST-05) | **Static** — a Control concept; the `scaling:` block is a proposal. The slider draws the configured min/max with the current count fixed at 1 and a "Control only" label |
| ST-05 windowed stream state | **Approximated** by `handler.batch.size: 1000` (one batch = one window, no cross-batch state); the proposed `stream-handler` export is the real shape |
| Broker restart recovery (beat 6) | **Live** — a Redpanda restart reconnects without ending a session; `make crash-st06` is what forces a replay |
| Rolling update (beat 7) | **Live** — a re-apply of ST-03 with a new env value |
| Consumer lag on the panels | **From the script** — `make validate` reads `rpk group describe` and publishes it to `line.metrics`; no workload computes lag |

## Layout

```
workloads/          one scaffold per workload (cosmonic new rust-kafka-<pattern>), src/lib.rs rewritten;
                    deploy/workload.yaml is the station's Workload (the source run.sh digest-pins)
  factory-simulator/data/<scenario>/<topic>.jsonl   the static record sets (generate.py regenerates them)
  line-dashboard/ui/index.html                      the page: docs/design.html §6, live
manifests/          digest-pinned Workload per station on ghcr.io (what --no-build applies); local/ holds a local build's (gitignored); kafka.yaml.example; refused/
scripts/            run.sh, down.sh, topics.sh, sim.sh, validate.sh, beats.sh, lib.sh
compose/            single-node Redpanda on 127.0.0.1:9092 + Redpanda Console; metrics/ (Prometheus + Grafana, --profile metrics)
docs/               design.html, icons.svg, DEMO-SCRIPT.md, DECISIONS.md, LEARNINGS.md
.github/workflows/  cargo build + wasm-tools checks per workload, shellcheck, the topic contract against Redpanda
```

## Notes

- Workloads are loaded through the daemon's socket API (`scripts/lib.sh`):
  Control's `cosmo` CLI is not part of a Desktop install. The path in use is
  printed at the top of every run.
- The transactional world's named import needs `wasm-component-ld >= 0.5.27`;
  rustc prefers its own bundled (older) copy, so `st06-final-test/.cargo/`
  carries a linker shim. `make up` checks the versions.
- `make release` (scripts/release.sh) pushes every built component to
  `ghcr.io/cosmonic-labs/kafka-factory-fab-demo/<name>:0.1.0`, attaches the
  packages to this repository, and re-pins `manifests/` to the new digests.
  A package's visibility is a GitHub UI setting (there is no API for it).
- CI builds every workload and tests the topic contract against a Redpanda
  service container. No job runs Cosmonic Desktop: there is no headless
  daemon in CI today.
- Fictional company and line; every sensor value is synthetic.

Apache-2.0.
