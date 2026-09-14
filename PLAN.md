# factory-fab-assembly-line — build brief for Claude Code

Target repo: `github.com/cosmonic-labs/factory-fab-assembly-line` (hyphens — the org's
convention: `awesome-cosmonic`, `mcp-server-template-rs`, `nats-2.8-testing`).

This brief is the whole task. Read it top to bottom before writing anything, then read the
sources of truth it names. The design doc (`docs/design.html`) is the story, the station →
template mapping, the dashboard layout and the demo script; this file is what to build so
that story runs on a laptop.

## 1. Goal

A public, self-contained demo repo: a fictional semiconductor back-end line (Meridian
Semiconductor, Fab 3 — six stations) whose data path is six `cosmonic:kafka@0.5.0`
WebAssembly workloads running in **Cosmonic Desktop** against a local **Redpanda**, fed by a
**simulator workload** that puts static, scripted sensor records on the topics, and watched
through a **line dashboard** workload with one panel per station. One script brings it all
up and validates it.

Everything must build and run from a clean checkout on a machine that has Cosmonic Desktop
installed and Docker (or Podman) available. No private dependencies, no cloud.

## 2. Sources of truth — read these first, in this order

1. `docs/design.html` in this repo (the design doc; open it in a browser). Section 3 is the
   alignment table every workload, topic and dashboard panel is one row of. Section 7 is a
   recap of the skill; section 8 lists what changed since the old README — do not build
   against the old README.
2. The `cosmonic-kafka` skill served by Cosmonic Desktop's MCP server (`skills/list` →
   `skill://cosmonic-kafka/SKILL.md`, then `references/patterns.md`,
   `references/cosmonic-kafka.md`, `references/troubleshooting.md`). The same files live in
   `cosmonic/agent-integrations` under `core/skills/cosmonic-kafka/` (v1.2.0). **The skill's
   hard rules are non-negotiable:** the guest never dials or configures Kafka; `topics` is
   the grant and must cover every subscription, the DLQ and every produce target;
   `handler.group.id` and `dead-letter.topic` are required; malformed input is
   `Err(Permanent)`, never a panic; async-only WASI p3 (wit-bindgen ≥ 0.58).
3. The four shipped starters — scaffold each with the Desktop MCP tool
   `cosmonic_project_create(template="rust-kafka-<pattern>")` or `cosmonic new
   rust-kafka-<pattern>` — and **start every station from one of them**. Keep the world
   name the scaffold gives you (only the crate is renamed); the header comment in
   `src/lib.rs` states the pattern's semantics.
4. `cosmonic-sandbox` skill (the build → dev → publish → apply loop) and Desktop's
   `daemon/docs/KAFKA.md` (where the broker comes from; `kafka.yaml`; the apply-time guard).

If anything in this brief contradicts the skill or the shipped scaffolds, the skill and the
scaffolds win — note the discrepancy in `docs/DECISIONS.md` and move on.

## 3. Repository layout

```
factory-fab-assembly-line/
  README.md                     what it is, one-command bring-up, the demo script, what is live vs mocked
  LICENSE                       Apache-2.0
  PLAN.md                       this file
  docs/
    design.html                 the design doc (ship as-is; it is the shareable artifact)
    icons.svg                   the sensor + station symbol library extracted from design.html
    DECISIONS.md                every place you deviated from this brief or the design doc, and why
  scripts/
    run.sh                      bring-up + validation (section 6)
    down.sh                     stop workloads, stop the broker container (keep volumes unless --purge)
    topics.sh                   create/verify topics (called by run.sh; idempotent)
    sim.sh                      drive the simulator: sim.sh start|stop|shift-change|poison|drift|excursion|calm|status
    validate.sh                 the produced == consumed + lag check (called by run.sh; runnable alone)
    lib.sh                      shared: cosmo invocation, daemon-socket fallback, rpk-in-container, colored output
  compose/
    redpanda.yaml               docker compose: single-node Redpanda, port 127.0.0.1:9092, rpk inside the container
  workloads/
    st01-probe-intake/          rust-kafka-http-producer
    st02-die-attach/            rust-kafka-handler-consumer + producer
    st03-wire-bond/             rust-kafka-pull-service
    st04-inspection/            rust-kafka-handler-consumer, handler.batch.size "1"   (work-queue shape)
    st05-mold-cure/             rust-kafka-handler-consumer, handler.batch.size "1000" (stream-processor stand-in)
    st06-final-test/            rust-kafka-transactional
    factory-simulator/          rust-kafka-pull-service shape: loops static scenario frames onto the topics; driven by sim.control
    line-dashboard/             rust-http shape + a cosmonic:kafka handler on line.metrics; serves the dashboard page
  manifests/
    *.workload.yaml             one committed Workload per station (the `cosmonic publish` draft, image digest-pinned)
    kafka.yaml.example          the optional Desktop default binding (section 6.3)
  Makefile                      make up / make down / make build / make validate / make start|stop|shift-change|poison|drift|excursion|calm
  .github/workflows/ci.yml     cargo build --target wasm32-wasip2 for every workload + wasm-tools checks (section 7)
```

Every workload directory is a scaffold from the matching starter with its `.wash/config.yaml`,
`workload.yaml`, `deploy/workload-deployment.yaml`, `wit/`, `wkg.lock`, `wkg-registries.toml`
kept, and `src/lib.rs` rewritten for the station.

## 4. Topics

Create exactly these, with these partition counts (the plugin creates none):

| Topic | Partitions | Written by | Read by |
|---|---|---|---|
| `probe.lots` | 6 | st01 (HTTP → `send_batch`) | dashboard (counts only) |
| `dieattach.readings` | 6 | simulator | st02 |
| `dieattach.flags` | 3 | st02 (`producer::send`) | dashboard |
| `dieattach.dlq` | 1 | host (st02 `Permanent`) | dashboard |
| `wirebond.raw` | 12 | simulator | st03 |
| `wirebond.metrics` | 6 | st03 | dashboard |
| `wirebond.dlq` | 1 | st03 (own DLQ routing) | dashboard |
| `inspect.jobs` | 12 | simulator | st04 |
| `inspect.results` | 6 | st04 | dashboard |
| `inspect.dlq` | 1 | host (st04 `Permanent`) | dashboard |
| `mold.telemetry` | 8 | simulator | st05 |
| `mold.profile` | 4 | st05 | dashboard |
| `mold.dlq` | 1 | host | dashboard |
| `finaltest.bins` | 6 | simulator | st06 |
| `lot.disposition` | 3 | st06 (in the transaction) | dashboard (`read_committed`) |
| `inventory.moves` | 3 | st06 (in the transaction) | dashboard (`read_committed`) |
| `line.metrics` | 3 | every station + simulator | dashboard |
| `sim.control` | 1 | `scripts/sim.sh` (rpk) | factory-simulator |
| `lot.disposition.twin`, `inventory.moves.twin` | 3 each | st06 twin | dashboard |

`line.metrics` is the one instrumentation path: every station emits small JSON records
(`{"station":"st02","kind":"consumed","n":100,"ts":…}`, `kind ∈ consumed | produced | dlq |
redelivered | partial | instance | window | txn`), the simulator emits `kind: produced`
per topic, and the dashboard folds them. Each handler instance emits one `kind: instance`
record with a random id the first time it is called (package-level state as a cache — the
skill's rule); the dashboard counts distinct ids seen in the last 30 s as "live instances",
which is the honest way to show the pool breathing since Desktop's API exposes no
per-workload instance count.

## 5. Workloads

Grants are spelled out because the skill says a binding without the full grant is refused at
bind. Every manifest carries `bootstrap.servers: 127.0.0.1:9092` and
`broker.address.family: v4` like the shipped scaffolds (section 6.3 explains the optional
`kafka.yaml` route). `allowedHosts: []` everywhere.

### st01-probe-intake — `rust-kafka-http-producer`
- Keep the scaffold's `wasi:http/handler@0.3.0` + `producer` shape. Add `POST /lot` taking
  a JSON lot (lot id, wafer records) → one `producer::send_batch("probe.lots", …)`; respond
  one line per wafer (`ok` / error code) exactly like the scaffold's `/produce-batch`.
  Reject a lot that fails schema with 400 before producing anything.
- Grant: `topics: probe.lots,line.metrics`. `poolSize: 4`.
- Emit `produced` + a `latency` metric (upload → last ack) to `line.metrics`.

### st02-die-attach — `rust-kafka-handler-consumer` + producer
- Handler over `dieattach.readings`. Per record: parse, check bond-line thickness /
  epoxy weight / placement offset against fixed spec limits, and `producer::send` an
  out-of-spec flag to `dieattach.flags`. Malformed (NaN, missing field) → return
  `Ok(Some(last_good_offset))` if anything succeeded, else `Err(Permanent(..))`. Never panic.
- Add `import cosmonic:kafka/producer@0.5.0;` to the world; `interfaces: [handler, producer]`.
- Grant: `topics: dieattach.readings,dieattach.dlq,dieattach.flags,line.metrics`;
  `handler.topics: dieattach.readings`; `handler.group.id: st02-die-attach`;
  `dead-letter.topic: dieattach.dlq`; `auto.offset.reset: earliest`.
- Keep the scaffold's `poolSize: 32`, `maxConcurrency: 1`, `reclaimWindowSeconds: 30`,
  `reclaimMinInstances: 0`.
- Also build **`st02-die-attach-naive`** (a feature flag or a second crate) that panics on
  the same malformed record — beat 4 of the demo shows the five-trap path next to the
  `Permanent` path. Ship it disabled; `run.sh --naive` swaps it in.

### st03-wire-bond — `rust-kafka-pull-service`
- `wasi:cli/run` service over `wirebond.raw` → `wirebond.metrics`. 5-second windows per
  bonder id (40 bonders) computing mean/σ of ultrasonic power, bond force, and an NSOP
  flag when power sags > 15 % against the bonder's trailing mean. Explicit
  `commit(vec![])` after a window's outputs are acked. Read `rebalances()` and flush open
  windows on revoke. On an NSOP alarm, `seek` that partition back ~60 s once and replay
  (guard against loops with a per-bonder "replayed" flag).
- Env contract from the scaffold: `IN_TOPIC=wirebond.raw`, `OUT_TOPIC=wirebond.metrics`,
  `DLQ_TOPIC=wirebond.dlq`, `BATCH_SIZE=100`.
- Grant: `topics: wirebond.raw,wirebond.metrics,wirebond.dlq,line.metrics`;
  `consumer.group.id: st03-wire-bond`; `enable.auto.commit: "false"`.
- Windows need a clock: use the timestamp on the consumed record (`rec.timestamp`), not a
  wall clock, so the service needs no extra WASI import.

### st04-inspection — `rust-kafka-handler-consumer` + producer (work-queue shape)
- Handler over `inspect.jobs`, `handler.batch.size: "1"` — one job per call, a verdict per
  job. "Work" is a deterministic CPU loop sized by the job's declared cost (200–400 ms) so
  latency is real; result (defect class, void %) to `inspect.results`.
- Grant: `topics: inspect.jobs,inspect.dlq,inspect.results,line.metrics`;
  `handler.group.id: st04-inspection`; `dead-letter.topic: inspect.dlq`.
- Same pool settings as st02. Emit `instance` heartbeats — this is the panel where the
  0→32 pool is the story.
- The design doc's `scaling:` block (replica scaling) is a **proposal** and Desktop is a
  single host; do not implement it. The dashboard draws the replica window from a static
  `min/max` in the panel config with "Control only" beside it (section 8).

### st05-mold-cure — `rust-kafka-handler-consumer` + producer (stream-processor stand-in)
- Handler over `mold.telemetry`, `handler.batch.size: "1000"`. One batch of one partition
  ≈ one window: compute per-press cure-profile conformance (8 zones against 175 ± 2 °C)
  and zone drift over the batch, produce one summary to `mold.profile`. No cross-batch
  state (the skill: required state cannot live in a handler instance) — say so in
  `DECISIONS.md`; the proposed `stream-handler` export is what would replace this.
- Grant: `topics: mold.telemetry,mold.dlq,mold.profile,line.metrics`;
  `handler.group.id: st05-mold-cure`; `dead-letter.topic: mold.dlq`;
  `max.poll.interval.ms: "900000"`.

### st06-final-test — `rust-kafka-transactional`
- Two bindings exactly as the scaffold: unnamed `[consumer]` over `finaltest.bins` with
  `consumer.group.id: st06-final-test`, `enable.auto.commit: "false"`; `name: transaction`
  `[transaction]` with `topics: finaltest.bins,lot.disposition,inventory.moves`,
  `transactional.id: st06-final-test-txn-1`, `transaction.group.id: st06-final-test`.
- Per batch (`BATCH_SIZE=100`): `begin` → `txn.send_batch("lot.disposition", …)` →
  `txn.send_batch("inventory.moves", …)` → `txn.send_offsets(one past last per partition,
  leader epoch carried)` → `commit`; on failure check `txn_requires_abort` / `fatal`,
  `abort`, exit so the supervisor restarts from committed offsets.
- `replicas: 1` in `deploy/`. Never more.
- Also build **`st06-final-test-twin`** from `rust-kafka-pull-service` writing the same
  outputs to `lot.disposition.twin` / `inventory.moves.twin` (add those two topics, 3
  partitions each). Beat 6 of the demo compares the ledgers. Ship it enabled.

### factory-simulator — `rust-kafka-pull-service` shape, looping (the demo's "start")
- This is "what sends the test messages", and it runs **on its own** once started: a
  `wasi:cli/run` service (scaffold from `rust-kafka-pull-service`; keep its world and add
  `import wasi:clocks/monotonic-clock@0.3.0;` — it is already in the scaffold's vendored
  `wit/deps`) that plays a **static, committed** record set onto the input topics on a
  1-second tick, forever, until told to stop. Timestamps in the data are relative offsets
  rebased to "now" each pass so st03/st05 windows behave.
- **Control is a Kafka topic**, `sim.control` (1 partition), so the simulator needs no
  HTTP surface and the demo driver is one shell script. The service owns one consumer
  session on `sim.control` (`consumer.group.id: factory-simulator`,
  `auto.offset.reset: latest`) and uses the binding's producer for the data topics. Loop:
  `futures::select!` between the control `records()` stream and
  `monotonic_clock::wait_for(1s)`; on a tick play the next 1-second frame of the current
  loop scenario plus any one-shot overlay; on a control record change state. Each frame's
  per-topic counts go to `line.metrics` as `kind: produced` so `validate.sh` can compare.
- **Scenarios — named for the demo, invoked as `scripts/sim.sh <verb>` and `make <verb>`:**

  | Verb | Control record | What plays |
  |---|---|---|
  | `start` | `{"cmd":"loop","scenario":"baseline"}` | The quiet-Tuesday loop on every input topic (+ one lot to st01 every 90 s). Idempotent; this is the first thing you run for a demo and you leave it running. |
  | `stop` | `{"cmd":"stop"}` | Stops the loop. |
  | `shift-change` | `{"cmd":"loop","scenario":"shift-change"}` | Switches the loop: 100× lots for 60 s, 2× mold telemetry, then the loop returns to `baseline` by itself (a scenario declares its own duration and `next`). |
  | `poison` | `{"cmd":"play","scenario":"poison"}` | One-shot overlay on top of the loop: one `dieattach.readings` record with `"thickness_um": "NaN"`. |
  | `drift` | `{"cmd":"play","scenario":"drift"}` | One-shot 90 s overlay: bonder 17's ultrasonic power sags 18 %. |
  | `excursion` | `{"cmd":"play","scenario":"excursion"}` | One-shot 120 s overlay: 20× `inspect.jobs`. |
  | `calm` | `{"cmd":"loop","scenario":"baseline"}` | Back to baseline early (same as `start`). |
  | `status` | — | Reads the simulator's last `line.metrics` heartbeat (`kind: sim`, carries current scenario + frame) and prints it. |

  `sim.sh` produces the record with `rpk topic produce sim.control` inside the Redpanda
  container. `run.sh` ends by calling `sim.sh start` unless `--no-sim`. The design doc's
  demo beats map 1:1: `start` → beat 1, `shift-change` → 3, `poison` → 4, `drift` +
  `excursion` → 5, `docker compose restart redpanda` → 6, re-publish st03 → 7.
- The simulator is itself a Kafka consumer, so the broker restart in beat 6 hits it too:
  it must survive the silent gap (the control stream ends → reopen the session and keep
  looping the last scenario; never exit on a consumer error, log and retry after 5 s).
- Record sets live in `workloads/factory-simulator/data/<scenario>/<topic>.jsonl` (frame
  number as the first field), embedded with `include_str!`. Keys: die id / bonder id /
  job id / press id / unit id so partition spread is realistic. Baseline is 60 frames and
  wraps. Emit a `kind: sim` heartbeat to `line.metrics` every 5 s with
  `{scenario, frame, overlay}` — the dashboard's summary strip shows it.
- st01's lot uploads are HTTP, so the simulator produces to `probe.lots` directly (same
  static lot records st01 would have produced) and the run script separately curls one
  real lot through st01's `POST /lot` during validation so that path is exercised too.
- Grant: `topics: sim.control,probe.lots,dieattach.readings,wirebond.raw,inspect.jobs,mold.telemetry,finaltest.bins,line.metrics`;
  `enable.auto.commit: "false"` (commit the control offset after acting on it).
- Optional second binary, `factory-simulator-http` from `rust-kafka-http-producer`:
  `POST /scenario/<name>` plays one pass synchronously. Useful for CI and for someone
  without `rpk`; not needed for the demo.

### line-dashboard — HTTP page + `line.metrics` consumer
- One workload, two entries: a `wasi:http` handler serving the dashboard page and a
  `cosmonic:kafka` handler over `line.metrics` (plus the output/DLQ topics it summarizes —
  or have the stations put every summary on `line.metrics` and subscribe to that alone;
  prefer the single-topic design). The skill refuses two components exporting `handler`
  in one workload, so this is ONE component exporting both `wasi:http/handler` and
  `cosmonic:kafka/handler`; if that composition does not build cleanly, split into
  `line-dashboard-web` (HTTP) and `line-dashboard-fold` (handler writing a rolled-up JSON
  document to a keyvalue/blobstore the web half reads) and record it in `DECISIONS.md`.
- State: in-memory ring per station (last 4 h at 1-minute resolution is enough) — it is a
  demo; a restart starts the panels empty and they refill.
- The page is `docs/design.html` section 6 made live: summary strip, six panels in floor
  order, faults ledger. Reuse the panel structure, the palette tokens, the fonts and
  `docs/icons.svg` verbatim (the icons are approved — do not redraw). Poll
  `GET /api/state` every 2 s; no websockets. Set the `<title>` to `Fab 3 Line`.
- `GET /validate` returns `{station: {produced, consumed, dlq, redelivered, lag}}`; lag
  comes from `rpk group describe` run by `validate.sh`, not from the workload — the
  workload reports the counts, the script adds lag and diffs.

## 6. `scripts/run.sh` — bring-up and validation

Idempotent; safe to rerun; every step prints PASS/FAIL with the sentence the failing tool
gave. Flags: `--naive` (swap st02 for the panicking build), `--no-build` (apply committed
manifests only), `--no-sim` (don't start the simulator loop), `--use-kafka-yaml`
(section 6.3), `--purge`.

**Workloads are loaded with the Control `cosmo` CLI**, pointed at the local Cosmonic
Desktop host. The first thing to do in this repo is `cosmo --help` on the machine and write
down in `scripts/lib.sh` the exact subcommands for: selecting/authenticating the target
(the local Desktop host), applying a Workload manifest, listing workloads with their
state, tailing a workload's logs, and deleting a workload. Wrap each in one `lib.sh`
function (`cosmo_apply`, `cosmo_list`, `cosmo_logs`, `cosmo_delete`) so the rest of the
scripts never spell a `cosmo` argument twice. If `cosmo` cannot target a Desktop host from
this machine, or lacks one of those verbs, fall back for that verb only to the daemon's
socket API (`POST /v1/workloads/validate`, `POST /v1/workloads`, `GET /v1/workloads`,
`GET /v1/kafka`, `POST /v1/kafka/test` over `curl --unix-socket $(cosmonicd paths …)`),
say so in `DECISIONS.md`, and print which path is in use at the top of every run. Never
silently mix the two for the same verb.

1. **Preflight.** `docker` or `podman` on PATH; `cosmo` on PATH and able to reach the
   Desktop host; `cosmonic` on PATH (ships with Desktop) and `cosmonic new --list` shows the
   four `rust-kafka-*` ids; `rustup target list --installed` includes `wasm32-wasip2`;
   `wasm-tools` and `wash` present (install hints on failure).
2. **Host reachable.** `cosmo_list` succeeds against Desktop; print the host version.
   Fail with "start Cosmonic Desktop" otherwise.
3. **Kafka plugin present.** The host reports `cosmonic:kafka@0.5.0` (via `cosmo`'s host
   or capability listing if it has one, else `GET /v1/kafka`). Fail with the skill's
   sentence ("a daemon built without the `kafka` feature") otherwise.
4. **Broker up.** `docker compose -f compose/redpanda.yaml up -d`; wait for
   `rpk cluster health` inside the container to report healthy (timeout 60 s).
5. **Topics.** `scripts/topics.sh` creates every topic in section 4 with its partition count
   and then verifies with `rpk topic describe` that each exists with exactly that count;
   a topic with the wrong count is a FAIL (say how to fix: `down.sh --purge`).
6. **Desktop can dial the broker.** `POST /v1/kafka/test` with `draft: true` and the same
   `bootstrap.servers`/`broker.address.family` the manifests carry; the response must
   report the cluster answered (print broker id / topic count it saw). This is "Cosmonic
   Desktop is configured to attach to this". (Socket API — `cosmo` has no equivalent
   unless `cosmo --help` says otherwise.)
7. **Build + publish** each workload (unless `--no-build`): `cosmonic publish` from each
   directory pushes to Desktop's built-in registry and prints a digest-pinned Workload
   draft; write it to `manifests/<name>.workload.yaml`. Before publishing, run the
   skill's checks and fail on a miss:
   `wasm-tools component wit <out>.wasm | grep -E 'cosmonic:kafka/(handler|producer|consumer|transaction)@0.5.0'`
   and `wasm-tools print <out>.wasm | grep -qE 'async-lift|task-return'`.
8. **Validate manifests.** `cosmo`'s dry-run/validate if it has one, else
   `POST /v1/workloads/validate`; a refusal prints the daemon's sentence (it names the key)
   and fails the run.
9. **Apply** with `cosmo_apply` in this order: line-dashboard, factory-simulator,
   st01…st06 (+ twin). Poll `cosmo_list` until every one is `running`; a `failed` state
   prints its message (bind refusals are permanent — the script must not retry them).
10. **Smoke.** `scripts/sim.sh start` (the baseline loop), wait 20 s, curl one real lot
    through st01's `POST /lot`, then `scripts/validate.sh`: for each station, produced
    (simulator/st01 counts on `line.metrics`) == consumed (station counts) + dlq, and
    `rpk group describe <group>` lag == 0 for every group (`factory-simulator`,
    `st02-die-attach`, `st03-wire-bond`, `st04-inspection`, `st05-mold-cure`,
    `st06-final-test`, `st06-final-test-twin`, `line-dashboard`). For st06 also check that
    `lot.disposition` read with `isolation.level=read_committed` has no duplicate unit ids.
    The loop keeps running after validation unless `--no-sim`.
11. **Print** the dashboard URL (the workload's `wasi:http` host, e.g.
    `http://line-dashboard.localhost:8200/`), the `sim.sh` verbs, and the demo beats
    from the design doc with their timings and the verb each one needs.

### 6.3 Broker configuration on Desktop — two routes, pick the default deliberately

- **Default: the manifest carries the broker** (`bootstrap.servers: 127.0.0.1:9092`), like
  the shipped scaffolds. Zero configuration, no daemon restart, and exactly what the skill
  documents.
- **Optional `--use-kafka-yaml`:** write `manifests/kafka.yaml.example` to
  `<state_dir>/kafka.yaml` (the daemon's `config:` base: `bootstrap.servers`,
  `broker.address.family`) via `PUT /v1/kafka`, strip `bootstrap.servers` from the
  manifests, and tell the user the defaults take effect on the next daemon restart — then
  step 6's test uses `draft: false, name: ""` to test the SAVED default binding. This is the
  "platform team sets the broker once" story in the design doc; keep it optional because
  it needs a restart.

Never put a credential value in a manifest or in `kafka.yaml`; the local Redpanda runs
without SASL. If you add SASL to the compose file, register the password as a Desktop
secret ref named `SASL_PASSWORD` and reference it with `secretFrom` (the skill: it arrives
as `sasl.password`).

## 7. CI (`.github/workflows/ci.yml`)

- `cargo build --release --target wasm32-wasip2` for every workload (matrix), then the two
  `wasm-tools` checks from step 7 as separate steps so a failure names the workload.
- `shellcheck` on `scripts/*.sh`.
- A job that starts Redpanda as a service container and runs `scripts/topics.sh` +
  `rpk topic describe` — the topic contract is tested even where Desktop is not.
- No job runs Cosmonic Desktop in CI (there is no headless daemon in CI today); say so in
  the README.

## 8. What is live and what is mocked — be explicit in the README

| Thing | On Desktop today |
|---|---|
| Six stations, simulator, dashboard, DLQs, transactions, twin ledger, lag | **Live** |
| Handler instances 0→32 per replica (`instance` heartbeats) | **Live** (counted from heartbeats, not from the daemon API) |
| Replica window min/max (ST-04, ST-05) | **Static** — a Control concept and, as a `scaling:` block, a proposal; the slider draws the configured min/max with the current count fixed at 1 and a "Control only" label |
| ST-05 windowed stream state | **Approximated** by `handler.batch.size: 1000`; the proposed `stream-handler` export is the real shape |
| Broker restart recovery (beat 6) | **Live** — `docker compose restart redpanda`; expect the silent gap the skill describes, then recovery; the probe record proves it |
| Rolling update (beat 7) | **Live** on Desktop as a re-publish + re-apply of st03 with a new NSOP threshold env value |

## 9. Definition of done

- `git clone … && make up` on a machine with Cosmonic Desktop + Docker ends with every
  station `running`, every validation step PASS, the simulator looping baseline, and a
  dashboard URL that shows six panels with numbers moving — without typing anything else.
- `make poison` puts exactly one record in `dieattach.dlq`, ST-02's panel shows it with
  the origin headers, and `make validate` still passes (produced == consumed + dlq).
- `make excursion` shows ST-04's live-instance count climb toward 12 and fall back to 0
  after the reclaim window; `make shift-change` returns to baseline on its own.
- `docker compose restart redpanda` while the loop runs: the simulator resumes by itself,
  `lot.disposition` has no duplicate unit ids while the twin's ledger has some; `make
  validate` reports both.
- `make stop` stops the loop; `make start` resumes it.
- `docs/DECISIONS.md` lists every deviation. `README.md` has the live/mocked table.
- CI green.

## 10. Working notes for the agent

- Scaffold first, then edit. `cosmonic new rust-kafka-handler-consumer workloads/st02-die-attach --name st02-die-attach`.
  Diff against the untouched scaffold before publishing anything so the binding keys the
  scaffold ships are all still there.
- Use `cosmonic dev` in a station directory while iterating (hot reload against the same
  broker); `run.sh` is for the full bring-up.
- Read a refused apply or bind sentence before touching code; the daemon names the key.
- Every produce target in a grant. Every DLQ in a grant. Every subscription in a grant.
  This is the failure you will hit most.
- Don't open a consumer per invocation, don't drain `records()` in a request-scoped call,
  don't keep required state in a handler instance, don't panic on input.
- Commit `manifests/*.workload.yaml` after a successful publish so `--no-build` works for
  someone without a Rust toolchain.
- `cosmo` first, socket second, and say which one is in use. Do not guess `cosmo`
  arguments from memory — read `cosmo --help` on this machine.
