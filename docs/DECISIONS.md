# Decisions — where the build deviates from PLAN.md / design.html, and why

Every deviation from the brief or the design doc, in the order it was hit.
Where the skill or the shipped scaffold disagreed with the brief, the skill
and the scaffold won (PLAN.md §2).

## Tooling and the target host

1. **No `cosmo` CLI; every workload verb uses the daemon's socket API.**
   Control's `cosmo` is not part of a Cosmonic Desktop install and is not on
   this machine. `scripts/lib.sh` wraps each verb once (`cosmo_apply`,
   `cosmo_validate`, `cosmo_list`, `cosmo_logs`, `cosmo_delete`,
   `cosmo_publish`) over `curl --unix-socket $(cosmonicd paths → socket)`, and
   `run.sh` prints which path is in use at the top of every run. Nothing mixes
   the two.
2. **`cosmonic publish` is `cosmonic promote` on this CLI (0.5.26), and it does
   not print the Workload draft** — only `pushed …` / `digest …`. `run.sh`
   therefore publishes through `POST /v1/projects` + `POST
   /v1/projects/{id}/publish` (what `promote` calls) and renders
   `manifests/<name>.workload.yaml` from each workload's committed
   `workload.yaml` with the image digest-pinned. That keeps the pool settings
   the brief specifies (the daemon's own draft carries `poolSize: 1,
   maxConcurrency: 0` for every component).
3. **The daemon takes JSON only** (`POST /v1/workloads` refuses YAML with 415).
   Manifests stay YAML for humans; `lib.sh yaml_to_json` converts with PyYAML
   (or ruby's stdlib) — a preflight check.
4. **This Desktop (0.5.28) has no `GET/PUT /v1/kafka` or `POST /v1/kafka/test`
   routes.** They are in the daemon's main branch (`daemon/docs/KAFKA.md`),
   not in this release. `run.sh` uses them when present and otherwise:
   step 3 reads the `kafka` label on `GET /v1/host` (`default broker` / `no
   default broker` / `not in this build`); step 6 degrades to "the first
   workload's bind is the dial test" and reports PASS once every kafka
   workload is running; `--use-kafka-yaml` writes `<state_dir>/kafka.yaml`
   directly (from `cosmonicd paths`), which the daemon reads at boot.
5. **A plugin bind refusal is retried on this build, not classified permanent.**
   A grant that misses its DLQ is refused by the plugin with the right
   sentence (`dead-letter topic … is outside the binding's topics grant`) but
   the daemon retries it (attempt n/5, 30 s apart) before showing Failed.
   `beats.sh refuse` reads the refusal from the daemon log instead of waiting
   for `failed`, and says so.
6. **`wasm-component-ld >= 0.5.27` is required for the transactional world,
   and rustc will not use the one on PATH.** wit-bindgen 0.58 encodes the
   named import (`import transaction: cosmonic:kafka/transaction@0.5.0`) with
   wit-component 0.251; the linker bundled with Rust 1.94–1.97 (0.5.20–0.5.22)
   fails with `invalid leading byte (0x2) for import name`, and rustc prefers
   its sysroot copy over PATH. The stock `rust-kafka-transactional` scaffold
   fails the same way on this toolchain. Fix: `workloads/st06-final-test/
   .cargo/wasm-component-ld`, a shim `.cargo/config.toml` names as the
   target's linker, which runs the PATH copy (`cargo install
   wasm-component-ld`) with the toolchain's own `wasm-ld`. It works for
   `cargo build`, `cosmonic dev` (builds run through `/bin/sh -c`) and CI.
   `wasm-tools >= 1.250` is likewise needed to *read* that component;
   `lib.sh` picks the newest of PATH's and `~/.cargo/bin`'s (Homebrew's
   1.243 shadowed the cargo-installed 1.259 here).

## Workload shape

7. **`wasi:cli/run` components go under `spec.service`, not `spec.components`.**
   wash-runtime drives `run` only for a Workload's `service`
   (`execute_service`); a component under `components` is pooled and invoked,
   never run. The pull-service and transactional scaffolds' `workload.yaml`
   put the service under `components`, which starts "running" and does
   nothing (no group member, no records). All four services here
   (factory-simulator, st03, st06, st06-twin) use `spec.service` with
   `maxRestarts: 100` — and, because Desktop's validator requires at least
   one component, the same image is listed once under `components` as an
   inert entry (it is never invoked). The scaffold's `deploy/` form already
   uses `service:`.
8. **The dashboard is one component with two exports and needs
   `maxConcurrency` ≥ its concurrent calls.** `poolSize: 1` alone is not
   enough: when the single warm instance is busy and the pool is full,
   wash-runtime serves a call from a throwaway store ("Saturated"), so the
   partition loops that dispatched concurrently folded into instances that
   vanished. With `maxConcurrency: 64` every call shares the one instance
   (the fold holds no borrow across an await, so overlap is safe) and
   `reclaimMinInstances: 1` keeps it. The plan's keyvalue fallback was not
   needed.
9. **The dashboard subscribes to seven topics, not one.** `line.metrics` is
   still the single instrumentation path for every station and the
   simulator, but the exact DLQ counts and the origin headers on each dead
   letter only exist on the DLQ topics (`dieattach.dlq`, `wirebond.dlq`,
   `inspect.dlq`, `mold.dlq`), and the live duplicate check needs
   `lot.disposition` / `lot.disposition.twin` read `read_committed`. A
   comma-separated `handler.topics` works. The dashboard is a handler, so it
   needs a `dead-letter.topic` of its own: **`line.metrics.dlq` (1 partition)
   was added to the topic set.**
10. **The dashboard's consumer group is reset on every apply** (`run.sh`
    deletes the group before applying `line-dashboard`) so the in-memory
    fold covers `line.metrics` from the beginning. Otherwise a rerun counts
    "produced" from the restart but "consumed" from the stations' whole
    history and the validation cannot balance.
11. **`replicas`/`scaling:` are not implemented** (Desktop is one host); the
    replica window on ST-04/ST-05 is drawn from a static min/max with a
    "Control only" label, current count fixed at 1, as PLAN.md §8 says.
12. **ST-05 keeps no cross-batch state.** One handler batch is one window;
    the host hands over whatever is fetched (~1 s of data, ~8 records per
    press), so a zone mean over fewer than 4 samples does not alarm — a
    single 3σ sample would otherwise page every few seconds. The proposed
    `stream-handler` export with real windows is what removes that limit.

## Consumer-session semantics learned the hard way

13. **Taking `rebalances()` makes assignment manual.** The plugin suppresses
    librdkafka's default assign/unassign once a guest takes the stream (so
    it cannot race the guest), and the guest must answer every event:
    `incremental-assign` / `incremental-unassign` under the cooperative
    protocol, `assign` (and `assign([])` on revoke) under the eager one that
    this Redpanda negotiates. ST-03's first build read the stream and never
    assigned — Stable group, one member, lag climbing, zero records. ST-03
    now holds the consumer in an `Rc` shared with the rebalance task and
    takes the stream *before* `subscribe`.
14. **`commit([])` answers `no-offset` when nothing is stored** — after a
    seek, or when every position was already committed. The scaffold treats
    any commit error as fatal and exits; ST-03 did too, restarted itself
    after every NSOP seek, lost its window state and relearned the baseline
    at the sagged level. `NoOffset` is now "nothing to commit"; retriable
    errors defer to the next flush; only fatal ones exit.
15. **ST-03's NSOP baseline only learns from healthy windows** (|Δ| < half
    the threshold, EMA 0.9/0.1). A sag landing just under the threshold
    would otherwise drag the trailing mean down and hide the windows that
    follow.

## Simulator

16. **Ids are stamped per pass and per simulator epoch** (`U-0000001~k7f2.3`).
    The static record set replays the same unit/die/job/lot ids every pass,
    which made the transactional ledger's duplicate check meaningless; the
    simulator now suffixes those keys (bonder and press ids are entities and
    stay). A restarted loop bumps the pass; a restarted simulator has a new
    random epoch.
17. **Overlays stack** (`drift` and `excursion` together are beat 5); the brief
    implied one at a time.
18. **The simulator starts the baseline loop by itself** on every start, so a
    redeploy or supervisor restart never leaves the floor silent; `sim.sh
    stop` still stops it.
19. **Rates are scaled to a laptop**: 30 die-attach readings/s, 40 bonds/s,
    one inspection job per 2 s (10/s in an excursion), 48 mold samples/s, 20
    units/s, one lot per pass (100× at shift change). The static set is
    ~1.9 MB of JSONL embedded with `include_str!`.
20. **The non-transactional twin commits every 5 batches** (`COMMIT_EVERY`) so
    a restart inside that window replays up to 500 units — the at-least-once
    duplicates beat 6 shows. It is a demo dial and documented as such.

## Validation

21. **"Caught up" is per group, as rpk reports lag** (`scripts/validate.sh`):
    the pull services have no batch timer, so a partial batch (< BATCH_SIZE)
    waits for the next record; the twin's lag also counts consumed-but-
    uncommitted batches (< 5 × BATCH_SIZE); a `read_committed` reader's
    position stops before each partition's transaction marker, which rpk
    counts as lag (≤ 6 for the dashboard). The loop is paused while the
    numbers are read so they are a snapshot, then resumed.
22. **Beat 6 is two verbs.** `make broker-restart` restarts Redpanda and waits
    for every group to come back, then proves the pipeline with a probe
    record — but Redpanda restarts fast enough that librdkafka reconnects
    without ending any session, so nothing replays. `make crash-st06` stops
    and starts both ST-06 workers mid-batch (`restart` only re-resolves the
    spec and keeps the running instance): the twin replays its uncommitted
    batches and its ledger gains duplicates; `lot.disposition` gains none.
23. **`validate.sh` publishes the lag it measured to `line.metrics`**
    (`station: ops, kind: lag`) so the panels can show it; the workloads still
    never compute lag themselves.
24. **The guest cannot see its own redeliveries after a trap.** The five-trap
    path in beat 4 dead-letters with the host's reason (`handler call did not
    complete`), which the dashboard shows; the per-instance redelivery
    counter cannot survive the trap that killed the instance.

## Repository

25. **The repo is `cosmonic-labs/kafka-factory-fab-demo`**, the remote this
    checkout was created with, not the `factory-fab-assembly-line` name the
    brief proposed. Nothing else depends on the name.
