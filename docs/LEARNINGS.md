# Learnings — what this build taught us, and what to change upstream

Written while building the Fab 3 line on Cosmonic Desktop 0.5.28 (daemon
`82dfd539f2fc6d21`, wasmCloud runtime 2.9.0, `cosmonic:kafka@0.5.0`,
plugin-kafka @ control `455c222`, skill `cosmonic-kafka` 1.2.0, templates @
`f5326286`), September 2026. Each item says what happened, what it cost, and
the concrete change that would have prevented it. Ordered by how much time
each one cost.

## 1. The Kafka templates (`rust-kafka-*`)

### 1.1 The pull-service and transactional `workload.yaml` do not run on Desktop
The scaffolds put the `wasi:cli/run` component under `spec.components`. On
Desktop that starts, reports `running`, and does nothing: wash-runtime drives
`run` only for `spec.service`. The `deploy/workload-deployment.yaml` in the
same scaffold already uses `service:`, so the two files disagree.
**Change:** emit `spec.service` (with `maxRestarts`) in the Desktop
`workload.yaml` and `.wash/config.yaml` of `rust-kafka-pull-service` and
`rust-kafka-transactional`; have `cosmonic publish` / the dev loop place a
component that exports `wasi:cli/run` under `service` automatically. Until
Desktop's validator accepts an empty `components`, the draft needs the inert
component entry too — or the validator should accept `service`-only specs.

### 1.2 The transactional scaffold does not link on the toolchain Desktop provisions
`import transaction: cosmonic:kafka/transaction@0.5.0` (a named import) is
encoded by wit-component 0.251 in a form `wasm-component-ld` 0.5.20–0.5.22
(bundled with Rust 1.94–1.97) rejects: `invalid leading byte (0x2) for import
name`. The stock scaffold fails; `wasm-tools` 1.243 (Homebrew's) cannot read
the output either. rustc prefers its sysroot linker over PATH, so `cargo
install wasm-component-ld` alone does not fix it.
**Change:** ship the linker shim this repo uses
(`workloads/st06-final-test/.cargo/`) in the template, or pin wit-bindgen /
wit-component to an encoding the bundled linker accepts, or have the Preflight
doctor provision `wasm-component-ld >= 0.5.27` and `wasm-tools >= 1.250` and
say so in the template notes. The skill's "verify with `wasm-tools component
wit`" step silently needs the newer wasm-tools.

### 1.3 A handler that produces should be the shipped default
The brief and the skill both say "start with the handler — it can produce",
but the `rust-kafka-handler-consumer` scaffold ships without the producer
import; every station here had to add `import cosmonic:kafka/producer@0.5.0`,
`producer` to `interfaces:` and the output topics to `topics`. **Change:**
ship the handler scaffold with the producer imported and a commented
`send` in `handle`, so the consume-transform-produce shape is the starting
point and the grant already shows the output topic.

### 1.4 The scaffold's error handling exits on `no-offset`
`commit([])` answers `no-offset` when nothing is stored since the last commit
— after a `seek`, or when a flush finds every position already committed. The
scaffold's `commit_stored` maps every error to exit, so a service that seeks
restarts itself and loses its state. **Change:** treat `no-offset` as "nothing
to commit" in the scaffold, defer `retriable` errors, exit only on `fatal`.

### 1.5 An instance heartbeat and an id belong in the template
Every handler here needed the same thread_local (an instance id from
`RandomState`, a heartbeat every N seconds of record time) to show the pool
breathing. **Change:** a `metrics` example in the handler scaffold, or a
`wasmcloud:host/identity`-style import the guest can read its instance id
from.

## 2. The `cosmonic:kafka` plugin and WIT

### 2.1 `rebalances()` silently switches assignment to manual
Taking the rebalance stream suppresses librdkafka's default assign/unassign;
the guest must answer every event with `incremental-assign`/`-unassign` (or
`assign`/`assign([])` under the eager protocol). Nothing in the WIT docs, the
skill or the scaffold says so; the symptom is a Stable group with one member,
climbing lag and zero records. **Change (plugin):** keep the default assignment
unless the guest calls `assign`/`incremental-assign` itself at least once —
reading the stream should not change behaviour. **Change (WIT doc + skill):**
state the contract in the `rebalances` doc comment and in the pull-service
section, with the five-line answer loop.

### 2.2 No delivery-attempt counter on `consumed-record`
A handler cannot tell a redelivery from a first delivery, and after a trap the
instance that counted is gone. Beat 4's "five traps" is invisible to the
guest; the dashboard only sees the final dead letter. **Change:** add
`delivery-attempt: u32` (and `redelivered: bool`) to `consumed-record`, set by
the handler loop, so a guest can log/metric the retry path.

### 2.3 Plugin bind refusals are retried on 0.5.28
The plugin refuses a grant that misses its DLQ with the right sentence, but
this daemon retries the bind (attempt n/5, 30 s apart) before showing
`Failed`. The daemon's main branch classifies it permanent. **Change:** ship
that classification; and expose the refusal on the workload status
(`message`) at the first attempt, not only in the log ring.

### 2.4 Transaction markers show up as lag for read_committed consumers
`rpk group describe` reports log-end minus committed, and a `read_committed`
consumer's position stops before each partition's control record, so every
transactional partition contributes one permanent unit of "lag". Anyone
alerting on lag == 0 over a transactional topic will page forever.
**Change (skill / tuning):** say so, with the expected number (one per
partition with an open or last-committed transaction); the dashboard's
validation tolerates it.

### 2.5 A commit inside a transaction cannot report its own latency
ST-06's `txn` metric had to go in a second transaction after `commit`.
**Change (WIT):** return the commit's timing (or a `produce-ack`-like
record) from `transaction.commit`.

### 2.6 The batch has no timer
A pull service reading `records()` with `BATCH_SIZE > 1` holds a partial
batch until the next record arrives; there is no way to wait "at most N ms"
without importing a clock and racing it against `next()` (cancel-safety of a
dropped `next()` is unclear). The skill says "no batch timer" once; the
consequence — lag never reads 0 on a quiet stream — is not spelled out.
**Change (WIT):** `records-with-timeout` / a `next(deadline)` on the stream, or
document the select-with-clock pattern and its cancel-safety.

### 2.7 Component stdout/stderr does not reach the log ring
`println!`/`eprintln!` from a component is dropped on this build, so every
service had to report through `line.metrics`. **Change (daemon):** capture
guest stdio into `GET /v1/logs` under the workload (source `component`).

## 3. Cosmonic Desktop: daemon, API and MCP server

### 3.1 The API in `daemon/docs/KAFKA.md` is ahead of the release
`GET/PUT /v1/kafka` and `POST /v1/kafka/test` are documented and in main, not
in 0.5.28. Scripts had to feature-detect (404) and fall back to the `kafka`
label on `GET /v1/host` and to writing `kafka.yaml` by hand. **Change:** ship
them; until then, version the docs or mark the routes "main only".

### 3.2 `cosmonic promote` prints no manifest; `cosmonic publish` does not exist
The brief, the skill and the scaffold READMEs say `cosmonic publish` "prints a
durable Workload draft". The CLI verb is `promote` and it prints only the
image and digest; the draft is in the API response. **Change:** print the
draft (or write it to `workload.yaml`), and align the name across the CLI, the
skill and the READMEs.

### 3.3 The publish draft flattens pool settings
The daemon's draft carries `poolSize: 1, maxInvocations: 0, maxConcurrency: 0`
for a handler whose scaffold `workload.yaml` says `poolSize: 32,
maxConcurrency: 1, reclaimWindowSeconds: 30, reclaimMinInstances: 0`. This
repo renders its own manifests from `workload.yaml` for that reason.
**Change:** carry `components[].poolSize/maxConcurrency/reclaim*` from
`.wash/config.yaml` (or `workload.yaml`) into the draft.

### 3.4 A warm pool that is full sheds calls to throwaway instances
`poolSize: 1, maxConcurrency: 1` on a handler that receives concurrent
partition batches quietly runs the extra calls on ephemeral stores
("Saturated"). For a stateful single-instance shape (a fold, a cache) that
means silently lost state with no log line. **Change (daemon/runtime):** a
debug log or a counter (`saturatedCalls`) on the workload status; **(skill):**
say that a stateful single-instance component needs `maxConcurrency` ≥ the
number of concurrent callers, and what "Saturated" does.

### 3.5 `restart` does not restart a service
`POST /v1/workloads/{ns}/{name}/restart` re-resolves the spec and leaves a
running `wasi:cli/run` instance in place; only `stop` + `start` ends the
consumer session. **Change:** make `restart` restart the service instance, or
document the difference on the MCP tool (`cosmonic_workload_restart`).

### 3.6 The MCP server was not reachable when the daemon was down
The `cosmonic` MCP server (spawned as `cosmonicd mcp serve`) fails to connect
when the daemon is not running at session start and never reconnects, so the
whole build ran over the socket API. **Change:** have `mcp serve` start (or
wait for) the daemon, and reconnect lazily on the first tool call; expose
`launchctl kickstart gui/$UID/com.cosmonic.cosmonicd` as a `cosmonic_host_start`
tool.

### 3.7 The daemon takes JSON only
`POST /v1/workloads` refuses YAML (415). Every manifest is YAML for humans, so
every script needs PyYAML or ruby. **Change:** accept `application/yaml` on
`/v1/workloads` and `/v1/workloads/validate` (the CLI already reads YAML).

### 3.8 The MCP `cosmonic_workload_validate` cannot see bind refusals
Apply-time guards (host-only keys) are reported by `validate`; the plugin's
bind-time refusals (a grant missing its DLQ, a missing group id) only appear
when the workload starts. **Change:** run the plugin's `validate_bindings` in
`/v1/workloads/validate` so the MCP tool says what the bind would say.

## 4. Tuning and capacity, as measured here (one laptop, Redpanda in Docker)

- **Handler dispatch:** 6-partition topics at 30 rec/s ran 6 live instances of
  32 (one per assigned partition), 12 ever after a restart; ST-04 with
  `handler.batch.size: 1` and 200–400 ms of CPU per job held 10 live instances
  at 546 jobs/min with p95 927 ms and lag 2 — the "useful concurrency =
  assigned partitions" rule is visible on the panel.
- **Transaction cost:** a 100-record transaction (two output topics + a
  metrics batch + offsets) commits in ~240 ms p95 on this machine; 20 units/s
  is 5 s per batch, so the transactional station is latency-bound by
  `BATCH_SIZE`, not throughput-bound.
- **Broker restart:** Redpanda in Docker is back in ~3 s; every group was
  Stable again within 50 s, with no session ended and no service restarted —
  the "minutes of silent no-delivery" the skill warns about is for a broker
  whose identity changed, not for a container restart.
- **Recovery after a stop/start of a pull service:** the twin replayed
  exactly `COMMIT_EVERY × BATCH_SIZE` minus the committed part; with
  `COMMIT_EVERY: 1` the window is one batch. Pick the commit cadence by how
  many duplicates a downstream can absorb, not by throughput.
- **Dashboard fold:** 7 topics / 13 partitions into one warm instance at
  ~20 metric records/s uses ~2 % CPU; `/api/state` is ~60 KB every 2 s.
  `maxConcurrency: 64` costs nothing when the calls are short.
- **Lag semantics:** see 2.4 and 2.6 — alert on lag < BATCH_SIZE for a
  batching pull service, and expect one unit per transactional partition for
  a read_committed consumer.

## 5. The skill (`cosmonic-kafka`)

Add, in this order of value:
1. The `rebalances()` ⇒ manual assignment contract, with the answer loop (2.1).
2. "`spec.service` for `wasi:cli/run`; `components` are invoked, not run" (1.1).
3. `no-offset` is not an error; the retriable/fatal ladder for `commit` (1.4).
4. What "no batch timer" means for lag, and the transaction-marker lag (2.4, 2.6).
5. `maxConcurrency` for a stateful single-instance handler; what "Saturated"
   does to state (3.4).
6. The toolchain floor: `wasm-component-ld >= 0.5.27`, `wasm-tools >= 1.250`,
   and that rustc ignores PATH for the linker (1.2).
7. `cosmonic promote` (not `publish`), and that the draft is in the API
   response (3.2).
8. Guest stdout is not logged; report through a topic or a metrics record (2.7).
9. A handler cannot count its own redeliveries across traps (2.2).
10. Two exports (`wasi:http/handler` + `cosmonic:kafka/handler`) on one
    component is a supported, useful shape — document it as the "dashboard /
    read model" pattern, with its pool settings.
