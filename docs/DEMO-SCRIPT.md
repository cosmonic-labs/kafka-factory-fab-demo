# Fab 3 — presenter's script

Twelve minutes, seven beats. Each beat says what to **say**, what to **run**,
and what to **point at**. The narrative it follows is docs/design.html §1;
the beats are §5. Everything here was run and verified on Cosmonic Desktop
0.5.29 with Redpanda in Docker.

## Before you start (5 min, off camera)

```sh
make up          # ≈ 1 min after the first build; ends with "validate: all checks passed"
make metrics     # Prometheus + Grafana (optional, for the throughput charts)
```

Open four browser tabs and arrange them:

| Tab | URL | What it is for |
|---|---|---|
| **Line** | http://fab-line-dashboard.localhost:8200/ | the dashboard: six panels, summary strip, faults ledger. This is the main screen |
| **Desktop** | Cosmonic Desktop → Workloads, search `fab-` | the nine workloads, their state and Tags; the manifest viewer for beat 2 |
| **Console** | http://localhost:8090/ | Redpanda Console: Topics (message browser shows headers), Consumer Groups (lag per partition) |
| **Grafana** | http://localhost:3000/d/fab3-line | records/s per topic, lag per group, broker latency, bytes/s (needs `make metrics`) |

Keep a terminal in the repo root. Run `make status` once; it should say
`running · scenario baseline · rate 1x`. If the line has been running for a
long time, `make up RUN_FLAGS=--no-build` resets the dashboard's fold in a
minute.

Optional: `make rate N=5` for a busier floor (5× every topic). Put it back
to `N=1` before the excursion beat so the ST-04 story stays legible.

---

## 00:00 — Beat 1: Quiet Tuesday

**Say.** "This is Meridian's Fab 3 back-end line — six stations, wafers in
on the left, reels out on the right. Every station is one WebAssembly
component built from one of the four `rust-kafka-*` templates Cosmonic
Desktop ships. None of them links a Kafka client: the host's plugin owns the
broker, the credential, the consumer group and the topic grant. The
component only ever sees the batches it is handed, or a producer it calls."

**Run.** Nothing — the loop is already running.

**Point at.**
- *Line tab*: the summary strip moving (Line UPH, first-pass yield), then read the six panels left to right in floor order. Name the template on each header: `http-producer`, `handler + producer`, `pull-service`, `handler batch.size 1`, `handler batch.size 1000`, `transactional`.
- *ST-02*: "Live instances 6 of 32" — one per assigned partition; "the pool breathes with partition work and returns to zero."
- *Desktop tab*: search `fab-factory` — nine rows, all `running`, the Tags column. "One search shows the whole line on a host that runs a hundred other things."
- *Grafana*: records produced/s stacked by topic (~270/s at 1×); lag per group flat at zero (ST-06's 20–99 is a partial batch waiting for the next record; the twin's is uncommitted-by-design — say so if asked).

**Then run** `make validate` in the terminal and read the last lines:
every group's lag, `produced == consumed + dlq` per station, `0 duplicates`.
"Numbers, not claims."

## 01:30 — Beat 2: the binding is the whole client config

**Say.** "Open any station's manifest: no broker in the code, no credential,
no client config — only the subscription, the grant, the group id and the
dead-letter topic. Two consequences. A manifest that tries to hand the host a
capability is refused at apply *by name*. And a grant that is missing
something fails the bind in the plugin's own words."

**Run.** `make refuse`

**Point at.**
- *Desktop tab*: open `fab-st02-die-attach` → manifest: `handler.topics`, `topics` (the grant covers the subscription, the DLQ, the flags topic and `line.metrics`), `handler.group.id`, `dead-letter.topic`. "The grant is not a hint — a topic outside it is `topic-authorization-failed`."
- *Terminal*, first half: `plugin.library.paths is host-only on Cosmonic Desktop … a workload manifest may not set it.` "It names the key, never a value — a value may have come from a secret."
- *Terminal*, second half: `dead-letter topic dieattach.dlq is outside the binding's topics grant`. "The apply is accepted; the *bind* is what refuses. Nothing retries a bad grant into working."

## 03:00 — Beat 3: shift change

**Say.** "06:00 and 14:00 used to page someone: every prober uploads its lot
summaries at once. ST-01 is the HTTP edge — one upload is one `send_batch`,
so a bad wafer record is one line in the response, not a failed lot, and
schema failures are 400s before anything is produced."

**Run.** `make shift-change` (60 s, returns to baseline by itself)

**Point at.**
- *ST-01 panel*: lots/hr climbing (100×), "Upload → ack p50/p95" not moving. *ST-05*: samples/s doubling, conformance flat.
- *Grafana*: `probe.lots` and `mold.telemetry` step up in the produced/s chart; lag stays near zero. "The handler stations absorb it inside one replica because useful concurrency is min(assigned partitions, pool)."
- Optional: `curl -s -X POST http://fab-st01-probe-intake.localhost:8200/lot -d '{"lot":"nope","prober":"P-07","wafers":[]}'` → `rejected: lot id must look like L-NNNNN-NN`; the ST-01 panel logs it as a fault with the prober id.

## 05:00 — Beat 4: poison pill, two ways

**Say.** "A mis-calibrated prober sends a die record whose thickness is the
string NaN. The rule in the skill is: malformed input is `Permanent`, never a
panic. Watch what each does."

**Run.** `make poison`, wait ~5 s.

**Point at.**
- *ST-02 panel*: the fault line: `1 record in dieattach.dlq · bl_um is not a finite number: "NaN" · x-dlq-original-partition=… x-dlq-original-offset=… · key D-POISON1~…`. "The host dead-lettered it with its origin as headers and the partition advanced."
- *Faults ledger*: three rows for one record, bottom-up: `partial batch: handled through offset N, rest redelivered (Ok(Some(offset)))` — "the handler kept the work it had already done" — then `→ dieattach.dlq … Err(Permanent)`, then the DLQ row.
- *Console tab*: Topics → `dieattach.dlq` → the message, expand headers (`x-dlq-reason`, `x-dlq-original-topic/partition/offset`).

**Then say.** "Now the same record against a build that panics instead."

**Run.** `make naive`, then `make poison`, wait ~20 s, then `make robust`.

**Point at.**
- *ST-02 panel*: "Live instances (… ever)" jumps — every trap burns an instance and the pool refills. The DLQ count ticks to 2, reason `handler call did not complete`: "five traps, five backoffs, then the host gave up on the record for you. Same outcome, later, with a stalled partition in between. That is why the rule is *Permanent, never panic*."

## 06:30 — Beat 5: bond excursion → inspection backlog

**Say.** "ST-03 is the one station that needed the consumer *session* — it
windows per bonder, commits after a window's outputs are acked, answers
rebalances, and on an NSOP alarm seeks the partition back 60 s to replay the
signature. ST-04 is the work queue: one job per call, 200–400 ms of real
compute each."

**Run.** `make drift` then `make excursion` (they stack; drift 90 s, excursion 120 s).

**Point at.**
- *ST-03 panel*, after ~15 s: bonder 17 turns red on the heat strip; the fault line `bonder 17 NSOP — transducer power −16 %; replaying last 60 s by seek`; in the ledger the `seek partition N back to offset M — replaying bonder 17` row; "Window · flushes · seeks · replayed" counting the replayed records. "The replayed records are counted as replayed, not consumed — `make validate` still balances."
- *ST-04 panel*: jobs/min ×20, live instances climbing toward 12 — "12 assigned partitions; the pool cannot help past that. Replica scaling is a Control concept; on one host the slider is drawn, not driven." p95 latency flat-ish, lag on Grafana climbs and drains.
- *Grafana*: `inspect.jobs` produced/s step; `st04-inspection` lag bump and drain.

## 09:00 — Beat 6: restart the broker mid-batch

**Say.** "Final test dispositions lots and moves inventory. Two outputs and
the input offsets commit in one transaction. Next to it runs a twin built from
the pull-service template — same code path, no transaction — so you can see
the difference rather than take it on faith."

**Run.** `make broker-restart` (≈1 min: waits for every group, then proves the pipeline with a probe record).

**Point at.**
- *Terminal*: groups coming back (`8/8 groups Stable`), then `ST-02 consumed it: the pipeline is live`. *Grafana*: the gap in every chart, then recovery. "The skill warns about minutes of silent no-delivery when a broker's *identity* changes; a container restart reconnects in under a minute."

**Then say.** "A broker restart that fast doesn't end a session. To force a
replay, kill the workers mid-batch."

**Run.** `make crash-st06` (≈50 s).

**Point at.**
- *Terminal*: `lot.disposition (transactional, read_committed): 0 duplicates` and `lot.disposition.twin (at-least-once): 200 duplicate units — the replayed batches, counted twice`.
- *ST-06 panel*: "Duplicates · ledger vs twin: 0 · Δ 200". "Same units, same code — the transaction is the only difference. And say it out loud: a database write inside that transaction is *not* covered, which is why ST-02 sinks idempotently instead."

## 11:00 — Beat 7: rolling update

**Say.** "Ship a tighter NSOP threshold to wire bond. The stable
`consumer.group.id` is what makes the new instance resume where the old one
stopped; cooperative-sticky rebalancing moves only what changes hands."

**Run.** `make rollout-st03` (PCT defaults to 12).

**Point at.**
- *ST-03 panel*: "BATCH_SIZE · NSOP threshold" reads `100 · 12%`; the ledger row `service started (BATCH_SIZE 100, NSOP threshold 12%) · resumed from committed offsets`; the heat strip never blanked.

**Close.** `make validate` — every group's lag, `produced == consumed + dlq`
for every station with the DLQ records accounted for, `0 duplicates`. "Every
beat had a number on the page that either matched or didn't."

---

## If something looks wrong

| Symptom | Do |
|---|---|
| Dashboard shows `simulator no heartbeat` | `make status`; if stopped, `make start` |
| A panel's rates read 0 | the loop is stopped (`make start`) or that station isn't running (Desktop tab) |
| `make validate` says the dashboard's fold is partial | `make up RUN_FLAGS=--no-build` (a minute) resets the fold from the start of `line.metrics` |
| Grafana empty | `make metrics` was not run, or the broker was recreated after it — `make metrics` again |
| Console shows no groups | the workloads are not bound yet — wait 10 s |
| The floor is too quiet / too busy | `make rate N=…` (1 is the design's rates) |

## What is live and what is not (be honest on stage)

Live: six stations, simulator, dashboard, DLQs, transactions, twin ledger,
instance heartbeats, lag (from `rpk` via `make validate`). Static: the replica
window on ST-04/ST-05 ("Control only"). Approximated: ST-05's windows (one
batch = one window; the stream-handler export is the real shape). Full list:
README.md, docs/DECISIONS.md.
