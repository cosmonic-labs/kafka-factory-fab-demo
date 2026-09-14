//! ST-06 Final test & disposition — `rust-kafka-transactional`.
//!
//! Exactly-once read-process-write. Per batch of `finaltest.bins` records
//! (`BATCH_SIZE`, default 100): `transaction::begin` → `send_batch` to
//! `lot.disposition` → `send_batch` to `inventory.moves` → `send_offsets`
//! (one past the last record per input partition, leader epoch carried) →
//! `commit`. Output records and input offsets commit atomically; a crash
//! anywhere replays the batch and downstream `read_committed` readers never
//! see the aborted half — a replayed batch after a broker restart cannot
//! ship the same units twice. Tape & Reel (and the dashboard) read
//! `read_committed`.
//!
//! Requirements (both bindings on this workload, never `producer` beside
//! `transaction`): the unnamed `[consumer]` with `enable.auto.commit=false`
//! (offsets travel in the transaction), and `name: transaction`
//! `[transaction]` with a stable `transactional.id` and
//! `transaction.group.id` = the consumer's group. **`replicas: 1`** — every
//! live transactional producer needs a distinct stable id and the interface
//! allocates none per instance.
//!
//! On any failure: read `txn_requires_abort` / `fatal`, `abort`, and exit so
//! the supervisor restarts from committed offsets (a fatal retires the native
//! client; a fresh `begin` re-fences).
//!
//! Environment (`localResources.environment.config`): IN_TOPIC, OUT_TOPIC
//! (lot.disposition), MOVES_TOPIC (inventory.moves), BATCH_SIZE.

mod bindings {
    use super::Component;
    wit_bindgen::generate!({ world: "kafka-transactional", generate_all });
    export!(Component);
}

use std::collections::BTreeMap;
use std::time::Instant;

use bindings::cosmonic::kafka::consumer::Consumer;
use bindings::cosmonic::kafka::types::{ConsumedRecord, Error, PartitionOffset, ProduceRecord};
use bindings::exports::wasi::cli::run::Guest as RunGuest;
use bindings::transaction::{self, Transaction};

struct Component;

const STATION: &str = "st06";
const METRICS_TOPIC: &str = "line.metrics";

fn env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn batch_size() -> usize {
    env("BATCH_SIZE", "100")
        .parse()
        .ok()
        .filter(|size| *size > 0)
        .unwrap_or(100)
        .min(100)
}

#[derive(serde::Deserialize)]
struct Unit {
    unit: String,
    lot: String,
    bin: u32,
    #[serde(default)]
    vout: Option<f64>,
    #[serde(default)]
    iq_ua: Option<f64>,
}

struct Outputs {
    dispositions: Vec<ProduceRecord>,
    moves: Vec<ProduceRecord>,
    bins: BTreeMap<u32, u64>,
    lots: BTreeMap<String, u64>,
    malformed: u64,
}

fn json_record(key: &str, value: serde_json::Value) -> ProduceRecord {
    ProduceRecord {
        partition: None,
        key: Some(key.as_bytes().to_vec()),
        value: Some(value.to_string().into_bytes()),
        headers: Vec::new(),
        timestamp: None,
    }
}

/// Pure function of the batch: one disposition and one inventory move per
/// unit. A record that is not a unit is skipped and counted (there is no
/// DLQ in the transactional pattern; the ledger simply never carries it).
fn transform(batch: &[ConsumedRecord]) -> Outputs {
    let mut out = Outputs {
        dispositions: Vec::with_capacity(batch.len()),
        moves: Vec::with_capacity(batch.len()),
        bins: BTreeMap::new(),
        lots: BTreeMap::new(),
        malformed: 0,
    };
    for rec in batch {
        let unit: Unit = match rec.value.as_deref().map(serde_json::from_slice) {
            Some(Ok(u)) => u,
            _ => {
                out.malformed += 1;
                continue;
            }
        };
        let pass = unit.bin == 1;
        *out.bins.entry(unit.bin).or_default() += 1;
        *out.lots.entry(unit.lot.clone()).or_default() += 1;
        out.dispositions.push(json_record(
            &unit.unit,
            serde_json::json!({
                "unit": unit.unit, "lot": unit.lot, "bin": unit.bin,
                "disposition": if pass { "pass" } else { "fail" },
                "vout": unit.vout, "iq_ua": unit.iq_ua,
                "origin": {"partition": rec.partition, "offset": rec.offset},
            }),
        ));
        out.moves.push(json_record(
            &unit.unit,
            serde_json::json!({
                "unit": unit.unit, "lot": unit.lot, "from": "FT",
                "to": if pass { "TR" } else { "SCRAP" }, "qty": 1,
            }),
        ));
    }
    out
}

/// Offsets to commit for a processed batch: one past the last record seen on
/// each input partition, carrying the leader epoch through for fencing.
fn commit_positions(batch: &[ConsumedRecord]) -> Vec<PartitionOffset> {
    let mut latest: BTreeMap<(String, i32), &ConsumedRecord> = BTreeMap::new();
    for rec in batch {
        let entry = latest.entry((rec.topic.clone(), rec.partition)).or_insert(rec);
        if rec.offset > entry.offset {
            *entry = rec;
        }
    }
    latest
        .into_values()
        .map(|rec| PartitionOffset {
            topic: rec.topic.clone(),
            partition: rec.partition,
            offset: rec.offset + 1,
            leader_epoch: rec.leader_epoch,
            metadata: None,
        })
        .collect()
}

fn metric(kind: &str, ts: Option<i64>, extra: serde_json::Value) -> ProduceRecord {
    let mut v = serde_json::json!({"station": STATION, "kind": kind, "ts": ts});
    if let (Some(dst), Some(src)) = (v.as_object_mut(), extra.as_object()) {
        for (k, val) in src {
            dst.insert(k.clone(), val.clone());
        }
    }
    json_record(STATION, v)
}

/// Metrics ride in the same transaction: they are exactly-once too, and a
/// `txn` record only exists for a batch that really committed.
async fn process_batch(txn_out: &str, moves_topic: &str, batch: &[ConsumedRecord]) -> Result<(), String> {
    let last_ts = batch.last().and_then(|r| r.timestamp);
    let txn: Transaction = transaction::begin().await.map_err(|e| describe("begin", &e))?;
    let outputs = transform(batch);
    let n_disp = outputs.dispositions.len() as u64;
    let n_moves = outputs.moves.len() as u64;

    let step = async {
        all_ok(txn.send_batch(txn_out.to_string(), outputs.dispositions).await, txn_out)?;
        all_ok(txn.send_batch(moves_topic.to_string(), outputs.moves).await, moves_topic)?;
        let metrics = vec![
            metric("consumed", last_ts, serde_json::json!({"n": batch.len(), "malformed": outputs.malformed, "bins": outputs.bins, "lots": outputs.lots.len()})),
            metric("produced", last_ts, serde_json::json!({"topic": txn_out, "n": n_disp})),
            metric("produced", last_ts, serde_json::json!({"topic": moves_topic, "n": n_moves})),
        ];
        all_ok(txn.send_batch(METRICS_TOPIC.to_string(), metrics).await, METRICS_TOPIC)?;
        txn.send_offsets(commit_positions(batch)).await.map_err(|e| describe("send-offsets", &e))?;
        Ok::<(), String>(())
    };
    if let Err(why) = step.await {
        let _ = txn.abort().await;
        return Err(why);
    }
    if let Err(e) = txn.commit().await {
        let why = describe("commit", &e);
        let _ = txn.abort().await;
        return Err(why);
    }
    Ok(())
}

fn all_ok(r: Result<Vec<Result<bindings::cosmonic::kafka::types::ProduceAck, Error>>, Error>, topic: &str) -> Result<(), String> {
    match r {
        Ok(outcomes) => match outcomes.into_iter().find_map(|o| o.err()) {
            None => Ok(()),
            Some(e) => Err(describe(&format!("send-batch {topic} record"), &e)),
        },
        Err(e) => Err(describe(&format!("send-batch {topic}"), &e)),
    }
}

fn describe(op: &str, e: &Error) -> String {
    format!(
        "{op}: {} ({:?}; requires_abort={} fatal={} retriable={})",
        e.message, e.code, e.txn_requires_abort, e.fatal, e.retriable
    )
}

impl RunGuest for Component {
    async fn run() -> Result<(), ()> {
        let in_topic = env("IN_TOPIC", "finaltest.bins");
        let out_topic = env("OUT_TOPIC", "lot.disposition");
        let moves_topic = env("MOVES_TOPIC", "inventory.moves");
        let batch_size = batch_size();

        let consumer = Consumer::open().await.map_err(|e| eprintln!("st06: open: {}", e.message))?;
        consumer
            .subscribe(vec![in_topic])
            .await
            .map_err(|e| eprintln!("st06: subscribe: {}", e.message))?;
        let (mut records, terminal) = consumer
            .records()
            .await
            .map_err(|e| eprintln!("st06: records: {}", e.message))?;
        let mut batch: Vec<ConsumedRecord> = Vec::with_capacity(batch_size);
        let mut committed: u64 = 0;
        while let Some(rec) = records.next().await {
            batch.push(rec);
            if batch.len() >= batch_size {
                let started = Instant::now();
                if let Err(why) = process_batch(&out_topic, &moves_topic, &batch).await {
                    eprintln!("st06: batch aborted: {why}");
                    // Outside any transaction: the abort itself is the fact
                    // the dashboard needs, and it must land even though the
                    // batch did not. Best effort through a fresh transaction.
                    report_abort(&why, batch.last().and_then(|r| r.timestamp)).await;
                    let _ = consumer.close().await;
                    return Err(());
                }
                committed += 1;
                report_txn(started.elapsed().as_millis() as u64, batch.len(), committed, batch.last().and_then(|r| r.timestamp)).await;
                batch.clear();
            }
        }
        if !batch.is_empty() {
            let _ = process_batch(&out_topic, &moves_topic, &batch).await;
        }
        let _ = consumer.close().await;
        match terminal.await {
            Ok(()) => Err(()),
            Err(e) => {
                eprintln!("st06: session ended: {}", e.message);
                Err(())
            }
        }
    }
}

/// A `txn` metric per committed batch, committed in its own transaction so
/// it is exactly-once like everything else this station writes.
async fn report_txn(ms: u64, n: usize, committed: u64, ts: Option<i64>) {
    if let Ok(txn) = transaction::begin().await {
        let m = metric("txn", ts, serde_json::json!({"ms": ms, "n": n, "committed": committed}));
        if txn.send_batch(METRICS_TOPIC.to_string(), vec![m]).await.is_ok() {
            let _ = txn.commit().await;
        } else {
            let _ = txn.abort().await;
        }
    }
}

async fn report_abort(why: &str, ts: Option<i64>) {
    let requires_abort = why.contains("requires_abort=true");
    let fatal = why.contains("fatal=true");
    if let Ok(txn) = transaction::begin().await {
        let m = metric("txn_abort", ts, serde_json::json!({"reason": why, "requires_abort": requires_abort, "fatal": fatal}));
        if txn.send_batch(METRICS_TOPIC.to_string(), vec![m]).await.is_ok() {
            let _ = txn.commit().await;
        } else {
            let _ = txn.abort().await;
        }
    }
}
