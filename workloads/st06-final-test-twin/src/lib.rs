//! ST-06 twin — the same final-test disposition as `st06-final-test`, built
//! from `rust-kafka-pull-service` **without** a transaction, writing the same
//! outputs to `lot.disposition.twin` / `inventory.moves.twin`.
//!
//! It exists for demo beat 6: restart the broker mid-batch and compare the
//! ledgers. This service produces, then commits — at-least-once — so a batch
//! whose commit never landed is replayed after the restart and the twin's
//! ledger carries the same units twice. The transactional station's cannot.
//! To make the window visible in a 12-minute demo the twin commits only
//! every `COMMIT_EVERY` batches (default 5): a restart inside that window
//! replays up to 5 × BATCH_SIZE units. That is a demo dial, and honest about
//! what at-least-once means (docs/DECISIONS.md).
//!
//! Semantics otherwise match the scaffold: one consumer session, the
//! host-owned producer, explicit `commit([])` of the stored positions after
//! the outputs are acked; a failure exits so the supervisor restarts from
//! committed offsets. Malformed records go to no DLQ here (the transactional
//! station has none either); they are counted and skipped.
//!
//! Environment (`localResources.environment.config`): IN_TOPIC, OUT_TOPIC
//! (lot.disposition.twin), MOVES_TOPIC (inventory.moves.twin), BATCH_SIZE,
//! COMMIT_EVERY.

mod bindings {
    use super::Component;
    wit_bindgen::generate!({ world: "kafka-pull-service", generate_all });
    export!(Component);
}

use bindings::cosmonic::kafka::consumer::Consumer;
use bindings::cosmonic::kafka::producer;
use bindings::cosmonic::kafka::types::{ConsumedRecord, ErrorCode, ProduceRecord};
use bindings::exports::wasi::cli::run::Guest as RunGuest;

struct Component;

const STATION: &str = "st06twin";
const METRICS_TOPIC: &str = "line.metrics";

fn env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_usize(key: &str, default: usize, max: usize) -> usize {
    env(key, "")
        .parse()
        .ok()
        .filter(|n| *n > 0)
        .unwrap_or(default)
        .min(max)
}

#[derive(serde::Deserialize)]
struct Unit {
    unit: String,
    lot: String,
    bin: u32,
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

fn metric(kind: &str, ts: Option<i64>, extra: serde_json::Value) -> ProduceRecord {
    let mut v = serde_json::json!({"station": STATION, "kind": kind, "ts": ts});
    if let (Some(dst), Some(src)) = (v.as_object_mut(), extra.as_object()) {
        for (k, val) in src {
            dst.insert(k.clone(), val.clone());
        }
    }
    json_record(STATION, v)
}

fn transform(batch: &[ConsumedRecord]) -> (Vec<ProduceRecord>, Vec<ProduceRecord>, u64) {
    let mut disp = Vec::with_capacity(batch.len());
    let mut moves = Vec::with_capacity(batch.len());
    let mut malformed = 0;
    for rec in batch {
        let unit: Unit = match rec.value.as_deref().map(serde_json::from_slice) {
            Some(Ok(u)) => u,
            _ => {
                malformed += 1;
                continue;
            }
        };
        let pass = unit.bin == 1;
        disp.push(json_record(
            &unit.unit,
            serde_json::json!({
                "unit": unit.unit, "lot": unit.lot, "bin": unit.bin,
                "disposition": if pass { "pass" } else { "fail" },
                "origin": {"partition": rec.partition, "offset": rec.offset},
            }),
        ));
        moves.push(json_record(
            &unit.unit,
            serde_json::json!({"unit": unit.unit, "lot": unit.lot, "from": "FT", "to": if pass { "TR" } else { "SCRAP" }, "qty": 1}),
        ));
    }
    (disp, moves, malformed)
}

async fn send_all(topic: &str, records: Vec<ProduceRecord>) -> Result<(), String> {
    if records.is_empty() {
        return Ok(());
    }
    let outcomes = producer::send_batch(topic.to_string(), records)
        .await
        .map_err(|e| format!("send_batch {topic}: {}", e.message))?;
    if let Some(Err(e)) = outcomes.into_iter().find(|o| o.is_err()) {
        return Err(format!("send_batch {topic}: record failed: {}", e.message));
    }
    Ok(())
}

impl RunGuest for Component {
    async fn run() -> Result<(), ()> {
        let in_topic = env("IN_TOPIC", "finaltest.bins");
        let out_topic = env("OUT_TOPIC", "lot.disposition.twin");
        let moves_topic = env("MOVES_TOPIC", "inventory.moves.twin");
        let batch_size = env_usize("BATCH_SIZE", 100, 100);
        let commit_every = env_usize("COMMIT_EVERY", 5, 100);

        let consumer = Consumer::open().await.map_err(|e| eprintln!("st06-twin: open: {}", e.message))?;
        consumer
            .subscribe(vec![in_topic])
            .await
            .map_err(|e| eprintln!("st06-twin: subscribe: {}", e.message))?;
        let (mut records, terminal) = consumer
            .records()
            .await
            .map_err(|e| eprintln!("st06-twin: records: {}", e.message))?;
        let mut batch: Vec<ConsumedRecord> = Vec::with_capacity(batch_size);
        let mut since_commit = 0usize;
        while let Some(rec) = records.next().await {
            batch.push(rec);
            if batch.len() < batch_size {
                continue;
            }
            let last_ts = batch.last().and_then(|r| r.timestamp);
            let (disp, moves, malformed) = transform(&batch);
            let n = disp.len() as u64;
            let step = async {
                send_all(&out_topic, disp).await?;
                send_all(&moves_topic, moves).await?;
                Ok::<(), String>(())
            };
            if let Err(why) = step.await {
                eprintln!("st06-twin: {why}");
                let _ = consumer.close().await;
                return Err(());
            }
            since_commit += 1;
            let committing = since_commit >= commit_every;
            let mut metrics = vec![
                metric("consumed", last_ts, serde_json::json!({"n": batch.len(), "malformed": malformed})),
                metric("produced", last_ts, serde_json::json!({"topic": out_topic, "n": n})),
                metric("produced", last_ts, serde_json::json!({"topic": moves_topic, "n": n})),
            ];
            if committing {
                metrics.push(metric("commit", last_ts, serde_json::json!({"batches": since_commit})));
            }
            let _ = producer::send_batch(METRICS_TOPIC.to_string(), metrics).await;
            if committing {
                // Stored positions, after the outputs are acked — but only
                // every COMMIT_EVERY batches (the demo's replay window).
                match consumer.commit(Vec::new()).await {
                    Ok(results) if results.iter().all(|r| matches!(r.error, None | Some(ErrorCode::NoOffset))) => since_commit = 0,
                    Err(e) if matches!(e.code, ErrorCode::NoOffset) => since_commit = 0,
                    Ok(results) => {
                        eprintln!("st06-twin: commit failed on a partition: {:?}", results.iter().find_map(|r| r.error.clone()));
                        let _ = consumer.close().await;
                        return Err(());
                    }
                    Err(e) => {
                        eprintln!("st06-twin: commit: {}", e.message);
                        let _ = consumer.close().await;
                        return Err(());
                    }
                }
            }
            batch.clear();
        }
        let _ = consumer.close().await;
        match terminal.await {
            Ok(()) => Err(()),
            Err(e) => {
                eprintln!("st06-twin: session ended: {}", e.message);
                Err(())
            }
        }
    }
}
