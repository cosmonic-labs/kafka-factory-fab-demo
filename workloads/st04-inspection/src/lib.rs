//! ST-04 AOI & X-ray inspection — `rust-kafka-handler-consumer` + producer,
//! in the work-queue shape: `handler.batch.size: "1"`, one job per call, a
//! verdict per job.
//!
//! "Work" is a deterministic CPU loop sized by the job's declared cost
//! (200–400 ms) so the latency on the dashboard is real. The result (defect
//! class, void %) goes to `inspect.results` through the binding's host-owned
//! producer.
//!
//! This is the panel where the 0→32 pool is the story: each instance emits
//! one `kind: instance` heartbeat the first time it is called, and the
//! dashboard counts distinct ids seen in the last 30 s as "live instances".
//! Within one replica useful concurrency is min(assigned partitions, 64,
//! poolSize × maxConcurrency) — 12 partitions here — so a 20× burst pins the
//! count at 12; growing the pool adds no group member. The design doc's
//! replica `scaling:` block is a proposal and Desktop is one host: the
//! replica window is drawn static on the dashboard (docs/DECISIONS.md).
//!
//! Return value semantics (at-least-once): `Ok(None)` handles the batch;
//! `Ok(Some(offset))` handles through that record; `Transient` retries;
//! `Permanent` dead-letters the head record to `inspect.dlq`. A malformed job
//! is `Permanent`, never a panic.

mod bindings {
    use super::Component;
    wit_bindgen::generate!({ world: "kafka-handler-consumer", generate_all });
    export!(Component);
}

use std::cell::RefCell;
use std::hash::{BuildHasher, Hasher};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use bindings::cosmonic::kafka::producer;
use bindings::cosmonic::kafka::types::{ConsumedRecord, ProduceRecord};
use bindings::exports::cosmonic::kafka::handler::{Guest as Handler, HandlerError};

struct Component;

const STATION: &str = "st04";
const RESULTS_TOPIC: &str = "inspect.results";
const METRICS_TOPIC: &str = "line.metrics";
const MIN_COST_MS: u64 = 50;
const MAX_COST_MS: u64 = 2_000;

#[derive(serde::Deserialize)]
struct Job {
    job: String,
    strip: String,
    #[serde(default)]
    lot: Option<String>,
    cost_ms: u64,
}

thread_local! {
    static INSTANCE_ID: RefCell<Option<String>> = const { RefCell::new(None) };
    static JOBS_ON_THIS_INSTANCE: RefCell<u64> = const { RefCell::new(0) };
}

fn instance_id() -> (String, bool) {
    INSTANCE_ID.with(|slot| {
        let mut slot = slot.borrow_mut();
        if let Some(id) = slot.as_ref() {
            return (id.clone(), false);
        }
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        h.write_u64(0x1a5b);
        let id = format!("{STATION}-{:08x}", h.finish() as u32);
        *slot = Some(id.clone());
        (id, true)
    })
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Deterministic CPU work for `cost_ms`: a hash loop the optimizer cannot
/// remove, checked against the monotonic clock. Returns the loop's digest so
/// the verdict is a pure function of the job.
fn inspect(job: &Job) -> (u64, u64) {
    let budget = job.cost_ms.clamp(MIN_COST_MS, MAX_COST_MS);
    let start = Instant::now();
    let mut acc: u64 = 0xcbf29ce484222325;
    let mut iters: u64 = 0;
    loop {
        for b in job.strip.bytes().chain(job.job.bytes()) {
            acc ^= b as u64;
            acc = acc.wrapping_mul(0x100000001b3);
        }
        iters += 1;
        if iters % 2048 == 0 && start.elapsed().as_millis() as u64 >= budget {
            break;
        }
    }
    (acc, start.elapsed().as_millis() as u64)
}

fn verdict(digest: u64) -> (&'static str, f64) {
    // ~1 in 12 strips shows a defect; the class and void % follow the digest.
    let pct = (digest % 10_000) as f64 / 100.0;
    match digest % 12 {
        0 => ("void>10%", 10.0 + pct % 15.0),
        1 => ("bridge", pct % 4.0),
        2 => ("lift", pct % 6.0),
        _ => ("none", pct % 3.0),
    }
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

fn parse(value: &[u8]) -> Result<Job, String> {
    let text = std::str::from_utf8(value).map_err(|_| "value is not valid UTF-8".to_string())?;
    let job: Job = serde_json::from_str(text).map_err(|e| format!("bad job: {e}"))?;
    if job.job.is_empty() || job.strip.is_empty() {
        return Err("job and strip ids are required".into());
    }
    Ok(job)
}

impl Handler for Component {
    async fn handle(records: Vec<ConsumedRecord>) -> Result<Option<i64>, HandlerError> {
        let mut handled: Option<i64> = None;
        let mut metrics: Vec<ProduceRecord> = Vec::new();
        let (id, fresh) = instance_id();
        let last_ts = records.last().and_then(|r| r.timestamp);
        if fresh {
            metrics.push(metric("instance", last_ts, serde_json::json!({"id": id})));
        }

        let mut results: Vec<ProduceRecord> = Vec::new();
        let mut failure: Option<(String, &ConsumedRecord)> = None;
        let mut latencies: Vec<u64> = Vec::new();
        let mut defects: Vec<&'static str> = Vec::new();

        for rec in &records {
            let Some(value) = rec.value.as_deref() else {
                handled = Some(rec.offset);
                continue;
            };
            let job = match parse(value) {
                Ok(j) => j,
                Err(reason) => {
                    failure = Some((reason, rec));
                    break;
                }
            };
            let (digest, work_ms) = inspect(&job);
            let (class, void_pct) = verdict(digest);
            let now = now_ms();
            let latency_ms = rec.timestamp.map(|t| (now - t).max(0) as u64).unwrap_or(work_ms);
            latencies.push(latency_ms);
            defects.push(class);
            JOBS_ON_THIS_INSTANCE.with(|n| *n.borrow_mut() += 1);
            results.push(json_record(
                &job.job,
                serde_json::json!({
                    "job": job.job, "strip": job.strip, "lot": job.lot,
                    "defect": class, "void_pct": (void_pct * 10.0).round() / 10.0,
                    "work_ms": work_ms, "latency_ms": latency_ms, "instance": id,
                    "origin": {"partition": rec.partition, "offset": rec.offset},
                }),
            ));
            handled = Some(rec.offset);
        }

        let produced = results.len() as u64;
        if produced > 0 {
            match producer::send_batch(RESULTS_TOPIC.to_string(), results).await {
                Ok(outcomes) if outcomes.iter().all(|o| o.is_ok()) => {}
                _ => {
                    // Nothing is stored until the results are on the topic.
                    let _ = producer::send_batch(METRICS_TOPIC.to_string(), metrics).await;
                    return Err(HandlerError::Transient(Some("results produce failed".into())));
                }
            }
            let mut sorted = latencies.clone();
            sorted.sort_unstable();
            let p50 = sorted.get(sorted.len() / 2).copied().unwrap_or(0);
            let p95 = sorted.get((sorted.len() * 95 / 100).min(sorted.len() - 1)).copied().unwrap_or(0);
            let jobs_here = JOBS_ON_THIS_INSTANCE.with(|n| *n.borrow());
            metrics.push(metric(
                "consumed",
                last_ts,
                serde_json::json!({
                    "n": produced, "latency_p50_ms": p50, "latency_p95_ms": p95,
                    "defects": defects, "instance": id, "jobs_on_instance": jobs_here,
                    "partition": records.first().map(|r| r.partition),
                }),
            ));
            metrics.push(metric("produced", last_ts, serde_json::json!({"topic": RESULTS_TOPIC, "n": produced})));
        }

        let out = match failure {
            None => Ok(None),
            Some((reason, rec)) => match handled {
                Some(offset) => {
                    metrics.push(metric("partial", rec.timestamp, serde_json::json!({
                        "offset": offset, "partition": rec.partition, "reason": reason,
                    })));
                    Ok(Some(offset))
                }
                None => {
                    metrics.push(metric("dlq", rec.timestamp, serde_json::json!({
                        "n": 1, "reason": reason,
                        "job": rec.key.as_deref().map(|k| String::from_utf8_lossy(k).into_owned()),
                        "origin": {"topic": rec.topic, "partition": rec.partition, "offset": rec.offset},
                    })));
                    Err(HandlerError::Permanent(Some(reason)))
                }
            },
        };
        let _ = producer::send_batch(METRICS_TOPIC.to_string(), metrics).await;
        out
    }
}
