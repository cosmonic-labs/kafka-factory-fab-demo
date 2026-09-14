//! ST-02 Die attach — `rust-kafka-handler-consumer` + producer.
//!
//! The host owns the consumer and calls `handle` with a batch of records from
//! one partition of `dieattach.readings`, in offset order. Per record: parse,
//! check bond-line thickness / epoxy weight / placement offset against fixed
//! spec limits, and `producer::send` an out-of-spec flag to `dieattach.flags`.
//! Calls may use different component instances.
//!
//! Return value semantics (at-least-once):
//! - `Ok(None)` handles the whole batch.
//! - `Ok(Some(offset))` handles through that record and retries the suffix.
//! - `Transient` retries from the first record.
//! - `Permanent` dead-letters the failing head record (a single-record batch
//!   goes to `dieattach.dlq` with the origin topic/partition/offset as headers
//!   and the partition advances).
//! - A trap retries the head record and dead-letters it after five failures.
//!
//! Malformed input (a NaN thickness, a missing field) is `Permanent`, never a
//! panic — except in the `naive` feature build, which panics on purpose so
//! demo beat 4 can show the five-trap path next to this one.
//!
//! Package-level state is a cache, never isolation: the instance id for the
//! `kind: instance` heartbeat and the per-partition high-water offset used to
//! spot redeliveries live in a `thread_local`, and mean nothing across
//! instances.

mod bindings {
    use super::Component;
    wit_bindgen::generate!({ world: "kafka-handler-consumer", generate_all });
    export!(Component);
}

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::hash::{BuildHasher, Hasher};

use bindings::cosmonic::kafka::producer;
use bindings::cosmonic::kafka::types::{ConsumedRecord, ProduceRecord};
use bindings::exports::cosmonic::kafka::handler::{Guest as Handler, HandlerError};

struct Component;

const STATION: &str = "st02";
const FLAGS_TOPIC: &str = "dieattach.flags";
const METRICS_TOPIC: &str = "line.metrics";

// Fixed spec limits for the die-attach process.
const BL_UM_LSL: f64 = 20.0;
const BL_UM_USL: f64 = 30.0;
const EPOXY_MG_LSL: f64 = 2.5;
const EPOXY_MG_USL: f64 = 4.0;
const PLACEMENT_UM_MAX: f64 = 15.0;

#[derive(serde::Deserialize)]
struct Reading {
    die: String,
    head: String,
    bl_um: f64,
    epoxy_mg: f64,
    dx_um: f64,
    dy_um: f64,
    #[serde(default)]
    prober: Option<String>,
}

thread_local! {
    static INSTANCE_ID: RefCell<Option<String>> = const { RefCell::new(None) };
    static LAST_HEARTBEAT: RefCell<i64> = const { RefCell::new(0) };
    static HIGH_WATER: RefCell<BTreeMap<i32, i64>> = const { RefCell::new(BTreeMap::new()) };
}

/// A random-ish id, minted the first time this instance is called (the
/// std hasher is seeded from the host's random source per instance).
/// The `kind: instance` heartbeat: on the first call of this instance and
/// then every 10 s of record time, so the dashboard's "seen in the last
/// 30 s" count follows the pool as instances are reused and reclaimed.
fn heartbeat_due(fresh: bool, ts: Option<i64>) -> bool {
    LAST_HEARTBEAT.with(|last| {
        let now = ts.unwrap_or(0);
        let mut last = last.borrow_mut();
        if fresh || now - *last >= 10_000 {
            *last = now;
            true
        } else {
            false
        }
    })
}

fn instance_id() -> (String, bool) {
    INSTANCE_ID.with(|slot| {
        let mut slot = slot.borrow_mut();
        if let Some(id) = slot.as_ref() {
            return (id.clone(), false);
        }
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        h.write_u64(0x5eed);
        let id = format!("{STATION}-{:08x}", h.finish() as u32);
        *slot = Some(id.clone());
        (id, true)
    })
}

/// Records at or below the partition's last handled offset are redeliveries.
fn note_redelivery(partition: i32, first: i64, last: i64) -> u64 {
    HIGH_WATER.with(|hw| {
        let mut hw = hw.borrow_mut();
        let seen = hw.get(&partition).copied();
        hw.insert(partition, last.max(seen.unwrap_or(last)));
        match seen {
            Some(s) if first <= s => (s - first + 1).min(last - first + 1).max(0) as u64,
            _ => 0,
        }
    })
}

enum Verdict {
    InSpec,
    Flag(String),
}

fn check(r: &Reading) -> Verdict {
    let mut reasons = Vec::new();
    if !(BL_UM_LSL..=BL_UM_USL).contains(&r.bl_um) {
        reasons.push(format!("bond-line {} um outside {BL_UM_LSL}-{BL_UM_USL}", r.bl_um));
    }
    if !(EPOXY_MG_LSL..=EPOXY_MG_USL).contains(&r.epoxy_mg) {
        reasons.push(format!("epoxy {} mg outside {EPOXY_MG_LSL}-{EPOXY_MG_USL}", r.epoxy_mg));
    }
    if r.dx_um.abs() > PLACEMENT_UM_MAX || r.dy_um.abs() > PLACEMENT_UM_MAX {
        reasons.push(format!("placement ({}, {}) um beyond ±{PLACEMENT_UM_MAX}", r.dx_um, r.dy_um));
    }
    if reasons.is_empty() {
        Verdict::InSpec
    } else {
        Verdict::Flag(reasons.join("; "))
    }
}

/// Parse one reading. `NaN` (a JSON string, since JSON has no NaN), a
/// non-finite number or a missing field is malformed: `Err(reason)`.
fn parse(value: &[u8]) -> Result<Reading, String> {
    let text = std::str::from_utf8(value).map_err(|_| "value is not valid UTF-8".to_string())?;
    let raw: serde_json::Value = serde_json::from_str(text).map_err(|e| format!("not JSON: {e}"))?;
    for field in ["bl_um", "epoxy_mg", "dx_um", "dy_um"] {
        match raw.get(field) {
            Some(serde_json::Value::Number(n)) if n.as_f64().is_some_and(f64::is_finite) => {}
            Some(other) => return Err(format!("{field} is not a finite number: {other}")),
            None => return Err(format!("{field} missing")),
        }
    }
    serde_json::from_value::<Reading>(raw).map_err(|e| format!("bad reading: {e}"))
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

/// Per-head Cpk of bond-line thickness over the batch (needs ≥ 2 samples).
fn cpk_by_head(samples: &BTreeMap<String, Vec<f64>>) -> serde_json::Value {
    let mut out = serde_json::Map::new();
    for (head, xs) in samples {
        if xs.len() < 2 {
            continue;
        }
        let n = xs.len() as f64;
        let mean = xs.iter().sum::<f64>() / n;
        let var = xs.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1.0);
        let sd = var.sqrt();
        if sd <= f64::EPSILON {
            continue;
        }
        let cpk = ((BL_UM_USL - mean) / (3.0 * sd)).min((mean - BL_UM_LSL) / (3.0 * sd));
        out.insert(head.clone(), serde_json::json!((cpk * 100.0).round() / 100.0));
    }
    serde_json::Value::Object(out)
}

impl Handler for Component {
    async fn handle(records: Vec<ConsumedRecord>) -> Result<Option<i64>, HandlerError> {
        let mut handled: Option<i64> = None;
        let mut metrics: Vec<ProduceRecord> = Vec::new();
        let (id, fresh) = instance_id();
        let last_ts = records.last().and_then(|r| r.timestamp);
        if heartbeat_due(fresh, last_ts) {
            metrics.push(metric("instance", last_ts, serde_json::json!({"id": id, "fresh": fresh})));
        }
        if let (Some(first), Some(last)) = (records.first(), records.last()) {
            let redelivered = note_redelivery(first.partition, first.offset, last.offset);
            if redelivered > 0 {
                metrics.push(metric(
                    "redelivered",
                    last_ts,
                    serde_json::json!({"n": redelivered, "partition": first.partition, "offset": first.offset}),
                ));
                // Sent before the work: a redelivery that traps again (the
                // naive build's five-trap path) would otherwise never report.
                let _ = producer::send_batch(METRICS_TOPIC.to_string(), std::mem::take(&mut metrics)).await;
            }
        }

        let mut consumed: u64 = 0;
        let mut in_spec: u64 = 0;
        let mut flags: u64 = 0;
        let mut samples: BTreeMap<String, Vec<f64>> = BTreeMap::new();
        #[allow(unused_mut)]
        let mut failure: Option<(String, &ConsumedRecord)> = None;

        for rec in &records {
            let Some(value) = rec.value.as_deref() else {
                // A tombstone: nothing to check.
                handled = Some(rec.offset);
                consumed += 1;
                continue;
            };
            let reading = match parse(value) {
                Ok(r) => r,
                Err(reason) => {
                    #[cfg(feature = "naive")]
                    {
                        // The five-trap path: a panic is retried like a
                        // transient error until the host gives up on the
                        // record. Deliberate, and only in this build.
                        panic!("st02-die-attach (naive): unparseable reading: {reason}");
                    }
                    #[cfg(not(feature = "naive"))]
                    {
                        failure = Some((reason, rec));
                        break;
                    }
                }
            };
            consumed += 1;
            samples.entry(reading.head.clone()).or_default().push(reading.bl_um);
            match check(&reading) {
                Verdict::InSpec => in_spec += 1,
                Verdict::Flag(reason) => {
                    let flag = json_record(
                        &reading.die,
                        serde_json::json!({
                            "die": reading.die, "head": reading.head, "reason": reason,
                            "bl_um": reading.bl_um, "epoxy_mg": reading.epoxy_mg,
                            "dx_um": reading.dx_um, "dy_um": reading.dy_um,
                            "prober": reading.prober,
                            "origin": {"partition": rec.partition, "offset": rec.offset},
                        }),
                    );
                    if let Err(e) = producer::send(FLAGS_TOPIC.to_string(), flag).await {
                        // The broker did not take the flag: nothing past this
                        // record is handled; keep the work already done.
                        let _ = e;
                        metrics.push(metric("consumed", last_ts, serde_json::json!({
                            "n": consumed - 1, "in_spec": in_spec, "flags": flags,
                            "cpk": cpk_by_head(&samples), "partition": rec.partition,
                            "first": records.first().map(|r| r.offset), "last": Some(rec.offset),
                        })));
                        let _ = producer::send_batch(METRICS_TOPIC.to_string(), metrics).await;
                        return handled.map_or(Err(HandlerError::Transient(Some("flag produce failed".into()))), |o| Ok(Some(o)));
                    }
                    flags += 1;
                }
            }
            handled = Some(rec.offset);
        }

        if consumed > 0 {
            metrics.push(metric(
                "consumed",
                last_ts,
                serde_json::json!({
                    "n": consumed, "in_spec": in_spec, "flags": flags,
                    "cpk": cpk_by_head(&samples),
                    "partition": records.first().map(|r| r.partition),
                    "first": records.first().map(|r| r.offset), "last": records.last().map(|r| r.offset),
                }),
            ));
        }
        if flags > 0 {
            metrics.push(metric("produced", last_ts, serde_json::json!({"topic": FLAGS_TOPIC, "n": flags})));
        }

        let verdict = match failure {
            None => Ok(None),
            Some((reason, rec)) => match handled {
                // Keep the work already done; the host redelivers from here.
                Some(offset) => {
                    metrics.push(metric("partial", rec.timestamp, serde_json::json!({
                        "offset": offset, "partition": rec.partition, "reason": reason,
                    })));
                    Ok(Some(offset))
                }
                // The head record alone: this is the verdict that dead-letters it.
                None => {
                    let die = rec.key.as_deref().map(|k| String::from_utf8_lossy(k).into_owned());
                    metrics.push(metric("dlq", rec.timestamp, serde_json::json!({
                        "n": 1, "reason": reason, "die": die,
                        "origin": {"topic": rec.topic, "partition": rec.partition, "offset": rec.offset},
                    })));
                    Err(HandlerError::Permanent(Some(reason)))
                }
            },
        };
        let _ = producer::send_batch(METRICS_TOPIC.to_string(), metrics).await;
        verdict
    }
}
