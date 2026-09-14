//! ST-05 Mold & cure — `rust-kafka-handler-consumer` + producer, standing in
//! for the proposed stream-processor export.
//!
//! The host calls `handle` with up to `handler.batch.size: "1000"` records
//! from one partition of `mold.telemetry` (records are keyed by press, so a
//! partition carries whole presses). One batch ≈ one window: per press,
//! cure-profile conformance (8 oven zones against 175 ± 2 °C) and per-zone
//! drift over the batch, produced as one summary per press to `mold.profile`.
//!
//! There is deliberately **no cross-batch state**: calls land on different
//! instances and idle ones are reclaimed, so required state cannot live in a
//! handler instance (the skill's rule). The proposed
//! `cosmonic:kafka/stream-handler@0.6.0` export — a per-partition record
//! stream with `flush()` on revoke — is what would replace this batch-as-window
//! approximation (docs/DECISIONS.md).
//!
//! A slow window wants `max.poll.interval.ms` (900000 here), not a smaller
//! pool: the per-call deadline derives from it and covers the whole batch.
//!
//! Return value semantics (at-least-once): `Ok(None)` handles the batch;
//! `Ok(Some(offset))` handles through that record; `Transient` retries;
//! `Permanent` dead-letters the head record to `mold.dlq`. Malformed
//! telemetry is `Permanent`, never a panic.

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

const STATION: &str = "st05";
const PROFILE_TOPIC: &str = "mold.profile";
const METRICS_TOPIC: &str = "line.metrics";
const PROFILE_C: f64 = 175.0;
const PROFILE_TOL_C: f64 = 2.0;
const ZONES: usize = 8;
const MIN_ZONE_SAMPLES: u64 = 4;

#[derive(serde::Deserialize)]
struct Sample {
    press: String,
    zone: usize,
    temp_c: f64,
    #[serde(default)]
    rh_pct: Option<f64>,
    #[serde(default)]
    particles: Option<u64>,
}

#[derive(Default)]
struct PressWindow {
    n: u64,
    in_band: u64,
    zone_sum: [f64; ZONES],
    zone_n: [u64; ZONES],
    rh_max: f64,
    particles_max: u64,
    first_ts: Option<i64>,
    last_ts: Option<i64>,
}

thread_local! {
    static INSTANCE_ID: RefCell<Option<String>> = const { RefCell::new(None) };
    static LAST_HEARTBEAT: RefCell<i64> = const { RefCell::new(0) };
}

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
        h.write_u64(0x1750);
        let id = format!("{STATION}-{:08x}", h.finish() as u32);
        *slot = Some(id.clone());
        (id, true)
    })
}

fn parse(value: &[u8]) -> Result<Sample, String> {
    let text = std::str::from_utf8(value).map_err(|_| "value is not valid UTF-8".to_string())?;
    let s: Sample = serde_json::from_str(text).map_err(|e| format!("bad sample: {e}"))?;
    if !(1..=ZONES).contains(&s.zone) {
        return Err(format!("zone {} outside 1..{ZONES}", s.zone));
    }
    if !s.temp_c.is_finite() {
        return Err("temp_c is not finite".into());
    }
    Ok(s)
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

fn round1(x: f64) -> f64 {
    (x * 10.0).round() / 10.0
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

        let mut windows: BTreeMap<String, PressWindow> = BTreeMap::new();
        let mut failure: Option<(String, &ConsumedRecord)> = None;
        let mut consumed: u64 = 0;

        for rec in &records {
            let Some(value) = rec.value.as_deref() else {
                handled = Some(rec.offset);
                consumed += 1;
                continue;
            };
            let s = match parse(value) {
                Ok(s) => s,
                Err(reason) => {
                    failure = Some((reason, rec));
                    break;
                }
            };
            let w = windows.entry(s.press.clone()).or_default();
            w.n += 1;
            if (s.temp_c - PROFILE_C).abs() <= PROFILE_TOL_C {
                w.in_band += 1;
            }
            w.zone_sum[s.zone - 1] += s.temp_c;
            w.zone_n[s.zone - 1] += 1;
            if let Some(rh) = s.rh_pct {
                w.rh_max = w.rh_max.max(rh);
            }
            if let Some(p) = s.particles {
                w.particles_max = w.particles_max.max(p);
            }
            if w.first_ts.is_none() {
                w.first_ts = rec.timestamp;
            }
            w.last_ts = rec.timestamp;
            consumed += 1;
            handled = Some(rec.offset);
        }

        // One summary per press for this window.
        let mut profiles: Vec<ProduceRecord> = Vec::new();
        let mut summary: Vec<serde_json::Value> = Vec::new();
        for (press, w) in &windows {
            let zones: Vec<Option<f64>> = (0..ZONES)
                .map(|z| (w.zone_n[z] > 0).then(|| round1(w.zone_sum[z] / w.zone_n[z] as f64)))
                .collect();
            let drift: Vec<Option<f64>> = zones
                .iter()
                .map(|t| t.map(|t| round1(t - PROFILE_C)))
                .collect();
            let worst = drift
                .iter()
                .enumerate()
                .filter_map(|(i, d)| d.map(|d| (i + 1, d)))
                .max_by(|a, b| a.1.abs().total_cmp(&b.1.abs()));
            let conformance = if w.n > 0 { round1(100.0 * w.in_band as f64 / w.n as f64) } else { 100.0 };
            // A zone mean over fewer than MIN_ZONE_SAMPLES samples is noise,
            // not drift: with one record per zone per batch (the host hands
            // over whatever is fetched, ~1 s of data) a 3σ sample would alarm
            // every few seconds. This is the batch-as-window approximation's
            // limit; a stream handler with real windows would not need it.
            let enough = w.zone_n.iter().all(|n| *n >= MIN_ZONE_SAMPLES);
            let alarm = enough && worst.is_some_and(|(_, d)| d.abs() > PROFILE_TOL_C);
            let hold = w.rh_max > 60.0 || w.particles_max > 1000;
            let v = serde_json::json!({
                "press": press, "samples": w.n, "conformance_pct": conformance,
                "zones_c": zones, "drift_c": drift, "worst_zone": worst.map(|(z, d)| serde_json::json!({"zone": z, "drift_c": d})),
                "alarm": alarm, "hold": hold, "enough_samples": enough, "rh_max": round1(w.rh_max), "particles_max": w.particles_max,
                "window": {"from": w.first_ts, "to": w.last_ts},
            });
            summary.push(v.clone());
            profiles.push(json_record(press, v));
        }
        let produced = profiles.len() as u64;
        if produced > 0 {
            match producer::send_batch(PROFILE_TOPIC.to_string(), profiles).await {
                Ok(outcomes) if outcomes.iter().all(|o| o.is_ok()) => {}
                _ => {
                    let _ = producer::send_batch(METRICS_TOPIC.to_string(), metrics).await;
                    return Err(HandlerError::Transient(Some("profile produce failed".into())));
                }
            }
        }
        if consumed > 0 {
            metrics.push(metric(
                "window",
                last_ts,
                serde_json::json!({
                    "n": consumed, "presses": summary, "batch": records.len(),
                    "partition": records.first().map(|r| r.partition),
                }),
            ));
            metrics.push(metric("consumed", last_ts, serde_json::json!({"n": consumed, "partition": records.first().map(|r| r.partition)})));
        }
        if produced > 0 {
            metrics.push(metric("produced", last_ts, serde_json::json!({"topic": PROFILE_TOPIC, "n": produced})));
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
                        "press": rec.key.as_deref().map(|k| String::from_utf8_lossy(k).into_owned()),
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
