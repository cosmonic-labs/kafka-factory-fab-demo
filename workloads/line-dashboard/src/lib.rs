//! Fab 3 line dashboard — one component, two entries.
//!
//! - `cosmonic:kafka/handler@0.5.0`: the host pushes batches of `line.metrics`
//!   (the one instrumentation path every station and the simulator report
//!   into), the four DLQ topics (for the exact DLQ count and the origin
//!   headers on each dead letter) and the two disposition ledgers (read
//!   `read_committed`, for the live duplicate check) into `handle`, which
//!   folds them into an in-memory model.
//! - `wasi:http/handler@0.3.0`: serves the page (`/`), `GET /api/state` (the
//!   model, polled every 2 s), `GET /validate` (per-station produced /
//!   consumed / dlq / redelivered for `scripts/validate.sh`, which adds lag
//!   from `rpk group describe` and diffs) and `GET /healthz`.
//!
//! Both entries land on the same warm instance (`poolSize: 1`,
//! `reclaimMinInstances: 1`), so the fold lives in package-level memory.
//! It is a demo: a restart starts the panels empty and they refill. Rings
//! keep the last 4 h at 1-minute resolution and the last 2 min per second.
//!
//! Handler semantics: every record here is informational, so a record that
//! does not parse is skipped, never dead-lettered — the binding still names
//! `line.metrics.dlq` because every handler binding must.

mod bindings {
    use super::Component;
    wit_bindgen::generate!({ world: "kafka-handler-consumer", generate_all });
    export!(Component);
}

use std::cell::RefCell;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

use bindings::cosmonic::kafka::types::ConsumedRecord;
use bindings::exports::cosmonic::kafka::handler::{Guest as Handler, HandlerError};
use bindings::exports::wasi::http::handler as http;
use bindings::wasi::http::types::{Headers, Method};
use bindings::{wit_future, wit_stream};
use serde_json::{Value, json};

struct Component;

const PAGE: &str = include_str!("../ui/index.html");
const ICONS: &str = include_str!("../ui/icons.svg");
const MINUTES_KEPT: i64 = 240;
const SECONDS_KEPT: i64 = 120;
const INSTANCE_LIVE_MS: i64 = 30_000;
const FAULTS_KEPT: usize = 60;
const SAMPLES_KEPT: usize = 200;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Model

#[derive(Default)]
struct Ring {
    per_min: BTreeMap<i64, u64>,
    per_sec: BTreeMap<i64, u64>,
}

impl Ring {
    fn add(&mut self, ts: i64, n: u64) {
        *self.per_min.entry(ts / 60_000).or_default() += n;
        *self.per_sec.entry(ts / 1_000).or_default() += n;
        let now_min = now_ms() / 60_000;
        while self.per_min.first_key_value().is_some_and(|(k, _)| *k < now_min - MINUTES_KEPT) {
            self.per_min.pop_first();
        }
        let now_sec = now_ms() / 1_000;
        while self.per_sec.first_key_value().is_some_and(|(k, _)| *k < now_sec - SECONDS_KEPT) {
            self.per_sec.pop_first();
        }
    }
    /// Records per second over the last `secs` seconds (ending now).
    fn rate(&self, secs: i64) -> f64 {
        let now = now_ms() / 1_000;
        let sum: u64 = self.per_sec.range((now - secs)..=now).map(|(_, n)| n).sum();
        sum as f64 / secs as f64
    }
    fn minutes(&self, n: i64) -> Vec<u64> {
        let now = now_ms() / 60_000;
        (0..n).rev().map(|i| self.per_min.get(&(now - i)).copied().unwrap_or(0)).collect()
    }
}

#[derive(Default)]
struct Samples(VecDeque<u64>);

impl Samples {
    fn push(&mut self, v: u64) {
        self.0.push_back(v);
        while self.0.len() > SAMPLES_KEPT {
            self.0.pop_front();
        }
    }
    fn pct(&self, p: usize) -> Option<u64> {
        if self.0.is_empty() {
            return None;
        }
        let mut v: Vec<u64> = self.0.iter().copied().collect();
        v.sort_unstable();
        v.get((v.len() * p / 100).min(v.len() - 1)).copied()
    }
}

#[derive(Default)]
struct Station {
    consumed: u64,
    produced: u64,
    dlq: u64,
    redelivered: u64,
    partial: u64,
    replayed: u64,
    ring: Ring,
    instances: BTreeMap<String, i64>,
    instances_ever: u64,
    last_seen: i64,
    /// Station-specific numbers the page reads as-is.
    detail: serde_json::Map<String, Value>,
    latency: Samples,
    dead_letters: VecDeque<Value>,
    faults: VecDeque<Value>,
}

impl Station {
    fn live_instances(&self, now: i64) -> usize {
        self.instances.values().filter(|t| now - **t <= INSTANCE_LIVE_MS).count()
    }
    fn num(&mut self, key: &str, v: impl Into<Value>) {
        self.detail.insert(key.to_string(), v.into());
    }
    fn bump(&mut self, key: &str, n: u64) {
        let cur = self.detail.get(key).and_then(Value::as_u64).unwrap_or(0);
        self.detail.insert(key.to_string(), json!(cur + n));
    }
    fn fault(&mut self, ts: i64, severity: &str, text: &str, state: &str) -> Value {
        let f = json!({"ts": ts, "severity": severity, "text": text, "state": state});
        self.faults.push_front(f.clone());
        while self.faults.len() > 10 {
            self.faults.pop_back();
        }
        f
    }
}

#[derive(Default)]
struct Ledger {
    records: u64,
    units: HashSet<String>,
    duplicates: u64,
    pass: u64,
    fail: u64,
}

#[derive(Default)]
struct Model {
    started: i64,
    stations: BTreeMap<String, Station>,
    /// Simulator: last heartbeat and produced counts per topic.
    sim: Value,
    sim_produced: BTreeMap<String, u64>,
    sim_seen: i64,
    /// Every fault on the line, newest first.
    faults: VecDeque<Value>,
    /// Lag per group, as `scripts/validate.sh` last published it.
    lag: Value,
    ledger: Ledger,
    twin: Ledger,
    metrics_records: u64,
    batches: u64,
    /// The first few raw records seen per (topic, partition): a debugging aid
    /// served at /api/debug.
    samples: BTreeMap<String, Vec<Value>>,
    unparsed: u64,
}

thread_local! {
    static MODEL: RefCell<Model> = RefCell::new(Model { started: now_ms(), ..Default::default() });
}

/// Which station a topic's dead letters belong to.
fn dlq_station(topic: &str) -> Option<&'static str> {
    match topic {
        "dieattach.dlq" => Some("st02"),
        "wirebond.dlq" => Some("st03"),
        "inspect.dlq" => Some("st04"),
        "mold.dlq" => Some("st05"),
        _ => None,
    }
}

fn station_label(id: &str) -> &'static str {
    match id {
        "st01" => "ST-01 probe intake",
        "st02" => "ST-02 die attach",
        "st03" => "ST-03 wire bond",
        "st04" => "ST-04 inspection",
        "st05" => "ST-05 mold & cure",
        "st06" => "ST-06 final test",
        "st06twin" => "ST-06 twin",
        "sim" => "simulator",
        _ => "line",
    }
}

fn push_fault(m: &mut Model, station: &str, ts: i64, severity: &str, text: &str, state: &str) {
    let f = m.stations.entry(station.to_string()).or_default().fault(ts, severity, text, state);
    let mut row = f;
    row["station"] = json!(station);
    row["label"] = json!(station_label(station));
    m.faults.push_front(row);
    while m.faults.len() > FAULTS_KEPT {
        m.faults.pop_back();
    }
}

fn fold_metric(m: &mut Model, rec: &ConsumedRecord, v: &Value) {
    let station = v.get("station").and_then(Value::as_str).unwrap_or("line").to_string();
    let kind = v.get("kind").and_then(Value::as_str).unwrap_or("").to_string();
    let ts = v.get("ts").and_then(Value::as_i64).or(rec.timestamp).unwrap_or_else(now_ms);
    let n = v.get("n").and_then(Value::as_u64).unwrap_or(0);
    m.metrics_records += 1;

    if station == "sim" {
        match kind.as_str() {
            "produced" => {
                let topic = v.get("topic").and_then(Value::as_str).unwrap_or("?").to_string();
                *m.sim_produced.entry(topic).or_default() += n;
                m.sim_seen = ts.max(m.sim_seen);
            }
            "sim" => {
                m.sim = v.clone();
                m.sim_seen = rec.timestamp.unwrap_or_else(now_ms);
            }
            _ => {}
        }
        return;
    }
    if station == "ops" {
        if kind == "lag" {
            let mut lag = v.clone();
            lag["received"] = json!(now_ms());
            m.lag = lag;
        }
        return;
    }

    let st = m.stations.entry(station.clone()).or_default();
    st.last_seen = ts.max(st.last_seen);
    match kind.as_str() {
        "consumed" => {
            st.consumed += n;
            st.ring.add(ts, n);
            match station.as_str() {
                "st02" => {
                    st.bump("in_spec", v.get("in_spec").and_then(Value::as_u64).unwrap_or(0));
                    st.bump("flags", v.get("flags").and_then(Value::as_u64).unwrap_or(0));
                    if let Some(cpk) = v.get("cpk").and_then(Value::as_object) {
                        let mut cur = st.detail.get("cpk").and_then(Value::as_object).cloned().unwrap_or_default();
                        for (head, val) in cpk {
                            if let Some(x) = val.as_f64() {
                                let prev = cur.get(head).and_then(Value::as_f64).unwrap_or(x);
                                cur.insert(head.clone(), json!(((0.7 * prev + 0.3 * x) * 100.0).round() / 100.0));
                            }
                        }
                        st.detail.insert("cpk".into(), Value::Object(cur));
                    }
                }
                "st04" => {
                    if let Some(p) = v.get("latency_p95_ms").and_then(Value::as_u64) {
                        st.latency.push(p);
                    }
                    if let Some(ds) = v.get("defects").and_then(Value::as_array) {
                        let mut cur = st.detail.get("defects").and_then(Value::as_object).cloned().unwrap_or_default();
                        for d in ds.iter().filter_map(Value::as_str) {
                            let c = cur.get(d).and_then(Value::as_u64).unwrap_or(0);
                            cur.insert(d.to_string(), json!(c + 1));
                        }
                        st.detail.insert("defects".into(), Value::Object(cur));
                    }
                }
                "st06" => {
                    if let Some(bins) = v.get("bins").and_then(Value::as_object) {
                        let mut cur = st.detail.get("bins").and_then(Value::as_object).cloned().unwrap_or_default();
                        for (b, c) in bins {
                            let prev = cur.get(b).and_then(Value::as_u64).unwrap_or(0);
                            cur.insert(b.clone(), json!(prev + c.as_u64().unwrap_or(0)));
                        }
                        st.detail.insert("bins".into(), Value::Object(cur));
                    }
                    st.bump("batches", 1);
                }
                _ => {}
            }
        }
        "produced" => {
            st.produced += n;
            if station == "st01" {
                st.bump("lots", 1);
                st.bump("wafers", n);
                if let Some(p) = v.get("prober").and_then(Value::as_str) {
                    st.num("last_prober", p);
                }
                st.ring.add(ts, 1);
            }
        }
        "latency" => {
            if let Some(ms) = v.get("ms").and_then(Value::as_u64) {
                st.latency.push(ms);
            }
            st.bump("record_errors", v.get("errors").and_then(Value::as_u64).unwrap_or(0));
        }
        "rejected" => {
            st.bump("rejected", n.max(1));
            let text = format!(
                "upload rejected 400 — {}{}",
                v.get("reason").and_then(Value::as_str).unwrap_or("schema"),
                v.get("prober").and_then(Value::as_str).map(|p| format!(" · prober {p}")).unwrap_or_default()
            );
            push_fault(m, &station, ts, "warn", &text, "rejected at the edge");
        }
        "instance" => {
            if let Some(id) = v.get("id").and_then(Value::as_str) {
                if st.instances.insert(id.to_string(), ts).is_none() {
                    st.instances_ever += 1;
                }
            }
        }
        "redelivered" => st.redelivered += n,
        "partial" => {
            st.partial += 1;
            let text = format!(
                "partial batch: handled through offset {} on partition {}, rest redelivered ({})",
                v.get("offset").and_then(Value::as_i64).unwrap_or(-1),
                v.get("partition").and_then(Value::as_i64).unwrap_or(-1),
                v.get("reason").and_then(Value::as_str).unwrap_or("")
            );
            push_fault(m, &station, ts, "info", &text, "Ok(Some(offset))");
        }
        "replayed" => st.replayed += n,
        "window" => {
            st.bump("windows", 1);
            match station.as_str() {
                "st03" => {
                    if let Some(b) = v.get("bonder").and_then(Value::as_u64) {
                        let mut bonders = st.detail.get("bonders").and_then(Value::as_object).cloned().unwrap_or_default();
                        let mut e = bonders.get(&b.to_string()).and_then(Value::as_object).cloned().unwrap_or_default();
                        let w = e.get("windows").and_then(Value::as_u64).unwrap_or(0) + 1;
                        let nsop = e.get("nsop").and_then(Value::as_u64).unwrap_or(0) + u64::from(v.get("nsop").and_then(Value::as_bool).unwrap_or(false));
                        e.insert("windows".into(), json!(w));
                        e.insert("nsop".into(), json!(nsop));
                        e.insert("delta".into(), v.get("power_delta_pct").cloned().unwrap_or(Value::Null));
                        e.insert("us_w".into(), v.get("us_w_mean").cloned().unwrap_or(Value::Null));
                        e.insert("ts".into(), json!(ts));
                        bonders.insert(b.to_string(), Value::Object(e));
                        st.detail.insert("bonders".into(), Value::Object(bonders));
                        if v.get("nsop").and_then(Value::as_bool).unwrap_or(false) {
                            st.bump("nsop_windows", 1);
                        }
                        if let Some(n) = v.get("n").and_then(Value::as_u64) {
                            st.bump("window_samples", n);
                        }
                    }
                }
                "st05" => {
                    if let Some(presses) = v.get("presses").and_then(Value::as_array) {
                        let mut cur = st.detail.get("presses").and_then(Value::as_object).cloned().unwrap_or_default();
                        for p in presses {
                            if let Some(id) = p.get("press").and_then(Value::as_str) {
                                let mut e = p.clone();
                                e["ts"] = json!(ts);
                                cur.insert(id.to_string(), e);
                                let alarm = p.get("alarm").and_then(Value::as_bool).unwrap_or(false);
                                let hold = p.get("hold").and_then(Value::as_bool).unwrap_or(false);
                                if alarm || hold {
                                    let worst = p.get("worst_zone");
                                    let text = if alarm {
                                        format!(
                                            "zone {} {:+.1} °C against the cure profile · press {id}",
                                            worst.and_then(|w| w.get("zone")).and_then(Value::as_u64).unwrap_or(0),
                                            worst.and_then(|w| w.get("drift_c")).and_then(Value::as_f64).unwrap_or(0.0)
                                        )
                                    } else {
                                        format!("humidity / particle hold · press {id}")
                                    };
                                    let already = m.faults.iter().take(5).any(|f| f["text"] == json!(text));
                                    if !already {
                                        push_fault(m, &station, ts, "warn", &text, "watching");
                                    }
                                }
                            }
                        }
                        let st = m.stations.entry(station.clone()).or_default();
                        st.detail.insert("presses".into(), Value::Object(cur));
                        let conf: Vec<f64> = presses.iter().filter_map(|p| p.get("conformance_pct").and_then(Value::as_f64)).collect();
                        if !conf.is_empty() {
                            st.num("conformance_pct", (conf.iter().sum::<f64>() / conf.len() as f64 * 10.0).round() / 10.0);
                        }
                        if let Some(b) = v.get("batch").and_then(Value::as_u64) {
                            st.num("last_batch", b);
                        }
                    }
                }
                _ => {}
            }
        }
        "rebalance" => {
            st.bump("rebalances", 1);
            st.bump("windows_flushed_on_rebalance", v.get("windows_flushed").and_then(Value::as_u64).unwrap_or(0));
            let text = format!(
                "rebalance {} partitions {}",
                v.get("event").and_then(Value::as_str).unwrap_or("?"),
                v.get("partitions").map(|p| p.to_string()).unwrap_or_default()
            );
            push_fault(m, &station, ts, "info", &text, "session");
        }
        "seek" => {
            st.bump("seeks", 1);
            let text = format!(
                "seek partition {} back to offset {} — replaying bonder {}",
                v.get("partition").and_then(Value::as_i64).unwrap_or(-1),
                v.get("to_offset").and_then(Value::as_i64).unwrap_or(-1),
                v.get("bonder").and_then(Value::as_u64).unwrap_or(0)
            );
            push_fault(m, &station, ts, "info", &text, "replay");
        }
        "started" => {
            for k in ["batch_size", "nsop_threshold_pct", "window_ms"] {
                if let Some(x) = v.get(k) {
                    st.detail.insert(k.into(), x.clone());
                }
            }
            st.bump("starts", 1);
            let text = format!(
                "service started (BATCH_SIZE {}, NSOP threshold {}%)",
                v.get("batch_size").and_then(Value::as_u64).unwrap_or(0),
                v.get("nsop_threshold_pct").and_then(Value::as_f64).unwrap_or(0.0)
            );
            push_fault(m, &station, ts, "info", &text, "resumed from committed offsets");
        }
        "txn" => {
            st.bump("txns", 1);
            if let Some(ms) = v.get("ms").and_then(Value::as_u64) {
                st.latency.push(ms);
            }
        }
        "txn_abort" => {
            st.bump("aborted", 1);
            if v.get("requires_abort").and_then(Value::as_bool).unwrap_or(false) {
                st.bump("requires_abort", 1);
            }
            if v.get("fatal").and_then(Value::as_bool).unwrap_or(false) {
                st.bump("fatal", 1);
            }
            let text = format!("transaction aborted — {}", v.get("reason").and_then(Value::as_str).unwrap_or(""));
            push_fault(m, &station, ts, "warn", &text, "replayed from committed offsets");
        }
        "commit" => st.bump("commits", 1),
        "dlq" => {
            // The count comes from the DLQ topic itself; this is the
            // station's own account of why.
            let text = format!(
                "→ {} · {}{}",
                v.get("origin").and_then(|o| o.get("topic")).and_then(Value::as_str).map(|t| dlq_for(t)).unwrap_or("dlq"),
                v.get("reason").and_then(Value::as_str).unwrap_or("permanent"),
                v.get("die").or(v.get("job")).or(v.get("press")).and_then(Value::as_str).map(|k| format!(" · {k}")).unwrap_or_default()
            );
            push_fault(m, &station, ts, "warn", &text, "Err(Permanent) → dead-lettered");
        }
        "fault" => {
            let sev = v.get("severity").and_then(Value::as_str).unwrap_or("warn").to_string();
            let text = v.get("text").or(v.get("reason")).and_then(Value::as_str).unwrap_or("fault").to_string();
            push_fault(m, &station, ts, &sev, &text, "open");
        }
        _ => {}
    }
}

fn dlq_for(topic: &str) -> &'static str {
    match topic {
        "dieattach.readings" => "dieattach.dlq",
        "wirebond.raw" => "wirebond.dlq",
        "inspect.jobs" => "inspect.dlq",
        "mold.telemetry" => "mold.dlq",
        _ => "dlq",
    }
}

fn fold_dead_letter(m: &mut Model, station: &str, rec: &ConsumedRecord) {
    let headers: serde_json::Map<String, Value> = rec
        .headers
        .iter()
        .map(|h| (h.key.clone(), json!(h.value.as_deref().map(|v| String::from_utf8_lossy(v).into_owned()))))
        .collect();
    let key = rec.key.as_deref().map(|k| String::from_utf8_lossy(k).into_owned());
    let value = rec.value.as_deref().map(|v| String::from_utf8_lossy(v).chars().take(300).collect::<String>());
    let ts = rec.timestamp.unwrap_or_else(now_ms);
    let st = m.stations.entry(station.to_string()).or_default();
    st.dlq += 1;
    st.dead_letters.push_front(json!({
        "ts": ts, "topic": rec.topic, "partition": rec.partition, "offset": rec.offset,
        "key": key, "headers": headers, "value": value,
    }));
    while st.dead_letters.len() > 5 {
        st.dead_letters.pop_back();
    }
    let reason = headers
        .iter()
        .find(|(k, _)| k.contains("reason") || k.contains("detail") || k.contains("error"))
        .and_then(|(_, v)| v.as_str())
        .unwrap_or("permanent");
    let origin = headers
        .iter()
        .filter(|(k, _)| k.contains("origin") || k.contains("source"))
        .map(|(k, v)| format!("{k}={}", v.as_str().unwrap_or("")))
        .collect::<Vec<_>>()
        .join(" ");
    let text = format!("1 record in {} · {reason}{}", rec.topic, if origin.is_empty() { String::new() } else { format!(" · {origin}") });
    push_fault(m, station, ts, "warn", &text, "dead-lettered · partition advanced");
}

fn fold_ledger(ledger: &mut Ledger, rec: &ConsumedRecord) {
    ledger.records += 1;
    let Some(v) = rec.value.as_deref().and_then(|b| serde_json::from_slice::<Value>(b).ok()) else { return };
    if let Some(unit) = v.get("unit").and_then(Value::as_str) {
        if !ledger.units.insert(unit.to_string()) {
            ledger.duplicates += 1;
        }
    }
    match v.get("disposition").and_then(Value::as_str) {
        Some("pass") => ledger.pass += 1,
        Some(_) => ledger.fail += 1,
        None => {}
    }
}

impl Handler for Component {
    async fn handle(records: Vec<ConsumedRecord>) -> Result<Option<i64>, HandlerError> {
        MODEL.with(|m| {
            let mut m = m.borrow_mut();
            m.batches += 1;
            for rec in &records {
                let key = format!("{}/{}", rec.topic, rec.partition);
                let seen = m.samples.entry(key).or_default();
                if seen.len() < 3 {
                    seen.push(json!({
                        "offset": rec.offset, "ts": rec.timestamp, "ts_type": format!("{:?}", rec.timestamp_type),
                        "key": rec.key.as_deref().map(|k| String::from_utf8_lossy(k).into_owned()),
                        "value_len": rec.value.as_ref().map(Vec::len),
                        "value_head": rec.value.as_deref().map(|v| String::from_utf8_lossy(&v[..v.len().min(120)]).into_owned()),
                        "headers": rec.headers.len(),
                    }));
                }
                match rec.topic.as_str() {
                    "line.metrics" => {
                        match rec.value.as_deref().map(serde_json::from_slice::<Value>) {
                            Some(Ok(v)) => fold_metric(&mut m, rec, &v),
                            _ => m.unparsed += 1,
                        }
                    }
                    "lot.disposition" => {
                        fold_ledger(&mut m.ledger, rec);
                        let dups = m.ledger.duplicates;
                        if dups > 0 && dups.is_power_of_two() {
                            push_fault(&mut m, "st06", rec.timestamp.unwrap_or_else(now_ms), "crit", &format!("{dups} duplicate unit(s) in lot.disposition"), "must read 0");
                        }
                    }
                    "lot.disposition.twin" => {
                        fold_ledger(&mut m.twin, rec);
                        let dups = m.twin.duplicates;
                        if dups > 0 && (dups == 1 || dups % 100 == 0) {
                            push_fault(&mut m, "st06twin", rec.timestamp.unwrap_or_else(now_ms), "warn", &format!("{dups} duplicate unit(s) in lot.disposition.twin (at-least-once replay)"), "expected");
                        }
                    }
                    t => {
                        if let Some(station) = dlq_station(t) {
                            fold_dead_letter(&mut m, station, rec);
                        }
                    }
                }
            }
        });
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// HTTP

fn station_json(id: &str, st: &Station, now: i64) -> Value {
    let mut d = Value::Object(st.detail.clone());
    d["consumed"] = json!(st.consumed);
    d["produced"] = json!(st.produced);
    d["dlq"] = json!(st.dlq);
    d["redelivered"] = json!(st.redelivered);
    d["partial"] = json!(st.partial);
    d["replayed"] = json!(st.replayed);
    d["rate_10s"] = json!((st.ring.rate(10) * 10.0).round() / 10.0);
    d["rate_60s"] = json!((st.ring.rate(60) * 10.0).round() / 10.0);
    d["per_min"] = json!(st.ring.minutes(240));
    d["live_instances"] = json!(st.live_instances(now));
    d["instances_ever"] = json!(st.instances_ever);
    d["last_seen"] = json!(st.last_seen);
    d["latency_p50"] = json!(st.latency.pct(50));
    d["latency_p95"] = json!(st.latency.pct(95));
    d["dead_letters"] = json!(st.dead_letters);
    d["faults"] = json!(st.faults);
    d["id"] = json!(id);
    d
}

fn state_json() -> Value {
    let now = now_ms();
    MODEL.with(|m| {
        let m = m.borrow();
        let mut stations = serde_json::Map::new();
        for id in ["st01", "st02", "st03", "st04", "st05", "st06", "st06twin"] {
            let st = m.stations.get(id).cloned_or_default();
            stations.insert(id.to_string(), station_json(id, &st, now));
        }
        let open_faults = m.faults.iter().filter(|f| f["severity"] == "crit" || f["severity"] == "warn").count();
        let total_lag: Option<u64> = m.lag.get("groups").and_then(Value::as_object).map(|g| g.values().filter_map(Value::as_u64).sum());
        json!({
            "now": now,
            "uptime_s": (now - m.started) / 1000,
            "title": "Fab 3 Line",
            "sim": {
                "heartbeat": m.sim,
                "seen": m.sim_seen,
                "age_s": if m.sim_seen > 0 { (now - m.sim_seen) / 1000 } else { -1 },
                "produced": m.sim_produced,
            },
            "stations": stations,
            "ledger": {
                "records": m.ledger.records, "units": m.ledger.units.len(), "duplicates": m.ledger.duplicates,
                "pass": m.ledger.pass, "fail": m.ledger.fail,
            },
            "twin": {
                "records": m.twin.records, "units": m.twin.units.len(), "duplicates": m.twin.duplicates,
            },
            "faults": m.faults,
            "open_faults": open_faults,
            "lag": m.lag,
            "total_lag": total_lag,
            "fold": {"records": m.metrics_records, "batches": m.batches},
        })
    })
}

trait ClonedOrDefault {
    fn cloned_or_default(&self) -> Station;
}
impl ClonedOrDefault for Option<&Station> {
    fn cloned_or_default(&self) -> Station {
        match self {
            Some(s) => Station {
                consumed: s.consumed,
                produced: s.produced,
                dlq: s.dlq,
                redelivered: s.redelivered,
                partial: s.partial,
                replayed: s.replayed,
                ring: Ring { per_min: s.ring.per_min.clone(), per_sec: s.ring.per_sec.clone() },
                instances: s.instances.clone(),
                instances_ever: s.instances_ever,
                last_seen: s.last_seen,
                detail: s.detail.clone(),
                latency: Samples(s.latency.0.clone()),
                dead_letters: s.dead_letters.clone(),
                faults: s.faults.clone(),
            },
            None => Station::default(),
        }
    }
}

/// `{station: {produced, consumed, dlq, redelivered, replayed}}` — produced is
/// what was put on the station's INPUT topic (the simulator's count, plus
/// ST-01's HTTP lots for probe.lots), consumed is the station's own count.
fn validate_json() -> Value {
    MODEL.with(|m| {
        let m = m.borrow();
        let inputs = [
            ("st02", "dieattach.readings"),
            ("st03", "wirebond.raw"),
            ("st04", "inspect.jobs"),
            ("st05", "mold.telemetry"),
            ("st06", "finaltest.bins"),
            ("st06twin", "finaltest.bins"),
        ];
        let mut out = serde_json::Map::new();
        for (id, topic) in inputs {
            let st = m.stations.get(id);
            out.insert(
                id.to_string(),
                json!({
                    "input_topic": topic,
                    "produced": m.sim_produced.get(topic).copied().unwrap_or(0),
                    "consumed": st.map(|s| s.consumed).unwrap_or(0),
                    "dlq": st.map(|s| s.dlq).unwrap_or(0),
                    "redelivered": st.map(|s| s.redelivered).unwrap_or(0),
                    "replayed": st.map(|s| s.replayed).unwrap_or(0),
                    "partial": st.map(|s| s.partial).unwrap_or(0),
                    "outputs": st.map(|s| s.produced).unwrap_or(0),
                }),
            );
        }
        let st01 = m.stations.get("st01");
        out.insert(
            "st01".into(),
            json!({
                "input_topic": "probe.lots",
                "produced": st01.map(|s| s.produced).unwrap_or(0),
                "produced_by_simulator": m.sim_produced.get("probe.lots").copied().unwrap_or(0),
                "lots": st01.and_then(|s| s.detail.get("lots").cloned()).unwrap_or(json!(0)),
                "rejected": st01.and_then(|s| s.detail.get("rejected").cloned()).unwrap_or(json!(0)),
            }),
        );
        json!({
            "stations": out,
            "ledger": {"records": m.ledger.records, "units": m.ledger.units.len(), "duplicates": m.ledger.duplicates},
            "twin": {"records": m.twin.records, "units": m.twin.units.len(), "duplicates": m.twin.duplicates},
            "sim": {"heartbeat": m.sim, "produced": m.sim_produced},
            "lag": m.lag,
        })
    })
}

impl http::Guest for Component {
    async fn handle(req: http::Request) -> Result<http::Response, http::ErrorCode> {
        let path = req.get_path_with_query().unwrap_or_default();
        let path = path.split('?').next().unwrap_or("").to_string();
        Ok(match (req.get_method(), path.as_str()) {
            (Method::Get, "/") | (Method::Get, "/index.html") => {
                let page = PAGE.replace("<!--ICONS-->", ICONS);
                resp(200, "text/html; charset=utf-8", page.into_bytes())
            }
            (Method::Get, "/api/state") => resp(200, "application/json", state_json().to_string().into_bytes()),
            (Method::Get, "/validate") => resp(200, "application/json", validate_json().to_string().into_bytes()),
            (Method::Get, "/healthz") => resp(200, "text/plain", b"ok".to_vec()),
            (Method::Get, "/api/debug") => {
                let v = MODEL.with(|m| {
                    let m = m.borrow();
                    json!({"samples": m.samples, "unparsed": m.unparsed, "batches": m.batches, "metrics_records": m.metrics_records})
                });
                resp(200, "application/json", v.to_string().into_bytes())
            }
            (Method::Get, "/icons.svg") => resp(200, "image/svg+xml", ICONS.as_bytes().to_vec()),
            _ => resp(404, "text/plain", b"no such route: /, /api/state, /validate, /healthz".to_vec()),
        })
    }
}

fn resp(status: u16, content_type: &str, body: Vec<u8>) -> http::Response {
    let (trailers_tx, trailers_rx) = wit_future::new(|| unreachable!());
    let (mut body_tx, body_rx) = wit_stream::new();
    wit_bindgen::spawn_local(async move {
        body_tx.write_all(body).await;
        let _ = trailers_tx.write(Ok(None)).await;
    });
    let headers = Headers::from_list(&[
        ("content-type".to_string(), content_type.as_bytes().to_vec()),
        ("cache-control".to_string(), b"no-store".to_vec()),
    ])
    .unwrap_or_else(|_| Headers::new());
    let (response, _fut) = http::Response::new(headers, Some(body_rx), trailers_rx);
    let _ = response.set_status_code(status);
    response
}
