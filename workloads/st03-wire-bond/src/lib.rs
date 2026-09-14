//! ST-03 Wire bond — `rust-kafka-pull-service`.
//!
//! The one station that needs the consumer *session*, which is the only
//! reason to leave the handler: 5-second windows per bonder (40 bonders)
//! over `wirebond.raw` computing mean/σ of ultrasonic power and bond force,
//! committed explicitly after a window's outputs are acked; `rebalances()`
//! read so a revoked partition flushes its open windows; and on an NSOP
//! alarm (power sags more than `NSOP_THRESHOLD_PCT` against the bonder's
//! trailing mean) a `seek` of that partition back `REPLAY_MS` to replay the
//! signature onto `wirebond.metrics` — once per bonder, guarded by a
//! "replayed" flag so it cannot loop.
//!
//! One long-lived instance owns one consumer session (`Consumer::open()` —
//! no arguments; the group, offsets policy and auto-commit come from the
//! binding). The host owns and reuses the binding-scoped producer.
//!
//! Semantics: at-least-once. `commit([])` commits the stored positions after
//! a flush's outputs are acknowledged; a crash between produce and commit
//! replays the records. Malformed bonds go to `DLQ_TOPIC` with an
//! `x-dlq-reason` header (this pattern owns its own dead-lettering). Any
//! session failure exits the service so the supervisor restarts it from
//! committed offsets — that is the broker-restart recovery path.
//!
//! Windows need a clock: the timestamp on the consumed record
//! (`rec.timestamp`), never a wall clock, so this world adds no WASI import.
//!
//! Configuration (`localResources.environment.config`): IN_TOPIC, OUT_TOPIC,
//! DLQ_TOPIC, BATCH_SIZE (default 100, capped at 100), NSOP_THRESHOLD_PCT
//! (default 15 — a re-publish with a tighter value is demo beat 7),
//! WINDOW_MS (5000), REPLAY_MS (60000).

mod bindings {
    use super::Component;
    wit_bindgen::generate!({ world: "kafka-pull-service", generate_all });
    export!(Component);
}

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use bindings::cosmonic::kafka::consumer::{Consumer, RebalanceEvent, RebalanceProtocol};
use bindings::cosmonic::kafka::producer;
use bindings::cosmonic::kafka::types::{ConsumedRecord, Header, PartitionRef, Position, ProduceRecord};
use bindings::exports::wasi::cli::run::Guest as RunGuest;

struct Component;

const STATION: &str = "st03";
const METRICS_TOPIC: &str = "line.metrics";

fn env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_num(key: &str, default: f64) -> f64 {
    env(key, "").parse().unwrap_or(default)
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
struct Bond {
    bonder: u32,
    us_w: f64,
    force_gf: f64,
}

#[derive(Default)]
struct Window {
    start: Option<i64>,
    partition: i32,
    power: Vec<f64>,
    force: Vec<f64>,
}

#[derive(Default)]
struct Bonder {
    window: Window,
    /// Trailing mean of ultrasonic power over closed, healthy windows.
    trailing: Option<f64>,
    windows: u64,
    nsop_windows: u64,
    replayed: bool,
}

#[derive(Default)]
struct State {
    bonders: BTreeMap<u32, Bonder>,
    /// Highest offset seen per partition: anything at or below is a replay.
    high_water: BTreeMap<i32, i64>,
    pending: Vec<ProduceRecord>,
    metrics: Vec<ProduceRecord>,
    consumed: u64,
    replayed: u64,
    dlq: u64,
    rebalances: u64,
    seeks: u64,
    /// A seek requested by a window close, executed by the main loop.
    seek_request: Option<(i32, i64, u32)>,
}

type Shared = Rc<RefCell<State>>;

struct Config {
    window_ms: i64,
    replay_ms: i64,
    nsop_pct: f64,
}

fn mean_sd(xs: &[f64]) -> (f64, f64) {
    if xs.is_empty() {
        return (0.0, 0.0);
    }
    let n = xs.len() as f64;
    let mean = xs.iter().sum::<f64>() / n;
    let var = if xs.len() > 1 {
        xs.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1.0)
    } else {
        0.0
    };
    (mean, var.sqrt())
}

fn r3(x: f64) -> f64 {
    (x * 1000.0).round() / 1000.0
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

/// Close one bonder's window: compute the stats, decide NSOP, queue the
/// output and the metrics, and request a replay seek on a fresh alarm.
fn close_window(st: &mut State, cfg: &Config, bonder_id: u32, end_ts: i64, reason: &str) {
    let Some(b) = st.bonders.get_mut(&bonder_id) else { return };
    let w = std::mem::take(&mut b.window);
    let Some(start) = w.start else { return };
    if w.power.is_empty() {
        return;
    }
    let (p_mean, p_sd) = mean_sd(&w.power);
    let (f_mean, f_sd) = mean_sd(&w.force);
    let delta_pct = b.trailing.map(|t| if t > 0.0 { 100.0 * (p_mean - t) / t } else { 0.0 });
    let nsop = delta_pct.is_some_and(|d| d < -cfg.nsop_pct);
    b.windows += 1;
    if nsop {
        b.nsop_windows += 1;
    } else {
        // Only healthy windows move the trailing mean, so a sag cannot hide
        // itself by dragging the baseline down.
        b.trailing = Some(match b.trailing {
            Some(t) => 0.8 * t + 0.2 * p_mean,
            None => p_mean,
        });
    }
    let out = serde_json::json!({
        "bonder": bonder_id, "window_start": start, "window_end": end_ts, "n": w.power.len(),
        "us_w_mean": r3(p_mean), "us_w_sd": r3(p_sd), "force_mean": r3(f_mean), "force_sd": r3(f_sd),
        "trailing_us_w": b.trailing.map(r3), "power_delta_pct": delta_pct.map(|d| (d * 10.0).round() / 10.0),
        "nsop": nsop, "closed_by": reason,
    });
    st.pending.push(json_record(&bonder_id.to_string(), out.clone()));
    st.metrics.push(metric("window", Some(end_ts), out));
    if nsop {
        let b = st.bonders.get_mut(&bonder_id).expect("bonder exists");
        let first_alarm = !b.replayed;
        st.metrics.push(metric(
            "fault",
            Some(end_ts),
            serde_json::json!({
                "severity": "crit", "bonder": bonder_id,
                "text": format!("bonder {bonder_id} NSOP — transducer power {:.1}%{}", delta_pct.unwrap_or(0.0),
                    if first_alarm { "; replaying last 60 s by seek" } else { "" }),
            }),
        ));
        if first_alarm {
            b.replayed = true;
            st.seek_request = Some((w.partition, end_ts - cfg.replay_ms, bonder_id));
        }
    }
}

fn ingest(st: &mut State, cfg: &Config, rec: &ConsumedRecord, bond: &Bond) {
    let ts = rec.timestamp.unwrap_or(0);
    let hw = st.high_water.entry(rec.partition).or_insert(-1);
    if rec.offset <= *hw {
        st.replayed += 1;
    } else {
        *hw = rec.offset;
        st.consumed += 1;
    }
    let b = st.bonders.entry(bond.bonder).or_default();
    // The window is over, or this is a replayed record from before it
    // opened (a seek rewound the partition): close it and start afresh.
    let close = b.window.start.is_some_and(|s| ts - s >= cfg.window_ms || s - ts > cfg.window_ms);
    if close {
        close_window(st, cfg, bond.bonder, ts, "window");
    }
    let b = st.bonders.entry(bond.bonder).or_default();
    if b.window.start.is_none() {
        b.window.start = Some(ts);
        b.window.partition = rec.partition;
    }
    b.window.power.push(bond.us_w);
    b.window.force.push(bond.force_gf);
}

async fn flush(consumer: &Consumer, out_topic: &str, state: &Shared) -> Result<(), String> {
    let (pending, mut metrics, consumed, replayed) = {
        let mut st = state.borrow_mut();
        (std::mem::take(&mut st.pending), std::mem::take(&mut st.metrics), std::mem::take(&mut st.consumed), std::mem::take(&mut st.replayed))
    };
    let produced = pending.len() as u64;
    if produced > 0 {
        let outcomes = producer::send_batch(out_topic.to_string(), pending)
            .await
            .map_err(|e| format!("send_batch {out_topic}: {}", e.message))?;
        if let Some(Err(e)) = outcomes.into_iter().find(|o| o.is_err()) {
            return Err(format!("send_batch {out_topic}: record failed: {}", e.message));
        }
    }
    let ts = None;
    if consumed > 0 || replayed > 0 {
        metrics.push(metric("consumed", ts, serde_json::json!({"n": consumed})));
    }
    if replayed > 0 {
        metrics.push(metric("replayed", ts, serde_json::json!({"n": replayed})));
    }
    if produced > 0 {
        metrics.push(metric("produced", ts, serde_json::json!({"topic": out_topic, "n": produced})));
    }
    if !metrics.is_empty() {
        let _ = producer::send_batch(METRICS_TOPIC.to_string(), metrics).await;
    }
    // Stored positions, only after the outputs are acked.
    let results = consumer.commit(Vec::new()).await.map_err(|e| format!("commit: {}", e.message))?;
    if let Some(bad) = results.iter().find(|r| r.error.is_some()) {
        return Err(format!("commit: partition {} failed: {:?}", bad.partition, bad.error));
    }
    Ok(())
}

async fn dead_letter(dlq_topic: &str, rec: &ConsumedRecord, reason: String) -> Result<(), String> {
    let mut headers = rec.headers.clone();
    headers.push(Header { key: "x-dlq-reason".into(), value: Some(reason.into_bytes()) });
    headers.push(Header { key: "x-origin-topic".into(), value: Some(rec.topic.as_bytes().to_vec()) });
    headers.push(Header { key: "x-origin-partition".into(), value: Some(rec.partition.to_string().into_bytes()) });
    headers.push(Header { key: "x-origin-offset".into(), value: Some(rec.offset.to_string().into_bytes()) });
    let dead = ProduceRecord {
        partition: None,
        key: rec.key.clone(),
        value: rec.value.clone(),
        headers,
        timestamp: None,
    };
    producer::send(dlq_topic.to_string(), dead)
        .await
        .map(|_| ())
        .map_err(|e| format!("dlq send: {}", e.message))
}

/// Read the session's rebalance events. Taking `rebalances()` makes
/// assignment MANUAL: the plugin stops applying librdkafka's default
/// assign/unassign so it cannot race the guest, and this loop must answer
/// every event itself — `incremental-assign` / `incremental-unassign` under
/// the cooperative protocol (the default), `assign` under the eager one. A
/// revoked partition flushes the open windows it carries first, so their
/// outputs ride the next flush and nothing is left in a window the partition
/// no longer owns.
async fn rebalance_loop(
    consumer: Rc<Consumer>,
    consumer_events: wit_bindgen::StreamReader<RebalanceEvent>,
    state: Shared,
    cfg: Rc<Config>,
) {
    let mut events = consumer_events;
    while let Some(ev) = events.next().await {
        let (kind, parts): (&str, Vec<PartitionRef>) = match ev {
            RebalanceEvent::Assign(p) => ("assign", p),
            RebalanceEvent::Revoke(p) => ("revoke", p),
            RebalanceEvent::Lost(p) => ("lost", p),
        };
        let partitions: Vec<i32> = parts.iter().map(|p| p.partition).collect();
        let mut flushed = 0u32;
        if kind != "assign" {
            let mut st = state.borrow_mut();
            let ids: Vec<(u32, i64)> = st
                .bonders
                .iter()
                .filter(|(_, b)| b.window.start.is_some() && partitions.contains(&b.window.partition))
                .map(|(id, b)| (*id, b.window.start.unwrap_or(0) + cfg.window_ms))
                .collect();
            for (id, end) in ids {
                close_window(&mut st, &cfg, id, end, kind);
                flushed += 1;
            }
        }
        let cooperative = matches!(consumer.rebalance_protocol().await, RebalanceProtocol::Cooperative);
        let applied = match (kind, cooperative) {
            ("assign", true) => consumer.incremental_assign(parts).await,
            ("assign", false) => consumer.assign(parts).await,
            (_, true) => consumer.incremental_unassign(parts).await,
            // Eager: the whole assignment is revoked at once.
            (_, false) => consumer.assign(Vec::new()).await,
        };
        let mut st = state.borrow_mut();
        st.rebalances += 1;
        st.metrics.push(metric(
            "rebalance",
            None,
            serde_json::json!({
                "event": kind, "partitions": partitions, "windows_flushed": flushed,
                "protocol": if cooperative { "cooperative" } else { "eager" },
                "applied": applied.as_ref().map(|_| true).unwrap_or(false),
                "error": applied.err().map(|e| e.message),
            }),
        ));
    }
}

impl RunGuest for Component {
    async fn run() -> Result<(), ()> {
        let in_topic = env("IN_TOPIC", "wirebond.raw");
        let out_topic = env("OUT_TOPIC", "wirebond.metrics");
        let dlq_topic = env("DLQ_TOPIC", "wirebond.dlq");
        let batch = batch_size();
        let cfg = Rc::new(Config {
            window_ms: env_num("WINDOW_MS", 5000.0) as i64,
            replay_ms: env_num("REPLAY_MS", 60000.0) as i64,
            nsop_pct: env_num("NSOP_THRESHOLD_PCT", 15.0),
        });
        let state: Shared = Rc::new(RefCell::new(State::default()));

        let consumer = Rc::new(Consumer::open().await.map_err(|e| eprintln!("st03: open: {}", e.message))?);
        // Take the rebalance stream BEFORE subscribing, so the first assign
        // event is ours to answer (see rebalance_loop).
        if let Ok(events) = consumer.rebalances().await {
            wit_bindgen::spawn_local(rebalance_loop(consumer.clone(), events, state.clone(), cfg.clone()));
        }
        consumer
            .subscribe(vec![in_topic.clone()])
            .await
            .map_err(|e| eprintln!("st03: subscribe: {}", e.message))?;
        let (mut records, terminal) = consumer
            .records()
            .await
            .map_err(|e| eprintln!("st03: records: {}", e.message))?;

        state.borrow_mut().metrics.push(metric(
            "started",
            None,
            serde_json::json!({"batch_size": batch, "nsop_threshold_pct": cfg.nsop_pct, "window_ms": cfg.window_ms}),
        ));

        let mut since_flush: usize = 0;
        while let Some(rec) = records.next().await {
            match rec.value.as_deref().map(serde_json::from_slice::<Bond>) {
                Some(Ok(bond)) if bond.us_w.is_finite() && bond.force_gf.is_finite() => {
                    ingest(&mut state.borrow_mut(), &cfg, &rec, &bond);
                }
                Some(Ok(_)) | Some(Err(_)) => {
                    let reason = match rec.value.as_deref().map(serde_json::from_slice::<Bond>) {
                        Some(Err(e)) => format!("bad bond: {e}"),
                        _ => "non-finite reading".to_string(),
                    };
                    dead_letter(&dlq_topic, &rec, reason.clone()).await.map_err(|e| eprintln!("st03: {e}"))?;
                    let mut st = state.borrow_mut();
                    st.dlq += 1;
                    st.consumed += 1;
                    st.metrics.push(metric("dlq", rec.timestamp, serde_json::json!({
                        "n": 1, "reason": reason,
                        "origin": {"topic": rec.topic, "partition": rec.partition, "offset": rec.offset},
                    })));
                }
                None => {
                    state.borrow_mut().consumed += 1;
                }
            }
            since_flush += 1;

            // A window closed on an NSOP alarm asked for a replay.
            let seek = state.borrow_mut().seek_request.take();
            if let Some((partition, from_ts, bonder)) = seek {
                let pref = PartitionRef { topic: in_topic.clone(), partition };
                let target = consumer
                    .offsets_for_times(vec![pref.clone()], from_ts)
                    .await
                    .ok()
                    .and_then(|rs| rs.into_iter().next())
                    .and_then(|r| r.offset);
                let result = match target {
                    Some(off) => consumer.seek(vec![pref], Position::Exact(off)).await.map(|()| off),
                    None => Err(bindings::cosmonic::kafka::types::Error {
                        code: bindings::cosmonic::kafka::types::ErrorCode::NoOffset,
                        message: "no offset for that time".into(),
                        fatal: false,
                        retriable: false,
                        txn_requires_abort: false,
                    }),
                };
                let mut st = state.borrow_mut();
                match result {
                    Ok(off) => {
                        st.seeks += 1;
                        st.metrics.push(metric("seek", rec.timestamp, serde_json::json!({
                            "bonder": bonder, "partition": partition, "to_offset": off, "from_ts": from_ts,
                        })));
                    }
                    Err(e) => st.metrics.push(metric("fault", rec.timestamp, serde_json::json!({
                        "severity": "warn", "bonder": bonder, "text": format!("replay seek failed: {}", e.message),
                    }))),
                }
            }

            let ready = since_flush >= batch || state.borrow().pending.len() >= batch;
            if ready {
                since_flush = 0;
                flush(&consumer, &out_topic, &state).await.map_err(|e| eprintln!("st03: flush: {e}"))?;
            }
        }
        // The stream ended (a broker restart, a rebalance that took every
        // partition): flush what is acked and exit so the supervisor restarts
        // this service from committed offsets.
        let _ = flush(&consumer, &out_topic, &state).await;
        let _ = consumer.close().await;
        match terminal.await {
            Ok(()) => Err(()),
            Err(e) => {
                eprintln!("st03: session ended: {}", e.message);
                Err(())
            }
        }
    }
}
