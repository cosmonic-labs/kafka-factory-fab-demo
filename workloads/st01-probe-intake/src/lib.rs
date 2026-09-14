//! ST-01 Wafer probe intake — `rust-kafka-http-producer`.
//!
//! Work starts from an HTTP request: probers upload STDF lot summaries and
//! this component lands them on `probe.lots` with one `send_batch` per lot —
//! one record per wafer, keyed by lot id — so a per-record error is one line
//! in the response, not a failed lot.
//!
//! Routes:
//! - `POST /lot` — body `{"lot": "L-24091-07", "prober": "P-03", "wafers": [ {...}, ... ]}`;
//!   one `producer::send_batch("probe.lots", …)`; the response is one line per
//!   wafer (`ok` or the error code), exactly like the scaffold's
//!   `/produce-batch`. A lot that fails schema is rejected with 400 before
//!   anything is produced.
//! - `POST /produce-batch?count=N&size=S` — the scaffold's synthetic batch
//!   route, kept for load checks.
//! - `GET /healthz`.
//!
//! The host owns and reuses the producer configured by the workload's Kafka
//! binding. The component can use only the binding's brokers and topic
//! grant. It emits `produced`, `latency` (upload → last ack) and `rejected`
//! records to `line.metrics`.

mod bindings {
    use super::Component;
    wit_bindgen::generate!({ world: "http-kafka-producer", generate_all });
    export!(Component);
}

use std::collections::BTreeMap;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use bindings::cosmonic::kafka::producer;
use bindings::cosmonic::kafka::types::ProduceRecord;
use bindings::exports::wasi::http::handler;
use bindings::wasi::http::types::{Headers, Method};
use bindings::{wit_future, wit_stream};

struct Component;

const STATION: &str = "st01";
const LOTS_TOPIC: &str = "probe.lots";
const METRICS_TOPIC: &str = "line.metrics";
const MAX_WAFERS: usize = 200;
const MAX_BODY_BYTES: usize = 4 * 1024 * 1024;
const MAX_BATCH_RECORDS: usize = 10_000;
const MAX_RECORD_BYTES: usize = 1024 * 1024;

#[derive(serde::Deserialize)]
struct Lot {
    lot: String,
    prober: String,
    wafers: Vec<Wafer>,
}

#[derive(serde::Deserialize, serde::Serialize)]
struct Wafer {
    wafer: u32,
    contact_mohm: f64,
    chuck_c: f64,
    yield_pct: f64,
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

impl handler::Guest for Component {
    async fn handle(req: handler::Request) -> Result<handler::Response, handler::ErrorCode> {
        let Some(pq) = req.get_path_with_query() else {
            return Ok(resp(400, "no path"));
        };
        let (path, query) = pq.split_once('?').unwrap_or((pq.as_str(), ""));
        Ok(match (req.get_method(), path) {
            (Method::Post, "/lot") => post_lot(req).await,
            (Method::Post, "/produce-batch") => produce_batch(query).await,
            (Method::Get, "/healthz") => resp(200, "ok"),
            _ => resp(404, "no such route: POST /lot, POST /produce-batch, GET /healthz"),
        })
    }
}

/// Read the whole request body (bounded).
async fn read_body(req: handler::Request) -> Result<Vec<u8>, String> {
    let (result_tx, result_rx) = wit_future::new(|| Ok(()));
    let (stream, _trailers) = handler::Request::consume_body(req, result_rx);
    let body = stream.collect().await;
    drop(result_tx);
    if body.len() > MAX_BODY_BYTES {
        return Err(format!("body larger than {MAX_BODY_BYTES} bytes"));
    }
    Ok(body)
}

/// Validate the lot before anything is produced.
fn validate(lot: &Lot) -> Result<(), String> {
    if lot.lot.is_empty() || !lot.lot.starts_with("L-") {
        return Err("lot id must look like L-NNNNN-NN".into());
    }
    if lot.prober.is_empty() {
        return Err("prober id is required".into());
    }
    if lot.wafers.is_empty() || lot.wafers.len() > MAX_WAFERS {
        return Err(format!("a lot carries 1..{MAX_WAFERS} wafers"));
    }
    for w in &lot.wafers {
        if w.wafer == 0 || w.wafer > 100 {
            return Err(format!("wafer {} out of range 1..100", w.wafer));
        }
        if !(w.contact_mohm.is_finite() && w.chuck_c.is_finite() && w.yield_pct.is_finite()) {
            return Err(format!("wafer {}: non-finite reading", w.wafer));
        }
        if !(0.0..=100.0).contains(&w.yield_pct) {
            return Err(format!("wafer {}: yield {} % outside 0..100", w.wafer, w.yield_pct));
        }
    }
    Ok(())
}

async fn post_lot(req: handler::Request) -> handler::Response {
    let started = Instant::now();
    let body = match read_body(req).await {
        Ok(b) => b,
        Err(e) => return resp(400, &e),
    };
    let lot: Lot = match serde_json::from_slice(&body) {
        Ok(l) => l,
        Err(e) => {
            emit(vec![metric("rejected", serde_json::json!({"n": 1, "reason": format!("not a lot: {e}")}))]).await;
            return resp(400, &format!("rejected: not a lot: {e}"));
        }
    };
    if let Err(reason) = validate(&lot) {
        emit(vec![metric("rejected", serde_json::json!({"n": 1, "reason": reason, "lot": lot.lot, "prober": lot.prober}))]).await;
        return resp(400, &format!("rejected: {reason}"));
    }
    let records: Vec<ProduceRecord> = lot
        .wafers
        .iter()
        .map(|w| ProduceRecord {
            partition: None,
            key: Some(lot.lot.as_bytes().to_vec()),
            value: Some(
                serde_json::json!({
                    "lot": lot.lot, "prober": lot.prober, "wafer": w.wafer,
                    "contact_mohm": w.contact_mohm, "chuck_c": w.chuck_c, "yield_pct": w.yield_pct,
                    "via": "http",
                })
                .to_string()
                .into_bytes(),
            ),
            headers: Vec::new(),
            timestamp: None,
        })
        .collect();
    let want = records.len() as u64;
    match producer::send_batch(LOTS_TOPIC.to_string(), records).await {
        Ok(outcomes) => {
            let latency_ms = started.elapsed().as_millis() as u64;
            let ok = outcomes.iter().filter(|o| o.is_ok()).count() as u64;
            let lines: Vec<String> = outcomes
                .into_iter()
                .map(|o| match o {
                    Ok(_) => "ok".into(),
                    Err(e) => format!("{:?}", e.code),
                })
                .collect();
            emit(vec![
                metric("produced", serde_json::json!({"topic": LOTS_TOPIC, "n": ok, "lot": lot.lot, "prober": lot.prober, "wafers": want})),
                metric("latency", serde_json::json!({"ms": latency_ms, "batch": want, "errors": want - ok})),
            ])
            .await;
            resp(200, &lines.join("\n"))
        }
        Err(e) => {
            emit(vec![metric("fault", serde_json::json!({"reason": format!("send-batch failed: {:?}", e.code), "lot": lot.lot}))]).await;
            resp(503, &format!("send-batch failed: {:?}", e.code))
        }
    }
}

async fn produce_batch(query: &str) -> handler::Response {
    let p = parse(query);
    let count = match p.get("count") {
        None => 100,
        Some(value) => match value.parse::<usize>() {
            Ok(count @ 1..=MAX_BATCH_RECORDS) => count,
            _ => return resp(400, "count must be between 1 and 10000"),
        },
    };
    let size = match p.get("size") {
        None => 64,
        Some(value) => match value.parse::<usize>() {
            Ok(size) if size <= MAX_RECORD_BYTES => size,
            _ => return resp(400, "size must be between 0 and 1048576"),
        },
    };
    let records: Vec<ProduceRecord> = (0..count)
        .map(|i| ProduceRecord {
            partition: None,
            key: Some(format!("k{i}").into_bytes()),
            value: Some(vec![b'x'; size]),
            headers: Vec::new(),
            timestamp: None,
        })
        .collect();
    match producer::send_batch(LOTS_TOPIC.to_string(), records).await {
        Ok(outcomes) => {
            let lines: Vec<String> = outcomes
                .into_iter()
                .map(|o| match o {
                    Ok(_) => "ok".into(),
                    Err(e) => format!("{:?}", e.code),
                })
                .collect();
            resp(200, &lines.join("\n"))
        }
        Err(e) => resp(500, &format!("send-batch failed: {:?}", e.code)),
    }
}

fn metric(kind: &str, extra: serde_json::Value) -> ProduceRecord {
    let mut v = serde_json::json!({"station": STATION, "kind": kind, "ts": now_ms()});
    if let (Some(dst), Some(src)) = (v.as_object_mut(), extra.as_object()) {
        for (k, val) in src {
            dst.insert(k.clone(), val.clone());
        }
    }
    ProduceRecord {
        partition: None,
        key: Some(STATION.as_bytes().to_vec()),
        value: Some(v.to_string().into_bytes()),
        headers: Vec::new(),
        timestamp: None,
    }
}

async fn emit(records: Vec<ProduceRecord>) {
    let _ = producer::send_batch(METRICS_TOPIC.to_string(), records).await;
}

fn parse(query: &str) -> BTreeMap<String, String> {
    form_urlencoded::parse(query.as_bytes())
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect()
}

fn resp(status: u16, body: &str) -> handler::Response {
    let (trailers_tx, trailers_rx) = wit_future::new(|| unreachable!());
    let (mut body_tx, body_rx) = wit_stream::new();
    let bytes = body.as_bytes().to_vec();
    wit_bindgen::spawn_local(async move {
        body_tx.write_all(bytes).await;
        let _ = trailers_tx.write(Ok(None)).await;
    });
    let (response, _fut) = handler::Response::new(Headers::new(), Some(body_rx), trailers_rx);
    let _ = response.set_status_code(status);
    response
}
