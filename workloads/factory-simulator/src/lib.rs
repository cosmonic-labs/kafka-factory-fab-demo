//! Factory simulator — "what sends the test messages", and it runs on its own.
//!
//! A `wasi:cli/run` service in the pull-service shape: one consumer session
//! on `sim.control` (the demo driver's channel, written by `scripts/sim.sh`
//! with `rpk topic produce`) and the binding's host-owned producer for the
//! data topics. It plays a **static, committed** record set — one
//! `data/<scenario>/<topic>.jsonl` per topic, embedded with `include_str!`,
//! frame number first — onto the input topics on a 1-second tick, forever,
//! until told to stop. It starts in the baseline loop by itself.
//!
//! Control records (JSON, one per line):
//! - `{"cmd":"loop","scenario":"baseline"}`     — switch the loop scenario (idempotent)
//! - `{"cmd":"play","scenario":"poison"}`       — one-shot overlay on top of the loop
//! - `{"cmd":"stop"}`                           — stop the loop
//!
//! A loop scenario declares its own `frames` and `next` (`shift-change` plays
//! 60 frames then returns to `baseline` by itself); an overlay adds records
//! for its duration and then removes itself. A loop scenario that omits a
//! topic falls back to baseline's frames for it.
//!
//! Every frame's per-topic counts go to `line.metrics` as `kind: produced`
//! so `scripts/validate.sh` can compare produced == consumed + dlq; a
//! `kind: sim` heartbeat every 5 s carries `{scenario, frame, overlay}`.
//!
//! The simulator is itself a Kafka consumer, so a broker restart hits it
//! too: when the control stream ends the session is reopened after 5 s and
//! the loop keeps playing the last scenario. Nothing here ever exits on a
//! consumer or producer error — it logs and retries.
//!
//! Timestamps: none are set on the records; the broker's create-time
//! (assigned by the host's producer at send) is what ST-03 / ST-05 window
//! on, so every pass is rebased to "now" for free.

mod bindings {
    use super::Component;
    wit_bindgen::generate!({ world: "kafka-pull-service", generate_all });
    export!(Component);
}

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use bindings::cosmonic::kafka::consumer::Consumer;
use bindings::cosmonic::kafka::producer;
use bindings::cosmonic::kafka::types::ProduceRecord;
use bindings::exports::wasi::cli::run::Guest as RunGuest;
use bindings::wasi::clocks::monotonic_clock;

struct Component;

const CONTROL_TOPIC: &str = "sim.control";
const METRICS_TOPIC: &str = "line.metrics";
const HEARTBEAT_EVERY: u64 = 5;
const NANOS_PER_SEC: u64 = 1_000_000_000;

// ---------------------------------------------------------------------------
// Static record sets

/// `(scenario, topic, jsonl)` — one entry per committed data file.
const DATA: &[(&str, &str, &str)] = &[
    ("baseline", "probe.lots", include_str!("../data/baseline/probe.lots.jsonl")),
    ("baseline", "dieattach.readings", include_str!("../data/baseline/dieattach.readings.jsonl")),
    ("baseline", "wirebond.raw", include_str!("../data/baseline/wirebond.raw.jsonl")),
    ("baseline", "inspect.jobs", include_str!("../data/baseline/inspect.jobs.jsonl")),
    ("baseline", "mold.telemetry", include_str!("../data/baseline/mold.telemetry.jsonl")),
    ("baseline", "finaltest.bins", include_str!("../data/baseline/finaltest.bins.jsonl")),
    ("shift-change", "probe.lots", include_str!("../data/shift-change/probe.lots.jsonl")),
    ("shift-change", "mold.telemetry", include_str!("../data/shift-change/mold.telemetry.jsonl")),
    ("poison", "dieattach.readings", include_str!("../data/poison/dieattach.readings.jsonl")),
    ("drift", "wirebond.raw", include_str!("../data/drift/wirebond.raw.jsonl")),
    ("excursion", "inspect.jobs", include_str!("../data/excursion/inspect.jobs.jsonl")),
];

const SCENARIOS: &[(&str, &str)] = &[
    ("baseline", include_str!("../data/baseline/scenario.json")),
    ("shift-change", include_str!("../data/shift-change/scenario.json")),
    ("poison", include_str!("../data/poison/scenario.json")),
    ("drift", include_str!("../data/drift/scenario.json")),
    ("excursion", include_str!("../data/excursion/scenario.json")),
];

/// The record key field per topic (die / bonder / job / press / unit / lot).
fn key_field(topic: &str) -> &'static str {
    match topic {
        "probe.lots" => "lot",
        "dieattach.readings" => "die",
        "wirebond.raw" => "bonder",
        "inspect.jobs" => "job",
        "mold.telemetry" => "press",
        "finaltest.bins" => "unit",
        _ => "key",
    }
}

#[derive(serde::Deserialize, Clone)]
struct ScenarioMeta {
    #[allow(dead_code)]
    name: String,
    frames: u32,
    next: Option<String>,
    overlay: bool,
}

struct Line {
    key: String,
    value: &'static str,
}

/// Parsed once at start: scenario → topic → frame → records.
struct Catalog {
    meta: BTreeMap<String, ScenarioMeta>,
    frames: BTreeMap<(String, String), Vec<Vec<Line>>>,
}

impl Catalog {
    fn load() -> Catalog {
        let mut meta = BTreeMap::new();
        for (name, json) in SCENARIOS {
            match serde_json::from_str::<ScenarioMeta>(json) {
                Ok(m) => {
                    meta.insert((*name).to_string(), m);
                }
                Err(e) => eprintln!("factory-simulator: bad scenario.json for {name}: {e}"),
            }
        }
        let mut frames: BTreeMap<(String, String), Vec<Vec<Line>>> = BTreeMap::new();
        for (scenario, topic, jsonl) in DATA {
            let n = meta.get(*scenario).map(|m| m.frames as usize).unwrap_or(1);
            let mut per_frame: Vec<Vec<Line>> = (0..n).map(|_| Vec::new()).collect();
            let kf = key_field(topic);
            for line in jsonl.lines() {
                if line.trim().is_empty() {
                    continue;
                }
                let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
                    continue;
                };
                let f = v.get("f").and_then(|f| f.as_u64()).unwrap_or(0) as usize;
                let key = match v.get(kf) {
                    Some(serde_json::Value::String(s)) => s.clone(),
                    Some(other) => other.to_string(),
                    None => String::new(),
                };
                if let Some(slot) = per_frame.get_mut(f) {
                    slot.push(Line { key, value: line });
                }
            }
            frames.insert(((*scenario).to_string(), (*topic).to_string()), per_frame);
        }
        Catalog { meta, frames }
    }

    fn topics_of(&self, scenario: &str) -> Vec<String> {
        self.frames
            .keys()
            .filter(|(s, _)| s == scenario)
            .map(|(_, t)| t.clone())
            .collect()
    }

    fn lines(&self, scenario: &str, topic: &str, frame: u32) -> Option<&Vec<Line>> {
        let per_frame = self.frames.get(&(scenario.to_string(), topic.to_string()))?;
        if per_frame.is_empty() {
            return None;
        }
        per_frame.get(frame as usize % per_frame.len())
    }
}

// ---------------------------------------------------------------------------
// Simulator state (shared between the control-reader task and the tick loop)

#[derive(Default)]
struct State {
    running: bool,
    scenario: String,
    frame: u32,
    /// One-shot overlay: (scenario, frame).
    overlay: Option<(String, u32)>,
    /// Counters for the heartbeat (and the per-pass id stamp).
    passes: u64,
    overlays_played: u64,
    control_seen: u64,
}

type Shared = Rc<RefCell<State>>;

#[derive(serde::Deserialize)]
struct Control {
    cmd: String,
    #[serde(default)]
    scenario: Option<String>,
}

fn apply_control(catalog: &Catalog, state: &Shared, raw: &[u8]) {
    let text = String::from_utf8_lossy(raw);
    let ctl: Control = match serde_json::from_str(text.trim()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("factory-simulator: ignoring malformed control record {text:?}: {e}");
            return;
        }
    };
    let mut st = state.borrow_mut();
    st.control_seen += 1;
    match ctl.cmd.as_str() {
        "stop" => {
            st.running = false;
            println!("factory-simulator: loop stopped");
        }
        "loop" => {
            let name = ctl.scenario.unwrap_or_else(|| "baseline".to_string());
            let Some(meta) = catalog.meta.get(&name) else {
                eprintln!("factory-simulator: unknown scenario {name:?}");
                return;
            };
            if meta.overlay {
                eprintln!("factory-simulator: {name:?} is an overlay; use cmd=play");
                return;
            }
            if st.running && st.scenario == name {
                println!("factory-simulator: loop already {name} (frame {})", st.frame);
                return;
            }
            st.running = true;
            st.scenario = name.clone();
            st.frame = 0;
            // A restart replays frame 0's records: a new pass number keeps
            // their ids unique.
            st.passes += 1;
            println!("factory-simulator: loop → {name} (pass {})", st.passes);
        }
        "play" => {
            let Some(name) = ctl.scenario else {
                eprintln!("factory-simulator: play needs a scenario");
                return;
            };
            let Some(meta) = catalog.meta.get(&name) else {
                eprintln!("factory-simulator: unknown scenario {name:?}");
                return;
            };
            if !meta.overlay {
                eprintln!("factory-simulator: {name:?} is a loop scenario; use cmd=loop");
                return;
            }
            st.overlay = Some((name.clone(), 0));
            st.overlays_played += 1;
            println!("factory-simulator: overlay → {name}");
        }
        other => eprintln!("factory-simulator: unknown cmd {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Control reader: one consumer session, reopened whenever its stream ends

async fn control_loop(catalog: Rc<Catalog>, state: Shared) {
    loop {
        let consumer = match Consumer::open().await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("factory-simulator: control consumer open failed: {} — retry in 5 s", e.message);
                sleep_secs(5).await;
                continue;
            }
        };
        if let Err(e) = consumer.subscribe(vec![CONTROL_TOPIC.to_string()]).await {
            eprintln!("factory-simulator: subscribe {CONTROL_TOPIC} failed: {} — retry in 5 s", e.message);
            let _ = consumer.close().await;
            sleep_secs(5).await;
            continue;
        }
        let (mut records, terminal) = match consumer.records().await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("factory-simulator: records() failed: {} — retry in 5 s", e.message);
                let _ = consumer.close().await;
                sleep_secs(5).await;
                continue;
            }
        };
        println!("factory-simulator: listening on {CONTROL_TOPIC}");
        while let Some(rec) = records.next().await {
            if let Some(value) = rec.value.as_deref() {
                apply_control(&catalog, &state, value);
            }
            // Commit the control offset after acting on it (auto-commit is off).
            if let Err(e) = consumer.commit(Vec::new()).await {
                eprintln!("factory-simulator: control commit failed: {}", e.message);
            }
        }
        match terminal.await {
            Ok(()) => eprintln!("factory-simulator: control stream ended — reopening in 5 s"),
            Err(e) => eprintln!("factory-simulator: control stream failed: {} — reopening in 5 s", e.message),
        }
        let _ = consumer.close().await;
        sleep_secs(5).await;
    }
}

async fn sleep_secs(secs: u64) {
    monotonic_clock::wait_for(secs * NANOS_PER_SEC).await;
}

// ---------------------------------------------------------------------------
// The tick loop

fn to_record(key: &str, value: &str) -> ProduceRecord {
    ProduceRecord {
        partition: None,
        key: Some(key.as_bytes().to_vec()),
        value: Some(value.as_bytes().to_vec()),
        headers: Vec::new(),
        timestamp: None,
    }
}

/// Record ids that must be unique across passes (the transactional ledger
/// is deduplicated by unit id; a replayed pass must not look like a replayed
/// batch). Entity ids — bonder, press — stay as they are.
fn unique_per_pass(topic: &str) -> bool {
    matches!(topic, "probe.lots" | "dieattach.readings" | "inspect.jobs" | "finaltest.bins")
}

/// `U-0000001` → `U-0000001~k7f2.3`: the simulator's epoch (random per
/// start, so a restarted simulator never repeats a live id) and the pass.
fn stamp(line: &Line, topic: &str, epoch: &str, pass: u64, overlay: bool) -> ProduceRecord {
    if !unique_per_pass(topic) {
        return to_record(&line.key, line.value);
    }
    let kf = key_field(topic);
    let tag = if overlay { format!("{epoch}.o{pass}") } else { format!("{epoch}.{pass}") };
    let key = format!("{}~{tag}", line.key);
    let value = line.value.replacen(
        &format!("\"{kf}\":\"{}\"", line.key),
        &format!("\"{kf}\":\"{key}\""),
        1,
    );
    to_record(&key, &value)
}

/// Play one frame: the loop scenario's topics (baseline fills any it omits),
/// then the overlay's. Returns per-topic produced counts.
async fn play_frame(catalog: &Catalog, state: &Shared, epoch: &str) -> BTreeMap<String, u64> {
    let (scenario, frame, overlay, pass, overlays) = {
        let st = state.borrow();
        (st.scenario.clone(), st.frame, st.overlay.clone(), st.passes, st.overlays_played)
    };
    let mut plan: BTreeMap<String, Vec<ProduceRecord>> = BTreeMap::new();
    for topic in catalog.topics_of("baseline") {
        let source = if catalog.lines(&scenario, &topic, frame).is_some() {
            scenario.as_str()
        } else {
            "baseline"
        };
        if let Some(lines) = catalog.lines(source, &topic, frame) {
            plan.entry(topic.clone())
                .or_default()
                .extend(lines.iter().map(|l| stamp(l, &topic, epoch, pass, false)));
        }
    }
    if let Some((name, oframe)) = overlay {
        for topic in catalog.topics_of(&name) {
            if let Some(lines) = catalog.lines(&name, &topic, oframe) {
                plan.entry(topic.clone())
                    .or_default()
                    .extend(lines.iter().map(|l| stamp(l, &topic, epoch, overlays, true)));
            }
        }
    }
    let mut produced = BTreeMap::new();
    for (topic, records) in plan {
        if records.is_empty() {
            continue;
        }
        let want = records.len() as u64;
        match producer::send_batch(topic.clone(), records).await {
            Ok(outcomes) => {
                let ok = outcomes.iter().filter(|o| o.is_ok()).count() as u64;
                if ok < want {
                    eprintln!("factory-simulator: {topic}: {} of {want} records failed", want - ok);
                }
                if ok > 0 {
                    produced.insert(topic, ok);
                }
            }
            Err(e) => eprintln!("factory-simulator: send_batch {topic} failed: {}", e.message),
        }
    }
    produced
}

/// Advance the loop frame and the overlay frame; handle scenario `next` and
/// overlay expiry.
fn advance(catalog: &Catalog, state: &Shared) {
    let mut st = state.borrow_mut();
    let frames = catalog.meta.get(&st.scenario).map(|m| m.frames).unwrap_or(60).max(1);
    st.frame += 1;
    if st.frame >= frames {
        st.frame = 0;
        st.passes += 1;
        let next = catalog.meta.get(&st.scenario).and_then(|m| m.next.clone());
        if let Some(next) = next {
            println!("factory-simulator: {} finished → {next}", st.scenario);
            st.scenario = next;
        }
    }
    if let Some((name, oframe)) = st.overlay.take() {
        let oframes = catalog.meta.get(&name).map(|m| m.frames).unwrap_or(1);
        if oframe + 1 < oframes {
            st.overlay = Some((name, oframe + 1));
        } else {
            println!("factory-simulator: overlay {name} finished");
        }
    }
}

async fn emit_metrics(records: Vec<ProduceRecord>) {
    if records.is_empty() {
        return;
    }
    if let Err(e) = producer::send_batch(METRICS_TOPIC.to_string(), records).await {
        eprintln!("factory-simulator: line.metrics send failed: {}", e.message);
    }
}

fn metric(kind: &str, extra: serde_json::Value) -> ProduceRecord {
    let mut v = serde_json::json!({"station": "sim", "kind": kind});
    if let (Some(dst), Some(src)) = (v.as_object_mut(), extra.as_object()) {
        for (k, val) in src {
            dst.insert(k.clone(), val.clone());
        }
    }
    to_record("sim", &v.to_string())
}

impl RunGuest for Component {
    async fn run() -> Result<(), ()> {
        let catalog = Rc::new(Catalog::load());
        // A random epoch per start (the std hasher is seeded from the host).
        let epoch = {
            use std::hash::{BuildHasher, Hasher};
            let mut h = std::collections::hash_map::RandomState::new().build_hasher();
            h.write_u64(0x51);
            format!("{:04x}", (h.finish() & 0xffff) as u16)
        };
        // The line starts itself: a (re)started simulator plays the baseline
        // loop until told otherwise, so a redeploy or a supervisor restart
        // never leaves the floor silent. `sim.sh stop` still stops it.
        let state: Shared = Rc::new(RefCell::new(State {
            running: true,
            scenario: "baseline".to_string(),
            passes: 1,
            ..Default::default()
        }));
        println!(
            "factory-simulator: {} scenarios, {} topic sets loaded; waiting for {CONTROL_TOPIC}",
            catalog.meta.len(),
            catalog.frames.len()
        );

        wit_bindgen::spawn_local(control_loop(catalog.clone(), state.clone()));

        let mut tick: u64 = 0;
        loop {
            sleep_secs(1).await;
            tick += 1;
            let running = state.borrow().running;
            let mut metrics = Vec::new();
            if running {
                let (scenario, frame) = {
                    let st = state.borrow();
                    (st.scenario.clone(), st.frame)
                };
                let produced = play_frame(&catalog, &state, &epoch).await;
                for (topic, n) in &produced {
                    metrics.push(metric(
                        "produced",
                        serde_json::json!({"topic": topic, "n": n, "scenario": scenario, "frame": frame}),
                    ));
                }
                advance(&catalog, &state);
            }
            if tick % HEARTBEAT_EVERY == 0 {
                let st = state.borrow();
                metrics.push(metric(
                    "sim",
                    serde_json::json!({
                        "running": st.running,
                        "scenario": st.scenario,
                        "frame": st.frame,
                        "overlay": st.overlay.as_ref().map(|(n, f)| serde_json::json!({"scenario": n, "frame": f})),
                        "passes": st.passes,
                        "epoch": epoch,
                        "controls": st.control_seen,
                    }),
                ));
            }
            emit_metrics(metrics).await;
        }
    }
}
