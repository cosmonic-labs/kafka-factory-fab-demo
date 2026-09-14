#!/usr/bin/env bash
# Bring the Fab 3 line up on the local Cosmonic Desktop and validate it.
# Idempotent; safe to rerun. Every step prints PASS/FAIL with the sentence the
# failing tool gave.
#
#   --naive          swap fab-st02-die-attach for the build that panics on the poison record (beat 4)
#   --no-build       apply the committed manifests/ only (no Rust toolchain needed)
#   --no-sim         do not start the simulator loop at the end
#   --use-kafka-yaml put the broker in Desktop's kafka.yaml default instead of the manifests (needs a daemon restart)
#   --purge          stop everything and remove the broker volume first
#   --skip-validate  skip the produced == consumed + lag check
set -uo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

NAIVE=false; NO_BUILD=false; NO_SIM=false; USE_KAFKA_YAML=false; PURGE=false; SKIP_VALIDATE=false
for a in "$@"; do
  case "$a" in
    --naive) NAIVE=true ;;
    --no-build) NO_BUILD=true ;;
    --no-sim) NO_SIM=true ;;
    --use-kafka-yaml) USE_KAFKA_YAML=true ;;
    --purge) PURGE=true ;;
    --skip-validate) SKIP_VALIDATE=true ;;
    -h|--help) sed -n '2,11p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "unknown flag $a (see --help)" ;;
  esac
done

failures=0
check() { # check <ok?> <pass text> <fail text>
  if [ "$1" = 0 ]; then pass "$2"; else fail "$3"; failures=$((failures + 1)); fi
}
abort_if_failed() { [ "$failures" = 0 ] || die "$failures preflight check(s) failed — fix them and rerun"; }

if $PURGE; then
  "$SCRIPT_DIR/down.sh" --purge || true
fi

# ----------------------------------------------------------------- 1. preflight
step "1. preflight"
print_workload_path
cli="$(container_cli)"; check $? "container runtime: $cli" "docker or podman is required on PATH (https://docs.docker.com/get-docker/)"
if command -v cosmo >/dev/null 2>&1; then
  info "cosmo: $(cosmo --version 2>/dev/null | head -n1) — present, but Control's CLI cannot target a Desktop host; using the socket API for every verb"
else
  info "cosmo: not on PATH — Control's CLI is not part of a Desktop install; using the socket API for every verb (docs/DECISIONS.md)"
fi
command -v cosmonic >/dev/null 2>&1; check $? "cosmonic $(cosmonic --version 2>/dev/null | awk '{print $2}') on PATH" "cosmonic is not on PATH (it ships with Cosmonic Desktop: ~/.local/bin/cosmonic)"
tpl="$(cosmonic new --list 2>/dev/null | awk '{print $1}' | grep -c '^rust-kafka-' || true)"
check "$([ "${tpl:-0}" -ge 4 ] && echo 0 || echo 1)" "cosmonic new --list shows the four rust-kafka-* templates" "cosmonic new --list does not list the four rust-kafka-* templates (is Desktop running? is it 0.5.28+?)"
if ! $NO_BUILD; then
  rustup target list --installed 2>/dev/null | grep -q '^wasm32-wasip2$'; check $? "rustup target wasm32-wasip2 installed ($(rustc --version 2>/dev/null | awk '{print $2}'))" "wasm32-wasip2 target missing: rustup target add wasm32-wasip2"
  wt="$(wasm_tools --version 2>/dev/null | awk '{print $2}')"
  check "$([ -n "$wt" ] && semver_ge "$wt" "1.250.0" && echo 0 || echo 1)" "wasm-tools $wt ($(wasm_tools_bin))" "wasm-tools >= 1.250 is required to read the transactional component (have '${wt:-none}'): cargo install wasm-tools"
  wcl="$(PATH="$(printf '%s' "$PATH" | tr ':' '\n' | grep -v '/rustlib/' | paste -sd: -)" wasm-component-ld --version 2>/dev/null | awk '{print $2}')"
  check "$([ -n "$wcl" ] && semver_ge "$wcl" "0.5.27" && echo 0 || echo 1)" "wasm-component-ld $wcl on PATH" "wasm-component-ld >= 0.5.27 is required on PATH for the named transaction import (have '${wcl:-none}'): cargo install wasm-component-ld"
  command -v wash >/dev/null 2>&1; check $? "wash $(wash --version 2>/dev/null | awk '{print $2}')" "wash is not on PATH (Desktop's Preflight doctor installs it; or: cargo install wash)"
fi
command -v python3 >/dev/null 2>&1; check $? "python3" "python3 is required (JSON/YAML handling in scripts/)"
python3 -c 'import yaml' 2>/dev/null || command -v ruby >/dev/null 2>&1; check $? "YAML→JSON converter (PyYAML or ruby)" "need python3 with PyYAML (pip3 install pyyaml) or ruby"
abort_if_failed

# ------------------------------------------------------------ 2. host reachable
step "2. Cosmonic Desktop reachable"
host="$(api GET /v1/host)"
if [ "$(api_status)" = "200" ]; then
  pass "daemon $(printf '%s' "$host" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("version"), "·", d.get("state"), "· runtime", d.get("runtimeVersion"), "· ingress", d.get("httpAddr"), "·", d.get("workloadCount"), "workloads")')"
else
  die "cannot reach Cosmonic Desktop at $(daemon_socket) — start Cosmonic Desktop (or: launchctl kickstart gui/\$(id -u)/com.cosmonic.cosmonicd)"
fi

# ---------------------------------------------------------- 3. kafka plugin
step "3. Kafka plugin present"
# GET /v1/kafka exists on newer daemons; every build labels GET /v1/host with
# `kafka`: "default broker" | "no default broker" | "not in this build".
kafka="$(api GET /v1/kafka)"
if [ "$(api_status)" = "200" ]; then
  if printf '%s' "$kafka" | python3 -c 'import json,sys; d=json.load(sys.stdin); sys.exit(0 if d.get("compiledIn", True) else 1)'; then
    pass "host serves cosmonic:kafka@0.5.0 (GET /v1/kafka: $(printf '%s' "$kafka" | python3 -c 'import json,sys; d=json.load(sys.stdin); a=d.get("active") or {}; c=a.get("config") or {}; print("kafka.yaml default broker" if c.get("bootstrap.servers") else "no kafka.yaml default; the manifests carry the broker")'))"
  else
    die "this host reports no cosmonic:kafka plugin — a daemon built without the \`kafka\` feature (\`requires cosmonic:kafka which this host does not provide\`)"
  fi
else
  label="$(printf '%s' "$host" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("kafka") or "")')"
  case "$label" in
    "default broker"|"no default broker"*) pass "host serves cosmonic:kafka@0.5.0 (GET /v1/host kafka: \"$label\"; this build has no GET /v1/kafka route)" ;;
    "") warn "this daemon reports no kafka label on GET /v1/host and no GET /v1/kafka route; the first bind will say whether cosmonic:kafka is served" ;;
    *) die "this host reports cosmonic:kafka \"$label\" — a daemon built without the \`kafka\` feature (\`requires cosmonic:kafka which this host does not provide\`)" ;;
  esac
fi

# ---------------------------------------------------------------- 4. broker
step "4. broker"
existing="$(broker_container || true)"
if [ -n "$existing" ] && [ "$existing" != "fab3-redpanda" ]; then
  warn "container '$existing' already publishes 127.0.0.1:9092 — reusing it (not managed by compose/redpanda.yaml; stop it and rerun to use the managed one)"
else
  compose up -d >/dev/null 2>&1 || compose up -d
fi
ok=1
for _ in $(seq 1 30); do
  if rpk cluster health 2>/dev/null | grep -qE 'Healthy:\s+true'; then ok=0; break; fi
  sleep 2
done
check $ok "broker healthy: $(broker_container) on $BROKER_ADDR ($(rpk version 2>/dev/null | head -n1 | tr -s ' '))" "broker did not report healthy within 60 s (docker compose -f compose/redpanda.yaml logs redpanda)"
abort_if_failed

# ---------------------------------------------------------------- 5. topics
step "5. topics"
"$SCRIPT_DIR/topics.sh" || die "topic contract not met"

# ------------------------------------------------- 6. Desktop can dial the broker
step "6. Desktop can dial the broker (POST /v1/kafka/test)"
DIAL_PROVEN=false
if $USE_KAFKA_YAML; then
  probe_body='{"name":""}'
  probe_note="the SAVED kafka.yaml default binding"
else
  probe_body='{"draft":true,"config":{"bootstrap.servers":"127.0.0.1:9092","broker.address.family":"v4","topics":"line.metrics,sim.control"}}'
  probe_note="a draft with the manifests' bootstrap.servers / broker.address.family"
fi
probe="$(api POST /v1/kafka/test -H 'content-type: application/json' --data-binary "$probe_body")"
case "$(api_status)" in
  200)
    if printf '%s' "$probe" | python3 -c 'import json,sys; sys.exit(0 if json.load(sys.stdin).get("ok") else 1)'; then
      pass "daemon dialed the broker: $(printf '%s' "$probe" | python3 -c 'import json,sys; d=json.load(sys.stdin); print("answered by", d.get("answered_by"), "·", d.get("broker_count"), "broker(s) ·", d.get("topic_count"), "topics ·", d.get("latency_ms"), "ms")') ($probe_note)"
      DIAL_PROVEN=true
    else
      die "daemon could not dial the broker: $(printf '%s' "$probe" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("stage"), "—", d.get("error") or d.get("message") or d)' 2>/dev/null || printf '%s' "$probe")"
    fi ;;
  404)
    warn "this Desktop build has no POST /v1/kafka/test route (it is in the daemon's main branch); the first workload's bind in step 9 is the dial test" ;;
  *)
    die "POST /v1/kafka/test answered $(api_status): $probe" ;;
esac

# ------------------------------------------------------------ 6.3 kafka.yaml
if $USE_KAFKA_YAML; then
  step "6.3 kafka.yaml default binding"
  body="$(yaml_to_json "$MANIFEST_DIR/kafka.yaml.example")"
  out="$(api PUT /v1/kafka -H 'content-type: application/json' --data-binary "$body")"
  case "$(api_status)" in
    200)
      pass "kafka.yaml written (PUT /v1/kafka); the default takes effect on the next daemon restart"
      if printf '%s' "$out" | python3 -c 'import json,sys; d=json.load(sys.stdin); a=(d.get("active") or {}).get("config") or {}; sys.exit(0 if a.get("bootstrap.servers") else 1)'; then
        info "the running host already has a default broker; the manifests will omit bootstrap.servers"
      else
        warn "the running host has no default broker yet — restart the daemon (Settings → Built-in plugins → Kafka → Restart), then rerun; applying without bootstrap.servers now would be refused"
        exit 1
      fi ;;
    404)
      state_dir="$("$(cosmonicd_bin)" paths 2>/dev/null | awk '/^state:/ {sub(/^state: */, ""); print; exit}')"
      [ -n "$state_dir" ] || die "no PUT /v1/kafka route and cosmonicd paths gave no state dir"
      if [ -f "$state_dir/kafka.yaml" ] && ! grep -q '^config:' "$state_dir/kafka.yaml"; then cp "$state_dir/kafka.yaml" "$state_dir/kafka.yaml.bak"; fi
      cp "$MANIFEST_DIR/kafka.yaml.example" "$state_dir/kafka.yaml"
      pass "wrote $state_dir/kafka.yaml (no PUT /v1/kafka route on this build); read at daemon boot"
      label="$(api GET /v1/host | python3 -c 'import json,sys; print(json.load(sys.stdin).get("kafka") or "")')"
      if [ "$label" = "default broker" ]; then
        info "the running host reports a default broker; the manifests will omit bootstrap.servers"
      else
        warn "the running host reports \"$label\" — restart the daemon, then rerun; applying without bootstrap.servers now would be refused"
        exit 1
      fi ;;
    *)
      die "PUT /v1/kafka refused: $out" ;;
  esac
fi

# ------------------------------------------------------------ 7. build + publish
WORKLOAD_LIST=("${WORKLOADS[@]}")
if $NAIVE; then
  WORKLOAD_LIST=("${WORKLOAD_LIST[@]/fab-st02-die-attach/fab-st02-die-attach-naive}")
fi
if ! $NO_BUILD; then
  step "7. build, verify and publish each workload (→ manifests/local/)"
  mkdir -p "$LOCAL_MANIFEST_DIR"
  for w in "${WORKLOAD_LIST[@]}"; do
    dir="$(workload_dir "$w")"; features=()
    if [ "$w" = "fab-st02-die-attach-naive" ]; then dir="$(workload_dir fab-st02-die-attach)"; features=(--features naive); fi
    [ -d "$dir" ] || { fail "no workload directory $dir"; failures=$((failures + 1)); continue; }
    out="$(cd "$dir" && cargo build --target wasm32-wasip2 --release ${features[@]+"${features[@]}"} 2>&1)" || { fail "$w: cargo build failed"; printf '%s\n' "$out" | tail -n 25; failures=$((failures + 1)); continue; }
    wasm="$dir/$(awk '/component_path:/ {print $2}' "$dir/.wash/config.yaml")"
    if [ "$w" = "fab-st02-die-attach-naive" ]; then
      cp "$wasm" "${wasm%.wasm}_naive.wasm"; wasm="${wasm%.wasm}_naive.wasm"
    fi
    lines="$(wasm_tools component wit "$wasm" 2>/dev/null | grep -E 'cosmonic:kafka/(handler|producer|consumer|transaction)@0.5.0' | sed 's/^ *//' | tr '\n' ' ')"
    if [ -z "$lines" ]; then fail "$w: no cosmonic:kafka interface in the built component"; failures=$((failures + 1)); continue; fi
    # (grep -c reads everything: -q would close the pipe and pipefail would
    # turn wasm-tools' SIGPIPE into a failure)
    lifts="$(wasm_tools print "$wasm" 2>/dev/null | grep -cE 'async-lift|task-return')"
    if [ "${lifts:-0}" = 0 ]; then fail "$w: component has no async lifts (sync build?)"; failures=$((failures + 1)); continue; fi
    pass "$w: built ($(du -h "$wasm" | cut -f1 | tr -d ' ')) · $lines"
    # Publish: build-if-needed on the daemon side is skipped (rebuild=false)
    # because the artifact is fresh; the naive build is pushed by hand.
    if [ "$w" = "fab-st02-die-attach-naive" ]; then
      wash oci push --insecure "oci.localhost:8200/apps/fab-st02-die-attach-naive:0.1.0" "$wasm" >/dev/null 2>&1 \
        || { fail "$w: wash oci push failed"; failures=$((failures + 1)); continue; }
      # The registry answers the manifest's digest in Docker-Content-Digest.
      digest="$(curl -sS -o /dev/null -D - -H 'Accept: application/vnd.oci.image.manifest.v1+json' \
        "$(ingress_base)/v2/apps/fab-st02-die-attach-naive/manifests/0.1.0" -H 'Host: oci.localhost' 2>/dev/null \
        | awk 'tolower($1)=="docker-content-digest:" {print $2}' | tr -d '\r')"
      ref="oci.localhost:8200/apps/fab-st02-die-attach-naive:0.1.0${digest:+@$digest}"
      sed -e "s|image: oci.localhost:8200/apps/fab-st02-die-attach:0.1.0|image: $ref|" \
          -e 's|name: "fab-st02-die-attach"|name: "fab-st02-die-attach"  # the naive build (run.sh --naive)|' \
          "$(workload_dir fab-st02-die-attach)/workload.yaml" > "$LOCAL_MANIFEST_DIR/fab-st02-die-attach-naive.workload.yaml"
      pass "$w: pushed $ref"
      continue
    fi
    digest="$(cosmo_publish "$dir" "$w:0.1.0" false)" || { fail "$w: publish failed: $digest"; failures=$((failures + 1)); continue; }
    sed "s|image: oci.localhost:8200/apps/$w:0.1.0|image: oci.localhost:8200/apps/$w:0.1.0@$digest|" "$dir/workload.yaml" > "$LOCAL_MANIFEST_DIR/$w.workload.yaml"
    pass "$w: published oci.localhost:8200/apps/$w:0.1.0@${digest:0:19}… → manifests/local/$w.workload.yaml"
  done
  abort_if_failed
else
  step "7. build skipped (--no-build): applying the committed manifests/ (public registry images)"
fi
# The manifests to validate and apply: the local build's, or the committed
# ones with --no-build; with --use-kafka-yaml, copies with bootstrap.servers
# stripped so the bindings inherit the host default (the committed files keep
# carrying the broker).
if $NO_BUILD; then APPLY_DIR="$MANIFEST_DIR"; else APPLY_DIR="$LOCAL_MANIFEST_DIR"; fi
if $USE_KAFKA_YAML; then
  SRC_DIR="$APPLY_DIR"; APPLY_DIR="$(mktemp -d)"
  for w in "${WORKLOAD_LIST[@]}"; do
    sed '/^ *bootstrap.servers: 127.0.0.1:9092$/d' "$SRC_DIR/$w.workload.yaml" > "$APPLY_DIR/$w.workload.yaml"
  done
  info "bootstrap.servers stripped from the manifests (in $APPLY_DIR): the bindings inherit kafka.yaml's default"
fi

# ---------------------------------------------------------- 8. validate manifests
step "8. validate manifests (POST /v1/workloads/validate)"
for w in "${WORKLOAD_LIST[@]}"; do
  f="$APPLY_DIR/$w.workload.yaml"
  [ -f "$f" ] || { fail "$w: no manifest at $f"; failures=$((failures + 1)); continue; }
  out="$(cosmo_validate "$f")"
  if [ "$(api_status)" = "200" ] && printf '%s' "$out" | python3 -c 'import json,sys; sys.exit(0 if json.load(sys.stdin).get("valid") else 1)'; then
    pass "$w: valid"
  else
    fail "$w: refused — $(printf '%s' "$out" | python3 -c 'import json,sys; d=json.load(sys.stdin); print("; ".join(d.get("errors") or [d.get("message") or str(d)]))' 2>/dev/null || printf '%s' "$out")"
    failures=$((failures + 1))
  fi
done
abort_if_failed

# ------------------------------------------------------------------- 9. apply
step "9. apply (dashboard, simulator, stations)"
# The naive swap replaces the same workload name so the group and the DLQ stay.
for w in "${WORKLOAD_LIST[@]}"; do
  out="$(cosmo_apply "$APPLY_DIR/$w.workload.yaml")"
  if [ "$(api_status)" = "200" ]; then
    pass "$w: applied"
  else
    fail "$w: apply refused ($(api_status)) — $(printf '%s' "$out" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("message") or d.get("error") or d)' 2>/dev/null || printf '%s' "$out")"
    failures=$((failures + 1))
  fi
done
abort_if_failed
# The dashboard folds line.metrics from the beginning: its in-memory model
# starts empty on every (re)start, so its group is reset now that the new
# spec is applied — stop (the instance and its group member go away), delete
# the group, start (the instance that comes up folds every partition from
# offset 0). Deleting and re-applying the workload instead races the
# reconciler into an extra restart that resumes from the first instance's
# committed positions. Without this a rerun counts "produced" from now but
# "consumed" from the stations' whole history.
api POST /v1/workloads/default/fab-line-dashboard/stop >/dev/null
for _ in $(seq 1 20); do
  rpk group describe line-dashboard 2>/dev/null | grep -qE '^MEMBERS\s+0|^STATE\s+(Dead|Empty)' && break
  rpk group describe line-dashboard >/dev/null 2>&1 || break
  sleep 1
done
rpk group delete line-dashboard >/dev/null 2>&1 || true
api POST /v1/workloads/default/fab-line-dashboard/start >/dev/null
info "fab-line-dashboard restarted with its group reset: it folds line.metrics from the beginning"
names=("${WORKLOAD_LIST[@]/fab-st02-die-attach-naive/fab-st02-die-attach}")
info "waiting for every workload to report running…"
for _ in $(seq 1 40); do
  pending=()
  for n in "${names[@]}"; do
    s="$(workload_state "$n")"
    case "$s" in
      running) ;;
      failed) fail "$n: failed — $(workload_message "$n")"; failures=$((failures + 1)) ;;
      *) pending+=("$n:$s") ;;
    esac
  done
  [ "$failures" = 0 ] || die "a bind refusal is permanent: read the sentence above (it names the key), fix the manifest, rerun"
  [ ${#pending[@]} = 0 ] && break
  sleep 3
done
if [ ${#pending[@]} != 0 ]; then
  die "not running after 120 s: ${pending[*]}"
fi
pass "every workload is running: ${names[*]}"
if ! $DIAL_PROVEN; then
  pass "Desktop dialed the broker: every cosmonic:kafka binding bound and its workload is running (the bind IS the dial on this build)"
fi

# ------------------------------------------------------------------- 10. smoke
step "10. smoke: start the loop, post one real lot through ST-01, validate"
"$SCRIPT_DIR/sim.sh" start
info "waiting 20 s for the first frames…"
sleep 20
lot='{"lot":"L-24091-99","prober":"P-03","wafers":[{"wafer":1,"contact_mohm":181.2,"chuck_c":85.1,"yield_pct":97.9},{"wafer":2,"contact_mohm":179.4,"chuck_c":85.0,"yield_pct":98.2},{"wafer":3,"contact_mohm":184.0,"chuck_c":84.9,"yield_pct":96.8}]}'
resp="$(http_post "$ST01_HOST" /lot -H 'content-type: application/json' --data-binary "$lot" -w '\n%{http_code}')"
code="$(printf '%s' "$resp" | tail -n1)"; body="$(printf '%s' "$resp" | sed '$d' | tr '\n' ' ')"
check "$([ "$code" = 200 ] && [ "$body" = "ok ok ok " ] && echo 0 || echo 1)" "ST-01 POST /lot → $code: $body(one line per wafer)" "ST-01 POST /lot → $code: $body"
bad='{"lot":"nope","prober":"P-07","wafers":[]}'
code="$(http_post "$ST01_HOST" /lot -H 'content-type: application/json' --data-binary "$bad" -o /dev/null -w '%{http_code}')"
check "$([ "$code" = 400 ] && echo 0 || echo 1)" "ST-01 POST /lot with a bad schema → 400 (rejected at the edge, nothing produced)" "ST-01 bad lot → $code (want 400)"
if $SKIP_VALIDATE; then
  info "validation skipped (--skip-validate)"
else
  if $NO_SIM; then "$SCRIPT_DIR/validate.sh" --keep-stopped; else "$SCRIPT_DIR/validate.sh"; fi || failures=$((failures + 1))
fi
if $NO_SIM; then "$SCRIPT_DIR/sim.sh" stop >/dev/null; info "loop stopped (--no-sim)"; fi

# ------------------------------------------------------------------- 11. print
base="$(ingress_base)"
port="${base##*:}"
step "Fab 3 is up"
cat <<EOF
  dashboard   http://$DASHBOARD_HOST:$port/      (Fab 3 Line — polls /api/state every 2 s)
  ST-01       http://$ST01_HOST:$port/lot        (POST a lot JSON; one line per wafer)
  console     http://localhost:8090/                   (Redpanda Console: topics, messages + headers, consumer-group lag)
  metrics     make metrics → http://localhost:3000/d/fab3-line   (Grafana: records/s per topic, lag per group, latency)
  on screen   search "fab-" or "fab-factory" in Desktop's Workloads grid (every workload is labeled app.kubernetes.io/part-of=fab-factory)
  validate    make validate                                     (produced == consumed + dlq, lag, duplicates)
  simulator   make start | stop | shift-change | poison | drift | excursion | calm | status

  demo beats (docs/design.html §5):
   00:00  Quiet Tuesday ......... make start          the baseline loop; every panel's numbers move; make validate reads lag 0
   01:30  The binding is the config  make refuse      a manifest with plugin.library.paths, and one whose grant misses its DLQ, both refused by name
   03:00  Shift change .......... make shift-change   100x lots for 60 s, 2x mold telemetry, then back to baseline by itself
   05:00  Poison pill, two ways . make poison         one NaN thickness → Err(Permanent) → dieattach.dlq with origin headers, partition advances
                                  make naive; make poison   the same record against the panicking build: five traps, then the DLQ
   06:30  Bond excursion ........ make drift; make excursion   bonder 17 NSOP → seek/replay; 20x inspection jobs → ST-04 instances climb toward 12
   09:00  Broker restart ........ make broker-restart the silent gap, then recovery, proven by a probe record
                                  make crash-st06     both ST-06 workers restarted mid-batch: the twin ledger gains duplicates, lot.disposition none
   11:00  Rolling update ........ make rollout-st03   re-publish ST-03 with NSOP_THRESHOLD_PCT=12; the group resumes where it stopped
EOF
[ "$failures" = 0 ] && exit 0 || exit 1
