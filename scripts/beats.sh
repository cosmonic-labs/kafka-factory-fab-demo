#!/usr/bin/env bash
# The demo beats that are not a simulator verb (docs/design.html §5):
#
#   beats.sh refuse          beat 2: validate a manifest with a host-only key (refused by name),
#                            then apply one whose grant misses its DLQ (bind fails permanently)
#   beats.sh naive           beat 4: swap fab-st02-die-attach for the build that panics on the poison record
#   beats.sh robust          beat 4: swap the Permanent build back in
#   beats.sh broker-restart  beat 6a: restart the broker mid-batch; wait for the line to recover
#   beats.sh crash-st06      beat 6b: restart both ST-06 workers mid-batch: the twin replays its
#                            uncommitted batches (duplicates), the transactional one cannot
#   beats.sh rollout-st03 [PCT]  beat 7: re-apply ST-03 with a tighter NSOP threshold (default 12)
#   beats.sh probe           put one probe record on dieattach.readings and watch ST-02 consume it
set -uo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

verb="${1:-}"; shift || true

pinned_image() { grep -oE 'image: oci.localhost:8200/apps/[^ ]+' "$MANIFEST_DIR/$1.workload.yaml" | head -n1 | cut -d' ' -f2; }

case "$verb" in
  refuse)
    step "beat 2a: a host-only key is refused at apply, by name (POST /v1/workloads/validate)"
    out="$(cosmo_validate "$MANIFEST_DIR/refused/host-only-key.workload.yaml")"
    printf '%s\n' "$out" | python3 -c '
import json, sys
d = json.load(sys.stdin)
print("valid:", d.get("valid"))
for e in d.get("errors") or []: print("  refused:", e)
for w in d.get("warnings") or []: print("  warning:", w)'
    step "beat 2b: a grant that misses its DLQ binds permanently Failed (no retry)"
    cosmo_apply "$MANIFEST_DIR/refused/grant-misses-dlq.workload.yaml" >/dev/null
    info "applied fab-st07-refused-grant [$(api_status)] (the apply is accepted; the BIND is what refuses); waiting…"
    err=""
    for _ in $(seq 1 20); do
      s="$(workload_state fab-st07-refused-grant)"
      err="$(cosmo_bind_error fab-st07-refused-grant)"
      [ -n "$err" ] && break
      [ "$s" = "running" ] && break
      sleep 2
    done
    echo "state:        $s"
    echo "bind refusal: ${err:-(none logged)}"
    if [ "$s" != failed ] && [ -n "$err" ]; then
      info "this Desktop build retries the refused bind (attempt n/5, 30 s apart) before it shows Failed; newer daemons classify the plugin's refusal as permanent on the first attempt"
    fi
    cosmo_delete fab-st07-refused-grant >/dev/null; info "fab-st07-refused-grant deleted"
    ;;
  naive|robust)
    m="$MANIFEST_DIR/fab-st02-die-attach$([ "$verb" = naive ] && echo -naive).workload.yaml"
    [ -f "$m" ] || die "no $m — run scripts/run.sh --naive once to build and push the naive image"
    step "beat 4: swapping fab-st02-die-attach → $(pinned_image "fab-st02-die-attach$([ "$verb" = naive ] && echo -naive)")"
    cosmo_apply "$m" >/dev/null; info "applied [$(api_status)]"
    for _ in $(seq 1 20); do [ "$(workload_state fab-st02-die-attach)" = running ] && break; sleep 2; done
    pass "fab-st02-die-attach is $(workload_state fab-st02-die-attach) on the $verb build (same group, same DLQ)"
    [ "$verb" = naive ] && info "now: make poison — the record traps five times (watch ST-02's redeliveries), then lands in dieattach.dlq"
    ;;
  broker-restart)
    c="$(broker_container)"; [ -n "$c" ] || die "no broker container running"
    step "beat 6: restarting broker container '$c' while the loop runs"
    docker restart "$c" >/dev/null 2>&1 || podman restart "$c" >/dev/null
    for _ in $(seq 1 30); do rpk cluster health 2>/dev/null | grep -qE 'Healthy:\s+true' && break; sleep 2; done
    pass "broker healthy again; expect a silent gap, then recovery (the plugin retries on a 10 s backoff; services restart from committed offsets)"
    info "watching consumer groups come back (up to 3 min)…"
    for i in $(seq 1 36); do
      back=0
      for g in "${GROUPS_TO_CHECK[@]}"; do
        rpk group describe "$g" 2>/dev/null | grep -qE '^STATE\s+Stable' && back=$((back + 1))
      done
      printf '\r      %2d/%d groups Stable (%ds)' "$back" "${#GROUPS_TO_CHECK[@]}" $((i * 5))
      [ "$back" = "${#GROUPS_TO_CHECK[@]}" ] && break
      sleep 5
    done
    echo
    step "probe: one record through ST-02 proves the pipeline is back"
    "$SCRIPT_DIR/beats.sh" probe
    info "a Redpanda restart is fast enough that librdkafka usually just reconnects; to force a replay: make crash-st06"
    ;;
  crash-st06)
    step "beat 6b: restarting fab-st06-final-test and fab-st06-final-test-twin mid-batch"
    before="$(http_get "$DASHBOARD_HOST" /validate | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["ledger"]["duplicates"], d["twin"]["duplicates"], d["twin"]["records"])')"
    info "before: ledger duplicates $(cut -d" " -f1 <<<"$before") · twin duplicates $(cut -d" " -f2 <<<"$before") · twin records $(cut -d" " -f3 <<<"$before")"
    # stop + start: `restart` only re-resolves the spec and keeps a running
    # service instance; a stop ends the consumer session mid-batch.
    for w in fab-st06-final-test fab-st06-final-test-twin; do
      api POST "/v1/workloads/default/$w/stop" >/dev/null
      info "$w stop [$(api_status)]"
    done
    sleep 3
    for w in fab-st06-final-test fab-st06-final-test-twin; do
      api POST "/v1/workloads/default/$w/start" >/dev/null
      info "$w start [$(api_status)]"
    done
    for _ in $(seq 1 30); do
      [ "$(workload_state fab-st06-final-test)" = running ] && [ "$(workload_state fab-st06-final-test-twin)" = running ] && break
      sleep 2
    done
    info "both running again; the twin replays from its last commit, ST-06 from the offsets its transactions committed — waiting 40 s…"
    sleep 40
    after="$(http_get "$DASHBOARD_HOST" /validate | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["ledger"]["duplicates"], d["twin"]["duplicates"], d["twin"]["records"])')"
    ld="$(cut -d" " -f1 <<<"$after")"; td="$(cut -d" " -f2 <<<"$after")"
    if [ "$ld" = 0 ]; then pass "lot.disposition (transactional, read_committed): $ld duplicates"; else fail "lot.disposition has $ld duplicates — that must never happen"; fi
    if [ "$td" -gt 0 ] 2>/dev/null; then pass "lot.disposition.twin (at-least-once): $td duplicate units — the replayed batches, counted twice"; else warn "the twin shows no duplicates: the restart landed right after a commit; run it again"; fi
    info "now: make validate — the transactional ledger still passes; the twin's duplicates are reported, not failed"
    ;;
  rollout-st03)
    pct="${1:-12}"
    step "beat 7: rolling update of ST-03 with NSOP_THRESHOLD_PCT=$pct (same consumer.group.id, resumes where it stopped)"
    sed -E "s/NSOP_THRESHOLD_PCT: \"[0-9.]+\"/NSOP_THRESHOLD_PCT: \"$pct\"/" "$MANIFEST_DIR/fab-st03-wire-bond.workload.yaml" > "$MANIFEST_DIR/fab-st03-wire-bond.rollout.yaml"
    cosmo_apply "$MANIFEST_DIR/fab-st03-wire-bond.rollout.yaml" >/dev/null; info "applied [$(api_status)]"
    rm -f "$MANIFEST_DIR/fab-st03-wire-bond.rollout.yaml"
    for _ in $(seq 1 20); do [ "$(workload_state fab-st03-wire-bond)" = running ] && break; sleep 2; done
    pass "fab-st03-wire-bond $(workload_state fab-st03-wire-bond) with NSOP threshold $pct% — the dashboard's ST-03 'started' fault row shows the new value; the heat strip never blanks"
    ;;
  probe)
    rec='{"f":0,"die":"D-PROBE001","head":"H1","bl_um":25.1,"epoxy_mg":3.2,"dx_um":1.0,"dy_um":-1.0,"theta_deg":0.01,"stage_c":25.4}'
    before="$(http_get "$DASHBOARD_HOST" /validate | python3 -c 'import json,sys; print(json.load(sys.stdin)["stations"]["st02"]["consumed"])')"
    printf '%s\n' "$rec" | rpk topic produce dieattach.readings -k D-PROBE001 >/dev/null
    # Count it as produced on line.metrics so validate.sh still balances.
    printf '{"station":"sim","kind":"produced","topic":"dieattach.readings","n":1,"probe":true}\n' | rpk topic produce line.metrics -k sim >/dev/null
    info "probe record produced to dieattach.readings; waiting for ST-02 to consume it…"
    for _ in $(seq 1 30); do
      after="$(http_get "$DASHBOARD_HOST" /validate | python3 -c 'import json,sys; print(json.load(sys.stdin)["stations"]["st02"]["consumed"])')"
      [ "$after" -gt "$before" ] 2>/dev/null && { pass "ST-02 consumed it ($before → $after): the pipeline is live"; exit 0; }
      sleep 2
    done
    fail "ST-02 did not consume the probe within 60 s (lag climbing? check make validate)"
    exit 1
    ;;
  ""|-h|--help) sed -n '2,10p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//' ;;
  *) die "unknown beat '$verb'" ;;
esac
