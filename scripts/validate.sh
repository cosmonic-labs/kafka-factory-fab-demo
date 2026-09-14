#!/usr/bin/env bash
# The produced == consumed + dlq check, plus every group's lag from the broker
# and the transactional ledger's duplicate check. Called by run.sh; runnable
# alone (`make validate`).
#
# The counts come from the dashboard (`GET /validate`: the simulator's and
# ST-01's produced counts and each station's consumed / dlq / redelivered /
# replayed, all folded from line.metrics); lag comes from `rpk group describe`
# run here, never from a workload. The loop is paused while the numbers are
# read so they are a snapshot, not a race, and resumed afterwards unless
# --keep-stopped (run.sh --no-sim passes it).
#
# Exit 0 when every check passes.
set -uo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

KEEP_STOPPED=false
for a in "$@"; do
  case "$a" in
    --keep-stopped) KEEP_STOPPED=true ;;
    -h|--help) sed -n '2,13p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "unknown flag $a" ;;
  esac
done

# What "caught up" means per group, as rpk reports lag (log-end minus the
# committed offset):
# - st03-wire-bond, st06-final-test: a pull service with no batch timer
#   holds a partial batch (ST-03: flushes and commits every BATCH_SIZE
#   records) until the next record arrives → lag < BATCH_SIZE.
# - st06-final-test-twin: the same, and it commits only every COMMIT_EVERY
#   batches (the demo's replay window) → lag < COMMIT_EVERY × BATCH_SIZE,
#   while produced − consumed must still be < BATCH_SIZE.
# - line-dashboard: reads transactional topics read_committed; its position
#   stops before each partition's transaction marker, which rpk counts →
#   lag ≤ the number of transactional partitions it reads (line.metrics ×3
#   + lot.disposition ×3 = 6).
BATCH_SIZE=100
COMMIT_EVERY=5
lag_limit() {
  case "$1" in
    st03-wire-bond|st06-final-test) echo "$BATCH_SIZE" ;;
    st06-final-test-twin) echo $((BATCH_SIZE * COMMIT_EVERY)) ;;
    line-dashboard) echo 6 ;;
    *) echo 0 ;;
  esac
}
lag_note() {
  case "$1" in
    st03-wire-bond|st06-final-test) echo "a partial batch waits for the next record — no batch timer, by design" ;;
    st06-final-test-twin) echo "consumed but uncommitted: the twin commits every $COMMIT_EVERY batches, the replay window beat 6 makes visible" ;;
    line-dashboard) echo "one transaction marker per transactional partition; a read_committed position stops before it" ;;
  esac
}
failures=0

step "validate: pause the loop and let every group drain"
was_running="$(http_get "$DASHBOARD_HOST" /api/state 2>/dev/null | python3 -c '
import json, sys
try:
    print("yes" if (json.load(sys.stdin)["sim"].get("heartbeat") or {}).get("running") else "no")
except Exception:
    print("unknown")')"
"$SCRIPT_DIR/sim.sh" stop >/dev/null 2>&1 || true

# group_lag <group> → prints the lag, or "absent" when the group has no member.
group_lag() {
  local out
  out="$(rpk group describe "$1" 2>/dev/null)" || { echo absent; return; }
  local state members
  state="$(awk '$1=="STATE" {print $2}' <<<"$out")"
  members="$(awk '$1=="MEMBERS" {print $2}' <<<"$out")"
  if [ "${members:-0}" = "0" ] || [ "$state" = "Dead" ] || [ "$state" = "Empty" ]; then echo absent; return; fi
  # Sum the per-partition LAG column ourselves (TOTAL-LAG can lag the table).
  awk 'header && NF>=6 && $6 ~ /^[0-9]+$/ {s+=$6} /^TOPIC/ {header=1} END {print s+0}' <<<"$out"
}

# bash 3.2 (macOS) has no associative arrays: one file per group.
LAG_DIR="$(mktemp -d)"
trap 'rm -rf "$LAG_DIR"' EXIT
lag_of() { cat "$LAG_DIR/$1" 2>/dev/null || echo absent; }
# Up to 4 min: a freshly applied dashboard replays line.metrics from the
# start (~5k records/s), and the stations drain their own backlog.
for i in $(seq 1 80); do
  drained=true; behind=""
  for g in "${GROUPS_TO_CHECK[@]}"; do
    l="$(group_lag "$g")"
    printf '%s' "$l" > "$LAG_DIR/$g"
    limit="$(lag_limit "$g")"
    if [ "$l" = "absent" ] || [ "$l" -gt "$limit" ] 2>/dev/null; then drained=false; behind="$behind $g=$l"; fi
  done
  $drained && break
  printf '\r      draining (%ds):%s   ' $((i * 3)) "$behind"
  sleep 3
done
[ -n "${behind:-}" ] && echo

step "validate: consumer-group lag (rpk group describe)"
lag_json="{"
for g in "${GROUPS_TO_CHECK[@]}"; do
  l="$(lag_of "$g")"
  limit="$(lag_limit "$g")"
  if [ "$l" = "absent" ]; then
    fail "group $g has no live member (workload not running or not bound)"
    failures=$((failures + 1))
    lag_json="$lag_json\"$g\":null,"
  elif [ "$l" -le "$limit" ]; then
    if [ "$limit" -gt 0 ] && [ "$l" -gt 0 ]; then
      pass "group $g lag $l (≤ $limit: $(lag_note "$g"))"
    else
      pass "group $g lag 0"
    fi
    lag_json="$lag_json\"$g\":$l,"
  else
    fail "group $g lag $l (want ≤ $limit)"
    failures=$((failures + 1))
    lag_json="$lag_json\"$g\":$l,"
  fi
done
lag_json="${lag_json%,}}"
# Publish the lag to line.metrics so the dashboard panels can show it too.
printf '{"station":"ops","kind":"lag","ts":%s,"groups":%s}\n' "$(date +%s000)" "$lag_json" \
  | rpk topic produce line.metrics -k ops >/dev/null 2>&1 || warn "could not publish lag to line.metrics"

step "validate: produced == consumed + dlq (dashboard /validate)"
sleep 2
v="$(http_get "$DASHBOARD_HOST" /validate)" || { die "dashboard /validate unreachable"; }
if ! python3 - "$v" "$lag_json" "$BATCH_SIZE" <<'PYEOF'
import json, sys
v = json.loads(sys.argv[1]); lag = json.loads(sys.argv[2]); batch = int(sys.argv[3])
ok = True
fold = v.get("fold") or {}
if fold and not fold.get("complete", True):
    # The dashboard instance took over mid-stream (a daemon restart or a
    # reclaimed pool replaced it): each partition resumed from its own
    # committed point, so produced and consumed cover different windows.
    print("\033[33mWARN\033[0m  the dashboard's fold is partial (resumed mid-stream on %d partition(s), up %ss): produced == consumed cannot be judged from it — rerun `make up` to reset the fold; lag and the duplicate check below still stand"
          % (len(fold.get("partial_partitions") or []), fold.get("uptime_s")))
    led = v["ledger"]
    print(("\033[32mPASS\033[0m  " if led["duplicates"] == 0 else "\033[31mFAIL\033[0m  ") + f"st06 ledger (read_committed, since the dashboard resumed): {led['records']} records, {led['units']} distinct units, {led['duplicates']} duplicates")
    sys.exit(0 if led["duplicates"] == 0 else 1)
GREEN, RED, OFF = "\033[32m", "\033[31m", "\033[0m"
def report(good, text):
    global ok
    if not good: ok = False
    print(("%sPASS%s  " % (GREEN, OFF) if good else "%sFAIL%s  " % (RED, OFF)) + text)
groups = {"st02": "st02-die-attach", "st03": "st03-wire-bond", "st04": "st04-inspection",
          "st05": "st05-mold-cure", "st06": "st06-final-test", "st06twin": "st06-final-test-twin"}
for st, s in v["stations"].items():
    if st == "st01":
        continue
    produced, consumed, dlq = s["produced"], s["consumed"], s["dlq"]
    inflight = lag.get(groups[st]) or 0
    if st in ("st03", "st06"):
        # The uncommitted partial batch is neither consumed nor lost.
        good = produced == consumed + dlq + inflight and inflight < batch
        report(good, f"{st}: produced {produced} == consumed {consumed} + dlq {dlq} + in-flight {inflight}" +
               ("" if good else f"  (diff {produced - consumed - dlq - inflight})"))
    elif st == "st06twin":
        # Consumed counts what it processed, whether or not it has committed
        # yet — including records it replayed after a restart, which are the
        # twin ledger's duplicates; only the partial batch may be outstanding.
        replayed = v["twin"]["duplicates"]
        pending = produced - (consumed - replayed) - dlq
        good = 0 <= pending < batch
        report(good, f"{st}: produced {produced} == consumed {consumed} − replayed {replayed} + dlq {dlq} + partial batch {pending} (uncommitted lag {inflight})")
    else:
        extra = f" · redelivered {s['redelivered']} · replayed {s['replayed']}" if s["redelivered"] or s["replayed"] else ""
        good = produced == consumed + dlq
        report(good, f"{st}: produced {produced} == consumed {consumed} + dlq {dlq}{extra}" +
               ("" if good else f"  (diff {produced - consumed - dlq})"))
s1 = v["stations"]["st01"]
report(True, f"st01: {s1['lots']} lot(s) via POST /lot → {s1['produced']} wafer records on probe.lots; rejected {s1['rejected']}; simulator put {s1['produced_by_simulator']} more")
led, twin = v["ledger"], v["twin"]
report(led["duplicates"] == 0, f"st06 ledger (read_committed): {led['records']} records, {led['units']} distinct units, {led['duplicates']} duplicates")
print(f"      twin ledger (at-least-once): {twin['records']} records, {twin['units']} distinct units, {twin['duplicates']} duplicates")
sys.exit(0 if ok else 1)
PYEOF
then failures=$((failures + 1)); fi

step "validate: lot.disposition read_committed has no duplicate unit ids (rpk)"
dups="$(rpk topic consume lot.disposition --read-committed -o :end -f '%k\n' </dev/null 2>/dev/null | sort | uniq -d | wc -l | tr -d ' ')"
total="$(rpk topic consume lot.disposition --read-committed -o :end -f '%k\n' </dev/null 2>/dev/null | wc -l | tr -d ' ')"
if [ "${dups:-0}" = "0" ]; then
  pass "lot.disposition: $total records, 0 duplicate unit ids"
else
  fail "lot.disposition: $dups unit id(s) appear more than once"
  failures=$((failures + 1))
fi
tdups="$(rpk topic consume lot.disposition.twin -o :end -f '%k\n' </dev/null 2>/dev/null | sort | uniq -d | wc -l | tr -d ' ')"
info "lot.disposition.twin: ${tdups:-0} duplicate unit id(s) (the non-transactional twin; nonzero after a broker restart is the point)"

if ! $KEEP_STOPPED && [ "$was_running" = "yes" ]; then
  "$SCRIPT_DIR/sim.sh" start >/dev/null 2>&1 && info "loop resumed"
fi

if [ "$failures" = 0 ]; then
  pass "validate: all checks passed"
else
  fail "validate: $failures check(s) failed"
  exit 1
fi
