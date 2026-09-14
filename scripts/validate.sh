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
# Piped, not a here-string: bash 5.x writes a here-string into a pipe before
# the reader starts, and on macOS that write blocks forever past 512 bytes.
group_lag() {
  local out
  out="$(rpk group describe "$1" 2>/dev/null)" || { echo absent; return; }
  # Sum the per-partition LAG column ourselves (TOTAL-LAG can lag the table).
  printf '%s\n' "$out" | awk '
    $1=="STATE" {state=$2}
    $1=="MEMBERS" {members=$2}
    header && NF>=6 && $6 ~ /^[0-9]+$/ {s+=$6}
    /^TOPIC/ {header=1}
    END {
      if (members+0 == 0 || state == "Dead" || state == "Empty") print "absent"; else print s+0
    }'
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
if ! python3 "$SCRIPT_DIR/validate_counts.py" "$v" "$lag_json" "$BATCH_SIZE"
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
