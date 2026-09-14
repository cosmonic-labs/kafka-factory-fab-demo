#!/usr/bin/env bash
# Drive the factory simulator over its control topic.
#
#   sim.sh start         the quiet-Tuesday loop (idempotent; leave it running)
#   sim.sh stop          stop the loop
#   sim.sh shift-change  100x lots + 2x mold telemetry for 60 s, then baseline by itself
#   sim.sh poison        one dieattach reading with a NaN thickness   (beat 4)
#   sim.sh drift         bonder 17's transducer power sags 18% for 90 s (beat 5)
#   sim.sh excursion     20x inspection jobs for 120 s                 (beat 5)
#   sim.sh calm          back to baseline early (same as start)
#   sim.sh rate N        play N frames per second — N× the records/s on every topic (1..60; 1 = the design's rates)
#   sim.sh status        the simulator's last line.metrics heartbeat
#
# Control is a Kafka topic (sim.control): each verb is one JSON record produced
# with rpk inside the Redpanda container, so the simulator needs no HTTP surface.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

verb="${1:-}"
control() {
  local record="$1"
  printf '%s\n' "$record" | rpk topic produce sim.control -k ctl >/dev/null
  info "sim.control ← $record"
}

case "$verb" in
  start|calm)     control '{"cmd":"loop","scenario":"baseline"}' ;;
  stop)           control '{"cmd":"stop"}' ;;
  shift-change)   control '{"cmd":"loop","scenario":"shift-change"}' ;;
  poison)         control '{"cmd":"play","scenario":"poison"}' ;;
  drift)          control '{"cmd":"play","scenario":"drift"}' ;;
  excursion)      control '{"cmd":"play","scenario":"excursion"}' ;;
  status)
    # The dashboard folds the heartbeat; ask it first, fall back to the topic.
    if out="$(http_get "$DASHBOARD_HOST" /api/state 2>/dev/null)" && [ -n "$out" ]; then
      printf '%s' "$out" | python3 -c '
import json, sys
d = json.load(sys.stdin); s = d["sim"]; hb = s.get("heartbeat") or {}
if not hb:
    print("no heartbeat seen yet (is factory-simulator running?)"); sys.exit(1)
ov = hb.get("overlay")
print("simulator:", "running" if hb.get("running") else "stopped",
      "· scenario", hb.get("scenario"), "· frame", hb.get("frame"),
      "· rate", str(hb.get("frames_per_tick", 1)) + "x",
      "· overlay", (ov["scenario"] + " frame " + str(ov["frame"])) if ov else "none",
      "· passes", hb.get("passes"), "· heartbeat", str(s.get("age_s")) + "s ago")
print("produced so far:", ", ".join(f"{k}={v}" for k, v in sorted(s.get("produced", {}).items())))
'
    else
      warn "dashboard not reachable; reading the last heartbeat from line.metrics"
      rpk topic consume line.metrics -n 40 -o -40 --format '%v\n' </dev/null 2>/dev/null \
        | grep '"kind":"sim"' | tail -n1 || echo "no heartbeat on line.metrics"
    fi
    ;;
  ""|-h|--help)
    sed -n '2,16p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    ;;
  *)
    die "unknown verb '$verb' (start|stop|shift-change|poison|drift|excursion|calm|rate N|status)"
    ;;
esac
