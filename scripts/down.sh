#!/usr/bin/env bash
# Stop the line: delete the workloads from Cosmonic Desktop and stop the
# broker container. The broker's data volume (topics, offsets) is kept unless
# --purge, which also removes the built images' manifests from the daemon's
# point of view (the built-in registry keeps the blobs).
set -uo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

PURGE=false
for a in "$@"; do
  case "$a" in
    --purge) PURGE=true ;;
    -h|--help) sed -n '2,5p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "unknown flag $a" ;;
  esac
done

step "stop the simulator loop"
if "$SCRIPT_DIR/sim.sh" stop >/dev/null 2>&1; then info "loop stopped"; else info "broker not running; nothing to stop"; fi

step "delete workloads"
if [ "$(api GET /v1/host >/dev/null; api_status)" = "200" ]; then
  for w in "${WORKLOADS[@]}"; do
    s="$(workload_state "$w")"
    if [ "$s" = "absent" ]; then info "$w: not present"; continue; fi
    cosmo_delete "$w" >/dev/null
    if [ "$(api_status)" = "200" ] || [ "$(api_status)" = "204" ]; then pass "$w: deleted"; else fail "$w: delete returned $(api_status)"; fi
  done
else
  warn "Cosmonic Desktop is not reachable; workloads left as they are"
fi

step "broker"
c="$(broker_container || true)"
if [ "$c" = "fab3-redpanda" ]; then
  # --profile metrics covers Prometheus/Grafana too when they were started.
  if $PURGE; then compose --profile metrics down -v >/dev/null 2>&1 && pass "broker, console and metrics stopped; volume removed"; else compose --profile metrics stop >/dev/null 2>&1 && pass "broker, console and metrics stopped (volume kept; --purge removes it)"; fi
elif [ -n "$c" ]; then
  warn "broker container '$c' is not managed by compose/redpanda.yaml; left running"
  if $PURGE; then
    for t in $(rpk topic list 2>/dev/null | awk 'NR>1 {print $1}'); do
      case "$t" in probe.*|dieattach.*|wirebond.*|inspect.*|mold.*|finaltest.*|lot.*|inventory.*|line.*|sim.*) rpk topic delete "$t" >/dev/null 2>&1 && info "deleted topic $t" ;; esac
    done
    for g in "${GROUPS_TO_CHECK[@]}"; do rpk group delete "$g" >/dev/null 2>&1 || true; done
    pass "line topics and groups removed from '$c'"
  fi
else
  info "no broker container running"
fi
