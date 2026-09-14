#!/usr/bin/env bash
# Create every topic the line uses with its partition count, then verify each
# exists with exactly that count. Idempotent: an existing topic with the right
# count is a PASS; one with the wrong count is a FAIL (fix: scripts/down.sh
# --purge, then rerun). The plugin creates no topics — this script does.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

# topic partitions — section 4 of PLAN.md, plus line.metrics.dlq: the
# dashboard is a cosmonic:kafka handler, and every handler binding needs a
# dead-letter.topic (docs/DECISIONS.md).
TOPICS=(
  "probe.lots 6"
  "dieattach.readings 6"
  "dieattach.flags 3"
  "dieattach.dlq 1"
  "wirebond.raw 12"
  "wirebond.metrics 6"
  "wirebond.dlq 1"
  "inspect.jobs 12"
  "inspect.results 6"
  "inspect.dlq 1"
  "mold.telemetry 8"
  "mold.profile 4"
  "mold.dlq 1"
  "finaltest.bins 6"
  "lot.disposition 3"
  "inventory.moves 3"
  "lot.disposition.twin 3"
  "inventory.moves.twin 3"
  "line.metrics 3"
  "line.metrics.dlq 1"
  "sim.control 1"
)

existing="$(rpk topic list 2>/dev/null | awk 'NR>1 {print $1, $2}')"
created=0
for entry in "${TOPICS[@]}"; do
  t="${entry% *}"; p="${entry#* }"
  if ! grep -q "^$t " <<<"$existing"; then
    if rpk topic create "$t" -p "$p" >/dev/null 2>&1; then
      created=$((created + 1))
    else
      die "could not create topic $t (-p $p)"
    fi
  fi
done
info "topics: $created created, $(( ${#TOPICS[@]} - created )) already present"

# Verify partition counts from `rpk topic describe`.
bad=0
now="$(rpk topic list 2>/dev/null | awk 'NR>1 {print $1, $2}')"
for entry in "${TOPICS[@]}"; do
  t="${entry% *}"; p="${entry#* }"
  have="$(awk -v t="$t" '$1==t {print $2}' <<<"$now")"
  if [ "$have" = "$p" ]; then
    :
  else
    fail "topic $t has ${have:-no} partitions, want $p — run scripts/down.sh --purge and rerun"
    bad=$((bad + 1))
  fi
done
if [ "$bad" = 0 ]; then
  pass "${#TOPICS[@]} topics exist with the expected partition counts"
else
  exit 1
fi
