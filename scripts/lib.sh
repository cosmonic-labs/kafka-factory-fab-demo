#!/usr/bin/env bash
# Shared helpers for every script in scripts/. Source it; do not run it.
#
# Workload verbs: the brief says "cosmo first, socket second, and say which
# one is in use". On this machine there is no `cosmo` binary (Control's CLI
# is not part of a Cosmonic Desktop install), so every verb below resolves to
# the daemon's Unix-socket API (`cosmonicd paths` → socket) — one function per
# verb, and the path in use is printed once at the top of every run.
# docs/DECISIONS.md records this. If a `cosmo` that can target a Desktop host
# shows up on PATH, wire its subcommands into the same four functions.

set -o pipefail

# ---------------------------------------------------------------- output ---
if [ -t 1 ] && [ -z "${NO_COLOR:-}" ]; then
  C_GREEN=$'\033[32m'; C_RED=$'\033[31m'; C_YEL=$'\033[33m'; C_DIM=$'\033[2m'; C_BOLD=$'\033[1m'; C_OFF=$'\033[0m'
else
  C_GREEN=""; C_RED=""; C_YEL=""; C_DIM=""; C_BOLD=""; C_OFF=""
fi
pass() { printf '%sPASS%s  %s\n' "$C_GREEN" "$C_OFF" "$*"; }
fail() { printf '%sFAIL%s  %s\n' "$C_RED" "$C_OFF" "$*" >&2; }
warn() { printf '%sWARN%s  %s\n' "$C_YEL" "$C_OFF" "$*" >&2; }
info() { printf '%s      %s%s\n' "$C_DIM" "$*" "$C_OFF"; }
step() { printf '\n%s== %s ==%s\n' "$C_BOLD" "$*" "$C_OFF"; }
die()  { fail "$@"; exit 1; }

# ------------------------------------------------------------------ paths ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$REPO_ROOT/compose/redpanda.yaml"
# manifests/            committed: digest-pinned to the PUBLIC registry images
#                       (scripts/release.sh) — what --no-build and a fresh
#                       machine apply.
# manifests/local/      what a local build+publish produced (run.sh step 7),
#                       pinned to Desktop's built-in registry; gitignored.
MANIFEST_DIR="$REPO_ROOT/manifests"
LOCAL_MANIFEST_DIR="$REPO_ROOT/manifests/local"
# manifest_for <name> → the manifest last applied for a workload: the local
# build when there is one, else the committed one.
manifest_for() {
  if [ -f "$LOCAL_MANIFEST_DIR/$1.workload.yaml" ]; then echo "$LOCAL_MANIFEST_DIR/$1.workload.yaml"; else echo "$MANIFEST_DIR/$1.workload.yaml"; fi
}
WORKLOAD_DIR="$REPO_ROOT/workloads"

# Every workload, in apply order (dashboard first so it sees every metric,
# then the simulator, then the stations). fab-st02-die-attach-naive is not here:
# run.sh --naive swaps it in for fab-st02-die-attach.
WORKLOADS=(fab-line-dashboard fab-factory-simulator fab-st01-probe-intake fab-st02-die-attach fab-st03-wire-bond fab-st04-inspection fab-st05-mold-cure fab-st06-final-test fab-st06-final-test-twin)
# Every workload name carries the `fab-` prefix (and the app.kubernetes.io/
# part-of=fab-factory label) so the line is one search away in the Workloads
# grid; the source directory is the name without the prefix.
workload_dir() { echo "$WORKLOAD_DIR/${1#fab-}"; }

# Consumer groups whose lag validate.sh checks.
GROUPS_TO_CHECK=(factory-simulator st02-die-attach st03-wire-bond st04-inspection st05-mold-cure st06-final-test st06-final-test-twin line-dashboard)

BROKER_ADDR="${BROKER_ADDR:-127.0.0.1:9092}"
DASHBOARD_HOST="fab-line-dashboard.localhost"
ST01_HOST="fab-st01-probe-intake.localhost"

# ------------------------------------------------------------- container ---
# docker or podman; the compose subcommand of whichever is present.
container_cli() {
  if command -v docker >/dev/null 2>&1; then echo docker; return; fi
  if command -v podman >/dev/null 2>&1; then echo podman; return; fi
  return 1
}
compose() {
  local cli; cli="$(container_cli)" || die "docker or podman is required on PATH"
  "$cli" compose -f "$COMPOSE_FILE" "$@"
}

# The container that owns the broker: ours (compose service `redpanda`) when
# it is running, else whatever container publishes 127.0.0.1:9092 (a
# pre-existing `docker run` Redpanda from another session), so rpk keeps
# working either way. Empty when nothing does.
broker_container() {
  local cli; cli="$(container_cli)" || return 1
  local ours
  ours="$("$cli" ps --filter name='^fab3-redpanda$' --filter status=running --format '{{.Names}}' 2>/dev/null | head -n1)"
  if [ -n "$ours" ]; then echo "$ours"; return 0; fi
  "$cli" ps --filter status=running --format '{{.Names}}\t{{.Ports}}' 2>/dev/null \
    | awk -F'\t' '$2 ~ /:9092->/ {print $1; exit}'
}

# rpk inside the broker container.
rpk() {
  local cli c; cli="$(container_cli)" || die "docker or podman is required on PATH"
  c="$(broker_container)"
  [ -n "$c" ] || die "no broker container is running on $BROKER_ADDR (run scripts/run.sh)"
  "$cli" exec -i "$c" rpk "$@"
}

# --------------------------------------------------------------- daemon ---
# Resolve the daemon socket: $COSMONIC_SOCKET, else `cosmonicd paths`, else
# the platform default.
cosmonicd_bin() {
  if command -v cosmonicd >/dev/null 2>&1; then command -v cosmonicd; return; fi
  local candidates=(
    "$HOME/Applications/Cosmonic Desktop.app/Contents/Resources/cosmonicd.app/Contents/MacOS/cosmonicd"
    "/Applications/Cosmonic Desktop.app/Contents/Resources/cosmonicd.app/Contents/MacOS/cosmonicd"
    "/opt/cosmonic-desktop/resources/cosmonicd"
    "/usr/lib/cosmonic-desktop/resources/cosmonicd"
  )
  local c
  for c in "${candidates[@]}"; do
    if [ -x "$c" ]; then echo "$c"; return; fi
  done
  return 1
}

daemon_socket() {
  if [ -n "${COSMONIC_SOCKET:-}" ]; then echo "$COSMONIC_SOCKET"; return; fi
  local bin
  if bin="$(cosmonicd_bin)"; then
    "$bin" paths 2>/dev/null | awk '/^socket:/ {sub(/^socket: */, ""); print; exit}'
    return
  fi
  case "$(uname -s)" in
    Darwin) echo "$HOME/Library/Application Support/Cosmonic/cosmonicd.sock" ;;
    *)      echo "${XDG_RUNTIME_DIR:-/tmp}/cosmonic/cosmonicd.sock" ;;
  esac
}

# api METHOD PATH [curl args…] — one request against the daemon socket.
# Prints the body; the HTTP status is read back with `api_status` (kept in a
# file so it survives a `$(api …)` subshell).
API_STATUS_FILE="${TMPDIR:-/tmp}/fab3-api-status.$$"
api() {
  local method="$1" url_path="$2"; shift 2
  local sock; sock="$(daemon_socket)"
  local tmp; tmp="$(mktemp)"
  curl -sS --unix-socket "$sock" -X "$method" -o "$tmp" -w '%{http_code}' "http://cosmonicd$url_path" "$@" 2>/dev/null > "$API_STATUS_FILE" || echo 000 > "$API_STATUS_FILE"
  cat "$tmp"; rm -f "$tmp"
}
api_status() { cat "$API_STATUS_FILE" 2>/dev/null || echo 000; }

WORKLOAD_PATH_LABEL="daemon socket API ($(daemon_socket 2>/dev/null || echo unresolved))"
print_workload_path() {
  if command -v cosmo >/dev/null 2>&1; then
    info "cosmo is on PATH but is not wired to a Desktop host here; workload verbs use the $WORKLOAD_PATH_LABEL"
  else
    info "workload verbs: $WORKLOAD_PATH_LABEL (no cosmo CLI on this machine)"
  fi
}

# The daemon takes JSON only; manifests are YAML for humans. python3 with
# PyYAML (present on most machines; `pip3 install pyyaml` otherwise), else
# ruby's stdlib.
yaml_to_json() {
  local f="$1"
  if python3 -c 'import yaml' 2>/dev/null; then
    python3 -c 'import json, sys, yaml; print(json.dumps(yaml.safe_load(open(sys.argv[1]))))' "$f"
  elif command -v ruby >/dev/null 2>&1; then
    ruby -ryaml -rjson -e 'puts JSON.generate(YAML.safe_load(File.read(ARGV[0])))' "$f"
  else
    die "need python3 with PyYAML (pip3 install pyyaml) or ruby to convert $f to JSON"
  fi
}
# cosmo_apply <manifest.yaml>  — POST /v1/workloads (idempotent by ns/name)
cosmo_apply() {
  local body; body="$(yaml_to_json "$1")" || return 1
  api POST /v1/workloads -H 'content-type: application/json' --data-binary "$body"
}
# cosmo_validate <manifest.yaml> — POST /v1/workloads/validate (dry run)
cosmo_validate() {
  local body; body="$(yaml_to_json "$1")" || return 1
  api POST /v1/workloads/validate -H 'content-type: application/json' --data-binary "$body"
}
# cosmo_publish <workload-dir> <ref> [rebuild] — register the project, build
# if needed, push to the built-in registry; prints the image digest.
cosmo_publish() {
  local dir="$1" ref="$2" rebuild="${3:-false}"
  local reg id
  reg="$(api POST /v1/projects -H 'content-type: application/json' --data-binary "{\"path\":\"$dir\"}")"
  [ "$(api_status)" = "200" ] || { printf '%s\n' "$reg" >&2; return 1; }
  id="$(printf '%s' "$reg" | python3 -c 'import json,sys; print(json.load(sys.stdin)["project"]["id"])')"
  local out
  out="$(api POST "/v1/projects/$id/publish" -H 'content-type: application/json' \
        --data-binary "{\"ref\":\"$ref\",\"insecure\":false,\"rebuild\":$rebuild}")"
  [ "$(api_status)" = "200" ] || { printf '%s\n' "$out" >&2; return 1; }
  printf '%s' "$out" | python3 -c 'import json,sys; print(json.load(sys.stdin)["digest"])'
}
# cosmo_list — GET /v1/workloads as "ns name state restarts message" lines
cosmo_list() {
  api GET /v1/workloads | python3 -c '
import json, sys
try:
    rows = json.load(sys.stdin)
except Exception:
    sys.exit(1)
for w in rows:
    m = w["workload"]["metadata"]; s = w.get("status", {})
    msg = (s.get("message") or s.get("reason") or "").replace("\n", " ")
    print(m.get("namespace", "default"), m["name"], s.get("state", "?"), s.get("restarts", 0), msg)
'
}
# cosmo_get <name> — one workload's status JSON
cosmo_get() { api GET "/v1/workloads/default/$1"; }
# cosmo_logs <name> [limit] — recent log lines for one workload
cosmo_logs() {
  local name="$1" limit="${2:-50}"
  api GET "/v1/logs?workload=default/$name&limit=$limit" | python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(0)
rows = d if isinstance(d, list) else d.get("records") or d.get("entries") or []
rows = list(reversed(rows))
for e in rows:
    if isinstance(e, dict):
        print(e.get("ts") or "", e.get("level", ""), e.get("source", ""), e.get("message") or e.get("msg") or e)
    else:
        print(e)
'
}
# cosmo_bind_error <name> — the plugin's bind refusal for a workload, from
# the daemon log (the sentence names the key; empty when none was logged)
cosmo_bind_error() {
  api GET "/v1/logs?workload=default/$1&limit=100" | python3 -c '
import json, sys
try:
    rows = json.load(sys.stdin).get("records") or []
except Exception:
    sys.exit(0)
for r in rows:  # newest first
    f = r.get("fields") or {}
    if r.get("level") == "ERROR" and "bind" in (r.get("message") or ""):
        print(f.get("err") or f.get("reason") or r.get("message")); break
'
}
# cosmo_delete <name> — DELETE /v1/workloads/default/<name>
cosmo_delete() { api DELETE "/v1/workloads/default/$1"; }

# workload_state <name> → running|failed|pending|starting|absent
workload_state() {
  local out
  out="$(cosmo_get "$1")"
  if [ "$(api_status)" = "404" ]; then echo absent; return; fi
  printf '%s' "$out" | python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    print("unknown"); sys.exit()
s = d.get("status", d)
print(s.get("state", "unknown"))
'
}
workload_message() {
  cosmo_get "$1" | python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit()
s = d.get("status", d)
print(s.get("message") or s.get("reason") or "")
'
}

# --------------------------------------------------------------- ingress ---
ingress_base() {
  api GET /v1/host | python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    print("http://127.0.0.1:8200"); sys.exit()
addr = d.get("httpAddr") or "127.0.0.1:8200"
print("http://" + addr)
'
}
# http_get <host> <path> — GET through the Desktop ingress with the Host header
http_get() {
  local host="$1" url_path="$2"; shift 2
  curl -sS -m 30 -H "Host: $host" "$(ingress_base)$url_path" "$@"
}
http_post() {
  local host="$1" url_path="$2"; shift 2
  curl -sS -m 30 -X POST -H "Host: $host" "$(ingress_base)$url_path" "$@"
}

# ------------------------------------------------------------------ misc ---
# semver_ge 0.5.30 0.5.27
semver_ge() {
  [ "$(printf '%s\n%s\n' "$2" "$1" | sort -t. -k1,1n -k2,2n -k3,3n | head -n1)" = "$2" ]
}

# The wasm-tools to use: the newest of the one on PATH and ~/.cargo/bin's
# (a Homebrew 1.24x often shadows a cargo-installed 1.25x). The transactional
# component's named import needs >= 1.250 to be read at all.
WASM_TOOLS=""
wasm_tools_bin() {
  if [ -n "$WASM_TOOLS" ]; then echo "$WASM_TOOLS"; return; fi
  local best="" best_v="0.0.0" c v
  for c in "$(command -v wasm-tools 2>/dev/null)" "$HOME/.cargo/bin/wasm-tools"; do
    [ -n "$c" ] && [ -x "$c" ] || continue
    v="$("$c" --version 2>/dev/null | awk '{print $2}')"
    if [ -n "$v" ] && semver_ge "$v" "$best_v"; then best="$c"; best_v="$v"; fi
  done
  WASM_TOOLS="$best"
  echo "$best"
}
wasm_tools() { "$(wasm_tools_bin)" "$@"; }

require() {
  command -v "$1" >/dev/null 2>&1 || die "$1 is required on PATH${2:+ — $2}"
}
