#!/usr/bin/env bash
# Publish every built component to the public registry and pin the committed
# manifests to those images, so `make up RUN_FLAGS=--no-build` works on any
# machine with Cosmonic Desktop + Docker and no Rust toolchain.
#
#   release.sh [--tag 0.1.0] [--registry ghcr.io/cosmonic-labs/kafka-factory-fab-demo]
#
# Needs: every workload built (`make build` or a previous `make up`), `wash`,
# and a GitHub token with write:packages (`gh auth token`, or $GHCR_TOKEN with
# $GHCR_USER). Pushes with `wash oci push` — the same image shape Desktop's
# built-in registry gets — reads each digest back from the registry, and
# writes manifests/<name>.workload.yaml (+ the naive ST-02 build). With `oras`
# on PATH it also re-pushes each manifest with org.opencontainers.image.source
# so the package is attached to the repository. Package
# visibility is a GitHub UI setting (there is no API for it): a new package
# is private until it is made public under the org's Packages page.
set -uo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

TAG="0.1.0"
REGISTRY="${RELEASE_REGISTRY:-ghcr.io/cosmonic-labs/kafka-factory-fab-demo}"
SOURCE_URL="${RELEASE_SOURCE_URL:-https://github.com/cosmonic-labs/kafka-factory-fab-demo}"
while [ $# -gt 0 ]; do
  case "$1" in
    --tag) TAG="$2"; shift 2 ;;
    --registry) REGISTRY="$2"; shift 2 ;;
    -h|--help) sed -n '2,14p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "unknown flag $1" ;;
  esac
done

require wash "cargo install wash"
user="${GHCR_USER:-$(gh api user -q .login 2>/dev/null)}"
token="${GHCR_TOKEN:-$(gh auth token 2>/dev/null)}"
[ -n "$user" ] && [ -n "$token" ] || die "no registry credentials: set GHCR_USER and GHCR_TOKEN, or log in with gh (write:packages)"
host="${REGISTRY%%/*}"
# ghcr.io takes the token itself, base64-encoded, as a bearer token.
auth="$(printf '%s' "$token" | base64 | tr -d '\n')"

# digest_of <repo-path> <tag> → the manifest digest the registry reports
digest_of() {
  curl -sS -o /dev/null -D - -H "Authorization: Bearer $auth" \
    -H 'Accept: application/vnd.oci.image.manifest.v1+json' \
    "https://$host/v2/$1/manifests/$2" 2>/dev/null \
    | awk 'tolower($1)=="docker-content-digest:" {print $2}' | tr -d '\r'
}

step "release: push every component to $REGISTRY (tag $TAG)"
failures=0
for w in "${WORKLOADS[@]}" fab-st02-die-attach-naive; do
  dir="$(workload_dir "${w%-naive}")"
  wasm="$dir/$(awk '/component_path:/ {print $2}' "$dir/.wash/config.yaml")"
  [ "$w" = fab-st02-die-attach-naive ] && wasm="${wasm%.wasm}_naive.wasm"
  if [ ! -f "$wasm" ]; then
    fail "$w: no built component at $wasm (run make up, or make up RUN_FLAGS=--naive for the naive build)"
    failures=$((failures + 1)); continue
  fi
  ref="$REGISTRY/$w:$TAG"
  if ! wash oci push -u "$user" -p "$token" "$ref" "$wasm" >/dev/null 2>&1; then
    fail "$w: wash oci push $ref failed"; failures=$((failures + 1)); continue
  fi
  # Attach the package to the repository: GitHub reads the
  # org.opencontainers.image.source annotation, which wash does not set.
  # Re-push the manifest with it (the blobs are already there).
  if command -v oras >/dev/null 2>&1; then
    m="$(mktemp)"
    oras manifest fetch "$ref" 2>/dev/null | python3 -c '
import json, sys
m = json.load(sys.stdin)
a = m.setdefault("annotations", {})
a["org.opencontainers.image.source"] = sys.argv[1]
a["org.opencontainers.image.description"] = sys.argv[2]
a["org.opencontainers.image.licenses"] = "Apache-2.0"
json.dump(m, sys.stdout, separators=(",", ":"))' "$SOURCE_URL" "Fab 3 line workload $w (cosmonic:kafka@0.5.0, Cosmonic Desktop)" > "$m"
    if [ -s "$m" ]; then
      oras manifest push --media-type application/vnd.oci.image.manifest.v1+json "$ref" "$m" >/dev/null 2>&1 || warn "$w: could not re-push the annotated manifest (package stays unlinked from the repo)"
    fi
    rm -f "$m"
  fi
  digest="$(digest_of "${REGISTRY#*/}/$w" "$TAG")"
  if [ -z "$digest" ]; then fail "$w: pushed but the registry returned no digest"; failures=$((failures + 1)); continue; fi
  src="$dir/deploy/workload.yaml"
  local_image="oci.localhost:8200/apps/${w%-naive}:0.1.0"
  sed -e "s|image: $local_image|image: $ref@$digest|" "$src" > "$MANIFEST_DIR/$w.workload.yaml"
  if [ "$w" = fab-st02-die-attach-naive ]; then
    sed -i.bak 's|name: "fab-st02-die-attach"|name: "fab-st02-die-attach"  # the naive build (run.sh --naive)|' "$MANIFEST_DIR/$w.workload.yaml" && rm -f "$MANIFEST_DIR/$w.workload.yaml.bak"
  fi
  pass "$w: $ref@${digest:0:19}… → manifests/$w.workload.yaml"
done
# The beat-2 manifests pull ST-02's image too.
for f in "$MANIFEST_DIR"/refused/*.yaml; do
  sed -i.bak "s|image: oci.localhost:8200/apps/fab-st02-die-attach:0.1.0|image: $REGISTRY/fab-st02-die-attach:$TAG|" "$f" && rm -f "$f.bak"
done
[ "$failures" = 0 ] || die "$failures push(es) failed"
pass "manifests/ now pins $REGISTRY images; commit them"
org="${REGISTRY#*/}"; org="${org%%/*}"
info "make the packages public: https://github.com/orgs/$org/packages → each package → Package settings → Change visibility"
