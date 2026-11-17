#!/bin/bash
# Verifies the Firebird internal formats the operator writes (replica control file, HDR_repl_seq
# header clump) against Firebird images, with their own engine and replica server: run it for
# every new Firebird major version or snapshot (TODO.md, "Firebird internal formats").
#
#   hack/firebird-formats/verify.sh [image ...]    (default: the images the CI runs)
#
# Needs Docker and the fb-agent binary: FB_AGENT_BIN, or a musl build in operator/agent/target.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
images=("$@")
[ ${#images[@]} -gt 0 ] || images=(firebirdsql/firebird:4 firebirdsql/firebird:5 firebirdsql/firebird:6-snapshot)
agent=${FB_AGENT_BIN:-$root/operator/agent/target/x86_64-unknown-linux-musl/release/fb-agent}
[ -x "$agent" ] || { echo "no fb-agent binary at $agent (set FB_AGENT_BIN)"; exit 1; }
scripts=$(mktemp -d)
trap 'rm -rf "$scripts"' EXIT
# the agent's binary, as the pods get it from the operator image
cp "$agent" "$scripts/fb-agent"
cp "$here/check.sh" "$scripts/"
status=0
for image in "${images[@]}"; do
  echo "### $image"
  if ! docker run --rm -v "$scripts:/scripts:ro" --entrypoint bash "$image" /scripts/check.sh; then
    echo "### $image: FAILED"
    status=1
  fi
done
exit $status
