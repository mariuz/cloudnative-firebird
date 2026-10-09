#!/bin/bash
# Verifies the Firebird internal formats the operator writes (replica control file, HDR_repl_seq
# header clump) against Firebird images, with their own engine and replica server: run it for
# every new Firebird major version or snapshot (TODO.md, "Firebird internal formats").
#
#   hack/firebird-formats/verify.sh [image ...]    (default: the images the CI runs)
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
images=("$@")
[ ${#images[@]} -gt 0 ] || images=(firebirdsql/firebird:4 firebirdsql/firebird:5 firebirdsql/firebird:6-snapshot)
scripts=$(mktemp -d)
trap 'rm -rf "$scripts"' EXIT
# the two scripts as the pods get them: "#@include <file>" lines replaced by the file (readScript
# in operator/src/utils/replication.ts)
src="$root/operator/src/replication"
for name in set-repl-seq.pl replica-control.pl; do
  awk -v dir="$src" '/^#@include [^ ]+$/ { while ((getline line < (dir "/" $2)) > 0) print line; close(dir "/" $2); next } { print }' \
    "$src/$name" > "$scripts/$name"
done
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
