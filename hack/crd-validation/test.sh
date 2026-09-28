#!/usr/bin/env bash
# Checks the CRD admission rules against an API server: every manifest in cases/ is applied with
# --dry-run=server; "# expect: valid" manifests must be accepted, the others rejected with the
# expected message. Then checks the update rules (immutable fields, storage shrink).
# Usage: hack/crd-validation/test.sh   (CRDs from config/crds must be installed)
set -uo pipefail
cd "$(dirname "$0")"
KUBECTL=${KUBECTL:-kubectl}
failed=0

for f in cases/*.yaml; do
  expect=$(sed -n '1s/^# expect: //p' "$f")
  out=$($KUBECTL apply --dry-run=server -f "$f" 2>&1)
  status=$?
  if [ "$expect" = valid ]; then
    if [ $status -ne 0 ]; then echo "FAIL $f: rejected: $out"; failed=1; else echo "ok   $f"; fi
  elif [ $status -eq 0 ]; then
    echo "FAIL $f: accepted, expected rejection ($expect)"; failed=1
  elif ! grep -qF -- "$expect" <<<"$out"; then
    echo "FAIL $f: rejected without \"$expect\": $out"; failed=1
  else
    echo "ok   $f"
  fi
done

# update rules need existing objects (no finalizers: deleted without a controller, before the
# operator is deployed in CI)
cleanup() {
  $KUBECTL delete firebirdcluster crd-validation-cluster --ignore-not-found >/dev/null 2>&1
  $KUBECTL delete firebirdbackup crd-validation-backup --ignore-not-found >/dev/null 2>&1
}
trap cleanup EXIT
sed 's/^  name: c1$/  name: crd-validation-cluster/' cases/valid-cluster.yaml | $KUBECTL apply -f - >/dev/null
sed 's/^  name: r1$/  name: crd-validation-backup/' cases/valid-backup.yaml | $KUBECTL apply -f - >/dev/null
expect_update() {
  local what=$1 expect=$2; shift 2
  out=$($KUBECTL patch "$@" --type merge --dry-run=server 2>&1)
  if [ $? -eq 0 ]; then echo "FAIL $what: accepted"; failed=1
  elif ! grep -qF -- "$expect" <<<"$out"; then echo "FAIL $what: $out"; failed=1
  else echo "ok   $what"; fi
}
expect_update 'storage shrink' 'cannot be decreased' firebirdcluster crd-validation-cluster -p '{"spec":{"storage":{"size":"512Mi"}}}'
expect_update 'backup clusterName change' 'clusterName is immutable' firebirdbackup crd-validation-backup -p '{"spec":{"clusterName":"c2"}}'
if ! out=$($KUBECTL patch firebirdcluster crd-validation-cluster --type merge --dry-run=server -p '{"spec":{"storage":{"size":"2Gi"}}}' 2>&1); then
  echo "FAIL storage growth: $out"; failed=1
else
  echo "ok   storage growth"
fi

exit $failed
