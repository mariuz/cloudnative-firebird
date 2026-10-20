#!/usr/bin/env bash
# The Helm chart, charts/cloudnative-firebird: the same operator as config/deploy, with values.
#
#   hack/release/chart.sh sync      copies config/crds into the chart (crds/, installed with it)
#   hack/release/chart.sh check     the chart's CRDs equal config/crds, helm lint, and the chart's
#                                   default rendering equals the install manifest
#                                   (hack/release/bundle.sh) but for the namespace, which Helm
#                                   creates, and the chart's own labels
#   hack/release/chart.sh package   writes cloudnative-firebird-<version>.tgz (prints its path)
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
chart="$root/charts/cloudnative-firebird"
version="$("$root/hack/release/version.sh")"

case "${1:-check}" in
  sync)
    rm -f "$chart"/crds/*.yaml
    cp "$root"/config/crds/*.yaml "$chart/crds/"
    echo "copied $(ls "$chart"/crds/*.yaml | wc -l) CRDs into $chart/crds"
    ;;
  check)
    diff -r "$root/config/crds" "$chart/crds" || { echo "the chart's CRDs differ from config/crds: run hack/release/chart.sh sync" >&2; exit 1; }
    helm lint --strict "$chart" >/dev/null
    tmp="$(mktemp -d)"
    trap 'rm -rf "$tmp"' EXIT
    "$root/hack/release/bundle.sh" "$tmp/bundle.yaml" >/dev/null
    helm template cloudnative-firebird "$chart" --namespace cloudnative-firebird-system --include-crds > "$tmp/chart.yaml"
    python3 - "$tmp/bundle.yaml" "$tmp/chart.yaml" <<'PY'
import sys, yaml
HELM_LABELS = {'app.kubernetes.io/instance', 'app.kubernetes.io/version', 'app.kubernetes.io/managed-by', 'helm.sh/chart'}
def load(path, helm):
    out = {}
    for d in yaml.safe_load_all(open(path)):
        if not d or d['kind'] == 'Namespace':
            continue
        if helm:
            for key in HELM_LABELS:
                d['metadata'].get('labels', {}).pop(key, None)
        out[(d['kind'], d['metadata'].get('namespace'), d['metadata']['name'])] = d
    return out
bundle, chart = load(sys.argv[1], False), load(sys.argv[2], True)
ok = True
for key in sorted(set(bundle) | set(chart)):
    if key not in chart: print(f'missing from the chart: {key}'); ok = False
    elif key not in bundle: print(f'not in config/deploy: {key}'); ok = False
    elif bundle[key] != chart[key]:
        ok = False
        print(f'differs: {key}')
        import difflib
        a = yaml.safe_dump(bundle[key], sort_keys=True).splitlines()
        b = yaml.safe_dump(chart[key], sort_keys=True).splitlines()
        print('\n'.join(difflib.unified_diff(a, b, 'config/deploy', 'chart', lineterm='', n=2)))
print(f'{len(chart)} objects' + (' equal' if ok else ''))
sys.exit(0 if ok else 1)
PY
    ;;
  package)
    [ "$(sed -n 's/^version: //p' "$chart/Chart.yaml")" = "$version" ] || { echo "Chart.yaml version is not $version" >&2; exit 1; }
    out="$(helm package "$chart" --destination "${2:-.}" | sed -n 's/.*saved it to: //p')"
    echo "$out"
    ;;
  *)
    echo "usage: $0 sync|check|package [dir]" >&2
    exit 2
    ;;
esac
