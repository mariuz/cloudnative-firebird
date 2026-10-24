#!/usr/bin/env bash
# Prints the operator's version: PLUGIN_VERSION in operator/src/plugin/cli.ts, bumped with every
# change. The deployment manifest must deploy the same version's image (so a checkout of a release
# deploys that release), which the release workflow and a unit test check here.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cli="$root/operator/src/plugin/cli.ts"
deployment="$root/config/deploy/deployment.yaml"
image="ghcr.io/mariuz/cloudnative-firebird"

version="$(sed -n "s/^export const PLUGIN_VERSION = '\([0-9]\+\.[0-9]\+\.[0-9]\+\)';$/\1/p" "$cli")"
if [ -z "$version" ]; then
  echo "no PLUGIN_VERSION = 'X.Y.Z' in $cli" >&2
  exit 1
fi

deployed="$(sed -n "s|^ *image: $image:\(.*\)$|\1|p" "$deployment")"
if [ "$deployed" != "$version" ]; then
  echo "$deployment deploys $image:${deployed:-?}, but the operator's version is $version: pin it to $image:$version" >&2
  exit 1
fi

chart="$root/charts/cloudnative-firebird/Chart.yaml"
for field in version appVersion; do
  value="$(sed -n "s/^$field: //p" "$chart" | tr -d '"')"
  if [ "$value" != "$version" ]; then
    echo "$chart has $field $value, but the operator's version is $version" >&2
    exit 1
  fi
done

# the Release notes come from ROADMAP.md: the version needs its entry
if ! "$root/hack/release/notes.sh" "$version" >/dev/null; then
  echo "add a ROADMAP.md entry marked v$version: the GitHub Release's notes are built from it (hack/release/notes.sh)" >&2
  exit 1
fi

echo "$version"
