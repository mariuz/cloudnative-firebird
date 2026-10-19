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

echo "$version"
