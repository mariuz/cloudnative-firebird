#!/usr/bin/env bash
# Prints a version's release notes from ROADMAP.md: every entry marked with that version
# ("- [x] **Title** *(v1.2.3 ...)*", the version as a whole token of the marker) with its
# indented bullets, as Markdown sections. The Release workflow puts them into the GitHub Release,
# and hack/release/version.sh requires an entry for the operator's version, so no version is
# released without saying what changed.
#
#   hack/release/notes.sh [version]    (default: the operator's version, PLUGIN_VERSION)
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
version="${1:-$(sed -n "s/^export const PLUGIN_VERSION = '\([0-9]\+\.[0-9]\+\.[0-9]\+\)';$/\1/p" "$root/operator/src/plugin/cli.ts")}"
version="${version#v}"
if [ -z "$version" ]; then
  echo "usage: $0 [version]" >&2
  exit 1
fi

awk -v v="$version" '
  BEGIN { raw = v; gsub(/\./, "\\.", v); found = 0; on = 0 }
  /^- \[x\] \*\*/ {
    on = 0
    if (match($0, /\*\([^)]*\)\*$/)) {
      marker = substr($0, RSTART + 2, RLENGTH - 4)
      if (match(marker, "(^|[^0-9.])v" v "([^0-9.]|$)")) {
        title = $0
        sub(/^- \[x\] \*\*/, "", title)
        sub(/\*\* \*\(.*$/, "", title)
        if (found) print ""
        print "### " title
        print ""
        print "_" marker "_"
        print ""
        on = 1
        found = 1
      }
    }
    next
  }
  on && /^  / { sub(/^  /, ""); print; next }
  on { on = 0 }
  END { if (!found) { print "ROADMAP.md has no entry marked v" raw > "/dev/stderr"; exit 1 } }
' "$root/ROADMAP.md"
