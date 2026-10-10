#!/bin/sh
# Image check (utils/major-upgrade.ts): the server version of this image and the on-disk structure
# (ODS) of a database it creates, written to the termination message as "ODS <major.minor>
# VERSION <version>". Runs embedded, in a scratch directory.
set -eu
t=$(mktemp -d)
echo "CREATE DATABASE '$t/check.fdb'; COMMIT;" | isql -q
ods=$(gstat -h "$t/check.fdb" | sed -n 's/^[[:space:]]*ODS version[[:space:]]*\([0-9][0-9.]*\).*/\1/p')
version=$(isql -z < /dev/null 2>&1 | sed -n 's/^ISQL Version: [A-Z0-9]*-[A-Z]\([0-9][0-9.]*\).*/\1/p' | head -n 1)
rm -rf "$t"
[ -n "$ods" ] || { echo "cannot read the ODS of a new database" >&2; exit 1; }
result="ODS $ods VERSION ${version:-unknown}"
echo "$result"
{ printf '%s' "$result" > /dev/termination-log; } 2>/dev/null || true
