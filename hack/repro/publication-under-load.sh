#!/bin/sh
# Issue 1 (ISSUES.md): a database that publishes to a replication journal hangs Firebird 5.0.4
# when several clients concurrently connect, commit and disconnect. Nothing else touches it.
#
#   ./publication-under-load.sh                          # hangs within about 10-40 seconds
#   PUBLICATION=0 ./publication-under-load.sh            # control: writers complete
#   CONNECTIONS=persistent ./publication-under-load.sh   # pooled connections: writers complete
#   ARCHIVE_TIMEOUT=60 ./publication-under-load.sh       # archive timeout does not matter: hangs
set -u
. "$(dirname "$0")/common.sh"
PUBLICATION=${PUBLICATION:-1}
work=$(mktemp -d)
start_server fb-repro-load "$work" "$PUBLICATION"
start_writers fb-repro-load 4 600
expected=2404
result="writers completed ($expected rows)"
last=-1
for n in $(seq 1 30); do
  sleep 10
  rows=$(timeout 15 docker exec fb-repro-load sh -c "echo 'select count(*) from app;' | isql -q localhost:$DB" | awk '$1 ~ /^[0-9]+$/ { print $1 }')
  echo "t=$((n * 10))s rows=${rows:-no answer}"
  if [ -z "$rows" ]; then result="server stopped answering after $((n * 10))s"; break; fi
  [ "$rows" = "$expected" ] && break
  if [ "$rows" = "$last" ]; then result="no progress at $rows rows after $((n * 10))s (server still answers)"; break; fi
  last=$rows
done
echo "$result (publication=$PUBLICATION)"
docker rm -f fb-repro-load >/dev/null
