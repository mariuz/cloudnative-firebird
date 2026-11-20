#!/bin/bash
# Checks the agent's declarative databases beside the cluster database
# (docs/multi-database-replication.md, phase 1) without a Firebird server: a "primary" segment
# server with the cluster database and two declarative ones (REPLICATED_DATABASES_FILE), and a
# "replica" puller that follows all three from it, on the segments' files alone.
#
#   - the requests without a selector are about the cluster database, as before;
#   - "DB <file> <request>" is about that database: its own archive, LIST, GET, WAIT;
#   - an unknown database, and the instance's requests with a selector, are refused;
#   - the puller fetches every database's segments into that database's source directory.
#
#   hack/agent/databases.sh
#
# Needs the fb-agent binary: FB_AGENT_BIN, or the newest build in operator/agent/target.
# Exit status 1 when a check fails.
set -u
here=$(cd "$(dirname "$0")" && pwd)
agent=${FB_AGENT_BIN:-}
if [ -z "$agent" ]; then
  # the newest build
  agent=$(ls -t "$here"/../../operator/agent/target/x86_64-unknown-linux-musl/release/fb-agent \
    "$here"/../../operator/agent/target/release/fb-agent "$here"/../../operator/agent/target/debug/fb-agent 2>/dev/null | head -1)
fi
[ -x "$agent" ] || { echo "no fb-agent binary (set FB_AGENT_BIN)"; exit 1; }
work=$(mktemp -d)
pids=()
cleanup() {
  for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null; done
  wait 2>/dev/null
  rm -rf "$work"
}
trap cleanup EXIT

port=$((20000 + RANDOM % 20000))
export ISC_PASSWORD=secret FIREBIRD_PORT=1 POD_NAME=db-0 SEGMENT_PORT=$port

# a segment of <length> bytes, complete: the 48-byte header FBCHANGELOG ... length
segment() {
  python3 -c 'import sys, struct
length = int(sys.argv[2])
header = b"FBCHANGELOG\0" + b"\0" * 28 + struct.pack("<Q", length)
open(sys.argv[1], "wb").write((header + b"x" * length)[:length])' "$1" "$2"
}

# the primary's volume
p=$work/primary
mkdir -p "$p/repl/journal" "$p/repl/archive" "$p/repl/source"
for db in app.fdb sales.fdb; do mkdir -p "$p/repl/databases/$db/journal" "$p/repl/databases/$db/archive" "$p/repl/databases/$db/source"; done
printf 'app.fdb\n# not replicated: the cluster database, a path, a repetition\nmydb.fdb\n../x.fdb\napp.fdb\nsales.fdb\n' > "$p/replicated-databases"
echo "db-0" > "$p/primary"
segment "$p/repl/archive/mydb.fdb.journal-000000001" 100
segment "$p/repl/archive/mydb.fdb.journal-000000002" 120
segment "$p/repl/databases/app.fdb/archive/app.fdb.journal-000000001" 200
segment "$p/repl/databases/app.fdb/archive/app.fdb.journal-000000002" 210
segment "$p/repl/databases/app.fdb/archive/app.fdb.journal-000000003" 220
segment "$p/repl/databases/sales.fdb/archive/sales.fdb.journal-000000005" 300
DATABASE_PATH=$p/mydb.fdb ARCHIVE_DIR=$p/repl/archive SOURCE_DIR=$p/repl/source REPLICATION_DIR=$p/repl \
  JOURNAL_DIR=$p/repl/journal PRIMARY_FILE=$p/primary REPLICATED_DATABASES_FILE=$p/replicated-databases \
  SEGMENT_LISTEN=127.0.0.1:$port "$agent" segment-server > "$work/server.log" 2>&1 &
pids+=($!)
for _ in $(seq 1 50); do "$agent" request 127.0.0.1 PING 2>/dev/null | grep -q '^OK' && break; sleep 0.1; done

failed=0
ok() { echo "ok: $1"; }
bad() { echo "FAILED: $1"; failed=1; }
ask() { "$agent" request 127.0.0.1 "$@" 2>&1; }
# is <what> <expected> <request ...>
is() { local what=$1 expected=$2 got; shift 2; got=$(ask "$@" | tr '\n' ' ' | sed 's/ $//'); if [ "$got" = "$expected" ]; then ok "$what: $got"; else bad "$what: \"$got\", expected \"$expected\""; fi; }

grep -q "also serving app.fdb (DB app.fdb)" "$work/server.log" && grep -q "also serving sales.fdb (DB sales.fdb)" "$work/server.log" \
  && ok "the server serves the two declarative databases" || bad "the server does not name the declarative databases: $(cat "$work/server.log")"
[ "$(grep -c "also serving" "$work/server.log")" = 2 ] && ok "the cluster database, a path and a repetition are left out" || bad "more than two databases served"
is "LIST, the cluster database" "mydb.fdb.journal-000000001 mydb.fdb.journal-000000002 ." LIST
is "LIST, app.fdb" "app.fdb.journal-000000001 app.fdb.journal-000000002 app.fdb.journal-000000003 ." DB app.fdb LIST
is "LIST, sales.fdb" "sales.fdb.journal-000000005 ." DB sales.fdb LIST
is "GET from another database's archive" "ERR bad request" DB sales.fdb GET app.fdb.journal-000000001
[ "$(ask DB app.fdb GET app.fdb.journal-000000002 | head -1)" = "OK 210" ] && ok "GET, app.fdb: OK 210" || bad "GET, app.fdb: $(ask DB app.fdb GET app.fdb.journal-000000002 | head -1)"
is "WAIT, app.fdb, after its newest" "OK timeout" DB app.fdb WAIT 1 app.fdb.journal-000000003
is "WAIT, app.fdb, after an earlier one" "OK new" DB app.fdb WAIT 1 app.fdb.journal-000000002
is "an unknown database" "ERR unknown database other.fdb" DB other.fdb LIST
is "a selector without a request" "ERR bad request" DB app.fdb
for request in PING VERSION FILES ISOLATION PRIMARYSEEN "FENCE test" REJOIN; do
  is "$request is the instance's, not a database's" "ERR bad request" DB app.fdb $request
done
is "PING without a selector" "OK" PING
# STATE: no server to ask for app.fdb either, about app.fdb's file
is "STATE, app.fdb" "ERR cannot read the state of $p/app.fdb" DB app.fdb STATE

# the replica: one puller, every database
r=$work/replica
mkdir -p "$r/repl/source"
cp "$p/replicated-databases" "$r/replicated-databases"
echo "127.0.0.1" > "$r/primary"
POD_NAME=db-1 DATABASE_PATH=$r/mydb.fdb SOURCE_DIR=$r/repl/source STATE_FILE=$r/repl/.last-pulled REPLICATION_DIR=$r/repl \
  PRIMARY_FILE=$r/primary REPLICATED_DATABASES_FILE=$r/replicated-databases POLL_SECONDS=1 \
  "$agent" puller > "$work/puller.log" 2>&1 &
pids+=($!)
pulled() { ls "$1" 2>/dev/null | grep -c '\.journal-'; }
for _ in $(seq 1 100); do
  [ "$(pulled "$r/repl/source")" = 2 ] && [ "$(pulled "$r/repl/databases/app.fdb/source")" = 3 ] \
    && [ "$(pulled "$r/repl/databases/sales.fdb/source")" = 1 ] && break
  sleep 0.1
done
[ "$(pulled "$r/repl/source")" = 2 ] && ok "the puller fetched the cluster database's segments" || bad "cluster database: $(ls "$r/repl/source")"
[ "$(pulled "$r/repl/databases/app.fdb/source")" = 3 ] && ok "the puller fetched app.fdb's segments into its source" || bad "app.fdb: $(ls "$r/repl/databases/app.fdb/source" 2>&1)"
[ "$(pulled "$r/repl/databases/sales.fdb/source")" = 1 ] && ok "the puller fetched sales.fdb's segment into its source" || bad "sales.fdb: $(ls "$r/repl/databases/sales.fdb/source" 2>&1)"
[ "$(cat "$r/repl/databases/app.fdb/.last-pulled" 2>/dev/null)" = app.fdb.journal-000000003 ] && ok "app.fdb's position is its own" || bad "app.fdb's position: $(cat "$r/repl/databases/app.fdb/.last-pulled" 2>&1)"
[ "$(cat "$r/repl/.last-pulled" 2>/dev/null)" = mydb.fdb.journal-000000002 ] && ok "the cluster database's position is where it was" || bad "the cluster database's position: $(cat "$r/repl/.last-pulled" 2>&1)"
cmp -s "$p/repl/databases/app.fdb/archive/app.fdb.journal-000000002" "$r/repl/databases/app.fdb/source/app.fdb.journal-000000002" \
  && ok "a segment arrives whole" || bad "app.fdb.journal-000000002 differs"
# a new segment: the waiting puller fetches it at once
segment "$p/repl/databases/sales.fdb/archive/sales.fdb.journal-000000006" 310
for _ in $(seq 1 50); do [ "$(pulled "$r/repl/databases/sales.fdb/source")" = 2 ] && break; sleep 0.1; done
[ "$(pulled "$r/repl/databases/sales.fdb/source")" = 2 ] && ok "a new segment of sales.fdb is fetched" || bad "sales.fdb's new segment was not fetched"
grep -q "segment puller (app.fdb): source=$r/repl/databases/app.fdb/source" "$work/puller.log" && ok "the puller names its loops" || bad "puller log: $(head -5 "$work/puller.log")"
if grep -q "pull failed" "$work/puller.log"; then bad "a pull failed: $(grep "pull failed" "$work/puller.log" | head -3)"; else ok "no pull failed"; fi

if [ "$failed" = 1 ]; then
  echo "--- server"; tail -20 "$work/server.log"; echo "--- puller"; tail -20 "$work/puller.log"
  echo "### databases: FAILED"; exit 1
fi
echo "### databases: passed"
