#!/bin/bash
# Checks the agent's FENCE, REJOIN, NBACKUP and isolation check (docs/rust-agent-segment-server.md, step 5)
# against real Firebird servers. The segment server does the full shutdown, the online and the
# nbackup through its own service attachments. Here fbsvcmgr is wrapped so that every call is
# recorded: none of its requests may need it.
#
#   hack/agent/role.sh [image ...]      (default: Firebird 4, 5 and the 6 snapshot)
#
# Needs Docker and the fb-agent binary: FB_AGENT_BIN, or a musl build in operator/agent/target.
# Exit status 1 when an image fails.
set -u
here=$(cd "$(dirname "$0")" && pwd)
agent=${FB_AGENT_BIN:-$here/../../operator/agent/target/x86_64-unknown-linux-musl/release/fb-agent}
[ -x "$agent" ] || { echo "no fb-agent binary at $agent (set FB_AGENT_BIN)"; exit 1; }
images=("$@")
[ ${#images[@]} -gt 0 ] || images=(firebirdsql/firebird:4 firebirdsql/firebird:5 firebirdsql/firebird:6-snapshot)
NAME=${NAME:-fbrole}
work=$(mktemp -d)
chmod 755 "$work"
cleanup() {
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  docker run --rm -v "$work:/w" --entrypoint sh "${images[0]}" -c 'rm -rf /w/*' >/dev/null 2>&1 || true
  rmdir "$work" 2>/dev/null || true
}
trap cleanup EXIT
cp "$agent" "$work/fb-agent"
mkdir "$work/wrapped"
cat > "$work/wrapped/fbsvcmgr" <<'WRAP'
#!/bin/sh
echo "$*" >> /data/fbsvcmgr.calls
exec /opt/firebird/bin/fbsvcmgr "$@"
WRAP
chmod 755 "$work/wrapped/fbsvcmgr"
cat > "$work/env.sh" <<'ENV'
export ISC_USER=SYSDBA ISC_PASSWORD=masterkey PATH=/opt/firebird/bin:$PATH
export DATABASE_PATH=/data/db.fdb ARCHIVE_DIR=/data/a SOURCE_DIR=/data/s REPLICATION_DIR=/data/repl
export PRIMARY_FILE=/data/primary POD_NAME=db-0 SCRIPT_DIR=/w
front() { SEGMENT_PORT=3051 /w/fb-agent request 127.0.0.1 "$@" 2>&1; }
calls() { cat /data/fbsvcmgr.calls 2>/dev/null | wc -l; }
rows() { echo "SET HEADING OFF; SELECT COUNT(*) FROM T;" | isql -q "$1" 2>&1 | tr -d ' \n'; }
shut() { gstat -h /data/db.fdb 2>&1 | grep -ci "shutdown"; }
ENV

status=0
for image in "${images[@]}"; do
  echo "### $image"
  docker rm -f "$NAME" >/dev/null 2>&1
  docker run -d --name "$NAME" -v "$work:/w" --entrypoint bash "$image" -c '. /w/env.sh
    grep -q "^AuthServer = Srp, Legacy_Auth" /opt/firebird/firebird.conf ||
      printf "AuthServer = Srp, Legacy_Auth\nUserManager = Srp, Legacy_UserManager\nWireCrypt = Enabled\n" >> /opt/firebird/firebird.conf
    mkdir -p /data/a /data/s /data/repl /data/out && echo $POD_NAME > /data/primary && chown -R firebird:firebird /data
    su -s /bin/sh firebird -c "/opt/firebird/bin/firebird" &
    sleep 3
    echo "CREATE DATABASE '\''localhost:/data/db.fdb'\'' USER SYSDBA PASSWORD '\''masterkey'\''; CREATE TABLE T (N INT); COMMIT; INSERT INTO T VALUES (1); INSERT INTO T VALUES (2); COMMIT;" | isql -q
    su -s /bin/sh firebird -c ". /w/env.sh; PATH=/w/wrapped:\$PATH /w/fb-agent segment-server > /data/front.log 2>&1" &
    sleep infinity' >/dev/null
  x() { docker exec "$NAME" bash -c ". /w/env.sh; $1"; }
  for _ in $(seq 1 40); do x 'front PING' 2>/dev/null | grep -q '^OK' && break; sleep 1; done
  failed=0
  ok() { echo "ok: $1"; }
  bad() { echo "FAILED: $1"; failed=1; }
  # is <what> <command> <expected>
  is() { local got; got=$(x "$2" | tail -1); if [ "$got" = "$3" ]; then ok "$1: $got"; else bad "$1: \"$got\", expected \"$3\""; fi; }
  echo "server: $(x 'isql -z </dev/null 2>/dev/null | head -n 1')"

  # the fence and its lifting: no fbsvcmgr
  is "FENCE" 'front FENCE lease lost' "OK"
  is "the database is in full shutdown" 'shut' "1"
  is "the marker says whose fence it is" 'cut -d" " -f2 /data/repl/self-fenced' "lease"
  is "STATE" 'front STATE' "OK fenced lease"
  is "FENCE again" 'front FENCE' "OK fenced"
  is "REJOIN" 'front REJOIN' "OK"
  is "the database is online" 'shut' "0"
  is "the marker is gone" 'ls /data/repl/self-fenced 2>/dev/null | wc -l' "0"
  is "REJOIN without a fence" 'front REJOIN' "OK"
  is "rows after the fence" 'rows localhost:/data/db.fdb' "2"
  is "fbsvcmgr was not needed" 'calls' "0"
  x 'grep -q "lease lost: fencing the primary (database in full shutdown)" /data/front.log' || bad "the server did not log the fence: $(x 'cat /data/front.log')"

  # the isolation check (fb-agent isolation-check, what the segment server runs as a thread): a primary
  # that reached nothing for longer than the timeout fences itself, and forgets the fence once
  # the database is online again; over the agent's own attachments, no fbsvcmgr
  check='su -s /bin/sh firebird -c ". /w/env.sh; PATH=/w/wrapped:\$PATH ONCE=true ISOLATION_TIMEOUT_SECONDS=20 CONNECT_TIMEOUT=1 KUBERNETES_SERVICE_HOST=127.0.0.1 KUBERNETES_SERVICE_PORT=1 SELF_FENCED_FILE=/data/repl/self-fenced LAST_CONNECTED=$1 /w/fb-agent isolation-check"'
  is "an isolated primary fences itself" "set -- 1; $check | grep -c 'neither the Kubernetes API server nor another instance reachable for .*; fencing the primary'" "1"
  is "the database is in full shutdown" 'shut' "1"
  is "STATE" 'front STATE' "OK fenced"
  is "the check leaves a fenced database alone" "set -- 1; $check | grep -c 'fencing\|lifted\|failed'" "0"
  x 'gfix -online localhost:/data/db.fdb'
  is "once the database is online the fence is forgotten" "set -- 1; $check" "database online again: isolation fence lifted"
  is "the marker is gone" 'ls /data/repl/self-fenced 2>/dev/null | wc -l' "0"
  is "a primary that was connected a moment ago only warns" "set -- \$(date +%s); $check | grep -c 'isolated for [0-9]*s (fencing after 20s)'" "1"
  is "the database is online" 'shut' "0"
  is "fbsvcmgr was not needed" 'calls' "0"

  # nbackup of the primary (no fbsvcmgr), which restores
  is "NBACKUP" 'front NBACKUP 0 front.nbk' "OK"
  is "fbsvcmgr was not needed" 'calls' "0"
  is "the nbackup restores" "nbackup -R /data/out/front.fdb /data/front.nbk >/dev/null 2>&1; chown firebird /data/out/front.fdb; rows /data/out/front.fdb" "2"
  x 'echo "INSERT INTO T VALUES (3); COMMIT;" | isql -q localhost:/data/db.fdb'
  is "an incremental NBACKUP" 'front NBACKUP 1 front-1.nbk' "OK"
  is "the incremental restores on its level 0" 'nbackup -R /data/out/inc.fdb /data/front.nbk /data/front-1.nbk >/dev/null 2>&1; chown firebird /data/out/inc.fdb; rows /data/out/inc.fdb' "3"
  is "a file that exists is refused" 'front NBACKUP 0 front.nbk' "ERR nbackup failed"

  # a replica: the puller (played here) pauses when asked, and the control file goes with the backup
  x 'echo db-1.db-headless > /data/primary
     # ControlFile::DataV1: "FBREPLCTL\\0", version 1, no transactions, after segment 5, offset 0, db sequence 0
     { printf "FBREPLCTL\\000\\001\\000\\000\\000\\000\\000\\005"; head -c 23 /dev/zero; } > "/data/s/{6A0B3C1D-0000-4000-8000-000000000001}"; chown firebird /data/s/*
     (while sleep 0.2; do [ -e /data/repl/.pause-pull ] && su -s /bin/sh firebird -c "touch /data/repl/.pull-paused"; done) >/dev/null 2>&1 &'
  before=$(x calls)
  is "NBACKUP of a replica" 'front NBACKUP 0 replica.nbk' "OK replica 5"
  is "its position is next to it" 'cmp /data/replica.nbk.ctl /data/s/\{*\} && echo same' "same"
  is "the puller is not left paused" 'ls /data/repl/.pause-pull 2>/dev/null | wc -l' "0"
  is "fbsvcmgr was not needed" 'calls' "$before"
  if [ "$failed" = 1 ]; then echo "### $image: FAILED"; x 'tail -25 /data/front.log; cat /data/fbsvcmgr.calls'; status=1; else echo "### $image: passed"; fi
done
exit $status
