#!/bin/bash
# Checks the agent's segment server without the Perl server behind it
# (docs/rust-agent-segment-server.md, step 7) against real Firebird servers. Three servers on
# the same database:
#
#   alone    (3051) reaches the Firebird server over its own attachments: no tool is run
#   tools    (3052) is given a port nothing listens on for its own attachments: it asks isql,
#            fbsvcmgr and gfix, as the Perl server did, and they are recorded
#   nothing  (3053) has neither: it refuses, in the Perl server's words
#
#   hack/agent/alone.sh [image ...]      (default: Firebird 4, 5 and the 6 snapshot)
#
# Needs Docker and the fb-agent binary: FB_AGENT_BIN, or a musl build in operator/agent/target.
# Exit status 1 when an image fails.
set -u
here=$(cd "$(dirname "$0")" && pwd)
agent=${FB_AGENT_BIN:-$here/../../operator/agent/target/x86_64-unknown-linux-musl/release/fb-agent}
[ -x "$agent" ] || { echo "no fb-agent binary at $agent (set FB_AGENT_BIN)"; exit 1; }
images=("$@")
[ ${#images[@]} -gt 0 ] || images=(firebirdsql/firebird:4 firebirdsql/firebird:5 firebirdsql/firebird:6-snapshot)
NAME=${NAME:-fbalone}
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
for tool in fbsvcmgr isql gfix; do
  printf '#!/bin/sh\necho "%s $*" >> /data/tools.calls\nexec /opt/firebird/bin/%s "$@"\n' "$tool" "$tool" > "$work/wrapped/$tool"
  chmod 755 "$work/wrapped/$tool"
done
cat > "$work/env.sh" <<'ENV'
export ISC_USER=SYSDBA ISC_PASSWORD=masterkey PATH=/opt/firebird/bin:$PATH
export DATABASE_PATH=/data/db.fdb ARCHIVE_DIR=/data/a SOURCE_DIR=/data/s
export PRIMARY_FILE=/data/primary POD_NAME=db-0 SCRIPT_DIR=/w ALLOW_LIVE_SEED=true
alone() { SEGMENT_PORT=3051 /w/fb-agent request 127.0.0.1 "$@" 2>&1; }
tools() { SEGMENT_PORT=3052 /w/fb-agent request 127.0.0.1 "$@" 2>&1; }
nothing() { SEGMENT_PORT=3053 /w/fb-agent request 127.0.0.1 "$@" 2>&1; }
calls() { cat /data/tools.calls 2>/dev/null | wc -l; }
called() { grep -c "^$1 " /data/tools.calls 2>/dev/null; }
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
    mkdir -p /data/a /data/s /data/repl /data/repl-tools /data/repl-nothing /data/out && echo $POD_NAME > /data/primary && chown -R firebird:firebird /data
    su -s /bin/sh firebird -c "/opt/firebird/bin/firebird" &
    sleep 3
    echo "CREATE DATABASE '\''localhost:/data/db.fdb'\'' USER SYSDBA PASSWORD '\''masterkey'\''; CREATE TABLE T (N INT); COMMIT; INSERT INTO T VALUES (1); INSERT INTO T VALUES (2); COMMIT;" | isql -q
    su -s /bin/sh firebird -c ". /w/env.sh; PATH=/w/wrapped:\$PATH REPLICATION_DIR=/data/repl SEGMENT_PORT=3051 /w/fb-agent segment-server > /data/alone.log 2>&1" &
    su -s /bin/sh firebird -c ". /w/env.sh; PATH=/w/wrapped:\$PATH REPLICATION_DIR=/data/repl-tools SEGMENT_PORT=3052 FIREBIRD_PORT=1 /w/fb-agent segment-server > /data/tools.log 2>&1" &
    su -s /bin/sh firebird -c ". /w/env.sh; PATH=/usr/bin:/bin REPLICATION_DIR=/data/repl-nothing SEGMENT_PORT=3053 FIREBIRD_PORT=1 /w/fb-agent segment-server > /data/nothing.log 2>&1" &
    sleep infinity' >/dev/null
  x() { docker exec "$NAME" bash -c ". /w/env.sh; $1"; }
  for _ in $(seq 1 40); do x 'alone PING && tools PING && nothing PING' 2>/dev/null | grep -c '^OK' | grep -q 3 && break; sleep 1; done
  failed=0
  ok() { echo "ok: $1"; }
  bad() { echo "FAILED: $1"; failed=1; }
  # is <what> <command> <expected>
  is() { local got; got=$(x "$2" | tail -1); if [ "$got" = "$3" ]; then ok "$1: $got"; else bad "$1: \"$got\", expected \"$3\""; fi; }
  echo "server: $(x 'isql -z </dev/null 2>/dev/null | head -n 1')"
  version=$(x 'echo "SET LIST ON; SELECT RDB\$GET_CONTEXT('"'SYSTEM', 'ENGINE_VERSION'"') AS V FROM RDB\$DATABASE;" | isql -q localhost:/data/db.fdb' | awk '$1 == "V" { print $2 }')

  # no Perl server anywhere: the agent's own processes only
  is "no Perl server runs" 'ps -eo args | grep -c "[s]egment-server.pl"' "0"
  is "the server says so" 'grep -c "no Perl server behind it" /data/alone.log' "1"

  # over its own attachments: every request, and no tool
  is "STATE" 'alone STATE' "OK online"
  is "VERSION" 'alone VERSION' "OK $version"
  is "FENCE" 'alone FENCE lease lost' "OK"
  is "the database is in full shutdown" 'shut' "1"
  is "STATE of the fenced database" 'alone STATE' "OK fenced lease"
  is "REJOIN" 'alone REJOIN' "OK"
  is "the database is online" 'shut' "0"
  is "NBACKUP" 'alone NBACKUP 0 alone.nbk' "OK"
  is "the nbackup restores" 'nbackup -R /data/out/alone.fdb /data/alone.nbk >/dev/null 2>&1; chown firebird /data/out/alone.fdb; rows /data/out/alone.fdb' "2"
  is "a live SEED is a database" 'alone SEED > /data/out/seed.raw; head -c 40 /data/out/seed.raw | head -n 1 | cut -d" " -f1' "OK"
  is "an unknown request" 'alone NOSUCH' "ERR bad request"
  is "a bad argument" 'alone NBACKUP 7 x.nbk' "ERR bad request"
  is "a file that is not there" 'alone GET db.fdb.journal-000000001' "ERR bad request"
  is "no tool was needed" 'calls' "0"

  # its own attachments refused: the tools, as the Perl server ran them
  is "STATE through fbsvcmgr" 'tools STATE' "OK online"
  is "fbsvcmgr ran" 'called fbsvcmgr' "1"
  is "VERSION through isql" 'tools VERSION' "OK $version"
  is "isql ran" 'called isql' "1"
  is "FENCE through fbsvcmgr" 'tools FENCE' "OK"
  is "the database is in full shutdown" 'shut' "1"
  is "STATE of the fenced database" 'tools STATE' "OK fenced lease"
  is "REJOIN through fbsvcmgr" 'tools REJOIN' "OK"
  is "the database is online" 'shut' "0"
  is "rows after the fence" 'rows localhost:/data/db.fdb' "2"
  is "NBACKUP through fbsvcmgr" 'tools NBACKUP 0 tools.nbk' "OK"
  is "the nbackup restores" 'nbackup -R /data/out/tools.fdb /data/tools.nbk >/dev/null 2>&1; chown firebird /data/out/tools.fdb; rows /data/out/tools.fdb' "2"
  is "a file that exists is refused, in the Perl server's words" 'tools NBACKUP 0 tools.nbk' "ERR nbackup failed"
  is "a live SEED through isql is a database" 'tools SEED > /data/out/seed-tools.raw; head -c 40 /data/out/seed-tools.raw | head -n 1 | cut -d" " -f1' "OK"
  is "the backup lock is released" 'echo "SET LIST ON; SELECT MON\$BACKUP_STATE AS V FROM MON\$DATABASE;" | isql -q localhost:/data/db.fdb | awk "\$1 == \"V\" { print \$2 }"' "0"

  # neither: refused, in the Perl server's words, and nothing left changed
  is "STATE with nobody to ask" 'nothing STATE' "ERR cannot read the state of /data/db.fdb"
  is "VERSION with nobody to ask" 'nothing VERSION' "ERR cannot read the engine version"
  is "FENCE with nobody to ask" 'nothing FENCE' "ERR cannot shut /data/db.fdb down"
  is "no fence marker is left" 'ls /data/repl-nothing/self-fenced 2>/dev/null | wc -l' "0"
  is "NBACKUP with nobody to ask" 'nothing NBACKUP 0 nothing.nbk' "ERR nbackup failed"
  is "PROMOTE with nobody to ask" 'nothing PROMOTE none' "ERR cannot read the replica mode"
  is "the requests from the files are answered all the same" 'nothing ISOLATION' "OK online"
  is "the database is online" 'shut' "0"
  if [ "$failed" = 1 ]; then echo "### $image: FAILED"; x 'tail -15 /data/alone.log /data/tools.log /data/nothing.log; cat /data/tools.calls'; status=1; else echo "### $image: passed"; fi
done
exit $status
