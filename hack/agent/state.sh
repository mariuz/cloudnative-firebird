#!/bin/bash
# Checks the agent's STATE and VERSION (docs/rust-agent-segment-server.md, step 2) against real Firebird
# servers: the segment server answers them from a service attachment that stays open. It is asked
# in every state of the database: online, full shutdown, fenced by the Lease holder, fenced by the
# isolation check, online again, the server stopped, the server back. Then the time per request.
#
#   hack/agent/state.sh [image ...]      (default: Firebird 4, 5 and the 6 snapshot)
#
# Needs Docker and the fb-agent binary: FB_AGENT_BIN, or a musl build in operator/agent/target.
# Exit status 1 when an image fails.
set -u
here=$(cd "$(dirname "$0")" && pwd)
agent=${FB_AGENT_BIN:-$here/../../operator/agent/target/x86_64-unknown-linux-musl/release/fb-agent}
[ -x "$agent" ] || { echo "no fb-agent binary at $agent (set FB_AGENT_BIN)"; exit 1; }
images=("$@")
[ ${#images[@]} -gt 0 ] || images=(firebirdsql/firebird:4 firebirdsql/firebird:5 firebirdsql/firebird:6-snapshot)
NAME=${NAME:-fbstate}
work=$(mktemp -d)
chmod 755 "$work"
cleanup() {
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  docker run --rm -v "$work:/w" --entrypoint sh "${images[0]}" -c 'rm -rf /w/*' >/dev/null 2>&1 || true
  rmdir "$work" 2>/dev/null || true
}
trap cleanup EXIT
cp "$agent" "$work/fb-agent"
cat > "$work/env.sh" <<'ENV'
export ISC_USER=SYSDBA ISC_PASSWORD=masterkey PATH=/opt/firebird/bin:$PATH
export DATABASE_PATH=/data/db.fdb ARCHIVE_DIR=/data/a SOURCE_DIR=/data/s REPLICATION_DIR=/data/repl
export PRIMARY_FILE=/data/primary POD_NAME=db-0 SCRIPT_DIR=/w
start_firebird() { su -s /bin/sh firebird -c "/opt/firebird/bin/firebird" & }
# the segment server's answer
front() { SEGMENT_PORT=3051 /w/fb-agent request 127.0.0.1 ${1:-STATE}; }
shut() { fbsvcmgr localhost:service_mgr action_properties dbname $DATABASE_PATH prp_shutdown_mode prp_sm_full prp_force_shutdown 0; }
online() { fbsvcmgr localhost:service_mgr action_properties dbname $DATABASE_PATH prp_online_mode prp_sm_normal; }
ENV

status=0
for image in "${images[@]}"; do
  echo "### $image"
  docker rm -f "$NAME" >/dev/null 2>&1
  docker run -d --name "$NAME" -v "$work:/w" --entrypoint bash "$image" -c '. /w/env.sh
    grep -q "^AuthServer = Srp, Legacy_Auth" /opt/firebird/firebird.conf ||
      printf "AuthServer = Srp, Legacy_Auth\nUserManager = Srp, Legacy_UserManager\nWireCrypt = Enabled\n" >> /opt/firebird/firebird.conf
    mkdir -p /data/a /data/s /data/repl && echo $POD_NAME > /data/primary && chown -R firebird:firebird /data
    start_firebird; sleep 3
    echo "CREATE DATABASE '\''localhost:/data/db.fdb'\'' USER SYSDBA PASSWORD '\''masterkey'\'';" | isql -q
    su -s /bin/sh firebird -c ". /w/env.sh; /w/fb-agent segment-server > /data/front.log 2>&1" &
    sleep infinity' >/dev/null
  x() { docker exec "$NAME" bash -c ". /w/env.sh; $1"; }
  for _ in $(seq 1 40); do x 'front' 2>/dev/null | grep -q '^OK online' && break; sleep 1; done
  failed=0
  # same <what> <expected>: the segment server says <expected>
  same() {
    local f
    f=$(x front 2>&1)
    if [ "$f" = "$2" ]; then echo "ok: $1: $f"; else echo "FAILED: $1: \"$f\", expected \"$2\""; failed=1; fi
  }
  echo "server: $(x 'isql -z </dev/null 2>/dev/null | head -n 1')"
  same "online" "OK online"
  # the engine version, from the server's version string: the one the database reports
  fv=$(x 'front VERSION' 2>&1)
  ev=$(x 'echo "SET HEADING OFF; SELECT RDB\$GET_CONTEXT('"'"'SYSTEM'"'"', '"'"'ENGINE_VERSION'"'"') FROM RDB\$DATABASE;" | isql -q localhost:/data/db.fdb | tr -d " \n"' 2>&1)
  if [ "$fv" = "OK $ev" ] && echo "$fv" | grep -Eq '^OK [0-9]+\.[0-9]+\.[0-9]+$'; then echo "ok: engine version: $fv"; else echo "FAILED: engine version: \"$fv\", the database says \"$ev\""; failed=1; fi
  x 'grep -q "database state from a service attachment" /data/front.log' || { echo "FAILED: the segment server did not attach to the services:"; x 'cat /data/front.log'; failed=1; }
  x 'shut'
  same "full shutdown" "OK shutdown"
  x 'echo "$(date +%s) lease" > /data/repl/self-fenced; chown firebird /data/repl/self-fenced'
  same "shut down by the Lease holder's fence" "OK fenced lease"
  x 'date +%s > /data/repl/self-fenced'
  same "shut down by the isolation check's fence" "OK fenced"
  x 'rm -f /data/repl/self-fenced; online'
  same "online again" "OK online"
  # a marker left while the database is online says nothing
  x 'echo "1 lease" > /data/repl/self-fenced'
  same "online with a stale marker" "OK online"
  x 'rm -f /data/repl/self-fenced'

  # the time of 30 requests each (the client process included)
  for what in STATE VERSION; do
    ms=$(x "s=\$(date +%s%N); for i in \$(seq 1 30); do front $what >/dev/null; done; echo \$(( (\$(date +%s%N) - s) / 30000000 ))")
    echo "$ms ms per $what"
  done

  # the server stopped: it cannot tell; back: it attaches again
  x 'pkill -x firebird; for i in $(seq 1 20); do pgrep -x firebird >/dev/null || break; sleep 0.5; done'
  same "the server stopped" "ERR cannot read the state of /data/db.fdb"
  x 'start_firebird; sleep 3' >/dev/null 2>&1
  for _ in $(seq 1 20); do x 'front' 2>/dev/null | grep -q '^OK online' && break; sleep 1; done
  same "the server back" "OK online"
  ms=$(x 's=$(date +%s%N); for i in $(seq 1 30); do front >/dev/null; done; echo $(( ($(date +%s%N) - s) / 30000000 ))')
  echo "after the restart: $ms ms per STATE"
  # from its attachment again: two of them, the one at the start and the one after the restart
  [ "$(x 'grep -c "database state from a service attachment" /data/front.log')" -ge 2 ] || { echo "FAILED: after the server's restart the segment server did not attach again"; failed=1; }
  if [ "$failed" = 1 ]; then echo "### $image: FAILED"; x 'cat /data/front.log' | tail -20; status=1; else echo "### $image: passed"; fi
done
exit $status
