#!/bin/bash
# Checks the agent's STATE (docs/rust-agent-segment-server.md, step 2) against real Firebird
# servers: the segment server's front answers it from a service attachment that stays open, the
# Perl server behind it with one fbsvcmgr per request. Both are asked in every state of the
# database and must say the same: online, full shutdown, fenced by the Lease holder, fenced by the
# isolation check, online again, the server stopped, the server back. Then the time per request.
#
#   hack/agent/state.sh [image ...]      (default: Firebird 4, 5 and the 6 snapshot)
#
# Needs Docker and the fb-agent binary: FB_AGENT_BIN, or a musl build in operator/agent/target.
# Exit status 1 when an image fails.
set -u
here=$(cd "$(dirname "$0")" && pwd)
src=$(cd "$here/../../operator/src/replication" && pwd)
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
awk -v dir="$src" '/^#@include [^ ]+$/ { while ((getline line < (dir "/" $2)) > 0) print line; close(dir "/" $2); next } { print }' \
  "$src/segment-server.pl" > "$work/segment-server.pl"
cp "$agent" "$work/fb-agent"
cat > "$work/env.sh" <<'ENV'
export ISC_USER=SYSDBA ISC_PASSWORD=masterkey PATH=/opt/firebird/bin:$PATH
export DATABASE_PATH=/data/db.fdb ARCHIVE_DIR=/data/a SOURCE_DIR=/data/s REPLICATION_DIR=/data/repl
export PRIMARY_FILE=/data/primary POD_NAME=db-0 SCRIPT_DIR=/w
start_firebird() { su -s /bin/sh firebird -c "/opt/firebird/bin/firebird" & }
# the front's answer and the Perl server's (its own port behind the front)
front() { SEGMENT_PORT=3051 /w/fb-agent request 127.0.0.1 STATE; }
perl_server() { SEGMENT_PORT=3071 /w/fb-agent request 127.0.0.1 STATE; }
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
    su -s /bin/sh firebird -c ". /w/env.sh; /w/fb-agent segment-server -- perl /w/segment-server.pl > /data/front.log 2>&1" &
    sleep infinity' >/dev/null
  x() { docker exec "$NAME" bash -c ". /w/env.sh; $1"; }
  for _ in $(seq 1 40); do x 'perl_server' 2>/dev/null | grep -q '^OK online' && break; sleep 1; done
  failed=0
  # same <what> <expected>: the front and the Perl server both say <expected>
  same() {
    local f p
    f=$(x front 2>&1); p=$(x perl_server 2>&1)
    if [ "$f" = "$2" ] && [ "$p" = "$2" ]; then echo "ok: $1: $f"; else echo "FAILED: $1: front \"$f\", Perl server \"$p\", expected \"$2\""; failed=1; fi
  }
  echo "server: $(x 'isql -z </dev/null 2>/dev/null | head -n 1')"
  same "online" "OK online"
  x 'grep -q "database state from a service attachment" /data/front.log' || { echo "FAILED: the front did not attach to the services:"; x 'cat /data/front.log'; failed=1; }
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
  for who in front perl_server; do
    ms=$(x "s=\$(date +%s%N); for i in \$(seq 1 30); do $who >/dev/null; done; echo \$(( (\$(date +%s%N) - s) / 30000000 ))")
    echo "$who: $ms ms per STATE"
    [ "$who" = front ] && front_ms=$ms || perl_ms=$ms
  done
  [ "$front_ms" -lt "$perl_ms" ] || { echo "FAILED: the front's STATE is not faster than the Perl server's"; failed=1; }

  # the server stopped: neither can tell; back: the front attaches again
  x 'pkill -x firebird; for i in $(seq 1 20); do pgrep -x firebird >/dev/null || break; sleep 0.5; done'
  same "the server stopped" "ERR cannot read the state of /data/db.fdb"
  x 'start_firebird; sleep 3' >/dev/null 2>&1
  for _ in $(seq 1 20); do x 'perl_server' 2>/dev/null | grep -q '^OK online' && break; sleep 1; done
  same "the server back" "OK online"
  ms=$(x 's=$(date +%s%N); for i in $(seq 1 30); do front >/dev/null; done; echo $(( ($(date +%s%N) - s) / 30000000 ))')
  echo "front after the restart: $ms ms per STATE"
  [ "$ms" -lt "$perl_ms" ] || { echo "FAILED: after the server's restart the front no longer answers from its attachment"; failed=1; }
  if [ "$failed" = 1 ]; then echo "### $image: FAILED"; x 'cat /data/front.log' | tail -20; status=1; else echo "### $image: passed"; fi
done
exit $status
