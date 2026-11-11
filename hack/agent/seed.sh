#!/bin/bash
# Checks the agent's SEED (docs/rust-agent-segment-server.md, step 4) against real Firebird
# servers. The segment server's front makes the seed copy itself: the backup lock, the
# replication sequence and the database's GUID through its own SQL attachments, where the Perl
# server behind it runs isql. Both are asked for each kind of seed, with the client the
# replication init uses (fetch-seed.pl), and each copy is opened as a database:
#   live      a copy of the running primary under the backup lock
#   offline   the bootstrap seed file, while the journal has not moved past it
#   replica   a replica's copy with its control file, after its puller paused
#
#   hack/agent/seed.sh [image ...]      (default: Firebird 4, 5 and the 6 snapshot)
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
NAME=${NAME:-fbseed}
work=$(mktemp -d)
chmod 755 "$work"
cleanup() {
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  docker run --rm -v "$work:/w" --entrypoint sh "${images[0]}" -c 'rm -rf /w/*' >/dev/null 2>&1 || true
  rmdir "$work" 2>/dev/null || true
}
trap cleanup EXIT
for script in segment-server.pl fetch-seed.pl; do
  awk -v dir="$src" '/^#@include [^ ]+$/ { while ((getline line < (dir "/" $2)) > 0) print line; close(dir "/" $2); next } { print }' \
    "$src/$script" > "$work/$script"
done
cp "$agent" "$work/fb-agent"
cat > "$work/env.sh" <<'ENV'
export ISC_USER=SYSDBA ISC_PASSWORD=masterkey PATH=/opt/firebird/bin:$PATH
export DATABASE_PATH=/data/db.fdb ARCHIVE_DIR=/data/a SOURCE_DIR=/data/s REPLICATION_DIR=/data/repl
export PRIMARY_FILE=/data/primary POD_NAME=db-0 SCRIPT_DIR=/w ALLOW_LIVE_SEED=true
rows() { echo "SET HEADING OFF; SELECT COUNT(*) FROM T;" | isql -q "$1" 2>&1 | tr -d ' \n'; }
# fetch <front|perl> <name>: a seed from the front (3051) or the Perl server behind it (3071)
fetch() { case $1 in front) port=3051 ;; *) port=3071 ;; esac; SEGMENT_PORT=$port perl /w/fetch-seed.pl 127.0.0.1 /data/out/$2 2>&1; }
# the copy as a database: unlocked (a locked copy carries the backup lock), then counted
opened() { cp /data/out/$1 /data/out/$1.fdb && chown firebird /data/out/$1.fdb && { [ "$2" = offline ] || nbackup -SEQ -F /data/out/$1.fdb; } && rows /data/out/$1.fdb; }
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
    su -s /bin/sh firebird -c ". /w/env.sh; /w/fb-agent segment-server -- perl /w/segment-server.pl > /data/front.log 2>&1" &
    sleep infinity' >/dev/null
  x() { docker exec "$NAME" bash -c ". /w/env.sh; $1"; }
  for _ in $(seq 1 40); do x 'SEGMENT_PORT=3071 /w/fb-agent request 127.0.0.1 PING' 2>/dev/null | grep -q '^OK' && break; sleep 1; done
  failed=0
  ok() { echo "ok: $1"; }
  bad() { echo "FAILED: $1"; failed=1; }
  # seed <kind> <expected rows>: both servers give a seed of that kind that opens with the rows
  seed() {
    local who out n
    for who in front perl; do
      out=$(x "fetch $who $1-$who")
      if ! echo "$out" | grep -q "^fetched $1 seed copy"; then bad "$1 seed from the $who: $out"; continue; fi
      n=$(x "opened $1-$who $1" 2>&1 | tail -1)
      if [ "$n" = "$2" ]; then ok "$1 seed from the $who: $(echo "$out" | sed 's/ from .*//'), opens with $n rows"; else bad "$1 seed from the $who does not open with $2 rows: $n"; fi
    done
  }
  echo "server: $(x 'isql -z </dev/null 2>/dev/null | head -n 1')"

  # live: the running primary under the backup lock; the database is unlocked and writable after
  seed live 2
  x 'grep -c "served live seed copy of the primary" /data/front.log' | grep -qx 2 || bad "the front and the Perl server did not each serve a live copy: $(x 'cat /data/front.log')"
  x 'gstat -h /data/db.fdb | grep -i "backup lock"' >/dev/null && bad "the database is still under the backup lock"
  x 'echo "INSERT INTO T VALUES (3); COMMIT;" | isql -q localhost:/data/db.fdb' || bad "cannot write after the live seeds"
  [ "$(x 'ls /data/repl/seed.copy 2>/dev/null')" = "" ] || bad "the seed copy was left behind"

  # offline: the bootstrap seed, a copy made while the server did not have the database open
  x 'gfix -shut full -force 0 localhost:/data/db.fdb && cp /data/db.fdb /data/repl/bootstrap-seed.fdb && chown firebird /data/repl/bootstrap-seed.fdb && gfix -online localhost:/data/db.fdb && gfix -shut full -force 0 /data/repl/bootstrap-seed.fdb 2>/dev/null; gfix -online /data/repl/bootstrap-seed.fdb 2>/dev/null; true'
  seed offline 3
  for who in front perl; do x "cmp /data/out/offline-$who /data/repl/bootstrap-seed.fdb" >/dev/null || bad "the offline seed from the $who is not the bootstrap seed"; done

  # replica: another instance is the primary; the puller (played here) pauses when asked, and
  # the control file, named after the database's GUID, goes with the copy
  x 'rm -f /data/repl/bootstrap-seed.fdb; echo db-1.db-headless > /data/primary
     guid=$(echo "SET HEADING OFF; SELECT MON\$GUID FROM MON\$DATABASE;" | isql -q localhost:/data/db.fdb | tr -d " \n")
     perl -e "print pack(q{a10 v V Q< V x4 Q<}, q{FBREPLCTL}, 1, 0, 5, 0, 0)" > "/data/s/$guid"; chown firebird "/data/s/$guid"
     (while sleep 0.2; do [ -e /data/repl/.pause-pull ] && su -s /bin/sh firebird -c "touch /data/repl/.pull-paused"; done) >/dev/null 2>&1 &'
  seed replica 3
  for who in front perl; do
    x "cmp /data/out/replica-$who.ctl /data/s/\{*\}" >/dev/null || bad "the replica seed from the $who came without the control file"
  done
  [ "$(x 'ls /data/repl/.pause-pull 2>/dev/null')" = "" ] || bad "the puller was left paused"
  x 'gstat -h /data/db.fdb | grep -i "backup lock"' >/dev/null && bad "the database is still under the backup lock"

  # the front made its copies itself (its own SQL attachments), the Perl server its own (isql)
  for by in "locked copy (fb-agent):" "locked copy:"; do
    x "grep -c '^$by locked=1 copied=1 unlocked=1' /data/front.log" | grep -qx 2 || bad "expected two copies (live, replica) logged as \"$by\": $(x 'grep "locked copy" /data/front.log')"
  done
  if [ "$failed" = 1 ]; then echo "### $image: FAILED"; x 'tail -20 /data/front.log'; status=1; else echo "### $image: passed"; fi
done
exit $status
