#!/bin/bash
# Checks the agent's PROMOTE (docs/rust-agent-segment-server.md, step 5) against real Firebird
# servers: a replica with a journal behind it is promoted in place, once by the segment server's
# front and once, from the same start, by the Perl server alone. Both must reply the same and
# leave the same behind: the replication sequence in the header, the database online, not a
# replica and publishing, its journal continuing after the promotion point, the replica's state
# gone, the lineage recorded, the offline bootstrap seed written. Before that, a promotion made to
# fail after the full shutdown must leave the replica as it was.
#
# The image's tools are wrapped so that every call is recorded. The front reaches the server
# over its own attachments: it must not run fbsvcmgr at all, and gfix and isql only on the
# bootstrap seed copy, a file no server has open.
#
#   hack/agent/promote.sh [image ...]      (default: Firebird 4, 5 and the 6 snapshot)
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
NAME=${NAME:-fbpromote}
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
cp "$src/set-repl-seq.pl" "$src/enable-publication.sql" "$work/"
cp "$agent" "$work/fb-agent"
mkdir "$work/wrapped"
for tool in fbsvcmgr gfix isql; do
  printf '#!/bin/sh\necho "%s $*" >> /data/tool.calls\nexec /opt/firebird/bin/%s "$@"\n' "$tool" "$tool" > "$work/wrapped/$tool"
  chmod 755 "$work/wrapped/$tool"
done
cat > "$work/env.sh" <<'ENV'
export ISC_USER=SYSDBA ISC_PASSWORD=masterkey PATH=/opt/firebird/bin:$PATH
export DATABASE_PATH=/data/db.fdb ARCHIVE_DIR=/data/a SOURCE_DIR=/data/s REPLICATION_DIR=/data/repl JOURNAL_DIR=/data/j
export PRIMARY_FILE=/data/primary POD_NAME=db-1 SCRIPT_DIR=/w
DB=/data/db.fdb
cat > /opt/firebird/replication.conf <<CONF
database
{
}
database = $DB
{
    journal_directory = /data/j
    journal_archive_directory = /data/a
    journal_archive_command = "test ! -f \$(archivepathname) && cp \$(pathname) \$(archivepathname)"
    journal_archive_timeout = 2
    journal_source_directory = /data/s
    apply_idle_timeout = 2
}
CONF
grep -q '^AuthServer = Srp, Legacy_Auth' /opt/firebird/firebird.conf ||
  printf 'AuthServer = Srp, Legacy_Auth\nUserManager = Srp, Legacy_UserManager\nWireCrypt = Enabled\n' >> /opt/firebird/firebird.conf
sql() { echo "$1" | /opt/firebird/bin/isql -q localhost:$DB; }
value() { echo "SET LIST ON; SELECT $1 AS V FROM MON\$DATABASE;" | /opt/firebird/bin/isql -q localhost:$DB | awk '$1 == "V" { print $2 }'; }
ask() { SEGMENT_PORT=$1 /w/fb-agent request 127.0.0.1 "${@:2}" 2>&1; }
ENV

# what a promotion left behind, one line per fact (compared between the front and the Perl server)
facts='. /w/env.sh
  echo "replica mode: $(value MON\$REPLICA_MODE)"
  echo "sequence at least 7: $([ "$(value "RDB\$GET_CONTEXT('"'"'SYSTEM'"'"', '"'"'REPLICATION_SEQUENCE'"'"')")" -ge 7 ] && echo yes || echo no)"
  echo "promoted flag: $(ls /data/repl/promoted 2>/dev/null | wc -l)"
  echo "journal floor: $(cat /data/repl/journal-floor 2>/dev/null)"
  echo "lineage: $(cat /data/repl/lineage 2>/dev/null | tr "\n" ";")"
  echo "source directory: $(ls -A /data/s | wc -l) files"
  echo "puller paused: $(ls /data/repl/.pause-pull 2>/dev/null | wc -l)"
  echo "puller position: $(ls /data/repl/.last-pulled 2>/dev/null | wc -l)"
  echo "bootstrap seed: $(ls /data/repl/bootstrap-seed.fdb 2>/dev/null | wc -l) at sequence $(cat /data/repl/bootstrap-seed.seq 2>/dev/null), copies left: $(ls /data/repl/*.tmp 2>/dev/null | wc -l)"
  cp /data/repl/bootstrap-seed.fdb /data/check.fdb 2>/dev/null && chown firebird /data/check.fdb
  echo "bootstrap seed rows: $(echo "SET HEADING OFF; SELECT COUNT(*) FROM APP;" | /opt/firebird/bin/isql -q /data/check.fdb 2>&1 | tr -d " \n")"
  echo "bootstrap seed a replica: $(/opt/firebird/bin/gstat -h /data/check.fdb | grep -ci "read-only replica\|read-write replica")"
  sql "INSERT INTO APP VALUES (100); COMMIT;" >/dev/null 2>&1; echo "writable: $?"
  for i in $(seq 1 15); do ls /data/a | grep -q "journal-0*8$" && break; sleep 1; done
  echo "journal continues: $(ls /data/a | sed "s/.*journal-0*//" | sort -n | head -1)"
  echo "old segments left: $(ls /data/a /data/j | grep -c "old")"'

status=0
for image in "${images[@]}"; do
  echo "### $image"
  for who in front perl; do
    docker rm -f "$NAME" >/dev/null 2>&1
    docker run -d --name "$NAME" -v "$work:/w" --entrypoint bash "$image" -c '. /w/env.sh
      mkdir -p /data/j /data/a /data/s /data/repl && echo db-0.db-headless > /data/primary && chown -R firebird:firebird /data
      su -s /bin/sh firebird -c "/opt/firebird/bin/firebird" &
      sleep infinity' >/dev/null
    x() { docker exec "$NAME" bash -c ". /w/env.sh; $1"; }
    sleep 3
    # a database with a journal behind it, then a read-only replica of itself: its control file at
    # segment 5, a fetched segment, a puller position, old files in its journal and archive
    x 'echo "CREATE DATABASE '\''localhost:/data/db.fdb'\'' USER SYSDBA PASSWORD '\''masterkey'\''; CREATE TABLE APP (N INT); COMMIT; ALTER DATABASE ENABLE PUBLICATION; ALTER DATABASE INCLUDE ALL TO PUBLICATION; COMMIT;" | /opt/firebird/bin/isql -q
       for n in 1 2 3; do sql "INSERT INTO APP VALUES ($n); COMMIT;"; sleep 3; done
       sql "ALTER DATABASE DISABLE PUBLICATION; COMMIT;"
       /opt/firebird/bin/gfix -replica read_only localhost:$DB
       guid=$(value MON\$GUID)
       perl -e "print pack(q{a10 v V Q< V x4 Q<}, q{FBREPLCTL}, 1, 0, 5, 0, 0)" > "/data/s/$guid"
       echo x > /data/s/old-fetched; echo x > /data/a/old-archived; echo x > /data/j/old-journal
       echo "db.fdb.journal-000000005" > /data/repl/.last-pulled
       chown -R firebird:firebird /data
       (while sleep 0.2; do [ -e /data/repl/.pause-pull ] && su -s /bin/sh firebird -c "touch /data/repl/.pull-paused"; done) >/dev/null 2>&1 &
       su -s /bin/sh firebird -c ". /w/env.sh; PATH=/w/wrapped:\$PATH /w/fb-agent segment-server -- perl /w/segment-server.pl > /data/front.log 2>&1" &' >/dev/null 2>&1
    for _ in $(seq 1 40); do x 'ask 3071 PING' 2>/dev/null | grep -q '^OK' && break; sleep 1; done
    case $who in front) port=3051 ;; *) port=3071 ;; esac
    {
      echo "before: replica mode $(x 'value MON\$REPLICA_MODE'), position $(x "ask $port POSITION")"
      # a promotion that fails after the full shutdown (the journal floor cannot be written)
      # restores the replica: online, still a replica, the puller no longer paused
      x 'mkdir /data/repl/journal-floor.tmp; chown firebird /data/repl/journal-floor.tmp'
      echo "PROMOTE that fails: $(x "ask $port PROMOTE 7")"
      echo "after it: replica mode $(x 'value MON\$REPLICA_MODE'), state $(x "ask $port STATE"), puller paused $(x 'ls /data/repl/.pause-pull 2>/dev/null | wc -l'), copies left $(x 'ls /data/repl/bootstrap-seed.fdb.tmp 2>/dev/null | wc -l'), position $(x "ask $port POSITION")"
      x 'rmdir /data/repl/journal-floor.tmp'
      echo "PROMOTE 7: $(x "ask $port PROMOTE 7")"
      x "$facts"
      echo "PROMOTE again: $(x "ask $port PROMOTE 7" | awk '{ print $1, NF }')"
      echo "position: $(x "ask $port POSITION")"
    } > "$work/$who.txt" 2>&1
    x 'cat /data/tool.calls 2>/dev/null' > "$work/$who.calls"
    x 'cat /data/front.log' > "$work/$who.log"
  done
  failed=0
  sed 's/^/  /' "$work/front.txt"
  if ! diff "$work/perl.txt" "$work/front.txt" >/dev/null; then echo "FAILED: the front and the Perl server differ:"; diff "$work/perl.txt" "$work/front.txt"; failed=1; fi
  for want in "PROMOTE that fails: ERR cannot write the journal floor" "after it: replica mode 1, state OK online, puller paused 0, copies left 0, position OK 5 0 0" \
              "PROMOTE 7: OK 7 1" "replica mode: 0" "sequence at least 7: yes" "journal floor: 7" "lineage: 5 7;" "source directory: 0 files" \
              "bootstrap seed: 1 at sequence 7, copies left: 0" "bootstrap seed rows: 3" "bootstrap seed a replica: 0" "writable: 0" "journal continues: 8" \
              "old segments left: 0" "PROMOTE again: OK 2" "position: OK primary" "puller paused: 0" "puller position: 0" "promoted flag: 1"; do
    grep -qxF "$want" "$work/front.txt" || { echo "FAILED: expected \"$want\""; failed=1; }
  done
  # the front: no fbsvcmgr, and gfix and isql on the bootstrap seed copy only
  if grep -q '^fbsvcmgr' "$work/front.calls"; then echo "FAILED: the front's promotion ran fbsvcmgr:"; cat "$work/front.calls"; failed=1; fi
  if grep -v 'bootstrap-seed.fdb.tmp' "$work/front.calls" | grep -q .; then echo "FAILED: the front's promotion ran a tool on something else than the seed copy:"; cat "$work/front.calls"; failed=1; fi
  grep -q '^fbsvcmgr' "$work/perl.calls" || { echo "FAILED: the Perl server's promotion did not run fbsvcmgr (the wrappers are not in its path)"; failed=1; }
  echo "  tools run by the front: $(wc -l < "$work/front.calls") (the seed copy), by the Perl server: $(wc -l < "$work/perl.calls")"
  if [ "$failed" = 1 ]; then echo "### $image: FAILED"; tail -25 "$work/front.log"; status=1; else echo "### $image: passed"; fi
done
exit $status
