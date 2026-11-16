#!/bin/bash
# Checks the replication init container with the agent's helpers (docs/rust-agent-roadmap.md)
# against real Firebird tools and servers. init-instance.sh runs as it is (its Perl helpers) and
# as the clusters with the agent run it (the same script with fb-agent's commands in their
# place, as utils/replication.ts derives it), from the same start, and the results are compared:
#
#   - a primary's first start: the database created offline, publication, the bootstrap seed
#   - a replica's first start: the seed fetched from the primary's segment server, turned into
#     a read-only replica, and its control file
#   - the journal floor: the replication sequence written into the header of a real database
#     (the on-disk structures of Firebird 4 and 5, and of 6), read back with gstat
#
#   hack/agent/init.sh [image ...]      (default: Firebird 4, 5 and the 6 snapshot)
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
NAME=${NAME:-fbinit}
work=$(mktemp -d)
chmod 755 "$work"
cleanup() {
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  docker run --rm -v "$work:/w" --entrypoint sh "${images[0]}" -c 'rm -rf /w/*' >/dev/null 2>&1 || true
  rmdir "$work" 2>/dev/null || true
}
trap cleanup EXIT
for script in fetch-seed.pl replica-control.pl set-repl-seq.pl segment-request.pl; do
  awk -v dir="$src" '/^#@include [^ ]+$/ { while ((getline line < (dir "/" $2)) > 0) print line; close(dir "/" $2); next } { print }' \
    "$src/$script" > "$work/$script"
done
cp "$src/pitr-plan.pl" "$src/enable-publication.sql" "$src/init-instance.sh" "$work/"
# the agent's variant, as agentInitScript (operator/src/utils/replication.ts) derives it; the
# unit tests hold that derivation to these replacements
sed -e 's|perl "\$SCRIPT_DIR/pitr-plan.pl" --reposition|"$FB_AGENT" reposition|g' \
    -e 's|perl "\$SCRIPT_DIR/set-repl-seq.pl"|"$FB_AGENT" set-repl-seq|g' \
    -e 's|perl "\$SCRIPT_DIR/replica-control.pl"|"$FB_AGENT" replica-control|g' \
    -e 's|perl "\$SCRIPT_DIR/segment-request.pl"|"$FB_AGENT" request|g' \
    -e 's|perl "\$SCRIPT_DIR/fetch-seed.pl"|"$FB_AGENT" fetch-seed|g' \
    -e 's|^set -eu$|set -eu\necho "replication init with the helpers of $("$FB_AGENT" version)"|' \
    "$src/init-instance.sh" > "$work/init-instance-agent.sh"
if grep -v '^ *#' "$work/init-instance-agent.sh" | grep -qw perl; then echo "the derived script still runs Perl"; exit 1; fi
cp "$agent" "$work/fb-agent"
cat > "$work/env.sh" <<'ENV'
export ISC_USER=SYSDBA ISC_PASSWORD=masterkey PATH=/opt/firebird/bin:$PATH SCRIPT_DIR=/w SEGMENT_PORT=3051
# instance <dir> <pod>: the environment of an instance whose volume is <dir>
instance() {
  export DATA_DIR=$1 DATABASE_PATH=$1/db.fdb REPLICATION_DIR=$1/repl JOURNAL_DIR=$1/repl/journal ARCHIVE_DIR=$1/repl/archive
  export SOURCE_DIR=$1/repl/source STATE_FILE=$1/repl/.last-pulled POD_NAME=$2 PRIMARY_FILE=$1/primary SEED_SOURCES_FILE=$1/seed-sources
  mkdir -p "$1/repl"
}
# init <perl|agent>: the replication init container, with the Perl helpers or the agent's
init() { case $1 in agent) FB_AGENT=/w/fb-agent sh /w/init-instance-agent.sh 2>&1 ;; *) sh /w/init-instance.sh 2>&1 ;; esac; }
rows() { echo "SET HEADING OFF; SELECT COUNT(*) FROM T;" | isql -q "$1" 2>&1 | tr -d ' \n'; }
sequence() { gstat -h "$1" | sed -n 's/^[[:space:]]*Replication sequence:[[:space:]]*\([0-9]*\).*/\1/p'; }
ENV

status=0
for image in "${images[@]}"; do
  echo "### $image"
  docker rm -f "$NAME" >/dev/null 2>&1
  docker run -d --name "$NAME" -v "$work:/w" --entrypoint bash "$image" -c '
    grep -q "^AuthServer = Srp, Legacy_Auth" /opt/firebird/firebird.conf ||
      printf "AuthServer = Srp, Legacy_Auth\nUserManager = Srp, Legacy_UserManager\nWireCrypt = Enabled\n" >> /opt/firebird/firebird.conf
    mkdir -p /data && chown firebird:firebird /data
    sleep infinity' >/dev/null
  x() { docker exec "$NAME" bash -c ". /w/env.sh; $1"; }
  failed=0
  ok() { echo "ok: $1"; }
  bad() { echo "FAILED: $1"; failed=1; }
  # is <what> <command> <expected>
  is() { local got; got=$(x "$2" | tail -1); if [ "$got" = "$3" ]; then ok "$1: $got"; else bad "$1: \"$got\", expected \"$3\""; fi; }
  echo "tools: $(x 'isql -z </dev/null 2>/dev/null | head -n 1')"

  # a primary's first start, each way: no primary named yet, so this instance is it
  for who in perl agent; do
    is "the $who init creates the primary" "instance /data/p-$who db-0; init $who" "created primary database and offline bootstrap seed"
    is "its database is not a replica" "gstat -h /data/p-$who/db.fdb | grep -c 'Attributes.*replica'" "0"
    is "its bootstrap seed is the database" "cmp /data/p-$who/db.fdb /data/p-$who/repl/bootstrap-seed.fdb && echo same" "same"
    is "it publishes" "echo 'SET LIST ON; SELECT RDB\$ACTIVE_FLAG AS A FROM RDB\$PUBLICATIONS;' | isql -q /data/p-$who/db.fdb | awk '\$1 == \"A\" { print \$2 }'" "1"
    is "a second start finds it" "instance /data/p-$who db-0; init $who" "database exists, nothing to initialise"
  done
  is "the agent's init says whose helpers it has" "instance /data/p-agent db-0; init agent | grep -c '^replication init with the helpers of fb-agent [0-9]'" "1"

  # rows in the primary, a fresh offline seed of it, then its server and its segment server
  x 'instance /data/p-perl db-0
     echo "CREATE TABLE T (N INT); COMMIT; INSERT INTO T VALUES (1); INSERT INTO T VALUES (2); COMMIT;" | isql -q $DATABASE_PATH
     cp $DATABASE_PATH $REPLICATION_DIR/bootstrap-seed.fdb; chown -R firebird:firebird /data/p-perl
     su -s /bin/sh firebird -c "/opt/firebird/bin/firebird" >/dev/null 2>&1 &
     sleep 3
     su -s /bin/sh firebird -c ". /w/env.sh; instance /data/p-perl db-0; /w/fb-agent segment-server > /data/server.log 2>&1" &'
  for _ in $(seq 1 40); do x '/w/fb-agent request 127.0.0.1 PING' 2>/dev/null | grep -q '^OK' && break; sleep 1; done

  # a replica's first start, each way: seeded from that primary
  for who in perl agent; do
    x "instance /data/r-$who db-1; echo 127.0.0.1 > \$PRIMARY_FILE"
    is "the $who init seeds a replica" "instance /data/r-$who db-1; init $who" "seeded replica from 127.0.0.1 (offline seed)"
    is "it is a read-only replica" "gstat -h /data/r-$who/db.fdb | grep -c 'Attributes.*replica'" "1"
    is "with the primary's rows" "rows /data/r-$who/db.fdb" "2"
    is "and one control file, named after the database" "ls /data/r-$who/repl/source | grep -c '^{[0-9A-F-]*}\$'" "1"
    is "nothing is left of the work files" "ls -A /data/r-$who | grep -c '^\\.init'" "0"
  done
  is "the two control files are the same" 'cmp /data/r-perl/repl/source/\{*\} /data/r-agent/repl/source/\{*\} && echo same' "same"
  is "the agent's init fetched the seed itself" "instance /data/r-agent2 db-2; echo 127.0.0.1 > \$PRIMARY_FILE; init agent | grep -c '^fetched offline seed copy (.* bytes) from 127.0.0.1'" "1"
  # a seed source that is refused is passed over, as with the Perl helper
  is "a source that does not answer is passed over" "instance /data/r-agent3 db-3; echo 127.0.0.1 > \$PRIMARY_FILE; echo no-such-instance.invalid > \$SEED_SOURCES_FILE; init agent" "seeded replica from 127.0.0.1 (offline seed)"

  # the journal floor: the header of a real database, each way, read back by gstat
  for who in perl agent; do
    x "cp /data/p-agent/db.fdb /data/floor-$who.fdb"
    case $who in agent) set='/w/fb-agent set-repl-seq' ;; *) set='perl /w/set-repl-seq.pl' ;; esac
    is "the $who helper sets the replication sequence" "$set /data/floor-$who.fdb 41 >/dev/null; sequence /data/floor-$who.fdb" "41"
    is "and sets it again" "$set /data/floor-$who.fdb 4660 >/dev/null; sequence /data/floor-$who.fdb" "4660"
  done
  is "the two databases are the same" 'cmp /data/floor-perl.fdb /data/floor-agent.fdb && echo same' "same"
  # and through the script: a primary whose header fell behind the floor
  for who in perl agent; do
    x "instance /data/p-agent db-0; echo 77 > \$REPLICATION_DIR/journal-floor; rm -f \$REPLICATION_DIR/bootstrap-seed.fdb; cp /data/floor-perl.fdb /data/keep.fdb"
    is "the $who init raises the header to the journal floor" "instance /data/p-agent db-0; cp /data/p-perl/repl/bootstrap-seed.fdb \$DATABASE_PATH; init $who >/dev/null; sequence \$DATABASE_PATH" "77"
  done
  if [ "$failed" = 1 ]; then echo "### $image: FAILED"; x 'tail -15 /data/server.log'; status=1; else echo "### $image: passed"; fi
done
exit $status
