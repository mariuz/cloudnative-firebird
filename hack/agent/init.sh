#!/bin/bash
# Checks the replication init container (init-instance.sh, with the agent's helpers:
# docs/rust-agent-roadmap.md) against real Firebird tools and servers:
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
cp "$src/enable-publication.sql" "$src/init-instance.sh" "$work/"
cp "$agent" "$work/fb-agent"
cat > "$work/env.sh" <<'ENV'
export ISC_USER=SYSDBA ISC_PASSWORD=masterkey PATH=/opt/firebird/bin:$PATH SCRIPT_DIR=/w SEGMENT_PORT=3051
# instance <dir> <pod>: the environment of an instance whose volume is <dir>
instance() {
  export DATA_DIR=$1 DATABASE_PATH=$1/db.fdb REPLICATION_DIR=$1/repl JOURNAL_DIR=$1/repl/journal ARCHIVE_DIR=$1/repl/archive
  export SOURCE_DIR=$1/repl/source STATE_FILE=$1/repl/.last-pulled POD_NAME=$2 PRIMARY_FILE=$1/primary SEED_SOURCES_FILE=$1/seed-sources
  mkdir -p "$1/repl"
}
# init: the replication init container, as the instance pods run it
init() { FB_AGENT=/w/fb-agent sh /w/init-instance.sh 2>&1; }
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

  # a primary's first start: no primary named yet, so this instance is it (two of them: one
  # serves the replicas below, the other's header is raised to the journal floor)
  for p in p q; do
    is "the init creates the primary" "instance /data/$p db-0; init" "created primary database and offline bootstrap seed"
    is "its database is not a replica" "gstat -h /data/$p/db.fdb | grep -c 'Attributes.*replica'" "0"
    is "its bootstrap seed is the database" "cmp /data/$p/db.fdb /data/$p/repl/bootstrap-seed.fdb && echo same" "same"
    is "it publishes" "echo 'SET LIST ON; SELECT RDB\$ACTIVE_FLAG AS A FROM RDB\$PUBLICATIONS;' | isql -q /data/$p/db.fdb | awk '\$1 == \"A\" { print \$2 }'" "1"
    is "a second start finds it" "instance /data/$p db-0; init" "database exists, nothing to initialise"
  done
  is "the init says whose helpers it has" "instance /data/p db-0; init | grep -c '^replication init with the helpers of fb-agent [0-9]'" "1"
  is "the init runs no Perl" "grep -v '^ *#' /w/init-instance.sh | grep -cw perl" "0"

  # rows in the primary, a fresh offline seed of it, then its server and its segment server
  x 'instance /data/p db-0
     echo "CREATE TABLE T (N INT); COMMIT; INSERT INTO T VALUES (1); INSERT INTO T VALUES (2); COMMIT;" | isql -q $DATABASE_PATH
     cp $DATABASE_PATH $REPLICATION_DIR/bootstrap-seed.fdb; chown -R firebird:firebird /data/p
     su -s /bin/sh firebird -c "/opt/firebird/bin/firebird" >/dev/null 2>&1 &
     sleep 3
     su -s /bin/sh firebird -c ". /w/env.sh; instance /data/p db-0; /w/fb-agent segment-server > /data/server.log 2>&1" &'
  for _ in $(seq 1 40); do x '/w/fb-agent request 127.0.0.1 PING' 2>/dev/null | grep -q '^OK' && break; sleep 1; done

  # a replica's first start: seeded from that primary
  x "instance /data/r db-1; echo 127.0.0.1 > \$PRIMARY_FILE"
  is "the init seeds a replica" "instance /data/r db-1; init" "seeded replica from 127.0.0.1 (offline seed)"
  is "it is a read-only replica" "gstat -h /data/r/db.fdb | grep -c 'Attributes.*replica'" "1"
  is "with the primary's rows" "rows /data/r/db.fdb" "2"
  is "and one control file, named after the database" "ls /data/r/repl/source | grep -c '^{[0-9A-F-]*}\$'" "1"
  # a replica control file (ControlFile::DataV1): the magic, and at least its 40 bytes of header
  is "the control file" "f=\$(ls /data/r/repl/source/\{*\}); echo \$(head -c 9 \"\$f\") \$([ \$(wc -c < \"\$f\") -ge 40 ] && echo whole)" "FBREPLCTL whole"
  is "nothing is left of the work files" "ls -A /data/r | grep -c '^\\.init'" "0"
  is "the init fetched the seed itself" "instance /data/r2 db-2; echo 127.0.0.1 > \$PRIMARY_FILE; init | grep -c '^fetched offline seed copy (.* bytes) from 127.0.0.1'" "1"
  # a seed source that is refused is passed over
  is "a source that does not answer is passed over" "instance /data/r3 db-3; echo 127.0.0.1 > \$PRIMARY_FILE; echo no-such-instance.invalid > \$SEED_SOURCES_FILE; init" "seeded replica from 127.0.0.1 (offline seed)"

  # the journal floor: the header of a real database, read back by gstat
  x "cp /data/q/db.fdb /data/floor.fdb"
  is "set-repl-seq sets the replication sequence" "/w/fb-agent set-repl-seq /data/floor.fdb 41 >/dev/null; sequence /data/floor.fdb" "41"
  is "and sets it again" "/w/fb-agent set-repl-seq /data/floor.fdb 4660 >/dev/null; sequence /data/floor.fdb" "4660"
  # and through the script: a primary whose header fell behind the floor
  x "instance /data/q db-0; echo 77 > \$REPLICATION_DIR/journal-floor; rm -f \$REPLICATION_DIR/bootstrap-seed.fdb"
  is "the init raises the header to the journal floor" "instance /data/q db-0; cp /data/p/repl/bootstrap-seed.fdb \$DATABASE_PATH; init >/dev/null; sequence \$DATABASE_PATH" "77"
  if [ "$failed" = 1 ]; then echo "### $image: FAILED"; x 'tail -15 /data/server.log'; status=1; else echo "### $image: passed"; fi
done
exit $status
