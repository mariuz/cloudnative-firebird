#!/bin/sh
# Firebird synchronous replication (sync_replica), the behaviour the operator's mode sync relies
# on (README, Synchronous Replication):
#
#   1. strict mode (report_errors = true, disable_on_error = false): a commit is applied on the
#      replica before it returns; with the replica down the write fails and is not committed; once
#      the replica is back, writes succeed again;
#   2. a replica that also applies the journal gets every change twice;
#   3. sync_replica can live in a file replication.conf includes, and Firebird reads it again when
#      the database is opened after a full shutdown (no server restart).
#
#   hack/repro/sync-replica.sh
#   IMAGE=firebirdsql/firebird:4 hack/repro/sync-replica.sh
set -eu
IMAGE=${IMAGE:-firebirdsql/firebird:5}
work=$(mktemp -d)
net=fbsync-repro
cleanup() { docker rm -f syncp syncr >/dev/null 2>&1; docker network rm "$net" >/dev/null 2>&1; rm -rf "$work"; }
trap cleanup EXIT
cleanup
mkdir -p "$work"
docker network create "$net" >/dev/null

cat > "$work/env.sh" <<'EOF'
export ISC_USER=SYSDBA ISC_PASSWORD=masterkey PATH=/opt/firebird/bin:$PATH
sec=$(ls /opt/firebird/security*.fdb | head -1)
echo "create or alter user SYSDBA password 'masterkey' using plugin Srp; commit;" | isql -q -user SYSDBA "$sec" >/dev/null
mkdir -p /data/journal /data/archive /data/source
EOF

# replica: waits for the seed, makes it a read-only replica (journal_source_directory /s/feed)
cat > "$work/replica.sh" <<'EOF'
. /s/env.sh
while [ ! -f /s/seed.ready ]; do sleep 1; done
if [ ! -f /data/db.fdb ]; then cp /s/seed.fdb /data/db.fdb; gfix -replica read_only /data/db.fdb; fi
mkdir -p /s/feed
printf 'database = /data/db.fdb\n{\n  journal_source_directory = /s/feed\n  apply_idle_timeout = 1\n}\n' > /opt/firebird/replication.conf
chown -R firebird /data /s/feed 2>/dev/null || true
exec firebird
EOF

cat > "$work/primary.sh" <<'EOF'
. /s/env.sh
echo "create database '/data/db.fdb'; create table t(i int); commit;" | isql -q
echo "alter database enable publication; alter database include all to publication; commit;" | isql -q /data/db.fdb
cp /data/db.fdb /s/seed.fdb; touch /s/seed.ready
: > /data/sync.conf
cat > /opt/firebird/replication.conf <<CONF
database = /data/db.fdb
{
  report_errors = true
  disable_on_error = false
  journal_directory = /data/journal
  journal_archive_directory = /data/archive
  journal_archive_command = "cp \$(pathname) \$(archivepathname)"
  journal_archive_timeout = 2
  include /data/sync.conf
}
CONF
exec firebird
EOF

docker run -d --name syncr --network "$net" -v "$work:/s" --entrypoint sh "$IMAGE" /s/replica.sh >/dev/null
docker run -d --name syncp --network "$net" -v "$work:/s" --entrypoint sh "$IMAGE" /s/primary.sh >/dev/null
sleep 15
q() { docker exec "$1" sh -c "echo \"$2\" | ISC_USER=SYSDBA ISC_PASSWORD=masterkey /opt/firebird/bin/isql -q localhost:/data/db.fdb" 2>&1; }
cnt() { q "$1" "set list on; select count(*) as c from t;" | awk '/^C /{print $2}'; }

echo "--- 3. sync_replica in an included file, read when the database is opened again"
q syncp "insert into t values (1); commit;" >/dev/null
docker exec syncp sh -c 'printf "sync_replica = syncr:/data/db.fdb\n{\n  username = SYSDBA\n  password_env = ISC_PASSWORD\n}\n" > /data/sync.conf'
q syncp "insert into t values (2); commit;" >/dev/null
echo "file written, database still open: primary $(cnt syncp) rows, replica $(cnt syncr) (nothing yet)"
docker exec syncp sh -c 'export ISC_USER=SYSDBA ISC_PASSWORD=masterkey
  fbsvcmgr localhost:service_mgr action_properties dbname /data/db.fdb prp_shutdown_mode prp_sm_full prp_force_shutdown 0
  fbsvcmgr localhost:service_mgr action_properties dbname /data/db.fdb prp_online_mode prp_sm_normal'
q syncp "insert into t values (3); commit;" >/dev/null
echo "after full shutdown and online: primary $(cnt syncp), replica $(cnt syncr) (row 3 only: replicated synchronously)"

echo "--- 1. strict synchronous replication"
q syncp "insert into t values (4); commit;" >/dev/null
echo "right after the commit: replica has $(q syncr 'set list on; select count(*) as c from t where i = 4;' | awk '/^C /{print $2}') row 4"
docker stop syncr >/dev/null
echo "replica stopped; a write on the primary:"
q syncp "insert into t values (5); commit;" | head -3
docker start syncr >/dev/null
sleep 10
q syncp "insert into t values (6); commit;" >/dev/null
echo "replica back: primary rows $(q syncp 'set list on; select list(i) as l from t;' | awk '/^L /{print $2}'), replica $(q syncr 'set list on; select list(i) as l from t;' | awk '/^L /{print $2}')"

echo "--- 2. the same replica fed the journal as well"
sleep 5
docker exec syncp sh -c 'cp /data/archive/* /s/feed/ && chown -R firebird /s/feed 2>/dev/null || true'
sleep 10
echo "primary $(cnt syncp) rows, replica $(cnt syncr) rows (changes applied twice)"
