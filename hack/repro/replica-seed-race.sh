#!/bin/sh
# Issue 2 (ISSUES.md, suspected, not reproduced): a replica created with Firebird's documented procedure (nbackup lock,
# file copy, unlock, "nbackup -SEQ -F", "gfix -replica read_only") while clients commit can
# silently miss a transaction committed at the moment of the lock.
#
# TRA_commit (src/jrd/tra.cpp) journals the commit (REPL_trans_commit) before it marks the
# transaction committed in the TIP (TRA_set_state). A lock landing between the two leaves the
# commit in replication segment S while the TIP change goes to the delta file: the copy has the
# transaction uncommitted, and the replica skips segment S because its header says S is included.
#
#   ./replica-seed-race.sh                                  # prints rows missing or different on the replica
#   WRITERS=8 CONNECTIONS=persistent ROWS=3000 ./replica-seed-race.sh # heavier load (persistent: avoids issue 1)
#   LOCK_FROM=sidecar ./replica-seed-race.sh                # take the lock from another container
set -u
. "$(dirname "$0")/common.sh"
work=$(mktemp -d)
start_server fb-race-primary "$work/p" 1
# WRITERS concurrent writers (default 1), each committing insert+update with no pause
WRITERS=${WRITERS:-1}
ROWS=${ROWS:-150}
for w in $(seq 1 "$WRITERS"); do
  if [ "${CONNECTIONS:-per-commit}" = persistent ]; then
    # one connection per writer (a connection pool): per-commit connections hit issue 1 under load
    docker exec -d fb-race-primary sh -c "for i in \$(seq ${w}0000 \$((${w}0000 + $ROWS))); do echo \"insert into app values (\$i, 'w'); update app set note = 'u' where id = \$i - 1; commit;\"; done | isql -q localhost:$DB; touch /tmp/done$w"
  else
    docker exec -d fb-race-primary sh -c "for i in \$(seq ${w}0000 \$((${w}0000 + $ROWS))); do echo \"insert into app values (\$i, 'w'); update app set note = 'u' where id = \$i - 1; commit;\" | isql -q localhost:$DB; done; touch /tmp/done$w"
  fi
done
sleep 5

# the documented replica creation, taken while the writer commits
if [ "${LOCK_FROM:-server}" = sidecar ]; then
  # nbackup from another container (independent embedded engine and lock table)
  docker run --rm -e ISC_USER=SYSDBA -e ISC_PASSWORD=pw -v "$work/p/data:/var/lib/firebird/data" \
    --entrypoint sh "$IMAGE" -c "nbackup -L $DB && cp $DB /var/lib/firebird/data/copy.fdb && nbackup -N $DB"
else
  docker exec fb-race-primary sh -c "nbackup -L $DB && cp $DB /var/lib/firebird/data/copy.fdb && nbackup -N $DB"
fi
mkdir -p "$work/r/data/replication/source"
mv "$work/p/data/copy.fdb" "$work/r/data/mydb.fdb"
docker run --rm -v "$work/r/data:/var/lib/firebird/data" --entrypoint sh "$IMAGE" -c "set -e
  nbackup -SEQ -F $DB
  echo 'ALTER DATABASE DISABLE PUBLICATION; COMMIT;' | isql -q -b $DB
  gfix -replica read_only $DB
  chown -R firebird:firebird /var/lib/firebird/data" || { echo "replica preparation failed"; exit 1; }
cat > "$work/r/replication.conf" <<CONF
database
{
}
database = $DB
{
    journal_source_directory = /var/lib/firebird/data/replication/source
    apply_idle_timeout = 2
}
CONF
docker rm -f fb-race-replica >/dev/null 2>&1
docker run -d --name fb-race-replica -e FIREBIRD_ROOT_PASSWORD=pw -e ISC_USER=SYSDBA -e ISC_PASSWORD=pw \
  -e FIREBIRD_DATABASE=mydb.fdb -v "$work/r/data:/var/lib/firebird/data" \
  -v "$work/r/replication.conf:/opt/firebird/replication.conf:ro" "$IMAGE" >/dev/null

# ship archived segments to the replica until the writer is done, then let it catch up
until docker exec fb-race-primary sh -c "ls /tmp/done* 2>/dev/null | wc -l | grep -qx $WRITERS"; do
  cp -n "$work"/p/data/replication/archive/* "$work/r/data/replication/source/" 2>/dev/null; sleep 2
done
sleep 8; cp -n "$work"/p/data/replication/archive/* "$work/r/data/replication/source/" 2>/dev/null; sleep 15

ids() { docker exec "$1" sh -c "echo 'select id, note from app order by id;' | isql -q localhost:$DB" | awk '$1 ~ /^[0-9]+$/ { print $1 "/" $2 }' | sort; }
ids fb-race-primary > "$work/primary.ids"
ids fb-race-replica > "$work/replica.ids"
missing=$(comm -23 "$work/primary.ids" "$work/replica.ids" | tr '\n' ' ')
echo "primary rows: $(wc -l < "$work/primary.ids"), replica rows: $(wc -l < "$work/replica.ids")"
echo "rows missing or different on the replica: ${missing:-none}"
[ -n "${KEEP:-}" ] || docker rm -f fb-race-primary fb-race-replica >/dev/null
