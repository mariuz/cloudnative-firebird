#!/bin/sh
# Commit/TIP window (ISSUES.md, issue 2): takes level-0 backups (action_nbak, as the operator's
# physical backups) of a publishing database under concurrent commits, restores each with its
# replication sequence S, and looks in the archived journal for transactions committed in
# segments <= S that the copy does not hold as committed. Each one is the window: the commit was
# journaled before the backup lock, its TIP state written after it.
#
#   hack/repro/tip-window.sh            # 20 backups, 8 writers with persistent connections
#   BACKUPS=50 WRITERS=16 hack/repro/tip-window.sh
#
# Persistent connections: per-commit connections hang a publishing database (issue 1).
set -eu
cd "$(dirname "$0")"
. ./common.sh
BACKUPS=${BACKUPS:-20}
WRITERS=${WRITERS:-8}
work=$(mktemp -d)
trap 'docker rm -f tipw >/dev/null 2>&1; rm -rf "$work"' EXIT
start_server tipw "$work" 1
cp tip-window.pl "$work/data/tip-window.pl"
CONNECTIONS=persistent start_writers tipw "$WRITERS" 1000000
sleep 5
hits=0 open_total=0
i=1
while [ "$i" -le "$BACKUPS" ]; do
  docker exec tipw fbsvcmgr localhost:service_mgr action_nbak dbname "$DB" \
    nbk_file "/var/lib/firebird/data/c.nbk" nbk_level 0 >/dev/null
  # the lock switched the journal: segment S is archived shortly after
  out=$(docker exec tipw sh -c '
    cd /var/lib/firebird/data && rm -f /tmp/c.fdb && nbackup -SEQ -R /tmp/c.fdb c.nbk >/dev/null
    h=$(gstat -h /tmp/c.fdb)
    f() { echo "$h" | sed -n "s/^[[:space:]]*$1:*[[:space:]]*\([0-9][0-9]*\).*/\1/p"; }
    S=$(f "Replication sequence"); oat=$(f "Oldest active"); next=$(f "Next transaction")
    printf "%s\n" "SET TERM ^;" "EXECUTE BLOCK RETURNS (t BIGINT) AS BEGIN t = $oat; WHILE (t < $next) DO BEGIN IF (COALESCE(RDB\$GET_TRANSACTION_CN(t), 0) <= 0) THEN SUSPEND; t = t + 1; END END^" |
      isql -q -user SYSDBA /tmp/c.fdb | awk "\$1 ~ /^[0-9]+\$/ { print \$1 }" > /tmp/cand
    for n in $(seq 1 30); do ls replication/archive | grep -q "journal-0*$S\$" && break; sleep 1; done
    echo "S=$S $(perl tip-window.pl replication/archive "$S" "$oat" "$next" /tmp/cand)"
    rm -f c.nbk /tmp/c.fdb')
  echo "backup $i: $out"
  n=$(echo "$out" | sed -n 's/.*window \([0-9]*\).*/\1/p')
  o=$(echo "$out" | sed -n 's/.* open \([0-9]*\).*/\1/p')
  hits=$((hits + ${n:-0})); open_total=$((open_total + ${o:-0}))
  i=$((i + 1))
done
echo "rows committed: $(sql tipw 'set list on; select count(*) as n from app;' | awk '/^N /{print $2}')"
echo "$BACKUPS backups: $hits transaction(s) in the commit/TIP window, $open_total open in the copies with journal blocks before the backup"
