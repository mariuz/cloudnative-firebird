#!/bin/sh
# Init step for replica instances: seeds an empty data directory with a physical copy of
# the primary (taken under nbackup lock, fixed up with -SEQ to keep the primary's
# replication sequence), then switches the copy to read-only replica mode.
set -eu
mkdir -p "$JOURNAL_DIR" "$ARCHIVE_DIR" "$SOURCE_DIR"
chown firebird:firebird "$JOURNAL_DIR" "$ARCHIVE_DIR" "$SOURCE_DIR"
if [ -f "$DATABASE_PATH" ]; then
  echo "database exists, nothing to seed"
  exit 0
fi
primary=$(cat "$PRIMARY_FILE" 2>/dev/null || true)
case "$primary" in
  ""|"$POD_NAME"|"$POD_NAME".*) echo "this instance is the primary; the entrypoint creates the database"; exit 0 ;;
esac
# Everything happens on a work file that is moved into place only once it is a complete
# replica, so a failed attempt is retried from scratch rather than treated as seeded.
work="$DATA_DIR/.seed.fdb"
rm -f "$work"
until perl "$SCRIPT_DIR/fetch-seed.pl" "$primary" "$work"; do
  echo "waiting for primary $primary to serve a seed copy..."
  sleep 5
done
# the copy was taken while the primary was locked: fix it up, keeping the primary's
# replication sequence
nbackup -SEQ -F "$work"
header=$(gstat -h "$work")
field() { echo "$header" | sed -n "s/^[[:space:]]*$1:*[[:space:]]*\\([0-9{][0-9A-F{}-]*\\).*/\\1/p"; }
seq=$(field "Replication sequence")
guid=$(field "Database GUID")
oat=$(field "Oldest active")
next=$(field "Next transaction")
if [ -z "$seq" ] || [ "$seq" -eq 0 ] || [ -z "$guid" ] || [ -z "$oat" ] || [ -z "$next" ]; then
  echo "cannot read replication sequence, GUID or transaction counters from the seed copy" >&2
  exit 1
fi
# Transactions not committed in the copy: open at lock time, or committed on the primary with
# the commit journaled before the lock but the commit mark written after it.
candidates=$(printf '%s\n' 'SET TERM ^;' \
  "EXECUTE BLOCK RETURNS (t BIGINT) AS BEGIN t = $oat; WHILE (t < $next) DO BEGIN
     IF (COALESCE(RDB\$GET_TRANSACTION_CN(t), 0) <= 0) THEN SUSPEND; t = t + 1; END END^" |
  isql -q "$work" | awk '$1 ~ /^[0-9]+$/ { print $1 }' | tr '\n' ' ')
# The copy inherits "publication enabled"; a replica must not journal the changes it applies,
# or its own sequence moves and the replica server treats it as a replaced database and
# fast-forwards past every segment. (This DDL is itself journaled and bumps the header.)
echo "ALTER DATABASE DISABLE PUBLICATION; COMMIT;" | isql -q "$work"
gfix -replica read_only "$work"
dbseq=$(gstat -h "$work" | sed -n 's/^[[:space:]]*Replication sequence:[[:space:]]*\([0-9]*\).*/\1/p')
# shellcheck disable=SC2086 # candidates is a space-separated id list
perl "$SCRIPT_DIR/replica-control.pl" "$primary" "$seq" "$dbseq" "$SOURCE_DIR/$guid" $candidates
chown -R firebird:firebird "$DATA_DIR"
mv "$work" "$DATABASE_PATH"
echo "seeded replica from $primary"
