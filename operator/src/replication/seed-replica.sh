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
# The copy's replication sequence S is the last primary segment it contains. A commit can
# be journaled in S while its pages went to the nbackup delta after the lock (missing from
# the copy), so the replica must replay S rather than skip it.
header=$(gstat -h "$work")
seq=$(echo "$header" | sed -n 's/^[[:space:]]*Replication sequence:[[:space:]]*\([0-9]*\).*/\1/p')
guid=$(echo "$header" | sed -n 's/^[[:space:]]*Database GUID:[[:space:]]*\({[0-9A-F-]*}\).*/\1/p')
# The copy inherits "publication enabled"; a replica must not journal the changes it applies,
# or its own sequence moves and the replica server treats it as a replaced database and
# fast-forwards past every segment. (This DDL is itself journaled and bumps the header.)
echo "ALTER DATABASE DISABLE PUBLICATION; COMMIT;" | isql -q "$work"
gfix -replica read_only "$work"
dbseq=$(gstat -h "$work" | sed -n 's/^[[:space:]]*Replication sequence:[[:space:]]*\([0-9]*\).*/\1/p')
if [ -z "$seq" ] || [ "$seq" -eq 0 ] || [ -z "$guid" ] || [ -z "$dbseq" ]; then
  echo "cannot read replication sequence/GUID from the seed copy" >&2
  exit 1
fi
# Replica control file (ReplServer.cpp ControlFile::DataV1): "applied through S-1" so segment
# S is replayed (row changes re-apply idempotently), and db_sequence = the copy's current
# header value so the server does not fast-forward.
perl -e 'print pack("a10 v V Q< V x4 Q<", "FBREPLCTL", 1, 0, $ARGV[0] - 1, 0, $ARGV[1])' "$seq" "$dbseq" > "$SOURCE_DIR/$guid"
echo "replica will replay from primary segment $seq (local sequence $dbseq)"
chown -R firebird:firebird "$DATA_DIR"
mv "$work" "$DATABASE_PATH"
echo "seeded replica from $primary"
