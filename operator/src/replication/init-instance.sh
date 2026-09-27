#!/bin/sh
# Replication init container for every instance.
#
# Primary, first start: create the database offline (no server running, nothing attached),
#   enable publication, run bootstrap.initSql, and keep a plain file copy as the offline
#   bootstrap seed for the first replicas. No nbackup lock is taken on the primary.
# Replica, empty volume: fetch a seed copy (from a ready replica first, then from the primary),
#   turn it into a read-only replica and write its replica control file.
# Existing database: nothing to do.
#
# Seeds avoid locking the primary: see ISSUES.md, issue 2.
set -eu
mkdir -p "$JOURNAL_DIR" "$ARCHIVE_DIR" "$SOURCE_DIR"
chown firebird:firebird "$JOURNAL_DIR" "$ARCHIVE_DIR" "$SOURCE_DIR"
if [ -f "$DATABASE_PATH" ]; then
  echo "database exists, nothing to initialise"
  exit 0
fi

primary=$(cat "$PRIMARY_FILE" 2>/dev/null || true)
# gstat omits "Replication sequence" while it is 0 (e.g. a database created offline)
seq_of() { s=$(gstat -h "$1" | sed -n 's/^[[:space:]]*Replication sequence:[[:space:]]*\([0-9]*\).*/\1/p'); echo "${s:-0}"; }
field() { echo "$header" | sed -n "s/^[[:space:]]*$1:*[[:space:]]*\\([0-9{][0-9A-F{}-]*\\).*/\\1/p"; }

# Everything happens on a work file that is moved into place only once it is complete, so a
# failed attempt is retried from scratch rather than treated as initialised. The work path is
# not the replication.conf database, so these offline steps are not journaled.
work="$DATA_DIR/.init.fdb"
rm -f "$work" "$work.ctl" "$work.kind"

case "$primary" in
  ""|"$POD_NAME"|"$POD_NAME".*)
    echo "CREATE DATABASE '$work'; COMMIT;" | isql -q
    isql -q -i "$SCRIPT_DIR/enable-publication.sql" "$work"
    if [ -f "$SCRIPT_DIR/init.sql" ]; then
      isql -q -i "$SCRIPT_DIR/init.sql" "$work"
    fi
    # offline copy: consistent because nothing is attached
    cp "$work" "$REPLICATION_DIR/bootstrap-seed.fdb.tmp"
    mv "$REPLICATION_DIR/bootstrap-seed.fdb.tmp" "$REPLICATION_DIR/bootstrap-seed.fdb"
    chown -R firebird:firebird "$DATA_DIR"
    mv "$work" "$DATABASE_PATH"
    echo "created primary database and offline bootstrap seed"
    exit 0
    ;;
esac

# Replica: ready replicas first (a replica does not publish, so locking it is safe), then the
# primary (its offline bootstrap seed, or a live copy only when explicitly allowed).
until
  fetched=""
  for source in $(cat "$SEED_SOURCES_FILE" 2>/dev/null || true) "$primary"; do
    case "$source" in "$POD_NAME"|"$POD_NAME".*) continue ;; esac
    if perl "$SCRIPT_DIR/fetch-seed.pl" "$source" "$work"; then fetched="$source"; break; fi
  done
  [ -n "$fetched" ]
do
  echo "no seed source available yet; retrying"
  sleep 10
done
kind=$(cat "$work.kind")

case "$kind" in
  replica)
    # a locked copy of a replica: already a read-only replica without publication
    nbackup -SEQ -F "$work"
    perl "$SCRIPT_DIR/replica-control.pl" --adopt "$work.ctl" "$(seq_of "$work")" "$SOURCE_DIR/.control.tmp"
    ;;
  offline|live)
    [ "$kind" = live ] && nbackup -SEQ -F "$work"
    header=$(gstat -h "$work")
    seq=$(field "Replication sequence")
    seq=${seq:-0}
    guid=$(field "Database GUID")
    oat=$(field "Oldest active")
    next=$(field "Next transaction")
    if [ -z "$guid" ] || [ -z "$oat" ] || [ -z "$next" ]; then
      echo "cannot read the GUID or transaction counters from the seed copy" >&2
      exit 1
    fi
    candidates=""
    if [ "$kind" = live ]; then
      # Transactions not committed in the copy: open at lock time, or committed on the primary
      # with the commit journaled before the lock but the commit mark written after it.
      candidates=$(printf '%s\n' 'SET TERM ^;' \
        "EXECUTE BLOCK RETURNS (t BIGINT) AS BEGIN t = $oat; WHILE (t < $next) DO BEGIN
           IF (COALESCE(RDB\$GET_TRANSACTION_CN(t), 0) <= 0) THEN SUSPEND; t = t + 1; END END^" |
        isql -q "$work" | awk '$1 ~ /^[0-9]+$/ { print $1 }' | tr '\n' ' ')
    fi
    # a replica must not journal the changes it applies
    echo "ALTER DATABASE DISABLE PUBLICATION; COMMIT;" | isql -q "$work"
    gfix -replica read_only "$work"
    # shellcheck disable=SC2086 # candidates is a space-separated id list
    perl "$SCRIPT_DIR/replica-control.pl" "$primary" "$seq" "$(seq_of "$work")" "$SOURCE_DIR/.control.tmp" $candidates
    ;;
esac

# the control file is named after the database GUID (the primary's, inherited by every copy)
guid=$(gstat -h "$work" | sed -n 's/^[[:space:]]*Database GUID:[[:space:]]*\({[0-9A-F-]*}\).*/\1/p')
mv "$SOURCE_DIR/.control.tmp" "$SOURCE_DIR/$guid"
chown -R firebird:firebird "$DATA_DIR"
rm -f "$work.ctl" "$work.kind"
mv "$work" "$DATABASE_PATH"
echo "seeded replica from $fetched ($kind seed)"
