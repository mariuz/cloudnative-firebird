#!/bin/sh
# Replication init container for every instance.
#
# Primary, first start: create the database offline (no server running, nothing attached) and
#   run bootstrap.initSql, or take the database restored or cloned by the bootstrap init
#   container ($DATA_DIR/.bootstrap.fdb); enable publication, and keep a plain file copy as the
#   offline bootstrap seed for the first replicas. No nbackup lock is taken on the primary.
# Replica, empty volume: fetch a seed copy (from a ready replica first, then from the primary),
#   turn it into a read-only replica and write its replica control file.
# Existing database: nothing to do.
#
# Seeds avoid locking the primary: see ISSUES.md, issue 2.
set -eu
mkdir -p "$JOURNAL_DIR" "$ARCHIVE_DIR" "$SOURCE_DIR"
chown firebird:firebird "$JOURNAL_DIR" "$ARCHIVE_DIR" "$SOURCE_DIR"
primary=$(cat "$PRIMARY_FILE" 2>/dev/null || true)

# gstat omits "Replication sequence" while it is 0 (e.g. a database created offline)
seq_of() { s=$(gstat -h "$1" | sed -n 's/^[[:space:]]*Replication sequence:[[:space:]]*\([0-9]*\).*/\1/p'); echo "${s:-0}"; }
field() { echo "$header" | sed -n "s/^[[:space:]]*$1:*[[:space:]]*\\([0-9{][0-9A-F{}-]*\\).*/\\1/p"; }
# token of an operator directive ("<pod> <token>" lines) for this pod, if not applied yet
pending() { t=$(awk -v p="$POD_NAME" '$1 == p { print $2 }' "$1" 2>/dev/null || true); [ -n "$t" ] && [ "$(cat "$2" 2>/dev/null || true)" != "$t" ] && echo "$t" || true; }
wipe_replication_state() {
  find "$SOURCE_DIR" "$JOURNAL_DIR" "$ARCHIVE_DIR" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
  rm -f "$STATE_FILE"
}

# Planned switchover (targetPrimary annotation). The operator stops writes on the old primary,
# waits until every ready replica has applied its last segment, then restarts the target with a
# "promote" and the old primary with a "demote" directive. Both are applied offline to a work copy
# (not journaled), can be resumed after a crash, and are recorded once complete.
sw="$DATA_DIR/.switchover.fdb"
promote_token=$(pending "${PROMOTE_FILE:-/dev/null}" "$REPLICATION_DIR/.promoted")
if [ -n "$promote_token" ] && { [ -f "$DATABASE_PATH" ] || [ -f "$sw" ]; }; then
  # the journal continues after the last segment this replica applied (its control file position)
  if [ ! -f "$REPLICATION_DIR/.promote-seq" ]; then
    ctl=$(find "$SOURCE_DIR" -maxdepth 1 -name '{*}' | head -n 1)
    seq=0
    if [ -n "$ctl" ]; then seq=$(od -An -tu8 -j16 -N8 "$ctl" | tr -d ' '); fi
    echo "$seq" > "$REPLICATION_DIR/.promote-seq"
  fi
  seq=$(cat "$REPLICATION_DIR/.promote-seq")
  echo "promoting this replica to primary; its journal continues after segment $seq"
  if [ -f "$DATABASE_PATH" ]; then mv "$DATABASE_PATH" "$sw"; fi
  perl "$SCRIPT_DIR/set-repl-seq.pl" "$sw" "$seq"
  gfix -replica none "$sw"
  isql -q -i "$SCRIPT_DIR/enable-publication.sql" "$sw"
  wipe_replication_state
  # offline copy: a consistent bootstrap seed for new replicas
  cp "$sw" "$REPLICATION_DIR/bootstrap-seed.fdb.tmp"
  mv "$REPLICATION_DIR/bootstrap-seed.fdb.tmp" "$REPLICATION_DIR/bootstrap-seed.fdb"
  chown -R firebird:firebird "$DATA_DIR"
  mv "$sw" "$DATABASE_PATH"
  echo "$promote_token" > "$REPLICATION_DIR/.promoted"
  rm -f "$REPLICATION_DIR/.promote-seq"
  echo "promoted to primary"
fi
demote_token=$(pending "${DEMOTE_FILE:-/dev/null}" "$REPLICATION_DIR/.demoted")
if [ -n "$demote_token" ] && { [ -f "$DATABASE_PATH" ] || [ -f "$sw" ]; }; then
  echo "demoting the former primary to a read-only replica"
  if [ -f "$DATABASE_PATH" ]; then mv "$DATABASE_PATH" "$sw"; fi
  # the switchover shut it down to stop writes
  gfix -online "$sw" 2>/dev/null || true
  echo "ALTER DATABASE DISABLE PUBLICATION; COMMIT;" | isql -q "$sw"
  gfix -replica read_only "$sw"
  wipe_replication_state
  rm -f "$REPLICATION_DIR/bootstrap-seed.fdb"
  # its own last segment is where the new primary's journal continues
  seq=$(seq_of "$sw")
  guid=$(gstat -h "$sw" | sed -n 's/^[[:space:]]*Database GUID:[[:space:]]*\({[0-9A-F-]*}\).*/\1/p')
  perl "$SCRIPT_DIR/replica-control.pl" none "$seq" "$seq" "$SOURCE_DIR/.control.tmp"
  mv "$SOURCE_DIR/.control.tmp" "$SOURCE_DIR/$guid"
  chown -R firebird:firebird "$DATA_DIR"
  mv "$sw" "$DATABASE_PATH"
  echo "$demote_token" > "$REPLICATION_DIR/.demoted"
  echo "demoted to replica after segment $seq"
fi

# Re-seed request (reseed pod annotation): the operator lists "<pod> <token>" in RESEED_FILE and
# deletes the pod. A replica discards its database and replication state (not its security
# database) and is seeded again; the token is recorded only after a complete seed, so an
# interrupted attempt starts over. The primary never discards its database.
reseed_token=$(awk -v p="$POD_NAME" '$1 == p { print $2 }' "${RESEED_FILE:-/dev/null}" 2>/dev/null || true)
if [ -n "$reseed_token" ] && [ "$(cat "$REPLICATION_DIR/.reseeded" 2>/dev/null || true)" != "$reseed_token" ]; then
  case "$primary" in
    ""|"$POD_NAME"|"$POD_NAME".*)
      echo "ignoring the re-seed request: this instance is the primary"
      reseed_token=""
      ;;
    *)
      echo "re-seed requested: discarding the database and replication state"
      rm -f "$DATABASE_PATH" "$STATE_FILE"
      find "$SOURCE_DIR" "$JOURNAL_DIR" "$ARCHIVE_DIR" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
      ;;
  esac
else
  reseed_token=""
fi

if [ -f "$DATABASE_PATH" ]; then
  echo "database exists, nothing to initialise"
  exit 0
fi

# Everything happens on a work file that is moved into place only once it is complete, so a
# failed attempt is retried from scratch rather than treated as initialised. The work path is
# not the replication.conf database, so these offline steps are not journaled.
work="$DATA_DIR/.init.fdb"
rm -f "$work" "$work.ctl" "$work.kind"

case "$primary" in
  ""|"$POD_NAME"|"$POD_NAME".*)
    if [ -f "$DATA_DIR/.bootstrap.fdb" ]; then
      mv "$DATA_DIR/.bootstrap.fdb" "$work"
      origin="bootstrapped"
    else
      echo "CREATE DATABASE '$work'; COMMIT;" | isql -q
      origin="created"
    fi
    isql -q -i "$SCRIPT_DIR/enable-publication.sql" "$work"
    if [ "$origin" = created ] && [ -f "$SCRIPT_DIR/init.sql" ]; then
      isql -q -i "$SCRIPT_DIR/init.sql" "$work"
    fi
    # offline copy: consistent because nothing is attached
    cp "$work" "$REPLICATION_DIR/bootstrap-seed.fdb.tmp"
    mv "$REPLICATION_DIR/bootstrap-seed.fdb.tmp" "$REPLICATION_DIR/bootstrap-seed.fdb"
    chown -R firebird:firebird "$DATA_DIR"
    mv "$work" "$DATABASE_PATH"
    echo "$origin primary database and offline bootstrap seed"
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
if [ -n "$reseed_token" ]; then echo "$reseed_token" > "$REPLICATION_DIR/.reseeded"; fi
echo "seeded replica from $fetched ($kind seed)"
