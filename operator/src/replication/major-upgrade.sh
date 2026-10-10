#!/bin/sh
# Major version upgrade of one instance volume (FirebirdCluster spec.imageName moved to an image
# with a newer on-disk structure, utils/major-upgrade.ts), run by a Job while every instance is
# stopped: "backup" with the old image, then "restore" with the new one.
#
# A newer engine cannot open a database in an older major ODS (Firebird 6, ODS 14, refuses the
# ODS 13 of Firebird 4 and 5), so each database is backed up with gbak by the engine that wrote it
# and restored by the new one, both embedded, nothing attached:
# - the security database (users) on every instance;
# - the database on the primary, and on every instance without replication (independent
#   databases). Its replication sequence carries over, so journal segment names continue after
#   the old ones; the replication state and the offline bootstrap seed (old ODS) are discarded:
#   the primary's init container writes a new seed when it starts.
# - a replica's database is discarded with its replication state: it is seeded again from the
#   converted primary when it starts.
# Each converted database replaces the old one only once it is complete; the old file is kept as
# $WORK_DIR/<name>.ods<old>.fdb. Every step can be repeated: an interrupted run resumes.
#
# Environment: MODE (backup|restore), ROLE (primary|replica|standalone), TARGET_ODS (major ODS of
# the new image), DATABASE_PATH, SECURITY_DB, DATA_DIR; with replication REPLICATION_DIR,
# JOURNAL_DIR, ARCHIVE_DIR, SOURCE_DIR, STATE_FILE, SCRIPT_DIR. ISC_USER for the embedded tools.
set -eu
W="$DATA_DIR/.major-upgrade"
own() { [ "$(id -u)" -ne 0 ] || chown "$@"; }
# major ODS from the header page (u16 at 18, without the 0x8000 flag): readable by any engine
ods_of() { v=$(od -An -tu2 -j18 -N2 "$1" | tr -d ' '); echo $((v & 32767)); }
# "<name> <path>" of the databases this instance converts
databases() {
  echo "security $SECURITY_DB"
  [ "$ROLE" = replica ] || echo "database $DATABASE_PATH"
}
mkdir -p "$W"
own firebird:firebird "$W"

case "$MODE" in
backup)
  databases | while read -r name path; do
    if [ ! -f "$path" ] || [ "$(ods_of "$path")" -ge "$TARGET_ODS" ] || [ -f "$W/$name.fbk" ]; then continue; fi
    echo "backing up $path (ODS $(ods_of "$path"))"
    rm -f "$W/$name.fbk.tmp"
    gbak -b -g "$path" "$W/$name.fbk.tmp"
    if [ "$name" = database ] && [ "$ROLE" = primary ]; then
      # gstat omits "Replication sequence" while it is 0
      seq=$(gstat -h "$path" | sed -n 's/^[[:space:]]*Replication sequence:[[:space:]]*\([0-9]*\).*/\1/p')
      echo "${seq:-0}" > "$W/$name.seq"
    fi
    mv "$W/$name.fbk.tmp" "$W/$name.fbk"
  done
  ;;
restore)
  databases | while read -r name path; do
    # resume: the converted database waits for the old one to be moved aside
    if [ ! -f "$path" ] && [ -f "$W/$name.fdb" ]; then mv "$W/$name.fdb" "$path"; echo "$path converted"; continue; fi
    if [ ! -f "$path" ] || [ "$(ods_of "$path")" -ge "$TARGET_ODS" ]; then rm -f "$W/$name.fbk"; continue; fi
    if [ ! -f "$W/$name.fbk" ]; then echo "no backup of $path: run the backup step first" >&2; exit 1; fi
    old=$(ods_of "$path")
    echo "restoring $path into ODS $TARGET_ODS"
    rm -f "$W/$name.fdb.tmp" "$W/$name.fdb"
    gbak -c "$W/$name.fbk" "$W/$name.fdb.tmp" > "$W/$name.restore.log" 2>&1 || { cat "$W/$name.restore.log" >&2; exit 1; }
    if [ -f "$W/$name.seq" ]; then
      # the journal continues after the old database's segments (Firebird 4 and 5 segment names
      # carry no GUID: a lower sequence would reuse the names of archived segments)
      perl "$SCRIPT_DIR/set-repl-seq.pl" "$W/$name.fdb.tmp" "$(cat "$W/$name.seq")"
    fi
    own firebird:firebird "$W/$name.fdb.tmp"
    mv "$W/$name.fdb.tmp" "$W/$name.fdb"
    mv "$path" "$W/$name.ods$old.fdb"
    mv "$W/$name.fdb" "$path"
    rm -f "$W/$name.fbk" "$W/$name.seq" "$W/$name.restore.log"
    echo "$path converted from ODS $old (old file kept as $W/$name.ods$old.fdb)"
  done
  if [ -n "${REPLICATION_DIR:-}" ]; then
    case "$ROLE" in
      primary)
        # segments and seed of the old database: the init container writes a new seed
        if [ -f "$REPLICATION_DIR/bootstrap-seed.fdb" ] && [ "$(ods_of "$REPLICATION_DIR/bootstrap-seed.fdb")" -lt "$TARGET_ODS" ]; then
          find "$SOURCE_DIR" "$JOURNAL_DIR" "$ARCHIVE_DIR" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
          rm -f "$STATE_FILE" "$REPLICATION_DIR/bootstrap-seed.fdb" "$REPLICATION_DIR/bootstrap-seed.seq"
          echo "replication state of the old database discarded"
        fi
        ;;
      replica)
        if [ -f "$DATABASE_PATH" ] && [ "$(ods_of "$DATABASE_PATH")" -lt "$TARGET_ODS" ]; then
          rm -f "$DATABASE_PATH" "$STATE_FILE" "$REPLICATION_DIR/sync-standby" "$REPLICATION_DIR/sync-seen"
          find "$SOURCE_DIR" "$JOURNAL_DIR" "$ARCHIVE_DIR" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
          rm -f "$REPLICATION_DIR/bootstrap-seed.fdb" "$REPLICATION_DIR/bootstrap-seed.seq"
          echo "replica database discarded: seeded again from the primary when the instance starts"
        fi
        ;;
    esac
  fi
  { printf 'converted' > /dev/termination-log; } 2>/dev/null || true
  ;;
*)
  echo "unknown MODE $MODE" >&2
  exit 1
  ;;
esac
