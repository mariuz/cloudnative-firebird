#!/bin/sh
# Point-in-time recovery Job (FirebirdRestore with pointInTime), Firebird container. Sourced by
# the container's command, which restores the level-0 backup "$nbk" this leaves in the primary's
# data directory into TARGET_PATH (the EXIT trap removes it afterwards).
#
# 1. Restores the nbackup chain into a scratch database here (nbackup -SEQ -R keeps the
#    replication sequence S of the last backup in the chain).
# 2. Picks the last journal segment L to apply: TARGET_SEGMENT, the last one archived at or before
#    TARGET_TIME, or the newest archived one.
# 3. Downloads the segments from S3 (through the S3 client container of the pod, see below) and
#    plans the replay with pitr-plan.pl: segments after S, plus the transactions open in the
#    backup from their first segment.
# 4. Makes the scratch database a read-only replica, starts a private Firebird server whose
#    replication.conf points the replica at the segments, and waits until it applied segment L.
# 5. Stops the server (transactions still open at L are rolled back), makes the database a
#    normal one, takes a level-0 nbackup of it and restores that into TARGET_PATH on the primary.
#
# The S3 client container lists the journal archive into $W/journals.list and serves download
# requests: "$W/req/<n>.req" lists segment names, answered by renaming it to .ok or .err. An S3
# chain is in $W/chain already (init container). This container writes $W/finished once it needs
# no more downloads (or fails), and the current time to $W/heartbeat until then.
#
# Environment: TARGET_PATH, FIREBIRD_HOST, ISC_USER/ISC_PASSWORD, RESTORE_NAME, CHAIN_COUNT,
# SERVER_CHAIN (chain file names on the primary, when not in S3), TARGET_TIME
# (YYYYMMDDTHHMMSSZ) or TARGET_SEGMENT, SCRIPT_DIR.
set -eu
W=/work
DB=$W/db/pitr.fdb
SRC=$W/source
mkdir -p "$W/db" "$SRC" "$W/req" "$W/lock" "$W/fb"
trap 'touch "$W/finished"' EXIT
( while :; do date +%s > "$W/heartbeat"; sleep 20; done ) &
heartbeat=$!

fail() { echo "point-in-time recovery failed: $*" >&2; exit 1; }
wait_for() { # <file>: wait until the S3 client container wrote it
  while [ ! -f "$1" ]; do
    [ -f "$W/fetcher.failed" ] && fail "the S3 client container failed: $(cat "$W/fetcher.failed")"
    sleep 1
  done
}
n=0
download() { # segment names as arguments
  n=$((n + 1))
  printf '%s\n' "$@" > "$W/req/$n.tmp"
  mv "$W/req/$n.tmp" "$W/req/$n.req"
  while [ ! -f "$W/req/$n.ok" ]; do
    [ -f "$W/req/$n.err" ] && fail "could not download journal segments: $(cat "$W/req/$n.err")"
    [ -f "$W/fetcher.failed" ] && fail "the S3 client container failed: $(cat "$W/fetcher.failed")"
    sleep 1
  done
}

# A private copy of the Firebird installation: its replication.conf and logs are written here
# (the image's are not writable by this non-root container)
cp -r /opt/firebird/. "$W/fb/" 2>/dev/null || true
export FIREBIRD="$W/fb" FIREBIRD_LOCK="$W/lock"
FB="$W/fb/bin"

# 1. the nbackup chain
if [ -n "${SERVER_CHAIN:-}" ]; then
  mkdir -p "$W/chain"
  i=0
  for f in $SERVER_CHAIN; do
    perl "$SCRIPT_DIR/backup-file.pl" get "$f" "$W/chain/$i.nbk"
    i=$((i + 1))
  done
fi
chain=""
i=0
while [ "$i" -lt "$CHAIN_COUNT" ]; do chain="$chain $W/chain/$i.nbk"; i=$((i + 1)); done
# shellcheck disable=SC2086 # chain is a list of plain paths
"$FB/nbackup" -SEQ -R "$DB" $chain
rm -f $chain
header=$("$FB/gstat" -h "$DB")
field() { echo "$header" | sed -n "s/^[[:space:]]*$1:*[[:space:]]*\\([0-9{][0-9A-F{}-]*\\).*/\\1/p"; }
if echo "$header" | grep -q '^[[:space:]]*Attributes.*replica'; then
  fail "the backup was taken on a replica; point-in-time recovery needs a backup taken on the primary"
fi
S=$(field "Replication sequence"); S=${S:-0}
oat=$(field "Oldest active")
next=$(field "Next transaction")
guid=$(field "Database GUID")
[ -n "$oat" ] && [ -n "$next" ] && [ -n "$guid" ] || fail "cannot read the restored database header"
echo "backup restored: replication sequence $S, transactions $oat to $next"

# 2. the segments in the archive: "<sequence> <name> <archive time>", from the archive time
# markers ("<segment>.archived-<time>"), or the upload time for segments without one
wait_for "$W/journals.list"
awk '{
  name = $4
  if (name ~ /\.journal-[0-9]+$/) {
    s = name; sub(/.*\.journal-0*/, "", s); if (s == "") s = 0
    d = $1; t = $2; gsub(/-/, "", d); gsub(/:/, "", t)
    seg[s + 0] = name; up[s + 0] = d "T" t "Z"
  } else if (name ~ /\.journal-[0-9]+\.archived-[0-9]+T[0-9]+Z$/) {
    s = name; sub(/\.archived-.*/, "", s); sub(/.*\.journal-0*/, "", s); if (s == "") s = 0
    at = name; sub(/.*\.archived-/, "", at); mark[s + 0] = at
  }
} END { for (s in seg) print s, seg[s], ((s in mark) ? mark[s] : up[s]) }' "$W/journals.list" | sort -n > "$W/segments.idx"
[ -s "$W/segments.idx" ] || fail "the journal archive is empty"
time_of() { awk -v s="$1" '$1 == s { print $3 }' "$W/segments.idx"; }
name_of() { awk -v s="$1" '$1 == s { print $2 }' "$W/segments.idx"; }

if [ -n "${TARGET_SEGMENT:-}" ]; then
  L=$TARGET_SEGMENT
  [ -n "$(name_of "$L")" ] || fail "segment $L is not in the journal archive"
elif [ -n "${TARGET_TIME:-}" ]; then
  base_time=$(time_of "$S")
  if [ -n "$base_time" ] && [ "$base_time" \> "$TARGET_TIME" ]; then
    fail "the backup is newer than the recovery target: its last segment ($S) was archived at $base_time"
  fi
  # the segments after S up to the first one archived after the target
  L=$(awk -v s="$S" -v t="$TARGET_TIME" '$1 > s { if ($3 > t) exit; l = $1 } END { print (l == "" ? s : l) }' "$W/segments.idx")
else
  L=$(tail -n 1 "$W/segments.idx" | cut -d' ' -f1)
fi
[ "$L" -ge "$S" ] || fail "the recovery target (segment $L) is before the backup (segment $S)"
echo "recovery target: segment $L, archived at $(time_of "$L")"

# 3. download and plan: the open transactions may need segments before S
"$FB/isql" -q -user SYSDBA "$DB" > "$W/candidates" <<SQL
SET TERM ^;
EXECUTE BLOCK RETURNS (t BIGINT) AS BEGIN t = $oat; WHILE (t < $next) DO BEGIN
  IF (COALESCE(RDB\$GET_TRANSACTION_CN(t), 0) <= 0) THEN SUSPEND; t = t + 1; END END^
SET TERM ;^
SQL
awk '$1 ~ /^[0-9]+$/ { print $1 }' "$W/candidates" > "$W/candidates.ids"
first=$S
[ "$first" -ge 1 ] || first=1
want() { # <from> <to>: segment names in the archive
  awk -v a="$1" -v b="$2" '$1 >= a && $1 <= b { print $2 }' "$W/segments.idx"
}
if [ "$L" -ge "$first" ]; then
  # shellcheck disable=SC2046 # segment names have no spaces
  download $(want "$first" "$L")
  for f in "$W/segments"/*.journal-*; do [ -f "$f" ] && mv "$f" "$SRC/"; done
fi
while :; do
  rc=0
  out=$(perl "$SCRIPT_DIR/pitr-plan.pl" "$SRC" "$S" "$oat" "$next" "$W/candidates.ids" "$L" "$SRC/$guid") || rc=$?
  echo "$out"
  [ "$rc" -eq 3 ] || break
  need=$(echo "$out" | sed -n 's/^need //p')
  [ -n "$(name_of "$need")" ] || fail "segment $need, needed for transactions open in the backup, is not in the journal archive"
  from=$((need - 15))
  [ "$from" -ge 1 ] || from=1
  # shellcheck disable=SC2046
  download $(want "$from" "$need")
  for f in "$W/segments"/*.journal-*; do [ -f "$f" ] && mv "$f" "$SRC/"; done
done
[ "$rc" -eq 0 ] || fail "cannot plan the replay"
# every download is done: the S3 client container can stop
touch "$W/finished"
kill "$heartbeat" 2>/dev/null || true

# 4. replay as a replica of the archived journal
echo "ALTER DATABASE DISABLE PUBLICATION; COMMIT;" | "$FB/isql" -q -user SYSDBA "$DB"
"$FB/gfix" -user SYSDBA -replica read_only "$DB"
if [ "$L" -gt "$S" ]; then
  printf '%s\n' 'database' '{' '}' "database = $DB" '{' "    journal_source_directory = $SRC" \
    '    apply_idle_timeout = 1' '    verbose_logging = true' '}' > "$W/fb/replication.conf"
  "$FB/firebird" &
  server=$!
  while :; do
    seq=$(od -An -tu8 -j16 -N8 "$SRC/$guid" | tr -d ' ')
    offset=$(od -An -tu4 -j24 -N4 "$SRC/$guid" | tr -d ' ')
    if [ "${seq:-0}" -ge "$L" ] && [ "${offset:-1}" = 0 ]; then break; fi
    if grep -q 'ERROR' "$W/fb/replication.log" 2>/dev/null; then
      tail -n 20 "$W/fb/replication.log" >&2
      fail "replaying the journal failed"
    fi
    kill -0 "$server" 2>/dev/null || fail "the Firebird server stopped while replaying the journal"
    sleep 2
  done
  grep -E 'is (replayed|replicated)' "$W/fb/replication.log" || true
  kill "$server"
  wait "$server" || true
fi
"$FB/gfix" -user SYSDBA -replica none "$DB"
echo "recovered to the end of segment $L (archived at $(time_of "$L"))"

# 5. into the target database on the primary
nbk="restore-$RESTORE_NAME-pitr.nbk"
"$FB/nbackup" -user SYSDBA -B 0 "$DB" "$W/$nbk"
rm -f "$DB"
trap 'perl "$SCRIPT_DIR/backup-file.pl" remove "$nbk" || true' EXIT
perl "$SCRIPT_DIR/backup-file.pl" put "$W/$nbk" "$nbk"
rm -f "$W/$nbk"
# sourced by the Job, which then restores "$nbk" (in the primary's data directory) into TARGET_PATH
