#!/bin/sh
# Replica cluster (spec.replica): the archive-follower sidecar of the designated primary, in the S3
# client image. The instance's database is a read-only replica of the source cluster; this copies
# the source's archived journal segments (its journal archive in S3, "<prefix>/journals/") into
# the replica's journal_source_directory, where the Firebird replica server applies them.
#
# Every POLL_SECONDS:
# - reads the replica's position (the control file in SOURCE_DIR: the last segment applied, and
#   the offset within the next one), lists the archive, and downloads the next segments after the
#   position (at most BATCH at a time), each through a temporary name;
# - across a failover in the source (lineage marker "<database>.lineage-<P>-<U>": the replica
#   promoted at segment P continued after the archive's segment U), downloads up to P only. Once P
#   is applied, it asks for a restart ($REPLICATION_DIR/reposition holds U): the replication init
#   container moves the position after U (pitr-plan.pl --reposition) when the operator restarts
#   the instance, the transactions still open at P being rolled back by the restart (they never
#   committed in the new lineage), and records U in $REPLICATION_DIR/repositioned. A replica that
#   applied segments of the lost primary after P cannot follow the new lineage: reported, it needs
#   a new bootstrap;
# - writes its state to $REPLICATION_DIR/follow.state, one line the segment server returns for
#   FOLLOW: "applied=<n> archived=<n>" plus " restart=<U>" or " error=<message>".
#
# Environment: SOURCE_DIR, REPLICATION_DIR, JOURNALS (s3://bucket/prefix/journals/), AWS (the aws
# command, with --endpoint-url when set), POLL_SECONDS, BATCH.
set -u
POLL=${POLL_SECONDS:-30}
BATCH=${BATCH:-20}
STATE="$REPLICATION_DIR/follow.state"
W=$(mktemp -d)

report() { printf '%s\n' "$1" > "$STATE.tmp" && mv "$STATE.tmp" "$STATE"; }
position() { # <control file>: "<applied> <offset>"
  printf '%s %s\n' "$(od -An -tu8 -j16 -N8 "$1" | tr -d ' ')" "$(od -An -tu4 -j24 -N4 "$1" | tr -d ' ')"
}

while :; do
  ctl=$(find "$SOURCE_DIR" -maxdepth 1 -name '{*}' 2>/dev/null | head -n 1)
  if [ -z "$ctl" ]; then
    report "applied=0 archived=0 error=no replica control file in $SOURCE_DIR (not bootstrapped yet)"
    sleep "$POLL"
    continue
  fi
  # shellcheck disable=SC2046 # two numbers
  set -- $(position "$ctl")
  applied=${1:-0}
  offset=${2:-0}
  if ! $AWS s3 ls "$JOURNALS" > "$W/listing" 2> "$W/ls.err"; then
    # an empty prefix lists nothing and fails: an archive with no segment yet
    if [ -s "$W/ls.err" ]; then
      report "applied=$applied archived=0 error=listing $JOURNALS failed: $(tr '\n' ' ' < "$W/ls.err" | cut -c1-300)"
      sleep "$POLL"
      continue
    fi
    : > "$W/listing"
  fi
  # "<sequence> <name>" of the archived segments, by sequence
  awk '{ n = $4; if (n ~ /\.journal-[0-9]+$/) { s = n; sub(/.*\.journal-0*/, "", s); if (s == "") s = 0; print s + 0, n } }' \
    "$W/listing" | sort -n > "$W/segments"
  archived=$(tail -n 1 "$W/segments" | cut -d' ' -f1)
  archived=${archived:-0}
  # lineage switches "<P> <U>", by U
  awk '{ n = $4; if (n ~ /\.lineage-[0-9]+-[0-9]+$/) { sub(/.*\.lineage-/, "", n); split(n, a, "-"); print a[1] + 0, a[2] + 0 } }' \
    "$W/listing" | sort -n -k2 -u > "$W/lineage"

  # the first switch ahead of the position decides how far this lineage goes
  limit=$archived
  restart=""
  error=""
  while read -r p u; do
    [ "$applied" -le "$u" ] || continue
    # repositioned after U already (the init container records it)
    grep -qx "$u" "$REPLICATION_DIR/repositioned" 2>/dev/null && continue
    if [ "$applied" -gt "$p" ]; then
      error="this replica applied segments $((p + 1)) to $applied of the primary the source lost in a failover after segment $p: it cannot follow the new lineage (bootstrap the replica cluster again)"
    else
      limit=$p
      if [ "$applied" -eq "$p" ] && [ "$offset" = 0 ]; then restart=$u; fi
    fi
    break
  done < "$W/lineage"

  if [ -z "$error" ] && [ -n "$restart" ]; then
    # applied up to the switch: the restart moves the position after U
    printf '%s\n' "$restart" > "$REPLICATION_DIR/reposition.tmp" && mv "$REPLICATION_DIR/reposition.tmp" "$REPLICATION_DIR/reposition"
    report "applied=$applied archived=$archived restart=$restart"
    sleep "$POLL"
    continue
  fi
  if [ -z "$error" ]; then
    # the next segments of this lineage not in the source directory yet
    awk -v a="$applied" -v l="$limit" '$1 > a && $1 <= l { print $2 }' "$W/segments" | head -n "$BATCH" > "$W/wanted"
    while read -r name; do
      [ -f "$SOURCE_DIR/$name" ] && continue
      if $AWS s3 cp --only-show-errors "$JOURNALS$name" "$SOURCE_DIR/.$name.tmp" 2> "$W/cp.err"; then
        chmod 644 "$SOURCE_DIR/.$name.tmp"
        mv "$SOURCE_DIR/.$name.tmp" "$SOURCE_DIR/$name"
      else
        rm -f "$SOURCE_DIR/.$name.tmp"
        error="downloading $name failed: $(tr '\n' ' ' < "$W/cp.err" | cut -c1-300)"
        break
      fi
    done < "$W/wanted"
  fi
  if [ -n "$error" ]; then
    report "applied=$applied archived=$archived error=$error"
  else
    report "applied=$applied archived=$archived"
  fi
  sleep "$POLL"
done
