#!/bin/bash
# Runs the reproduction scripts of ISSUES.md against one Firebird image and tells for each issue
# whether it reproduced, for the "Firebird issues" workflow (weekly, and on demand for any image)
# and by hand:
#
#   hack/repro/run-all.sh [image] [log dir]         # default firebirdsql/firebird:5, logs in a temp dir
#   RUNS=3 hack/repro/run-all.sh firebirdsql/firebird:6-snapshot   # issue 1 three times
#   PARSE_ONLY=1 hack/repro/run-all.sh firebirdsql/firebird:5 logs # the verdicts of logs kept earlier
#
# Needs Docker. Every script's output goes to the console and to <log dir>/<test>.log. The
# verdicts (reproduced, not reproduced, unclear) are printed as a Markdown table, appended to
# $GITHUB_STEP_SUMMARY when set. The exit status says whether every script ran (a failed script
# or Docker is 1), never what Firebird did: the issues are Firebird's, the verdicts are the result.
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
image=${1:-firebirdsql/firebird:5}
logs=${2:-$(mktemp -d)}
mkdir -p "$logs"
logs=$(cd "$logs" && pwd)
export IMAGE=$image
RUNS=${RUNS:-1}
status=0
rows=()

# run <test> <command...>: the output to the console and the test's log; a failed script is recorded
run() {
  local name=$1
  shift
  [ "${PARSE_ONLY:-0}" = 1 ] && return 0
  echo "### $name: $*"
  if ! "$@" 2>&1 | tee "$logs/$name.log"; then
    echo "### $name: the script failed (exit ${PIPESTATUS[0]})"
    status=1
  fi
}

# verdict <issue> <test> <outcome> <details>
verdict() { rows+=("| $1 | $2 | $3 | $4 |"); }

# the last line of a log matching a pattern (empty when the log is missing)
last() { [ -f "$2" ] && grep -E "$1" "$2" | tail -1 || true; }

# 1: a publishing database deadlocks under concurrent connect / commit / disconnect
hung=0 completed=0 details=""
for n in $(seq 1 "$RUNS"); do
  run "issue1-run$n" "$here/publication-under-load.sh"
  line=$(last '^(writers completed|server stopped answering|no progress)' "$logs/issue1-run$n.log")
  case "$line" in
    "writers completed"*) completed=$((completed + 1)) ;;
    "server stopped answering"* | "no progress"*) hung=$((hung + 1)) ;;
  esac
  details="$details${details:+; }run $n: ${line:-no result}"
done
if [ "$hung" -gt 0 ]; then
  verdict 1 "publication-under-load.sh, 8 writers" reproduced "$hung of $RUNS run(s) hung: $details"
elif [ "$completed" = "$RUNS" ]; then
  verdict 1 "publication-under-load.sh, 8 writers" "not reproduced" "$details"
else
  verdict 1 "publication-under-load.sh, 8 writers" unclear "$details"
fi
# the control: without publication the writers complete
run issue1-control env PUBLICATION=0 "$here/publication-under-load.sh"
line=$(last '^(writers completed|server stopped answering|no progress)' "$logs/issue1-control.log")
case "$line" in
  "writers completed"*) verdict 1 "control: publication off" "completed" "$line" ;;
  *) verdict 1 "control: publication off" unclear "${line:-no result}" ;;
esac

# 2: commits journaled before their TIP state, so lock-based copies miss transactions
run issue2-tip-window "$here/tip-window.sh"
line=$(last '^[0-9]+ backups: ' "$logs/issue2-tip-window.log")
backups=$(echo "$line" | sed -n 's/^\([0-9]*\) backups: .*/\1/p')
window=$(echo "$line" | sed -n 's/.*: \([0-9]*\) transaction(s) in the commit\/TIP window.*/\1/p')
copies=$( [ -f "$logs/issue2-tip-window.log" ] && grep -c 'window [1-9]' "$logs/issue2-tip-window.log" || echo 0)
if [ -z "$window" ]; then
  verdict 2 "tip-window.sh, 8 writers" unclear "${line:-no result}"
elif [ "$window" -gt 0 ]; then
  verdict 2 "tip-window.sh, 8 writers" reproduced "$window window transaction(s) in $copies of $backups copies"
else
  verdict 2 "tip-window.sh, 8 writers" "not reproduced" "no window transaction in $backups copies"
fi
run issue2-seed-race env WRITERS=8 CONNECTIONS=persistent ROWS=3000 "$here/replica-seed-race.sh"
line=$(last '^rows missing or different on the replica: ' "$logs/issue2-seed-race.log")
missing=${line#rows missing or different on the replica: }
counts=$(last '^primary rows: ' "$logs/issue2-seed-race.log")
if [ -z "$line" ]; then
  verdict 2 "replica-seed-race.sh, 8 writers, persistent connections" unclear "no result"
elif [ "$missing" = none ]; then
  verdict 2 "replica-seed-race.sh, 8 writers, persistent connections" "not reproduced" "replica identical to the primary ($counts)"
elif [ "$(echo "$missing" | wc -w)" -gt 100 ]; then
  # the issue loses a transaction or two: far more means the replica stopped applying (its replication.log is in the test's log)
  verdict 2 "replica-seed-race.sh, 8 writers, persistent connections" unclear "the replica stopped $(echo "$missing" | wc -w) rows short of the primary ($counts): see its replication.log in the test's log"
else
  # the list can be long: the count and the first rows (id/note)
  verdict 2 "replica-seed-race.sh, 8 writers, persistent connections" reproduced "$(echo "$missing" | wc -w) row(s) missing or different on the replica ($counts): $(echo "$missing" | tr -s ' ' '\n' | head -5 | tr '\n' ' ')..."
fi

# 8 and 9: synchronous replication (sync-replica.sh prints the counts after each step)
run sync-replica "$here/sync-replica.sh"
log=$logs/sync-replica.log
# 8: Firebird 4 commits while the synchronous replica is unreachable (row 5, inserted with the replica stopped)
line=$(last '^replica back: primary rows ' "$log")
primary=$(echo "$line" | sed -n 's/^replica back: primary rows \(.*\), replica .*/\1/p')
replica=$(echo "$line" | sed -n 's/^replica back: primary rows .*, replica \(.*\)$/\1/p')
has() { echo " $1 " | grep -q " $2 "; }
if [ -z "$line" ]; then
  verdict 8 "sync-replica.sh, a write with the replica stopped" unclear "no result"
elif has "$primary" 5 && ! has "$replica" 5; then
  verdict 8 "sync-replica.sh, a write with the replica stopped" reproduced "committed on the primary, never on the replica (primary rows $primary; replica $replica)"
elif ! has "$primary" 5; then
  verdict 8 "sync-replica.sh, a write with the replica stopped" "not reproduced" "the write failed, nothing committed (primary rows $primary; replica $replica)"
else
  verdict 8 "sync-replica.sh, a write with the replica stopped" unclear "primary rows $primary; replica $replica"
fi
# 9: sync_replica is read only when the database is opened: a change is ignored while an attachment
# stays open (row 31 still replicated) and applies once every attachment closed (row 32 not replicated,
# row 33 replicated again). The replica count follows the primary's by one row at each step that
# replicated (the primary's first row predates the sync_replica setting, so the totals never match).
counts() { last "^$1" "$log" | sed -n 's/.*: primary \([0-9]*\), replica \([0-9]*\) .*/\1 \2/p'; }
read -r p0 r0 <<<"$(counts 'after full shutdown and online')"
read -r p1 r1 <<<"$(counts 'sync_replica removed, an attachment still open')"
read -r p2 r2 <<<"$(counts 'every attachment closed, no shutdown')"
read -r p3 r3 <<<"$(counts 'sync_replica back while nothing was attached')"
steps="attached: primary ${p1:-?}, replica ${r1:-?}; all closed: primary ${p2:-?}, replica ${r2:-?}; set again: primary ${p3:-?}, replica ${r3:-?}"
if [ -z "${r0:-}" ] || [ -z "${r1:-}" ] || [ -z "${r2:-}" ] || [ -z "${r3:-}" ]; then
  verdict 9 "sync-replica.sh, sync_replica removed while attached" unclear "no result ($steps)"
elif [ "$r1" = $((r0 + 1)) ] && [ "$r2" = "$r1" ] && [ "$r3" = $((r2 + 1)) ]; then
  verdict 9 "sync-replica.sh, sync_replica removed while attached" reproduced "the removal was ignored while an attachment stayed open and read once every attachment closed, without a shutdown ($steps)"
elif [ "$r1" = "$r0" ]; then
  verdict 9 "sync-replica.sh, sync_replica removed while attached" "not reproduced" "the removal took effect while an attachment stayed open ($steps)"
else
  verdict 9 "sync-replica.sh, sync_replica removed while attached" unclear "$steps"
fi

server=$(docker run --rm --entrypoint sh "$image" -c 'isql -z </dev/null 2>&1 | head -n 1' 2>/dev/null | sed -n 's/.*\(LI-[A-Z][0-9.]*[0-9]\).*/\1/p')
{
  echo
  echo "## Firebird issues: $image${server:+ ($server)}"
  echo
  echo "| Issue | Test | Outcome | Details |"
  echo "|---|---|---|---|"
  printf '%s\n' "${rows[@]}"
  echo
  [ "$status" = 0 ] || echo "A script failed to run: see its log."
} | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"
exit $status
