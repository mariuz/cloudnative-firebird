#!/bin/bash
# Runs inside a Firebird image (verify.sh): checks the two Firebird internal formats the operator
# writes itself against that image's own engine and replica server.
#
# 1. HDR_repl_seq header clump (set-repl-seq.pl, src/jrd/ods.h): the sequence the script writes
#    is the one the engine reports (REPLICATION_SEQUENCE), when the clump is added and replaced.
# 2. Replica control file (replica-control.pl, ControlFile::DataV1 in ReplServer.cpp): Firebird's
#    replica server goes by the position the script writes. Without a control file it would start
#    from the database's own sequence, so the file claims one segment more than the copy has: the
#    primary's next segment (43) must be skipped and the one after (44) applied, and the server
#    moves the file's position on in the same format.
set -euo pipefail
S=/scripts
W=/tmp/formats
rm -rf "$W" && mkdir -p "$W/journal" "$W/source"
export ISC_USER=SYSDBA ISC_PASSWORD=masterkey
fail() { echo "FAIL: $*" >&2; exit 1; }
sql() { echo "$2" | isql -q "$1" 2>&1; }
value() { sql "$1" "$2" | awk 'NF && $1 !~ /^=+$/ { v = $1 } END { print v }'; }
seq_of() { value "$1" "select rdb\$get_context('SYSTEM','REPLICATION_SEQUENCE') from rdb\$database;"; }

echo "== $(isql -z </dev/null 2>&1 | head -1)"

# --- 1. header clump (no server: the embedded engine) ---
P="$W/primary.fdb"
sql "" "create database '$P'; create table t (id integer primary key); commit;" >/dev/null
echo "ODS: $(gstat -h "$P" | sed -n 's/^[[:space:]]*ODS version[[:space:]]*\([0-9.]*\).*/\1/p')"
[ "$(seq_of "$P")" = 0 ] || fail "a new database reports replication sequence $(seq_of "$P")"
perl "$S/set-repl-seq.pl" "$P" 41 >/dev/null
[ "$(seq_of "$P")" = 41 ] || fail "header clump added: the engine reports $(seq_of "$P"), expected 41"
perl "$S/set-repl-seq.pl" "$P" 42 >/dev/null
[ "$(seq_of "$P")" = 42 ] || fail "header clump replaced: the engine reports $(seq_of "$P"), expected 42"
echo "ok: HDR_repl_seq header clump (added and replaced) is the sequence the engine reports"

# --- 2. replica control file (the server and its replica server) ---
sql "$P" "alter database enable publication; alter database include all to publication; commit;" >/dev/null
R="$W/replica.fdb"
cp "$P" "$R"
gfix -replica read_only "$R"
guid=$(gstat -h "$R" | sed -n 's/^[[:space:]]*Database GUID:[[:space:]]*\({[0-9A-F-]*}\).*/\1/p')
[ -n "$guid" ] || fail "no database GUID in gstat -h"
# applied up to segment 43, while the copy itself is at 42: no candidates, no primary to ask
perl "$S/replica-control.pl" none 43 42 "$W/source/.control.tmp" >/dev/null
mv "$W/source/.control.tmp" "$W/source/$guid"
cat > /opt/firebird/replication.conf <<CONF
database = $P
{
  journal_directory = $W/journal
  journal_archive_directory = $W/source
  journal_archive_timeout = 1
}
database = $R
{
  journal_source_directory = $W/source
  apply_idle_timeout = 1
}
CONF
sec=$(ls /opt/firebird/security*.fdb | head -1)
sql "$sec" "create or alter user SYSDBA password 'masterkey' using plugin Srp; commit;" >/dev/null
/opt/firebird/bin/firebird >"$W/server.log" 2>&1 &
for _ in $(seq 1 30); do sql "localhost:$P" "select 1 from rdb\$database;" | grep -q 1 && break; sleep 1; done
diagnose() { ls -la "$W/source" >&2; cat /opt/firebird/replication.log >&2 2>/dev/null || true; }
archived() { ls "$W/source" | grep -q "journal-0*$1\$"; }
# segment 43: the control file says it was applied, so the replica server skips it
sql "localhost:$P" "insert into t values (1); commit;" >/dev/null
for _ in $(seq 1 60); do archived 43 && break; sleep 1; done
archived 43 || { diagnose; fail "the primary did not archive segment 43"; }
sleep 10
# segment 44: applied
sql "localhost:$P" "insert into t values (2); commit;" >/dev/null
for _ in $(seq 1 60); do
  [ "$(value "localhost:$R" 'select count(*) from t;')" = 1 ] && break
  sleep 1
done
rows=$(value "localhost:$R" 'select list(id) from t;')
if [ "$rows" != 2 ]; then
  diagnose
  fail "the replica should hold the row of segment 44 only (segment 43 is before the control file's position); it holds: ${rows:-none}"
fi
position=$(perl -e 'open(my $f, "<:raw", $ARGV[0]) or die; local $/; my $d = <$f>; my (undef, $v, undef, $s) = unpack("a10 v V Q<", $d); print "$v $s"' "$W/source/$guid")
[ "${position%% *}" = 1 ] || fail "the replica server rewrote the control file with version ${position%% *}"
[ "${position#* }" -ge 44 ] || fail "the replica server left the control file at segment ${position#* }"
echo "ok: replica control file: the replica server skipped segment 43 as written, applied 44 and moved the position to ${position#* }"
