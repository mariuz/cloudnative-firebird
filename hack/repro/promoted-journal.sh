#!/bin/bash
# Does the journal of a replica promoted in place continue after the promotion point once its
# server restarts? A kind CI run on the Firebird 6 snapshot saw it restart at segment 1 instead
# of 63 (the operator's journal floor, v0.144.0, now raises such a header before the server
# starts; ISSUES.md, issue 10). This replays that run's sequence with the operator's own steps,
# three instances in their own containers (fresh containers on restart, as pods):
#
#   1. i0 primary, i1 and i2 read-only replicas fed with i0's archived segments (the segment
#      puller's job), rows written;
#   2. i0 is lost: i1 is promoted in place (segment-server.pl PROMOTE: full shutdown, header
#      sequence set offline, journal and archive cleared, online, replica mode none, publication)
#      and i0 is re-seeded from a locked copy of the replica i2 (nbackup -SEQ -F, its control file);
#   3. rows written on i1; then i1 is lost: i0 is promoted in place, and i1 is re-seeded right
#      away with a live locked copy of i0 (ALTER DATABASE BEGIN/END BACKUP) while the promotion
#      prepares its bootstrap seed copy (the window in which the CI run's re-seed found none);
#   4. every container is recreated, i0 (the primary) last, as a rolling update does;
#   5. rows written on i0: its header sequence and the first new segment tell whether the journal
#      continued after the promotion point, and the replicas whether they received the rows.
#
#   hack/repro/promoted-journal.sh                            # firebirdsql/firebird:6-snapshot
#   IMAGE=firebirdsql/firebird:5 RUNS=3 hack/repro/promoted-journal.sh
#
# Prints per run "journal continued after segment P (first new segment N)" or "journal restarted
# below the promotion point P (first new segment N)", then "runs: <k> of <n> restarted below".
# Needs Docker only.
set -eu
IMAGE=${IMAGE:-firebirdsql/firebird:6-snapshot}
RUNS=${RUNS:-1}
here=$(cd "$(dirname "$0")" && pwd)
scripts=$(cd "$here/../../operator/src/replication" && pwd)
work=
cleanup() {
  [ -n "$work" ] || return 0
  for i in 0 1 2; do docker rm -f "fbj$i" >/dev/null 2>&1 || true; done
  # the data belongs to the containers' user: removed with the image's own rm
  docker run --rm -v "$work:/w" --entrypoint sh "$IMAGE" -c 'rm -rf /w/*' >/dev/null 2>&1 || true
  rm -rf "$work" 2>/dev/null || true
}
trap cleanup EXIT

# sourced in every container: the replication.conf of the instance's database, and helpers
env_sh=$(cat <<'EOF'
export ISC_USER=SYSDBA ISC_PASSWORD=masterkey PATH=/opt/firebird/bin:$PATH
DB=/data/db.fdb
cat > /opt/firebird/replication.conf <<CONF
database
{
}
database = $DB
{
    journal_directory = /data/j
    journal_archive_directory = /data/a
    journal_archive_command = "test ! -f \$(archivepathname) && cp \$(pathname) \$(archivepathname)"
    journal_archive_timeout = 2
    journal_source_directory = /data/s
    apply_idle_timeout = 2
}
CONF
grep -q '^AuthServer = Srp, Legacy_Auth' /opt/firebird/firebird.conf ||
  printf 'AuthServer = Srp, Legacy_Auth\nUserManager = Srp, Legacy_UserManager\nWireCrypt = Enabled\n' >> /opt/firebird/firebird.conf
sql() { echo "$1" | isql -q localhost:$DB; }
value() { echo "SET LIST ON; SELECT $1 AS V FROM RDB\$DATABASE;" | isql -q localhost:$DB | awk '$1 == "V" { print $2 }'; }
EOF
)

# the HDR_repl_seq clump of a header page (layout as in set-repl-seq.pl), "none" without it
hdr_seq_pl=$(cat <<'EOF'
open(my $fh, '<:raw', $ARGV[0]) or die "open: $!\n";
read($fh, my $head, 20);
my $size = unpack('v', substr($head, 16, 2));
seek($fh, 0, 0); read($fh, my $page, $size);
my ($end_at, $p) = @{{13 => [66, 128], 14 => [36, 148]}->{unpack('v', substr($page, 18, 2)) & 0x7fff} // die "unknown ODS\n"};
my $end = unpack('v', substr($page, $end_at, 2));
while ($p < $end) {
  my ($type, $len) = unpack('C C', substr($page, $p, 2));
  last if $type == 0;
  if ($type == 11) { print unpack('Q<', substr($page, $p + 2, 8)); exit 0 }
  $p += 2 + $len;
}
print "none";
EOF
)

# start <i>: a fresh container on the instance's volume, its server running as firebird
start() {
  docker rm -f "fbj$1" >/dev/null 2>&1 || true
  mkdir -p "$work/i$1/j" "$work/i$1/a" "$work/i$1/s"
  docker run -d --name "fbj$1" -v "$work/i$1:/data" -v "$work:/w" -v "$scripts:/scripts:ro" --entrypoint bash "$IMAGE" \
    -c '. /w/env.sh; chown -R firebird:firebird /data; su -s /bin/sh firebird -c "/opt/firebird/bin/firebird"' >/dev/null
  for _ in $(seq 1 30); do x "$1" 'value 1' 2>/dev/null | grep -q 1 && return 0; sleep 1; done
  echo "instance $1 did not start"; docker logs "fbj$1"; return 1
}
# x <i> <commands>: in the instance's container, with env.sh
x() { docker exec "fbj$1" bash -c ". /w/env.sh; $2"; }
# pull <from> <to...>: the segment puller, archived segments of the primary into the replicas' source
pull() {
  local f=$1; shift
  for t in "$@"; do
    cp -n "$work/i$f/a/"* "$work/i$t/s/" 2>/dev/null || true
    docker exec "fbj$t" chown -R firebird:firebird /data/s
  done
}
count() { x "$1" "echo 'SET LIST ON; SELECT COUNT(*) AS V FROM app;' | isql -q localhost:\$DB" | awk '$1 == "V" { print $2 }'; }
write() { for n in $(seq "$2" "$3"); do x "$1" "sql 'insert into app values ($n); commit;'" >/dev/null; done; }
# applied <i>: the replica's control file position (u64 at 16)
applied() { od -An -tu8 -j16 -N8 "$(ls -d "$work/i$1/s/"\{* | head -1)" | tr -d ' '; }
header() { x "$1" 'perl /w/hdr-seq.pl $DB'; }
# promote <i> <seq>: segment-server.pl promote_here, in place (the server keeps running); the
# header page's replication sequence, read from the file (not through an engine), is printed
# after each step
promote() {
  x "$1" "h() { printf '%s %s; ' \"\$1\" \"\$(perl /w/hdr-seq.pl \$DB)\"; }
    fbsvcmgr localhost:service_mgr action_properties dbname \$DB prp_shutdown_mode prp_sm_full prp_force_shutdown 0 &&
    perl /scripts/set-repl-seq.pl \$DB $2 >/dev/null && h set && rm -f /data/j/* /data/a/* /data/seed.fdb && cp \$DB /data/seed.fdb.tmp &&
    fbsvcmgr localhost:service_mgr action_properties dbname \$DB prp_online_mode prp_sm_normal && h online &&
    gfix -replica none localhost:\$DB && h replica-none && isql -q -b -i /scripts/enable-publication.sql localhost:\$DB && h published &&
    rm -f /data/s/*; echo"
  echo "instance $1 promoted in place after segment $2; live sequence $(x "$1" "value \"RDB\\\$GET_CONTEXT('SYSTEM', 'REPLICATION_SEQUENCE')\"")"
}
# the promotion's bootstrap seed: the copy taken in full shutdown made a publishing primary database
prepare_seed() { x "$1" 'gfix -online normal /data/seed.fdb.tmp && gfix -replica none /data/seed.fdb.tmp &&
  isql -q -b -i /scripts/enable-publication.sql /data/seed.fdb.tmp && mv /data/seed.fdb.tmp /data/seed.fdb'; }
# reseed_from_replica <from> <to>: locked copy of a replica with its control file (seed_from_replica)
reseed_from_replica() {
  docker rm -f "fbj$2" >/dev/null 2>&1 || true
  rm -rf "$work/i$2"; mkdir -p "$work/i$2/s"
  x "$1" "sql 'ALTER DATABASE BEGIN BACKUP;'"
  cp "$work/i$1/db.fdb" "$work/i$2/db.fdb"; cp "$work/i$1/s/"\{* "$work/i$2/s/"
  x "$1" "sql 'ALTER DATABASE END BACKUP;'"
  docker run --rm -v "$work/i$2:/data" --entrypoint sh "$IMAGE" -c 'PATH=/opt/firebird/bin:$PATH nbackup -SEQ -F /data/db.fdb' >/dev/null
  start "$2"
}
# reseed_live <from> <to>: live locked copy of the primary (seed_from_primary without a bootstrap seed)
reseed_live() {
  docker rm -f "fbj$2" >/dev/null 2>&1 || true
  rm -rf "$work/i$2"; mkdir -p "$work/i$2/s"
  x "$1" "sql 'ALTER DATABASE BEGIN BACKUP;'"
  cp "$work/i$1/db.fdb" "$work/i$2/db.fdb"
  x "$1" "sql 'ALTER DATABASE END BACKUP;'"
  docker run --rm -v "$work/i$2:/data" -v "$work:/w" --entrypoint bash "$IMAGE" -c '. /w/env.sh; nbackup -SEQ -F $DB >/dev/null;
    echo "ALTER DATABASE DISABLE PUBLICATION; COMMIT;" | isql -q $DB; gfix -replica read_only $DB' >/dev/null
  start "$2"
}

restarted=0
for run in $(seq 1 "$RUNS"); do
  cleanup
  work=$(mktemp -d)
  printf '%s\n' "$env_sh" > "$work/env.sh"
  printf '%s\n' "$hdr_seq_pl" > "$work/hdr-seq.pl"

  # 1. a primary and two replicas seeded offline
  mkdir -p "$work/i0"
  docker run --rm -v "$work/i0:/data" -v "$work:/w" -v "$scripts:/scripts:ro" --entrypoint bash "$IMAGE" -c '. /w/env.sh
    echo "CREATE DATABASE '\''$DB'\'' USER SYSDBA; CREATE TABLE app (id INT PRIMARY KEY); COMMIT;" | isql -q
    isql -q -i /scripts/enable-publication.sql $DB' >/dev/null
  for i in 1 2; do
    mkdir -p "$work/i$i"; cp "$work/i0/db.fdb" "$work/i$i/db.fdb"
    docker run --rm -v "$work/i$i:/data" -v "$work:/w" --entrypoint bash "$IMAGE" -c '. /w/env.sh
      echo "ALTER DATABASE DISABLE PUBLICATION; COMMIT;" | isql -q $DB; gfix -replica read_only $DB' >/dev/null
  done
  for i in 0 1 2; do start "$i"; done
  for round in $(seq 1 10); do write 0 "${round}0" "${round}1"; sleep 3; pull 0 1 2; done
  sleep 8; pull 0 1 2; sleep 6
  echo "run $run: i0 rows $(count 0), i1 $(count 1), i2 $(count 2); i1 applied $(applied 1)"

  # 2. i0 lost: i1 promoted in place, i0 re-seeded from the replica i2
  docker rm -f fbj0 >/dev/null
  p1=$(applied 1)
  promote 1 "$p1"; prepare_seed 1
  reseed_from_replica 2 0
  for round in $(seq 11 20); do write 1 "${round}0" "${round}1"; sleep 3; pull 1 0 2; done
  sleep 8; pull 1 0 2; sleep 6
  echo "run $run: i1 rows $(count 1), i0 $(count 0), i2 $(count 2); i0 applied $(applied 0)"

  # 3. i1 lost: i0 promoted in place, i1 re-seeded at once from a live locked copy of i0
  docker rm -f fbj1 >/dev/null
  p0=$(applied 0)
  promote 0 "$p0"
  reseed_live 0 1 &
  prepare_seed 0
  wait
  echo "run $run: header of the new primary i0 after its promotion: $(header 0)"

  # 4. the rolling update: every container recreated, the primary last
  for i in 2 1 0; do start "$i"; sleep 3; done
  echo "run $run: header of i0 after its restart: $(header 0)"

  # 5. rows on the primary: where its journal continues
  write 0 3010 3014; sleep 5
  first=$(ls "$work/i0/a" 2>/dev/null | sed -n 's/.*\.journal-0*\([0-9][0-9]*\)$/\1/p' | sort -n | head -1)
  pull 0 1 2; sleep 8
  echo "run $run: i0 rows $(count 0), i1 $(count 1), i2 $(count 2)"
  if [ -n "$first" ] && [ "$first" -le "$p0" ]; then
    echo "run $run: journal restarted below the promotion point $p0 (first new segment $first)"
    restarted=$((restarted + 1))
  else
    echo "run $run: journal continued after segment $p0 (first new segment ${first:-none})"
  fi
done
echo "runs: $restarted of $RUNS restarted below"
