#!/bin/bash
# Does an attachment refused because the database is in full shutdown write the header page?
# (ISSUES.md, issue 10: the cause of the lost replication sequence.) The engine opens the
# database for the attachment, reads the header page into its cache, takes a new attachment id
# (PAG_attachment_id: ++hdr_attachment_id, the header page marked dirty), and only then refuses the
# attachment for the shutdown. Releasing the database flushes that header page. A header written
# offline in between (the replication sequence of an in-place promotion) is overwritten with the
# copy read before it.
#
#   1. the counter: in full shutdown, ATTEMPTS refused attachments; the header's "Next attachment
#      ID" (gstat -h, read from the file) before and after;
#   2. the race: CYCLES full shutdowns, each with RETRIERS clients retrying an attachment in a loop
#      (as the replica server retries its own) while the HDR_repl_seq clump is written offline
#      (operator/src/replication/set-repl-seq.pl), then online; the sequence in the header after
#      going online. First without a lock (the in-place promotion before v0.147.0), then holding
#      the exclusive lock (flock) a SuperServer holds while it has the file open, taken once the
#      server closed the file (segment-server.pl since v0.147.0).
#
#   hack/repro/refused-attachment.sh                         # firebirdsql/firebird:5
#   IMAGE=firebirdsql/firebird:6-snapshot CYCLES=20 hack/repro/refused-attachment.sh
#
# Prints "attachment id <a> -> <b> after <n> refused attachments", per cycle "set <s>; online <o>",
# then "unlocked: lost <k> of <n>" and "locked: lost <k> of <n>". Needs Docker only.
set -eu
IMAGE=${IMAGE:-firebirdsql/firebird:5}
NAME=${NAME:-fbrefused}
ATTEMPTS=${ATTEMPTS:-5}
CYCLES=${CYCLES:-10}
RETRIERS=${RETRIERS:-4}
here=$(cd "$(dirname "$0")" && pwd)
scripts=$(cd "$here/../../operator/src/replication" && pwd)
work=$(mktemp -d)
cleanup() {
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  docker run --rm -v "$work:/w" --entrypoint sh "$IMAGE" -c 'rm -rf /w/*' >/dev/null 2>&1 || true
  rm -rf "$work" 2>/dev/null || true
}
trap cleanup EXIT

cat > "$work/env.sh" <<'EOF'
export ISC_USER=SYSDBA ISC_PASSWORD=masterkey PATH=/opt/firebird/bin:$PATH
DB=/w/db.fdb
grep -q '^AuthServer = Srp, Legacy_Auth' /opt/firebird/firebird.conf ||
  printf 'AuthServer = Srp, Legacy_Auth\nUserManager = Srp, Legacy_UserManager\nWireCrypt = Enabled\n' >> /opt/firebird/firebird.conf
shut() { fbsvcmgr localhost:service_mgr action_properties dbname $DB prp_shutdown_mode prp_sm_full prp_force_shutdown 0; }
online() { fbsvcmgr localhost:service_mgr action_properties dbname $DB prp_online_mode prp_sm_normal; }
attach() { echo 'SELECT 1 FROM RDB$DATABASE;' | isql -q localhost:$DB 2>&1; }
attachment_id() { gstat -h $DB | awk -F'\t' '/Next attachment ID/ { print $3 }'; }
EOF
# the HDR_repl_seq clump of the header page, read from the file ("none" without it)
cat > "$work/hdr-seq.pl" <<'EOF'
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
# write.pl <db> <seq> <locked>: set-repl-seq.pl, with <locked> 1 once the server closed the file
# and holding its lock during the write
cat > "$work/write.pl" <<'EOF'
use Fcntl qw(:flock);
my ($db, $seq, $locked) = @ARGV;
open(my $fh, '<', $db) or die "open $db: $!\n";
if ($locked) {
  my $start = time;
  until (flock($fh, LOCK_EX | LOCK_NB)) { die "still open after 30 s\n" if time - $start > 30; select(undef, undef, undef, 0.01) }
}
exit(system('perl', '/scripts/set-repl-seq.pl', $db, $seq) == 0 ? 0 : 1);
EOF

docker run -d --name "$NAME" -v "$work:/w" -v "$scripts:/scripts:ro" --entrypoint bash "$IMAGE" -c '. /w/env.sh
  echo "CREATE DATABASE '\''$DB'\'' USER SYSDBA; CREATE TABLE app (id INT PRIMARY KEY); COMMIT;" | isql -q
  exec /opt/firebird/bin/firebird' >/dev/null
x() { docker exec "$NAME" bash -c ". /w/env.sh; $1"; }
for _ in $(seq 1 30); do x 'attach' 2>/dev/null | grep -q '^ *1$' && break; sleep 1; done
echo "server: $(x 'isql -z </dev/null | head -n 1')"

# 1. the counter
x 'shut'
before=$(x 'attachment_id')
refused=$(x "for i in \$(seq 1 $ATTEMPTS); do attach; done" | grep -c 'shutdown' || true)
after=$(x 'attachment_id')
x 'online'
echo "attachment id $before -> $after after $refused refused attachments (database in full shutdown)"

# 2. the race
seq=100
for locked in 0 1; do
  lost=0
  for c in $(seq 1 "$CYCLES"); do
    seq=$((seq + 1))
    x "for n in \$(seq 1 5); do echo 'INSERT INTO app VALUES ($seq * 100 + '\$n'); COMMIT;' | isql -q localhost:\$DB; done" >/dev/null
    out=$(x "shut; rm -f /tmp/stop
      for r in \$(seq 1 $RETRIERS); do (while [ ! -f /tmp/stop ]; do attach >/dev/null; done) & done
      sleep 0.3; perl /w/write.pl \$DB $seq $locked >/dev/null; set=\$(perl /w/hdr-seq.pl \$DB)
      sleep 0.3; touch /tmp/stop; wait; online; echo \"set \$set; online \$(perl /w/hdr-seq.pl \$DB)\"")
    echo "$([ $locked = 1 ] && echo locked || echo unlocked) cycle $c, sequence $seq: $out"
    [ "${out##*online }" = "$seq" ] || lost=$((lost + 1))
  done
  echo "$([ $locked = 1 ] && echo locked || echo unlocked): lost $lost of $CYCLES"
done
