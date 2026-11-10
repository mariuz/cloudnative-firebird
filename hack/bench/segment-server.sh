#!/bin/bash
# Measures the segment server (operator/src/replication/segment-server.pl) as a primary's sidecar
# runs it (docs/rust-agent-roadmap.md, Phase 0). Needs Docker, and perl with Time::HiRes on the
# host for the client (the Firebird images' perl-base has none):
#
#   1. request cost: PING (connection + signature) and STATE (one fbsvcmgr per request);
#   2. GET throughput of one archived segment (SEGMENT_MB);
#   3. a live seed downloaded slowly by a new replica (SEED_RATE_MB per second, as over a
#      network), with the isolation check running as in a cluster with automatic failover
#      (CONTACT_TIMEOUT_SECONDS, peers in DNS). Meanwhile, every second: STATE as the Lease
#      holder asks it (10 s limit, from the server's pod) and PING as a replica's puller would.
#      Reports the slowest answers, the probes that timed out, and whether the primary was
#      fenced while it served the seed.
#
#   hack/bench/segment-server.sh                       # firebirdsql/firebird:5
#   IMAGE=firebirdsql/firebird:6-snapshot DOUBLINGS=17 SEED_RATE_MB=5 hack/bench/segment-server.sh
#
# The database holds 2^DOUBLINGS rows of 3.6 kB (16: about 260 MB). With the defaults the seed
# takes longer than CONTACT_TIMEOUT_SECONDS.
set -eu
IMAGE=${IMAGE:-firebirdsql/firebird:5}
NAME=${NAME:-fbbench}
DOUBLINGS=${DOUBLINGS:-16}
SEED_RATE_MB=${SEED_RATE_MB:-3}
SEGMENT_MB=${SEGMENT_MB:-256}
CONTACT_TIMEOUT_SECONDS=${CONTACT_TIMEOUT_SECONDS:-60}
here=$(cd "$(dirname "$0")" && pwd)
src=$(cd "$here/../../operator/src/replication" && pwd)
work=$(mktemp -d)
cleanup() {
  docker rm -f "$NAME-0" "$NAME-1" >/dev/null 2>&1 || true
  docker network rm "$NAME" >/dev/null 2>&1 || true
  docker run --rm -v "$work:/w" --entrypoint sh "$IMAGE" -c 'rm -rf /w/*' >/dev/null 2>&1 || true
  rmdir "$work" 2>/dev/null || true
}
trap cleanup EXIT
# the instance's processes run as the image's firebird user
chmod 755 "$work"
mkdir -p "$work/scripts"
# the scripts as the pods get them: "#@include <file>" lines replaced by the file
for name in segment-server.pl isolation-check.pl set-repl-seq.pl; do
  awk -v dir="$src" '/^#@include [^ ]+$/ { while ((getline line < (dir "/" $2)) > 0) print line; close(dir "/" $2); next } { print }' \
    "$src/$name" > "$work/scripts/$name"
done
# client.pl latency <host> <count> <interval> <limit> <request...>: one request per interval, the
#   time to the first reply line (ms), "timeout" after <limit> seconds
# client.pl download <host> <MB per second, 0 unlimited> <request...>: reads the whole reply
{ cat <<'PL'
use strict; use warnings; use IO::Socket::INET; use Time::HiRes ();
sub now { Time::HiRes::time() }
PL
  cat "$src/segment-auth.pl"
  cat <<'PL'
my ($mode, $host, @rest) = @ARGV;
my $secret = $ENV{ISC_PASSWORD} // '';
if ($mode eq 'latency') {
  my ($count, $interval, $limit, @request) = @rest;
  my (@ms, $timeouts, $stop, %errors);
  $timeouts = 0;
  $SIG{TERM} = $SIG{INT} = sub { $stop = 1 };
  for my $i (1 .. $count) {
    last if $stop;
    my $start = now();
    my $line = eval {
      local $SIG{ALRM} = sub { die "timeout\n" };
      alarm $limit;
      my $sock = segment_open($host, 3051, $secret, join(' ', @request), $limit);
      my $l = <$sock>; close $sock; alarm 0; $l;
    };
    alarm 0;
    if (!defined $line) { $timeouts++; } elsif ($line !~ /^OK/) { $errors{$line =~ s/\s+$//r}++; } else { push @ms, (now() - $start) * 1000; }
    my $left = $interval - (now() - $start);
    Time::HiRes::sleep($left) if $left > 0 && $i < $count && !$stop;
  }
  @ms = sort { $a <=> $b } @ms;
  printf "%s: %d answered", join(' ', @request), scalar @ms;
  printf ", median %.0f ms, slowest %.0f ms", $ms[$#ms / 2], $ms[-1] if @ms;
  printf ", %d not answered within %d s", $timeouts, $limit;
  printf ", %d x \"%s\"", $errors{$_}, $_ for sort keys %errors;
  print "\n";
} else {
  my ($rate, @request) = @rest;
  my $start = now();
  my $sock = segment_open($host, 3051, $secret, join(' ', @request));
  my $head = <$sock> // '';
  chomp $head;
  binmode $sock;
  my ($bytes, $buf) = (0, '');
  while (my $n = read($sock, $buf, 65536)) {
    $bytes += $n;
    if ($rate > 0) { my $ahead = $bytes / ($rate * 1048576) - (now() - $start); Time::HiRes::sleep($ahead) if $ahead > 0; }
  }
  printf "%s: %s; %.0f MB in %.1f s (%.0f MB/s)\n", join(' ', @request), $head, $bytes / 1048576, now() - $start, $bytes / 1048576 / (now() - $start);
}
PL
} > "$work/scripts/client.pl"
cat > "$work/env.sh" <<EOF2
export ISC_USER=SYSDBA ISC_PASSWORD=masterkey PATH=/opt/firebird/bin:\$PATH SEGMENT_PORT=3051
export DATABASE_PATH=/data/db.fdb ARCHIVE_DIR=/data/a SOURCE_DIR=/data/s REPLICATION_DIR=/data/repl JOURNAL_DIR=/data/j
export PRIMARY_FILE=/data/primary POD_NAME=$NAME-0 SCRIPT_DIR=/w/scripts ALLOW_LIVE_SEED=true
export ISOLATION_TIMEOUT_SECONDS=20 CONTACT_TIMEOUT_SECONDS=$CONTACT_TIMEOUT_SECONDS PEERS_SERVICE=$NAME-peers
EOF2

docker network create "$NAME" >/dev/null
# the primary: its server, a database of 2^DOUBLINGS rows, and the segment server with its isolation check
docker run -d --name "$NAME-0" --network "$NAME" --network-alias "$NAME-peers" -v "$work:/w" --entrypoint bash "$IMAGE" -c '. /w/env.sh
  grep -q "^AuthServer = Srp, Legacy_Auth" /opt/firebird/firebird.conf ||
    printf "AuthServer = Srp, Legacy_Auth\nUserManager = Srp, Legacy_UserManager\nWireCrypt = Enabled\n" >> /opt/firebird/firebird.conf
  mkdir -p /data/a /data/s /data/repl /data/j && echo $POD_NAME > /data/primary && chown -R firebird:firebird /data
  su -s /bin/sh firebird -c "/opt/firebird/bin/firebird" &
  sleep 3
  echo "CREATE DATABASE '\''localhost:/data/db.fdb'\'' USER SYSDBA PASSWORD '\''masterkey'\''; CREATE TABLE app (id INT, filler VARCHAR(4000)); COMMIT;" | isql -q
  wait' >/dev/null
ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$NAME-0")
x() { docker exec -e POD_IP="$ip" "$NAME-0" bash -c ". /w/env.sh; $1"; }
for _ in $(seq 1 30); do x 'echo "select 1 from rdb\$database;" | isql -q localhost:$DATABASE_PATH' 2>/dev/null | grep -q 1 && break; sleep 1; done
x "echo \"set term ^; execute block as declare s varchar(4000) = ''; declare i int = 0; begin
     while (i < 100) do begin s = s || uuid_to_char(gen_uuid()); i = i + 1; end
     insert into app values (1, :s); end^ set term ;^ commit;\" | isql -q localhost:\$DATABASE_PATH
   for n in \$(seq 1 $DOUBLINGS); do echo 'insert into app select id, filler from app; commit;' | isql -q localhost:\$DATABASE_PATH; done
   head -c ${SEGMENT_MB}M /dev/urandom > /data/a/db.fdb.journal-000000001; chown firebird:firebird /data/a/*"
echo "server: $(x 'isql -z </dev/null 2>/dev/null | head -n 1'); database $(x 'du -m $DATABASE_PATH | cut -f1') MB"
docker exec -d -e POD_IP="$ip" "$NAME-0" \
  bash -c '. /w/env.sh; exec su -s /bin/sh firebird -c "perl /w/scripts/segment-server.pl > /data/segment-server.log 2>&1"'
# another instance in DNS (the isolation check fences only a primary that has peers)
docker run -d --name "$NAME-1" --network "$NAME" --network-alias "$NAME-peers" --entrypoint perl "$IMAGE" -MIO::Socket::INET \
  -e 'my $s = IO::Socket::INET->new(LocalPort => 3051, Listen => 5, ReuseAddr => 1) or die; while (my $c = $s->accept) { close $c }' >/dev/null
# the client runs on the host, against the primary's address
c() { ISC_PASSWORD=masterkey perl "$work/scripts/client.pl" "$@"; }
for _ in $(seq 1 20); do c latency "$ip" 1 0 5 PING 2>/dev/null | grep -q '1 answered' && break; sleep 1; done

echo "== 1. request cost"
c latency "$ip" 100 0 5 PING
c latency "$ip" 20 0 10 STATE
echo "== 2. GET of a ${SEGMENT_MB} MB segment"
c download "$ip" 0 GET db.fdb.journal-000000001
echo "== 3. a live seed at ${SEED_RATE_MB} MB/s, with STATE (the Lease holder, 10 s limit) and PING (a puller) every second"
c download "$ip" "$SEED_RATE_MB" SEED > "$work/seed.out" 2>&1 &
seed=$!
sleep 2
ISC_PASSWORD=masterkey perl "$work/scripts/client.pl" latency "$ip" 100000 1 10 STATE > "$work/state.out" 2>&1 &
state=$!
ISC_PASSWORD=masterkey perl "$work/scripts/client.pl" latency "$ip" 100000 1 10 PING > "$work/ping.out" 2>&1 &
ping=$!
wait $seed || true
# the probes end with their summary on TERM
kill -TERM $state $ping 2>/dev/null || true
wait $state $ping 2>/dev/null || true
cat "$work/seed.out" "$work/state.out" "$work/ping.out"
if x 'test -f /data/repl/self-fenced'; then
  echo "the primary was FENCED while it served the seed:"
else
  echo "the primary was not fenced"
fi
x "grep -E 'isolated|fenc|locked copy|served' /data/segment-server.log" || true
echo "database state: $(x "fbsvcmgr localhost:service_mgr action_db_stats dbname \$DATABASE_PATH sts_hdr_pages 2>&1 | grep -ciE 'shutdown' | sed 's/^0\$/online/; s/^[1-9].*/shut down/'")"
