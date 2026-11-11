#!/bin/bash
# Compares the segment pullers (docs/rust-agent-roadmap.md, Phases 2 and 3) on one archive that
# gets a new segment every INTERVAL seconds, with three replicas side by side:
#   perl: segment-puller.pl polling a Perl segment server (segment-server.pl)
#   rust: `fb-agent puller` polling the same Perl segment server
#   wait: `fb-agent puller` waiting on the segment server's front (`fb-agent segment-server`)
# Reports for each how long a segment took from the primary's archive to the replica's journal
# source directory, and the puller's peak memory and CPU time.
#
#   hack/bench/puller.sh                                  # firebirdsql/firebird:5, POLL_SECONDS=5
#   POLL_SECONDS=1 SEGMENTS=60 SEGMENT_MB=16 hack/bench/puller.sh
#
# Needs Docker, perl with Time::HiRes on the host, and the fb-agent binary: FB_AGENT_BIN, or a
# musl build in operator/agent/target (cargo build --release --target x86_64-unknown-linux-musl).
set -eu
IMAGE=${IMAGE:-firebirdsql/firebird:5}
NAME=${NAME:-fbpull}
POLL_SECONDS=${POLL_SECONDS:-5}
SEGMENTS=${SEGMENTS:-30}
SEGMENT_MB=${SEGMENT_MB:-16}
INTERVAL=${INTERVAL:-2}
here=$(cd "$(dirname "$0")" && pwd)
src=$(cd "$here/../../operator/src/replication" && pwd)
agent=${FB_AGENT_BIN:-$here/../../operator/agent/target/x86_64-unknown-linux-musl/release/fb-agent}
[ -x "$agent" ] || { echo "no fb-agent binary at $agent (set FB_AGENT_BIN)"; exit 1; }
work=$(mktemp -d)
cleanup() {
  docker rm -f "$NAME-primary" "$NAME-front" "$NAME-perl" "$NAME-rust" "$NAME-wait" >/dev/null 2>&1 || true
  docker network rm "$NAME" >/dev/null 2>&1 || true
  docker run --rm -v "$work:/w" --entrypoint sh "$IMAGE" -c 'rm -rf /w/*' >/dev/null 2>&1 || true
  rmdir "$work" 2>/dev/null || true
}
trap cleanup EXIT
chmod 755 "$work"
mkdir -p "$work/scripts" "$work/primary/a" "$work/primary/s" "$work/primary/repl" "$work/primary/repl-front" "$work/perl/s" "$work/rust/s" "$work/wait/s"
for name in segment-server.pl segment-puller.pl; do
  awk -v dir="$src" '/^#@include [^ ]+$/ { while ((getline line < (dir "/" $2)) > 0) print line; close(dir "/" $2); next } { print }' \
    "$src/$name" > "$work/scripts/$name"
done
cp "$agent" "$work/scripts/fb-agent"
echo "$NAME-primary" > "$work/primary/repl/primary"
echo "$NAME-primary" > "$work/perl/primary"
echo "$NAME-primary" > "$work/rust/primary"
echo "$NAME-front" > "$work/primary/repl-front/primary"
echo "$NAME-front" > "$work/wait/primary"

docker network create "$NAME" >/dev/null
docker run -d --name "$NAME-primary" --network "$NAME" -v "$work:/w" --entrypoint perl \
  -e ISC_PASSWORD=masterkey -e ARCHIVE_DIR=/w/primary/a -e DATABASE_PATH=/w/primary/db.fdb -e SOURCE_DIR=/w/primary/s \
  -e REPLICATION_DIR=/w/primary/repl -e PRIMARY_FILE=/w/primary/repl/primary -e POD_NAME="$NAME-primary" \
  "$IMAGE" /w/scripts/segment-server.pl >/dev/null
# the same archive behind the front
docker run -d --name "$NAME-front" --network "$NAME" -v "$work:/w" --entrypoint /w/scripts/fb-agent \
  -e ISC_PASSWORD=masterkey -e ARCHIVE_DIR=/w/primary/a -e DATABASE_PATH=/w/primary/db.fdb -e SOURCE_DIR=/w/primary/s \
  -e REPLICATION_DIR=/w/primary/repl-front -e PRIMARY_FILE=/w/primary/repl-front/primary -e POD_NAME="$NAME-front" \
  "$IMAGE" segment-server -- perl /w/scripts/segment-server.pl >/dev/null
replica() { # <perl|rust|wait> <command...>
  local kind=$1; shift
  docker run -d --name "$NAME-$kind" --network "$NAME" -v "$work:/w" --entrypoint "$1" \
    -e ISC_PASSWORD=masterkey -e SOURCE_DIR="/w/$kind/s" -e STATE_FILE="/w/$kind/.last-pulled" -e PRIMARY_FILE="/w/$kind/primary" \
    -e REPLICATION_DIR="/w/$kind" -e POD_NAME="$NAME-$kind" -e POLL_SECONDS="$POLL_SECONDS" \
    "$IMAGE" "${@:2}" >/dev/null
}
replica perl perl /w/scripts/segment-puller.pl
replica rust /w/scripts/fb-agent puller
replica wait /w/scripts/fb-agent puller
sleep 2

# the host writes the segments into the primary's archive and watches them arrive
perl -MTime::HiRes=time,sleep -e '
  my ($work, $count, $mb, $interval) = @ARGV;
  my (%made, %seen);
  my $data = join("", map { chr(int(rand(256))) } 1 .. 1048576) x $mb;
  my $next = time;
  my $n = 0;
  while (1) {
    if ($n < $count && time >= $next) {
      $n++;
      my $name = sprintf("db.fdb.journal-%09d", $n);
      open(my $fh, ">:raw", "$work/primary/a/.$name") or die "write: $!";
      print $fh $data; close $fh;
      rename("$work/primary/a/.$name", "$work/primary/a/$name") or die "rename: $!";
      $made{$name} = time;
      $next += $interval;
    }
    for my $kind (qw(perl rust wait)) {
      for my $name (keys %made) { $seen{$kind}{$name} //= time if -e "$work/$kind/s/$name"; }
    }
    last if $n >= $count && !grep { keys(%{$seen{$_} // {}}) < $count } qw(perl rust wait);
    die "timed out\n" if time - $next > 120;
    sleep 0.02;
  }
  for my $kind (qw(perl rust wait)) {
    my @d = sort { $a <=> $b } map { $seen{$kind}{$_} - $made{$_} } keys %made;
    my $sum = 0; $sum += $_ for @d;
    printf "%s puller: %d segments of %d MB; delay median %.2f s, mean %.2f s, longest %.2f s\n", $kind, scalar @d, $mb, $d[$#d / 2], $sum / @d, $d[-1];
  }
' "$work" "$SEGMENTS" "$SEGMENT_MB" "$INTERVAL"

echo "server: $(docker run --rm --entrypoint sh "$IMAGE" -c 'isql -z </dev/null 2>/dev/null | head -n 1'); poll every ${POLL_SECONDS}s, a segment every ${INTERVAL}s"
for kind in perl rust wait; do
  # the puller is the container's PID 1: its peak resident memory, and its CPU time in clock ticks (100 per second)
  docker exec "$NAME-$kind" sh -c 'awk "/VmHWM/ { printf \"peak memory %.1f MB\", \$2 / 1024 }" /proc/1/status; awk "{ printf \", CPU %.2f s user + %.2f s system\n\", \$14 / 100, \$15 / 100 }" /proc/1/stat' | sed "s/^/$kind puller: /"
done
