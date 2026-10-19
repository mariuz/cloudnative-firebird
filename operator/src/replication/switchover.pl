#!/usr/bin/perl
# Planned switchover, first phase (run as a Job by the operator):
#
#   1. stop writes on the old primary: full shutdown through its service manager (idempotent).
#      With SWITCHOVER_DELAY_SECONDS (spec.switchoverDelay) this Job first waits up to that long
#      for the primary's remote client attachments to end (MON$ATTACHMENTS every 2 s, as the
#      smart shutdown does; the sidecars attach through localhost and do not count), then shuts
#      the database down, disconnecting the clients still attached. The wait is the Job's own:
#      Firebird's shutdown modes with a timeout do not wait for idle attachments (observed with
#      Firebird 5 and 6). New attachments are not refused meanwhile.
#   2. read its final replication sequence S from the header (through its segment server when the
#      server refuses header statistics in full shutdown, as Firebird 6 does);
#   3. wait until the target and every other ready replica has applied everything up to S
#      (POSITION on their segment servers: control file at S or beyond, nothing pending).
#
#   4. with PROMOTE_IN_PLACE=true, promote the target without a restart (its segment server's
#      PROMOTE, ARCHIVED: the journal archive's last segment or "none").
#
# Exits 0 when the target can be promoted, with "inplace <S>" in its result (RESULT_FILE, the
# termination message) when step 4 promoted it, or "restart <reason>" otherwise. The operator then
# moves the primary and restarts the old primary, whose init container demotes it offline, and
# (without "inplace") the target, whose init container promotes it offline (init-instance.sh).
# On failure the operator brings the old primary back online.
#
# Environment: OLD_PRIMARY, TARGET, REPLICAS (space separated hosts), DATABASE_PATH,
# ISC_USER / ISC_PASSWORD, SEGMENT_PORT, TIMEOUT_SECONDS, SWITCHOVER_DELAY_SECONDS,
# PROMOTE_IN_PLACE, ARCHIVED, RESULT_FILE.
use strict;
use warnings;
use IO::Socket::INET;
#@include segment-auth.pl

my $old     = $ENV{OLD_PRIMARY} or die "OLD_PRIMARY is required\n";
my $target  = $ENV{TARGET} or die "TARGET is required\n";
my @others  = grep { length } split /\s+/, ($ENV{REPLICAS} // '');
my $db      = $ENV{DATABASE_PATH} or die "DATABASE_PATH is required\n";
my $token   = $ENV{ISC_PASSWORD} // '';
my $port    = $ENV{SEGMENT_PORT} // 3051;
my $timeout = $ENV{TIMEOUT_SECONDS} // 300;
my $delay   = $ENV{SWITCHOVER_DELAY_SECONDS} // 0;
$delay = 0 unless $delay =~ /^\d+$/;
my $poll    = $ENV{SWITCHOVER_POLL_SECONDS} // 2;
$| = 1;

# The old primary's remote client attachments (not this Job's, not the sidecars' through
# localhost), or undef and isql's output when they cannot be counted
sub remote_attachments {
  my $sql = "SELECT 'remote=' || COUNT(*) FROM MON\$ATTACHMENTS WHERE MON\$SYSTEM_FLAG = 0"
    . " AND MON\$ATTACHMENT_ID <> CURRENT_CONNECTION AND MON\$REMOTE_ADDRESS IS NOT NULL"
    . " AND MON\$REMOTE_ADDRESS NOT STARTING WITH '127.' AND MON\$REMOTE_ADDRESS <> '::1'"
    . " AND MON\$REMOTE_ADDRESS NOT STARTING WITH '::ffff:127.';";
  # single-quoted for the shell (its $ signs must reach isql); the query's own quotes as '\''
  (my $quoted = $sql) =~ s/'/'\\''/g;
  my $out = `echo '$quoted' | isql -q '$old:$db' 2>&1`;
  return $out =~ /remote=(\d+)/ ? ($1, undef) : (undef, $out);
}

# Header statistics of the old primary through its service manager; undef when the server refuses
# them because the database is in full shutdown (Firebird 6: "database ... shutdown")
sub header {
  my $out = `fbsvcmgr "$old:service_mgr" action_db_stats dbname "$db" sts_hdr_pages 2>&1`;
  return $out unless $?;
  return undef if $out =~ /^database .* shutdown\s*$/m;
  die "cannot read the header of $old: $out";
}

sub request {
  my ($host, $line) = @_;
  my $sock = eval { segment_open($host, $port, $token, $line) } or return undef;
  $sock->timeout(60);
  my @lines;
  while (my $l = <$sock>) { $l =~ s/\r?\n$//; push @lines, $l; last if $l eq '.' || $l =~ /^(OK|ERR)/; }
  close $sock;
  return \@lines;
}

sub wait_until {
  my ($what, $check) = @_;
  my $deadline = time + $timeout;
  while (time < $deadline) {
    return 1 if $check->();
    sleep 2;
  }
  die "timed out waiting for $what\n";
}

# 1. stop writes
my $before = header();
if (!defined $before || $before =~ /full shutdown/) {
  print "$old already shut down\n";
} else {
  my $started = time;
  my $left;
  if ($delay > 0) {
    print "stopping writes on $old: its clients have up to ${delay}s to finish\n";
    my $deadline = $started + $delay;
    while (1) {
      my $error;
      ($left, $error) = remote_attachments();
      if (!defined $left) { (my $why = $error // '') =~ s/\s+/ /g; print "cannot count the clients of $old ($why); waiting\n"; }
      elsif ($left == 0) { print "no client attached to $old\n"; last; }
      my $remaining = $deadline - time;
      if ($remaining <= 0) {
        print(defined $left ? "$left client(s) still attached to $old after ${delay}s: disconnecting them\n" : "the clients of $old could not be counted for ${delay}s: stopping writes\n");
        last;
      }
      print "$left client(s) attached to $old, waiting (${remaining}s left)\n" if defined $left;
      sleep($remaining < $poll ? $remaining : $poll);
    }
  }
  system('fbsvcmgr', "$old:service_mgr", 'action_properties', 'dbname', $db,
    'prp_shutdown_mode', 'prp_sm_full', 'prp_force_shutdown', '0') == 0
    or die "cannot shut down $old\n";
  my $outcome = !defined $left ? 'its clients were not counted' : $left ? "$left client(s) disconnected" : 'its clients disconnected by themselves';
  print "writes stopped: $old is in full shutdown" . ($delay > 0 ? " after " . (time - $started) . "s ($outcome)" : '') . "\n";
}

# 2. final sequence (no header entry while it is 0); from the header page on disk, through the old
# primary's segment server, when its server refuses header statistics in full shutdown
my $stats = header();
my $final;
if (defined $stats) {
  ($final) = $stats =~ /Replication sequence:\s*(\d+)/;
  $final //= 0;
} else {
  my $r = request($old, 'HEADER') or die "cannot reach the segment server of $old\n";
  ($final) = ($r->[0] // '') =~ /^OK (\d+)$/ or die "cannot read the header of $old: " . ($r->[0] // 'no reply') . "\n";
}
print "final replication sequence of $old: $final\n";

# 3. replicas caught up (applied up to S also means archived; a primary just promoted and not
# written to has no segment S, its journal starting after S)
for my $host ($target, @others) {
  wait_until("$host to apply segment $final", sub {
    my $r = request($host, 'POSITION') or return 0;
    # the target promoted in place by an earlier run of this Job
    return 1 if $host eq $target && ($r->[0] // '') eq 'OK primary';
    my ($seq, $offset, $pending) = ($r->[0] // '') =~ /^OK (\d+) (\d+) (\d+)$/ or return 0;
    return $seq >= $final && $pending == 0;
  });
  print "$host has applied everything up to segment $final\n";
}
print "switchover ready: promote $target after segment $final\n";

# 4. promote the target in place; any failure leaves it a replica for the offline promotion
my $outcome = 'restart in-place promotion not requested';
if (($ENV{PROMOTE_IN_PLACE} // '') eq 'true') {
  my $archived = ($ENV{ARCHIVED} // '') =~ /^(\d+)$/ ? $1 : 'none';
  my $r = request($target, "PROMOTE $archived");
  my $reply = $r ? ($r->[0] // 'no reply') : 'segment server not reachable';
  if ($reply =~ /^OK (\d+)$/) {
    $outcome = "inplace $1";
    print "$target promoted in place: its journal continues after segment $1\n";
  } else {
    $outcome = "restart $reply";
    print "in-place promotion of $target failed ($reply): it is promoted offline instead\n";
  }
}
my $result = $ENV{RESULT_FILE} // '/dev/termination-log';
if (open(my $fh, '>', $result)) { print $fh $outcome; close $fh; }
