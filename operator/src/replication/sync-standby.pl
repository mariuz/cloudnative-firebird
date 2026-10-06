#!/usr/bin/perl
# Synchronous replication: attaches or detaches the synchronous standby (run as a Job by the
# operator, replication.mode sync).
#
# A replica cannot receive the same changes from the journal and synchronously: it would apply
# them twice. The standby therefore switches between the two at a point where both agree, the end
# of the primary's last segment while its database is in full shutdown (no transaction open), and
# Firebird reads sync_replica when the database is opened again.
#
# attach:
#   1. stop writes on the primary: full shutdown (idempotent);
#   2. read its final replication sequence S, wait until the standby has applied everything up to
#      S (POSITION);
#   3. SYNC <standby> (and OTHERS) on the primary (sync_replica in the file replication.conf includes), then
#      STANDBY on (the standby stops applying journal segments);
#   4. bring the primary back online: from its next attachment on, every commit is applied on the
#      standby before it completes.
# detach:
#   1. stop writes on the primary, read its final sequence S;
#   2. STANDBY off S on the standby (its replica control file and segment puller continue after S),
#      then SYNC OTHERS (none without) on the primary; a standby that cannot be reached is reported, and the
#      operator re-seeds it;
#   3. bring the primary back online.
#
# On failure the primary is brought back online, synchronous replication off (detach), or as it
# was (attach). The outcome goes to the termination message: "attached", "detached",
# "detached unreachable", "failed clean" (nothing changed on the standby) or "failed".
#
# With several synchronous standbys (synchronous.number), OTHERS lists the ones that stay
# attached: every SYNC names them too, so only STANDBY changes.
#
# Environment: ACTION (attach | detach), PRIMARY, STANDBY (hosts), OTHERS (comma-separated
# hosts, may be empty), DATABASE_PATH, ISC_USER / ISC_PASSWORD, SEGMENT_PORT, TIMEOUT_SECONDS,
# RESULT_FILE.
use strict;
use warnings;
use IO::Socket::INET;
#@include segment-auth.pl

my $action  = $ENV{ACTION} // '';
my $primary = $ENV{PRIMARY} or die "PRIMARY is required\n";
my $standby = $ENV{STANDBY} or die "STANDBY is required\n";
my $db      = $ENV{DATABASE_PATH} or die "DATABASE_PATH is required\n";
my $token   = $ENV{ISC_PASSWORD} // '';
my $port    = $ENV{SEGMENT_PORT} // 3051;
my $timeout = $ENV{TIMEOUT_SECONDS} // 300;
my $result  = $ENV{RESULT_FILE} // '/dev/termination-log';
my @others  = grep { length } split /,/, ($ENV{OTHERS} // '');
# the SYNC argument: the standbys that stay attached, with or without this one
sub sync_list { my @h = (@others, @_); return @h ? join(',', @h) : 'none'; }
die "ACTION must be attach or detach\n" unless $action eq 'attach' || $action eq 'detach';
$| = 1;

sub request {
  my ($host, $line) = @_;
  my $sock = eval { segment_open($host, $port, $token, $line) } or return undef;
  $sock->timeout(60);
  my @lines;
  while (my $l = <$sock>) { $l =~ s/\r?\n$//; push @lines, $l; last if $l eq '.' || $l =~ /^(OK|ERR)/; }
  close $sock;
  return \@lines;
}

sub ok { my ($host, $line) = @_; my $r = request($host, $line); return $r && ($r->[0] // '') eq 'OK'; }

sub wait_until {
  my ($what, $check) = @_;
  my $deadline = time + $timeout;
  my $next_report = time + 10;
  while (time < $deadline) {
    return 1 if $check->();
    if (time >= $next_report) { print "waiting for $what\n"; $next_report = time + 10; }
    sleep 2;
  }
  die "timed out waiting for $what\n";
}

# Header statistics of the primary; undef when the server refuses them in full shutdown (Firebird 6)
sub header {
  my $out = `fbsvcmgr "$primary:service_mgr" action_db_stats dbname "$db" sts_hdr_pages 2>&1`;
  return $out unless $?;
  return undef if $out =~ /^database .* shutdown\s*$/m;
  die "cannot read the header of $primary: $out";
}

sub online {
  system('fbsvcmgr', "$primary:service_mgr", 'action_properties', 'dbname', $db,
    'prp_online_mode', 'prp_sm_normal') == 0 or print "could not bring $primary back online\n";
}

sub report {
  my ($outcome) = @_;
  print "$outcome\n";
  if (open(my $fh, '>', $result)) { print $fh $outcome; close $fh; }
}

# 1. stop writes
my $before = header();
if (!defined $before || $before =~ /full shutdown/) {
  print "$primary already shut down\n";
} else {
  system('fbsvcmgr', "$primary:service_mgr", 'action_properties', 'dbname', $db,
    'prp_shutdown_mode', 'prp_sm_full', 'prp_force_shutdown', '0') == 0
    or die "cannot shut down $primary\n";
  print "writes stopped: $primary is in full shutdown\n";
}

my $standby_on;   # the sequence the standby stopped applying the journal at
my $outcome = eval {
  # 2. final sequence: from the header page on disk, through the segment server (Firebird 6
  # refuses header statistics in full shutdown)
  my $r = request($primary, 'HEADER') or die "cannot reach the segment server of $primary\n";
  my ($final) = ($r->[0] // '') =~ /^OK (\d+)$/ or die "cannot read the header of $primary: " . ($r->[0] // 'no reply') . "\n";
  print "final replication sequence of $primary: $final\n";

  if ($action eq 'detach') {
    # the standby first: its segment puller applies the journal again only once the primary no
    # longer names it (SYNCTO), and nothing after segment S exists while the primary is shut down
    my $reached = ok($standby, "STANDBY off $final");
    if (!ok($primary, 'SYNC ' . sync_list())) {
      # still attached: the standby must not apply the journal (the primary is still shut down)
      ok($standby, 'STANDBY on') if $reached;
      die "cannot turn synchronous replication off on $primary\n";
    }
    print "synchronous replication off on $primary\n";
    if ($reached) {
      print "$standby continues from the journal after segment $final\n";
      return 'detached';
    }
    print "$standby cannot be reached: it has to be re-seeded\n";
    return 'detached unreachable';
  }

  # Applied up to S also means archived: a segment with changes is applied only once archived. A
  # primary just promoted and not written to has no segment S at all (its journal starts after
  # S), so waiting for segment S to be archived would never end.
  wait_until("$standby to apply segment $final", sub {
    my $p = request($standby, 'POSITION') or return 0;
    my ($seq, $offset, $pending) = ($p->[0] // '') =~ /^OK (\d+) (\d+) (\d+)$/ or return 0;
    return $seq >= $final && $pending == 0;
  });
  print "$standby has applied everything up to segment $final\n";
  # the primary first: the standby's segment puller stops applying the journal only once the
  # primary names it (SYNCTO) and it is the standby
  ok($primary, 'SYNC ' . sync_list($standby)) or die "cannot turn synchronous replication on on $primary\n";
  print "synchronous replication to $standby on $primary (from its next opening)\n";
  $standby_on = $final;
  ok($standby, 'STANDBY on') or die "$standby refused to become the synchronous standby\n";
  return 'attached';
};
my $error = $@;
if (!defined $outcome) {
  # back as it was: no synchronous standby; "failed clean" tells the operator that the standby
  # never received a change synchronously, anything else that it has to be re-seeded
  my $clean = 1;
  if ($action eq 'attach') {
    ok($primary, 'SYNC ' . sync_list()) or $clean = 0;
    if (defined $standby_on) { ok($standby, "STANDBY off $standby_on") or $clean = 0; }
  }
  online();
  report($clean ? 'failed clean' : 'failed');
  die $error;
}
online();
print "$primary is online\n";
report($outcome);
