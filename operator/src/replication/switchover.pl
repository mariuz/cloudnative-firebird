#!/usr/bin/perl
# Planned switchover, first phase (run as a Job by the operator):
#
#   1. stop writes on the old primary: full shutdown through its service manager (idempotent);
#   2. read its final replication sequence S from the header;
#   3. wait until segment S is archived on the old primary;
#   4. wait until the target and every other ready replica has applied everything up to S
#      (POSITION on their segment servers: control file at S or beyond, nothing pending).
#
# Exits 0 when the target can be promoted: the operator then moves the primary and restarts the
# target and the old primary, whose init containers promote and demote them offline
# (init-instance.sh). On failure the operator brings the old primary back online.
#
# Environment: OLD_PRIMARY, TARGET, REPLICAS (space separated hosts), DATABASE_PATH,
# ISC_USER / ISC_PASSWORD, SEGMENT_PORT, TIMEOUT_SECONDS.
use strict;
use warnings;
use IO::Socket::INET;

my $old     = $ENV{OLD_PRIMARY} or die "OLD_PRIMARY is required\n";
my $target  = $ENV{TARGET} or die "TARGET is required\n";
my @others  = grep { length } split /\s+/, ($ENV{REPLICAS} // '');
my $db      = $ENV{DATABASE_PATH} or die "DATABASE_PATH is required\n";
my $token   = $ENV{ISC_PASSWORD} // '';
my $port    = $ENV{SEGMENT_PORT} // 3051;
my $timeout = $ENV{TIMEOUT_SECONDS} // 300;
my $name_re = qr/^[A-Za-z0-9._-]+\.journal-(\d+)$/;
$| = 1;

sub header {
  my $out = `fbsvcmgr "$old:service_mgr" action_db_stats dbname "$db" sts_hdr_pages 2>&1`;
  die "cannot read the header of $old: $out" if $?;
  return $out;
}

sub request {
  my ($host, $line) = @_;
  my $sock = IO::Socket::INET->new(PeerHost => $host, PeerPort => $port, Proto => 'tcp', Timeout => 10)
    or return undef;
  $sock->timeout(60);
  print $sock "$token $line\n";
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
if (header() =~ /full shutdown/) {
  print "$old already shut down\n";
} else {
  system('fbsvcmgr', "$old:service_mgr", 'action_properties', 'dbname', $db,
    'prp_shutdown_mode', 'prp_sm_full', 'prp_force_shutdown', '0') == 0
    or die "cannot shut down $old\n";
  print "writes stopped: $old is in full shutdown\n";
}

# 2. final sequence (no header entry while it is 0)
my ($final) = header() =~ /Replication sequence:\s*(\d+)/;
$final //= 0;
print "final replication sequence of $old: $final\n";

# 3. the last segment is archived when the journal is closed
if ($final > 0) {
  wait_until("segment $final to be archived on $old", sub {
    my $list = request($old, 'LIST') or return 0;
    return grep { $_ =~ $name_re && $1 >= $final } @$list;
  });
}

# 4. replicas caught up
for my $host ($target, @others) {
  wait_until("$host to apply segment $final", sub {
    my $r = request($host, 'POSITION') or return 0;
    my ($seq, $offset, $pending) = ($r->[0] // '') =~ /^OK (\d+) (\d+) (\d+)$/ or return 0;
    return $seq >= $final && $pending == 0;
  });
  print "$host has applied everything up to segment $final\n";
}
print "switchover ready: promote $target after segment $final\n";
