#!/usr/bin/perl
# Automatic failover, election phase (run as a Job by the operator once the primary has been
# unavailable for failover.delaySeconds):
#
#   1. give every candidate replica a moment to apply the segments it already received
#      (POSITION: nothing pending), then read its applied position (replica control file);
#   2. pick the most advanced candidate (ties: the first listed, i.e. the lowest ordinal).
#
# The result goes to the container termination message, which the operator reads from the pod
# status: "target=<host> sequence=<S> positions=<host>:<seq>,...". Nothing is changed on any
# instance here, so an election for a primary that recovers meanwhile is simply discarded.
#
# Environment: CANDIDATES (space separated hosts), ISC_PASSWORD, SEGMENT_PORT, SETTLE_SECONDS.
use strict;
use warnings;
use IO::Socket::INET;
#@include segment-auth.pl

my @candidates = grep { length } split /\s+/, ($ENV{CANDIDATES} // '');
die "no candidate replicas\n" unless @candidates;
my $token  = $ENV{ISC_PASSWORD} // '';
my $port   = $ENV{SEGMENT_PORT} // 3051;
my $settle = $ENV{SETTLE_SECONDS} // 60;
my $result = $ENV{RESULT_FILE} // '/dev/termination-log';
$| = 1;

sub position {
  my ($host) = @_;
  my $sock = eval { segment_open($host, $port, $token, 'POSITION') } or return undef;
  $sock->timeout(30);
  my $line = <$sock> // '';
  close $sock;
  return $line =~ /^OK (\d+) (\d+) (\d+)/ ? { sequence => $1, offset => $2, pending => $3 } : undef;
}

my %pos;
my $deadline = time + $settle;
for my $host (@candidates) {
  my $p;
  while (1) {
    $p = position($host);
    last if $p && $p->{pending} == 0;
    last if time >= $deadline;
    sleep 2;
  }
  if ($p) {
    $pos{$host} = $p->{sequence};
    print "$host: applied up to segment $p->{sequence}" . ($p->{pending} ? " ($p->{pending} segment(s) still pending)" : '') . "\n";
  } else {
    print "$host: no position (unreachable or not a replica), not a candidate\n";
  }
}
my @ranked = grep { exists $pos{$_} } @candidates;
die "no candidate reported a position\n" unless @ranked;
my $best = $ranked[0];
for my $host (@ranked) { $best = $host if $pos{$host} > $pos{$best}; }   # ties keep the listed order
my $summary = "target=$best sequence=$pos{$best} positions=" . join(',', map { "$_:$pos{$_}" } @ranked);
print "$summary\n";
if (open(my $fh, '>', $result)) { print $fh $summary; close $fh; }
