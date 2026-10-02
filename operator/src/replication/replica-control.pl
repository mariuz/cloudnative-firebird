#!/usr/bin/perl
# Writes the replica control file for a freshly seeded replica.
#
#   replica-control.pl --adopt <source-control> <db-sequence> <control-file>
#     Seed copied from another replica: keep that replica's position and active transactions
#     (the copy was taken while it had applied everything it received) and set db_sequence to
#     the copy's own header value.
#
#   replica-control.pl [--next <id>] <primary-host> <S> <db-sequence> <control-file> [candidate-id ...]
#     Seed copied from the primary (offline bootstrap seed, or a live locked copy; --next, the
#     copy's next transaction, for live copies):
# The seed copy contains every change journaled in segments <= S, except transactions whose
# commit was journaled before the nbackup lock but whose commit mark (TIP) was written after it
# (Firebird journals the commit before setting the TIP state). Those, and transactions still
# open at lock time, are "candidates": not committed in the copy. The primary's segment server
# reports which candidates actually appear in the journal and where they start; they are
# recorded as active transactions, so the replica server replays exactly their blocks from
# segments <= S and replicates normally after S. Candidates absent from the journal (read-only
# or not yet flushed) are left out: they would otherwise stay "active" forever.
#
# Layout (src/remote/server/ReplServer.cpp, ControlFile::DataV1): char[10] "FBREPLCTL",
# u16 version=1, u32 txn_count, u64 sequence, u32 offset, pad, u64 db_sequence, then txn_count
# x {u64 tra_id, u64 sequence} sorted by tra_id.
use strict;
use warnings;
use IO::Socket::INET;

if (@ARGV && $ARGV[0] eq '--adopt') {
  my (undef, $from, $dbseq, $target) = @ARGV;
  die "usage: replica-control.pl --adopt <source-control> <db-sequence> <control-file>\n" unless defined $target;
  open(my $in, '<:raw', $from) or die "read $from: $!\n";
  local $/;
  my $data = <$in>;
  close $in;
  die "$from is not a replica control file\n" unless length($data) >= 40 && substr($data, 0, 9) eq 'FBREPLCTL';
  substr($data, 32, 8) = pack('Q<', $dbseq);
  open(my $out, '>:raw', "$target.tmp") or die "write $target.tmp: $!\n";
  print $out $data;
  close $out;
  rename("$target.tmp", $target) or die "rename $target: $!\n";
  my (undef, undef, $count, $seq) = unpack('a10 v V Q<', $data);
  print "replica control: adopted position after segment $seq with $count active transaction(s)\n";
  exit 0;
}

my $next;
if (@ARGV && $ARGV[0] eq '--next') { (undef, $next) = splice(@ARGV, 0, 2); }
my ($host, $seq, $dbseq, $target, @candidates) = @ARGV;
die "usage: replica-control.pl [--next <id>] <host> <S> <db-sequence> <control-file> [ids...]\n" unless defined $target;
my $port = $ENV{SEGMENT_PORT} // 3051;

# one request to the primary's segment server; undef when it does not know the command
sub ask {
  my ($line) = @_;
  my $sock = IO::Socket::INET->new(PeerHost => $host, PeerPort => $port, Proto => 'tcp', Timeout => 10)
    or die "connect $host:$port: $!\n";
  $sock->timeout(300);
  print $sock (($ENV{ISC_PASSWORD} // '') . " $line\n");
  my @lines;
  while (my $l = <$sock>) {
    $l =~ s/\r?\n$//;
    last if $l eq '.';
    if ($l =~ /^ERR/) {
      close $sock;
      return undef if $l eq 'ERR bad request';
      die "server: $l\n";
    }
    push @lines, $l;
  }
  close $sock;
  return \@lines;
}

# With --next (live seeds), PLAN also covers transactions numbered <next> or above with blocks in
# segments <= S, and tells whether each first block begins its transaction: one that began in a
# segment no longer archived cannot be replayed whole, so the seed fails rather than replaying it
# partially. An older segment server only knows TXNS (the candidates' first segments).
my %start;
my $plan = defined $next ? ask("PLAN $seq $next " . join(',', @candidates)) : undef;
if ($plan) {
  for (@$plan) {
    next unless /^(\d+) (\d+) ([01])$/;
    die "transaction $1 began before the oldest archived segment of $host ($2 is its first); cannot seed from this copy\n" unless $3;
    $start{$1} = $2;
  }
} elsif (@candidates) {
  my $lines = ask("TXNS $seq " . join(',', @candidates)) // die "server: TXNS not supported\n";
  for (@$lines) { $start{$1} = $2 if /^(\d+) (\d+)$/; }
}

my @active = sort { $a <=> $b } keys %start;
open(my $out, '>:raw', "$target.tmp") or die "write $target.tmp: $!\n";
print $out pack('a10 v V Q< V x4 Q<', 'FBREPLCTL', 1, scalar(@active), $seq, 0, $dbseq);
print $out pack('Q< Q<', $_, $start{$_}) for @active;
close $out;
rename("$target.tmp", $target) or die "rename $target: $!\n";
print "replica control: continue after segment $seq, replay " . scalar(@active) . " transaction(s)"
  . (@active ? " (" . join(', ', map { "$_ from segment $start{$_}" } @active) . ")" : '') . "\n";
