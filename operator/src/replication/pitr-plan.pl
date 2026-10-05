#!/usr/bin/perl
# Point-in-time recovery: plans the replay of archived journal segments onto a restored nbackup
# chain, and writes the replica control file for it.
#
#   pitr-plan.pl <segment dir> <S> <OAT> <next> <candidates file> <L> <control file>
#
# <S> is the replication sequence in the restored database's header: the nbackup lock switches
# the journal (BEGIN BACKUP, src/jrd/nbak.cpp), so changes journaled after the lock go to segments
# after S, and every transaction committed in the copy has all its blocks in segments <= S.
# <OAT> and <next> are the copy's oldest active and next transaction; <candidates file> lists the
# transactions in [OAT, next) that are not committed in the copy (RDB$GET_TRANSACTION_CN <= 0).
# A transaction is "open in the copy" when it is a candidate or started at or after <next>.
#
# The segments in <segment dir> must be contiguous and end at <L>, the last segment to apply.
# Open transactions with blocks in segments <= S are recorded as active transactions in the
# control file, with the segment of their first block, so the replica server replays exactly
# their blocks from segments <= S and applies every segment after S; segments <= S are not
# applied otherwise.
#
# Every open transaction must be complete in the directory: its first block (BLOCK_BEGIN_TRANS)
# present. If one is not, the earlier segments are needed: the script prints "need <k>", k being
# the segment just before the first one present, and exits with 3. Any other problem exits with 1.
#
# Segment layout (src/jrd/replication/ChangeLog.h, Protocol.h): a 48-byte header {char[12]
# signature, u16 version, u16 state, guid[16], u64 sequence, u64 length}, then blocks {u64
# traNumber, u16 protocol, u16 flags, u32 length} + payload up to the header length.
# Control file layout: see replica-control.pl.
use strict;
use warnings;

use constant { BLOCK_BEGIN_TRANS => 1, EXIT_NEED => 3 };
# die exits with errno, which could be 3: always 1
$SIG{__DIE__} = sub { print STDERR $_[0]; exit 1; };

#   pitr-plan.pl --reposition <U> <control file>
# Across a failover (lineage switch): the replay stopped at the end of the promoted replica's
# segment P; the next lineage continues after the archive's segment U. Rewrites the control file
# to continue after U with no active transaction (those still open at P were never committed in
# the new lineage; stopping the server rolled them back), keeping db_sequence.
if (@ARGV && $ARGV[0] eq '--reposition') {
  my (undef, $seq, $control) = @ARGV;
  die "usage: pitr-plan.pl --reposition <U> <control file>\n" unless defined $control && $seq =~ /^\d+$/;
  open(my $in, '<:raw', $control) or die "read $control: $!\n";
  local $/;
  my $data = <$in>;
  close $in;
  die "$control is not a replica control file\n" unless length($data) >= 40 && substr($data, 0, 9) eq 'FBREPLCTL';
  my ($magic, $version) = unpack('a10 v', $data);
  my $dbseq = unpack('Q<', substr($data, 32, 8));
  open(my $out, '>:raw', "$control.tmp") or die "write $control.tmp: $!\n";
  print $out pack('a10 v V Q< V V Q<', $magic, $version, 0, $seq, 0, 0, $dbseq);
  close $out;
  rename("$control.tmp", $control) or die "rename $control: $!\n";
  print "the replay continues after segment $seq\n";
  exit 0;
}

my ($dir, $base, $oat, $next, $cand_file, $last, $control) = @ARGV;
die "usage: pitr-plan.pl <dir> <S> <OAT> <next> <candidates> <L> <control>\n" unless defined $control;
for ($base, $oat, $next, $last) { die "not a number: $_\n" unless /^\d+$/; }
die "the last segment ($last) is before the backup's segment ($base)\n" if $last < $base;

my %candidate;
open(my $cf, '<', $cand_file) or die "read $cand_file: $!\n";
while (my $l = <$cf>) { $candidate{$1} = 1 if $l =~ /^\s*(\d+)\s*$/; }
close $cf;
sub open_in_copy { my ($t) = @_; return $t >= $oat && ($t >= $next || $candidate{$t}); }

opendir(my $dh, $dir) or die "read $dir: $!\n";
my %files;
for my $name (readdir($dh)) {
  next unless $name =~ /\.journal-(\d+)$/ && -f "$dir/$name";
  my $seq = $1 + 0;
  next if $seq > $last;
  die "two files for segment $seq\n" if $files{$seq};
  $files{$seq} = $name;
}
closedir($dh);
my @seqs = sort { $a <=> $b } keys %files;
die "segment $last is not available\n" unless @seqs && $seqs[-1] == $last;
for my $i (1 .. $#seqs) {
  die "segment " . ($seqs[$i - 1] + 1) . " is missing\n" if $seqs[$i] != $seqs[$i - 1] + 1;
}
my $first_present = $seqs[0];
if ($first_present > $base && $base > 0) { print "need " . ($first_present - 1) . "\n"; exit EXIT_NEED; }

my (%first, %begins);
for my $seq (@seqs) {
  my $path = "$dir/$files{$seq}";
  open(my $fh, '<:raw', $path) or die "read $path: $!\n";
  read($fh, my $hdr, 48) == 48 or die "$path: short header\n";
  my ($sig, undef, undef, undef, $hseq, $length) = unpack('a12 v v a16 Q< Q<', $hdr);
  die "$path is not a journal segment\n" unless substr($sig, 0, 11) eq 'FBCHANGELOG';
  die "$path holds segment $hseq\n" unless $hseq == $seq;
  my $pos = 48;
  while ($pos < $length) {
    read($fh, my $blk, 16) == 16 or die "$path: truncated block at $pos\n";
    my ($tra, undef, $flags, $len) = unpack('Q< v v V', $blk);
    if ($len) {
      seek($fh, $len, 1) or die "$path: seek: $!\n";
      if ($tra) {
        if (!exists $first{$tra}) {
          $first{$tra} = $seq;
          $begins{$tra} = ($flags & BLOCK_BEGIN_TRANS) ? 1 : 0;
        }
        if ($seq > $base && !open_in_copy($tra)) {
          die "transaction $tra is complete in the backup but has changes in segment $seq, after its segment $base\n";
        }
      }
    }
    $pos += 16 + $len;
  }
  close $fh;
}

my @incomplete = grep { open_in_copy($_) && !$begins{$_} } sort { $a <=> $b } keys %first;
if (@incomplete) {
  print "need " . ($first_present - 1) . "\n";
  print STDERR "transaction(s) " . join(', ', @incomplete) . " started before segment $first_present\n";
  exit EXIT_NEED;
}

my @active = sort { $a <=> $b } grep { open_in_copy($_) && $first{$_} <= $base } keys %first;
open(my $out, '>:raw', "$control.tmp") or die "write $control.tmp: $!\n";
print $out pack('a10 v V Q< V x4 Q<', 'FBREPLCTL', 1, scalar(@active), $base, 0, $base);
print $out pack('Q< Q<', $_, $first{$_}) for @active;
close $out or die "write $control.tmp: $!\n";
rename("$control.tmp", $control) or die "rename $control: $!\n";
print "replay: " . ($last > $base ? "segments " . ($base + 1) . " to $last, and " : '') . scalar(@active) . " transaction(s) open in the backup"
  . (@active ? " (" . join(', ', map { "$_ from segment $first{$_}" } @active) . ")" : '') . "\n";
