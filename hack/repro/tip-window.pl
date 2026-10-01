#!/usr/bin/perl
# Counts, for one backup copy, the transactions the journal shows as complete in segments <= S
# that the copy does not hold as committed: the commit/TIP window (ISSUES.md, issue 2).
#
#   tip-window.pl <archive dir> <S> <OAT> <next> <candidates file>
#
# A transaction is "not committed in the copy" when it is a candidate (in [OAT, next) with
# RDB$GET_TRANSACTION_CN <= 0 in the copy) or at or after <next>. Prints
# "window <n> open <m> [ids]": n such transactions whose commit (a BLOCK_END_TRANS block ending
# with opCommitTransaction) is in a segment <= S, m with blocks in segments <= S at all.
use strict;
use warnings;
use constant OP_COMMIT => 3;   # opCommitTransaction, src/jrd/replication/Protocol.h

my ($dir, $base, $oat, $next, $cand_file) = @ARGV;
die "usage: tip-window.pl <dir> <S> <OAT> <next> <candidates>\n" unless defined $cand_file;
my %candidate;
open(my $cf, '<', $cand_file) or die "read $cand_file: $!\n";
while (my $l = <$cf>) { $candidate{$1} = 1 if $l =~ /^\s*(\d+)\s*$/; }
close $cf;
sub open_in_copy { my ($t) = @_; return $t >= $oat && ($t >= $next || $candidate{$t}); }

opendir(my $dh, $dir) or die "read $dir: $!\n";
my @files = grep { /\.journal-(\d+)$/ && $1 <= $base } readdir($dh);
closedir($dh);
my (%seen, %ended);
for my $name (@files) {
  open(my $fh, '<:raw', "$dir/$name") or die "read $name: $!\n";
  read($fh, my $hdr, 48) == 48 or next;
  my (undef, undef, undef, undef, $seq, $length) = unpack('a12 v v a16 Q< Q<', $hdr);
  next if $seq > $base;
  my $pos = 48;
  while ($pos < $length) {
    last unless read($fh, my $blk, 16) == 16;
    my ($tra, undef, $flags, $len) = unpack('Q< v v V', $blk);
    last unless read($fh, my $payload, $len) == $len;
    if ($tra && open_in_copy($tra)) {
      $seen{$tra} = 1;
      # the block ending a transaction ends with its last operation: commit (3) or rollback (4)
      $ended{$tra} = 1 if ($flags & 2) && $len && ord(substr($payload, -1)) == OP_COMMIT;
    }
    $pos += 16 + $len;
  }
  close $fh;
}
my @window = sort { $a <=> $b } keys %ended;
printf "window %d open %d%s\n", scalar(@window), scalar(keys %seen), (@window ? " [" . join(',', @window) . "]" : '');
