#!/usr/bin/perl
# Offline tool: sets the replication sequence of a database that no server has open.
#
#   set-repl-seq.pl <database> <sequence>
#
# Firebird stores the sequence as the HDR_repl_seq header clump (src/jrd/ods.h), which the journal
# (ChangeLog) continues from: its next segment is <sequence> + 1. A promoted replica has no clump
# (its sequence is 0), so without it its journal would restart at segment 1 and the other replicas,
# which applied the old primary's segments up to <sequence>, would skip the new ones.
#
# Header page layout: u16 hdr_page_size at 16, u16 hdr_ods_version at 18 (with the 0x8000
# Firebird flag), then u16 hdr_end (offset of the HDR_end byte) and the clumps {u8 type, u8 length,
# data}: ODS 13 (Firebird 4 and 5) has hdr_end at 66 and clumps from 128, ODS 14 (Firebird 6) has
# hdr_end at 36 and clumps from 148. There is no page checksum.
use strict;
use warnings;

my ($db, $seq) = @ARGV;
die "usage: $0 <database> <sequence>\n" unless defined $seq && $seq =~ /^\d+$/;
use constant { HDR_END => 0, HDR_REPL_SEQ => 11 };
# ODS major version => [offset of hdr_end, start of the clumps]
my %layout = (13 => [66, 128], 14 => [36, 148]);

open(my $fh, '+<:raw', $db) or die "open $db: $!\n";
read($fh, my $head, 18) == 18 or die "short read\n";
my $page_size = unpack('v', substr($head, 16, 2));
die "unexpected page size $page_size\n" unless $page_size >= 4096 && $page_size <= 65536;
seek($fh, 0, 0);
read($fh, my $page, $page_size) == $page_size or die "short read of the header page\n";
die "not a header page\n" unless ord(substr($page, 0, 1)) == 1;   # pag_header

my $ods = unpack('v', substr($page, 18, 2)) & 0x7fff;
die "unsupported on-disk structure $ods (known: " . join(', ', sort keys %layout) . ")\n" unless $layout{$ods};
my ($end_at, $data_start) = @{$layout{$ods}};
my $end = unpack('v', substr($page, $end_at, 2));
my $value = pack('Q<', $seq);
my ($p, $found) = ($data_start, 0);
while ($p < $end) {
  my ($type, $len) = unpack('C C', substr($page, $p, 2));
  last if $type == HDR_END;
  if ($type == HDR_REPL_SEQ) {
    die "unexpected clump length $len\n" unless $len == 8;
    substr($page, $p + 2, 8) = $value;
    $found = 1;
    last;
  }
  $p += 2 + $len;
}
unless ($found) {
  die "no room for the clump on the header page\n" if $end + 11 > $page_size;
  substr($page, $end, 11) = pack('C C', HDR_REPL_SEQ, 8) . $value . pack('C', HDR_END);
  substr($page, $end_at, 2) = pack('v', $end + 10);
}
seek($fh, 0, 0);
print $fh $page or die "write: $!\n";
close($fh) or die "close: $!\n";
print "replication sequence of $db set to $seq\n";
