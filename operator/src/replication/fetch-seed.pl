#!/usr/bin/perl
# Fetches a seed copy from the primary's segment server: fetch-seed.pl <host> <target-file>
use strict;
use warnings;
use IO::Socket::INET;
my ($host, $target) = @ARGV;
my $port = $ENV{SEGMENT_PORT} // 3051;
my $sock = IO::Socket::INET->new(PeerHost => $host, PeerPort => $port, Proto => 'tcp', Timeout => 10)
  or die "connect $host:$port: $!\n";
$sock->timeout(600);
print $sock (($ENV{ISC_PASSWORD} // '') . " SEED\n");
my $hdr = <$sock> // '';
die "server: $hdr" unless $hdr =~ /^OK (\d+)/;
my $size = $1;
open(my $out, '>:raw', "$target.part") or die "write $target.part: $!\n";
binmode $sock;
my ($got, $buf) = (0, '');
while ($got < $size) {
  my $n = read($sock, $buf, 65536);
  die "short read ($got of $size bytes)\n" unless $n;
  print $out $buf;
  $got += $n;
}
close $out;
rename("$target.part", $target) or die "rename: $!\n";
print "fetched seed copy ($size bytes) from $host\n";
