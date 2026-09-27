#!/usr/bin/perl
# Fetches a seed copy from an instance's segment server:
#   fetch-seed.pl <host> <target-file>
# Writes the database to <target-file>, the replica control file (if the source sent one) to
# <target-file>.ctl and the seed kind (replica | offline | live) to <target-file>.kind.
use strict;
use warnings;
use IO::Socket::INET;

my ($host, $target) = @ARGV;
die "usage: fetch-seed.pl <host> <target-file>\n" unless defined $target;
my $port = $ENV{SEGMENT_PORT} // 3051;
my $sock = IO::Socket::INET->new(PeerHost => $host, PeerPort => $port, Proto => 'tcp', Timeout => 10)
  or die "connect $host:$port: $!\n";
$sock->timeout(900);
print $sock (($ENV{ISC_PASSWORD} // '') . " SEED\n");
my $hdr = <$sock> // '';
die "$host: $hdr" unless $hdr =~ /^OK (\d+) (\d+) (replica|offline|live)$/;
my ($db_size, $ctl_size, $kind) = ($1, $2, $3);
binmode $sock;

sub receive {
  my ($path, $size) = @_;
  open(my $out, '>:raw', "$path.part") or die "write $path.part: $!\n";
  my ($got, $buf) = (0, '');
  while ($got < $size) {
    my $want = $size - $got < 65536 ? $size - $got : 65536;
    my $n = read($sock, $buf, $want);
    die "short read ($got of $size bytes)\n" unless $n;
    print $out $buf;
    $got += $n;
  }
  close $out;
  rename("$path.part", $path) or die "rename $path: $!\n";
}

unlink "$target.ctl";
receive($target, $db_size);
receive("$target.ctl", $ctl_size) if $ctl_size;
open(my $k, '>', "$target.kind") or die "write $target.kind: $!\n";
print $k "$kind\n";
close $k;
print "fetched $kind seed copy ($db_size bytes) from $host\n";
