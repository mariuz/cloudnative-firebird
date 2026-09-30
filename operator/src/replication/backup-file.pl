#!/usr/bin/perl
# Backup and restore Jobs: move nbackup files between the primary's data directory and the Job pod
# through the primary's segment server (FILE / STORE / REMOVE, see segment-server.pl):
#
#   backup-file.pl get <name> <local file>     copy the server file <name> to <local file>
#   backup-file.pl put <local file> <name>     copy <local file> to the server file <name>
#   backup-file.pl remove <name>...            delete server files (missing files are fine)
#
# Exits non-zero on any error so the Job retries.
use strict;
use warnings;
use IO::Socket::INET;

my $host  = $ENV{FIREBIRD_HOST} or die "FIREBIRD_HOST is required\n";
my $token = $ENV{ISC_PASSWORD} // '';
my $port  = $ENV{SEGMENT_PORT} // 3051;
$| = 1;

sub request {
  my ($line) = @_;
  my $sock = IO::Socket::INET->new(PeerHost => $host, PeerPort => $port, Proto => 'tcp', Timeout => 10)
    or die "connect $host:$port: $!\n";
  $sock->timeout(600);
  binmode $sock;
  print $sock "$token $line\n";
  return $sock;
}

my ($mode, @args) = @ARGV;
$mode //= '';
if ($mode eq 'get' && @args == 2) {
  my ($name, $file) = @args;
  my $s = request("FILE $name");
  my $hdr = <$s> // '';
  die "server: " . ($hdr eq '' ? "no reply\n" : $hdr) unless $hdr =~ /^OK (\d+)/;
  my $size = $1;
  open(my $fh, '>:raw', "$file.part") or die "write $file: $!\n";
  my ($got, $buf) = (0, '');
  while ($got < $size) {
    my $n = read($s, $buf, 65536);
    die "short read for $name ($got of $size bytes)\n" unless $n;
    print $fh $buf or die "write $file: $!\n";
    $got += $n;
  }
  close $fh or die "write $file: $!\n";
  close $s;
  rename("$file.part", $file) or die "rename $file: $!\n";
  print "copied $name ($size bytes) from $host\n";
} elsif ($mode eq 'put' && @args == 2) {
  my ($file, $name) = @args;
  my $size = -s $file;
  die "cannot read $file\n" unless defined $size;
  open(my $fh, '<:raw', $file) or die "read $file: $!\n";
  my $s = request("STORE $name $size");
  my $buf;
  while (read($fh, $buf, 65536)) { print $s $buf or die "send $name: $!\n"; }
  close $fh;
  my $reply = <$s> // '';
  close $s;
  die "server: " . ($reply eq '' ? "no reply\n" : $reply) unless $reply =~ /^OK/;
  print "copied $file ($size bytes) to $name on $host\n";
} elsif ($mode eq 'remove' && @args) {
  for my $name (@args) {
    my $s = request("REMOVE $name");
    my $reply = <$s> // '';
    close $s;
    die "server: " . ($reply eq '' ? "no reply\n" : $reply) unless $reply =~ /^OK/;
    print "removed $name on $host\n";
  }
} else {
  die "usage: backup-file.pl get <name> <file> | put <file> <name> | remove <name>...\n";
}
