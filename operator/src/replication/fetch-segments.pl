#!/usr/bin/perl
# Journal archive Job: fetches the primary's archived journal segments from its segment server
# (LIST / GET, see segment-server.pl) into OUT_DIR, skipping the names listed in SKIP_FILE
# (segments already in the object store). Exits non-zero on any error so the Job retries.
use strict;
use warnings;
use IO::Socket::INET;

my $host  = $ENV{FIREBIRD_HOST} or die "FIREBIRD_HOST is required\n";
my $out   = $ENV{OUT_DIR} or die "OUT_DIR is required\n";
my $skip  = $ENV{SKIP_FILE} // '';
my $token = $ENV{ISC_PASSWORD} // '';
my $port  = $ENV{SEGMENT_PORT} // 3051;
my $name_re = qr/^[A-Za-z0-9._-]+\.journal-\d+$/;
$| = 1;

sub request {
  my ($line) = @_;
  my $sock = IO::Socket::INET->new(PeerHost => $host, PeerPort => $port, Proto => 'tcp', Timeout => 10)
    or die "connect $host:$port: $!\n";
  $sock->timeout(60);
  print $sock "$token $line\n";
  return $sock;
}

my %done;
if ($skip ne '' && open(my $fh, '<', $skip)) {
  while (my $l = <$fh>) { $l =~ s/\s+$//; $done{$l} = 1 if $l ne ''; }
  close $fh;
}
mkdir $out unless -d $out;

my $sock = request('LIST');
my @names;
while (my $l = <$sock>) {
  $l =~ s/\r?\n$//;
  last if $l eq '.';
  die "server: $l\n" if $l =~ /^ERR/;
  push @names, $l if $l =~ $name_re;
}
close $sock;

my $fetched = 0;
for my $name (sort @names) {
  next if $done{$name};
  my $s = request("GET $name");
  my $hdr = <$s> // '';
  if ($hdr =~ /^ERR/) { close $s; print "skipping $name: $hdr"; next; }   # pruned meanwhile
  die "server: $hdr" unless $hdr =~ /^OK (\d+)/;
  my $size = $1;
  open(my $fh, '>:raw', "$out/.$name.part") or die "write $name: $!\n";
  binmode $s;
  my ($got, $buf) = (0, '');
  while ($got < $size) {
    my $n = read($s, $buf, 65536);
    die "short read for $name\n" unless $n;
    print $fh $buf;
    $got += $n;
  }
  close $fh;
  close $s;
  rename("$out/.$name.part", "$out/$name") or die "rename $name: $!\n";
  $fetched++;
}
printf "fetched %d new segment(s) of %d archived on %s\n", $fetched, scalar(@names), $host;
