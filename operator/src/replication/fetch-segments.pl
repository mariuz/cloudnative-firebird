#!/usr/bin/perl
# Journal archive Job: fetches the primary's archived journal segments from its segment server
# (LIST / GET, see segment-server.pl) into OUT_DIR, skipping the names listed in SKIP_FILE
# (segments already in the object store). Exits non-zero on any error so the Job retries.
# The highest listed segment sequence is written to LISTED_FILE: once the upload succeeded, every
# segment up to it is in the object store.
#
# With REPORT=true it only tells the primary's segment server that (UPLOADED <S>), after the upload.
use strict;
use warnings;
use IO::Socket::INET;

my $host  = $ENV{FIREBIRD_HOST} or die "FIREBIRD_HOST is required\n";
my $out   = $ENV{OUT_DIR} or die "OUT_DIR is required\n";
my $skip  = $ENV{SKIP_FILE} // '';
my $token = $ENV{ISC_PASSWORD} // '';
my $port  = $ENV{SEGMENT_PORT} // 3051;
my $name_re = qr/^[A-Za-z0-9._-]+\.journal-\d+$/;
my $listed_file = $ENV{LISTED_FILE} // '';
$| = 1;

sub request {
  my ($line) = @_;
  my $sock = IO::Socket::INET->new(PeerHost => $host, PeerPort => $port, Proto => 'tcp', Timeout => 10)
    or die "connect $host:$port: $!\n";
  $sock->timeout(60);
  print $sock "$token $line\n";
  return $sock;
}

if (($ENV{REPORT} // '') eq 'true') {
  my $max = '';
  if ($listed_file ne '' && open(my $fh, '<', $listed_file)) { $max = <$fh> // ''; close $fh; }
  $max =~ s/\s+$//;
  if ($max !~ /^\d+$/) { print "nothing listed, nothing to report\n"; exit 0; }
  my $s = request("UPLOADED $max");
  my $reply = <$s> // '';
  close $s;
  die "server: " . ($reply eq '' ? "no reply\n" : $reply) unless $reply =~ /^OK/;
  print "reported segments up to $max as uploaded to $host\n";
  exit 0;
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

if ($listed_file ne '') {
  my ($max) = sort { $b <=> $a } map { /journal-(\d+)$/ ? $1 + 0 : () } @names;
  open(my $fh, '>', $listed_file) or die "write $listed_file: $!\n";
  print $fh (defined $max ? "$max\n" : '');
  close $fh;
}

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
