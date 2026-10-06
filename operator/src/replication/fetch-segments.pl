#!/usr/bin/perl
# Journal archive Job: fetches the primary's archived journal segments from its segment server
# (LIST / GET, see segment-server.pl) into OUT_DIR, skipping the names listed in SKIP_FILE
# (segments already in the object store). Exits non-zero on any error so the Job retries.
# The highest listed segment sequence is written to LISTED_FILE: once the upload succeeded, every
# segment up to it is in the object store.
#
# Each fetched segment gets an empty companion file "<segment>.archived-<YYYYMMDDTHHMMSSZ>", the
# time the primary archived it (ARCHIVED): point-in-time recovery picks the segments archived up to
# its target time from these names.
#
# Lineage markers of the primary (LINEAGE) are written as empty files too, and each fetched
# segment's recovery points (POINTS) as "<segment>.points".
#
# With RESULT_FILE set, "listed=<S>" (that highest sequence) is written to it as well: anything
# this Job uploads is at most S, and the operator promotes replicas after it.
#
# With REPORT=true it only tells the primary's segment server that (UPLOADED <S>), after the upload.
use strict;
use warnings;
use IO::Socket::INET;
#@include segment-auth.pl

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
  my $sock = segment_open($host, $port, $token, $line);
  $sock->timeout(60);
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
if (defined $ENV{RESULT_FILE} && $ENV{RESULT_FILE} ne '') {
  my ($max) = sort { $b <=> $a } map { /journal-(\d+)$/ ? $1 + 0 : () } @names;
  if (defined $max && open(my $rf, '>', $ENV{RESULT_FILE})) { print $rf "listed=$max"; close $rf; }
}

# lineage markers (LINEAGE): empty objects "<database>.lineage-<P>-<U>", one per failover that
# promoted the primary from segment P after the archive's segment U; point-in-time recovery skips
# segments P+1..U past it. A segment server without LINEAGE answers ERR: none.
my $lineage = 0;
my $lsock = request('LINEAGE');
while (my $l = <$lsock>) {
  $l =~ s/\r?\n$//;
  last if $l eq '.' || $l =~ /^ERR/;
  next unless $l =~ /^[A-Za-z0-9._-]+\.lineage-\d+-\d+$/ && !$done{$l};
  open(my $mark, '>', "$out/$l") or die "write $l: $!\n";
  close $mark;
  $lineage++;
}
close $lsock;
print "$lineage new lineage marker(s)\n" if $lineage;

# archive time of each segment, from its age on the primary (ARCHIVED)
my %archived;
my $asock = request('ARCHIVED');
my $now = time;
while (my $l = <$asock>) {
  $l =~ s/\r?\n$//;
  last if $l eq '.';
  die "server: $l\n" if $l =~ /^ERR/;
  if ($l =~ /^(\d+) (-?\d+)$/) {
    my @t = gmtime($now - ($2 > 0 ? $2 : 0));
    $archived{$1 + 0} = sprintf('%04d%02d%02dT%02d%02d%02dZ', $t[5] + 1900, $t[4] + 1, @t[3, 2, 1, 0]);
  }
}
close $asock;

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
  my ($seq) = $name =~ /journal-(\d+)$/;
  # its recovery points ("<epoch> <length>", POINTS), when the primary sampled any
  if (defined $seq) {
    my $ps = request('POINTS ' . ($seq + 0));
    my @points;
    while (my $l = <$ps>) {
      last if $l =~ /^(?:\.|ERR)/;
      push @points, $l if $l =~ /^\d+ \d+\n$/;
    }
    close $ps;
    if (@points) {
      open(my $pf, '>', "$out/$name.points") or die "write points of $name: $!\n";
      print $pf @points;
      close $pf;
    }
  }
  if (defined $seq && $archived{$seq + 0}) {
    open(my $mark, '>', "$out/$name.archived-$archived{$seq + 0}") or die "write marker for $name: $!\n";
    close $mark;
  }
  $fetched++;
}
printf "fetched %d new segment(s) of %d archived on %s\n", $fetched, scalar(@names), $host;
