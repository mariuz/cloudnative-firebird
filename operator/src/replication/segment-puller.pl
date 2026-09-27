#!/usr/bin/perl
# Replica-side journal shipping: polls the primary's segment server and places new
# archived segments into journal_source_directory, where the replica applies them.
# The primary host is re-read from PRIMARY_FILE on every poll, so it follows the
# operator's view of the current primary; the puller idles on the primary itself.
use strict;
use warnings;
use IO::Socket::INET;

my $source   = $ENV{SOURCE_DIR}   or die "SOURCE_DIR is required\n";
my $state    = $ENV{STATE_FILE}   or die "STATE_FILE is required\n";
my $primary_file = $ENV{PRIMARY_FILE} or die "PRIMARY_FILE is required\n";
my $token    = $ENV{ISC_PASSWORD} // '';
my $port     = $ENV{SEGMENT_PORT} // 3051;
my $interval = $ENV{POLL_SECONDS} // 5;
my $self     = $ENV{POD_NAME} // '';
my $name_re  = qr/^[A-Za-z0-9._-]+\.journal-\d+$/;
$| = 1;

sub slurp { my ($f) = @_; open(my $fh, '<', $f) or return ''; local $/; my $v = <$fh>; close $fh; $v //= ''; $v =~ s/\s+$//; return $v; }

sub request {
  my ($host, $line) = @_;
  my $sock = IO::Socket::INET->new(PeerHost => $host, PeerPort => $port, Proto => 'tcp', Timeout => 10)
    or die "connect $host:$port: $!\n";
  $sock->timeout(60);
  print $sock "$token $line\n";
  return $sock;
}

sub pull_once {
  my $primary = slurp($primary_file);
  return if $primary eq '' || $primary =~ /^\Q$self\E(\.|$)/;   # we are the primary
  my $last = slurp($state);
  my $sock = request($primary, 'LIST');
  my @names;
  while (my $l = <$sock>) {
    $l =~ s/\r?\n$//;
    last if $l eq '.';
    die "server: $l\n" if $l =~ /^ERR/;
    push @names, $l if $l =~ $name_re;
  }
  close $sock;
  for my $name (sort @names) {
    next if $last ne '' && $name le $last;
    my $s = request($primary, "GET $name");
    my $hdr = <$s> // '';
    die "server: $hdr" unless $hdr =~ /^OK (\d+)/;
    my $size = $1;
    my $tmp = "$source/.$name.part";
    open(my $out, '>:raw', $tmp) or die "write $tmp: $!\n";
    binmode $s;
    my ($got, $buf) = (0, '');
    while ($got < $size) {
      my $n = read($s, $buf, 65536);
      die "short read for $name\n" unless $n;
      print $out $buf;
      $got += $n;
    }
    close $out;
    close $s;
    rename($tmp, "$source/$name") or die "rename $name: $!\n";   # atomic hand-off to the replica
    open(my $st, '>', "$state.tmp") or die "write $state.tmp: $!\n";
    print $st "$name\n";
    close $st;
    rename("$state.tmp", $state) or die "rename $state: $!\n";
    $last = $name;
    print "fetched $name from $primary\n";
  }
}

print "segment puller: source=$source primary-file=$primary_file\n";
while (1) {
  eval { pull_once(); 1 } or print "pull failed: $@";
  sleep $interval;
}
