#!/usr/bin/perl
# Serves archived replication journal segments to replicas over a minimal line protocol:
#   "<token> LIST\n"        -> one segment name per line, terminated by ".\n"
#   "<token> GET <name>\n"  -> "OK <size>\n" followed by the file bytes, or "ERR <reason>\n"
#   "<token> SEED\n"        -> a physical copy of the database, framed like GET. It is taken
#                              with nbackup -L / copy / nbackup -N: locking switches the journal
#                              to a new segment, so the copy's replication sequence marks exactly
#                              the segments it contains. Replicas fix it up with "nbackup -SEQ -F"
#                              and apply only later segments. (nbackup -B 0 records the new,
#                              still-active segment instead, and replicas would skip its writes.)
#   "<token> TXNS <S> <id,..>" -> "<id> <segment>" for every listed transaction that has blocks in
#                              archived segments <= S (its first such segment), terminated by ".\n".
#                              Replicas use it to replay transactions whose commit is journaled in
#                              the seed copy's segments but whose commit mark missed the copy.
# The token is the SYSDBA password (ISC_PASSWORD). Archived segments older than
# SEGMENT_RETENTION_SECONDS are pruned.
use strict;
use warnings;
use IO::Socket::INET;

my $dir       = $ENV{ARCHIVE_DIR} or die "ARCHIVE_DIR is required\n";
my $database  = $ENV{DATABASE_PATH} or die "DATABASE_PATH is required\n";
my $seed_file = "$dir/../seed.nbk";
my $token     = $ENV{ISC_PASSWORD} // '';
my $port      = $ENV{SEGMENT_PORT} // 3051;
my $retention = $ENV{SEGMENT_RETENTION_SECONDS} // 86400;
my $name_re   = qr/^[A-Za-z0-9._-]+\.journal-\d+$/;
$| = 1;

my $server = IO::Socket::INET->new(LocalPort => $port, Listen => 16, ReuseAddr => 1, Proto => 'tcp')
  or die "listen on $port: $!\n";
print "segment server listening on $port, serving $dir\n";

sub segments {
  opendir(my $dh, $dir) or return ();
  my @names = sort grep { $_ =~ $name_re && -f "$dir/$_" } readdir($dh);
  closedir($dh);
  return @names;
}

sub send_file {
  my ($client, $path) = @_;
  open(my $fh, '<:raw', $path) or return print $client "ERR cannot read\n";
  my $size = -s $fh;
  print $client "OK $size\n";
  binmode $client;
  my $buf;
  while (read($fh, $buf, 65536)) { print $client $buf; }
  close $fh;
}

# Segment file layout (src/jrd/replication/ChangeLog.h, Protocol.h): a 48-byte SegmentHeader
# {char[12] signature, u16 version, u16 state, guid[16], u64 sequence, u64 length} followed by
# blocks {u64 traNumber, u16 protocol, u16 flags, u32 length} + payload, up to header length.
sub segment_sequence {
  my ($path) = @_;
  open(my $fh, '<:raw', $path) or return undef;
  my $n = read($fh, my $hdr, 48);
  close $fh;
  return undef unless $n == 48 && substr($hdr, 0, 11) eq 'FBCHANGELOG';
  my (undef, undef, undef, undef, $seq) = unpack('a12 v v a16 Q<', $hdr);
  return $seq;
}

sub first_segments {
  my ($upto, $wanted) = @_;   # $wanted: hashref of transaction ids
  my %first;
  my @files = map { [$_, segment_sequence("$dir/$_")] } segments();
  for my $entry (sort { $a->[1] <=> $b->[1] } grep { defined $_->[1] && $_->[1] <= $upto } @files) {
    my ($name, $seq) = @$entry;
    open(my $fh, '<:raw', "$dir/$name") or next;
    read($fh, my $hdr, 48);
    my (undef, undef, undef, undef, undef, $length) = unpack('a12 v v a16 Q< Q<', $hdr);
    my $pos = 48;
    while ($pos + 16 <= $length) {
      seek($fh, $pos, 0);
      last unless read($fh, my $blk, 16) == 16;
      my ($tra, undef, undef, $len) = unpack('Q< v v V', $blk);
      $first{$tra} //= $seq if $wanted->{$tra};
      $pos += 16 + $len;
    }
    close $fh;
  }
  return \%first;
}

sub archived_upto {
  my ($seq) = @_;
  for my $name (segments()) {
    my $s = segment_sequence("$dir/$name");
    return 1 if defined $s && $s >= $seq;
  }
  return 0;
}

sub prune {
  my $cutoff = time - $retention;
  for my $name (segments()) {
    my $mtime = (stat("$dir/$name"))[9];
    unlink "$dir/$name" if defined $mtime && $mtime < $cutoff;
  }
}

my $last_prune = 0;
while (1) {
  if (time - $last_prune > 60) { prune(); $last_prune = time; }
  $server->timeout(30);
  my $client = $server->accept or next;
  $client->timeout(30);
  my $line = <$client>;
  if (!defined $line) { close $client; next; }
  $line =~ s/\r?\n$//;
  my ($given, $cmd, $arg) = split / /, $line, 3;
  if (!defined $cmd || $given ne $token) {
    print $client "ERR unauthorized\n";
  } elsif ($cmd eq 'LIST') {
    print $client "$_\n" for segments();
    print $client ".\n";
  } elsif ($cmd eq 'GET' && defined $arg && $arg =~ $name_re && -f "$dir/$arg") {
    send_file($client, "$dir/$arg");
  } elsif ($cmd eq 'TXNS' && defined $arg && $arg =~ /^(\d+) ([\d,]*)$/) {
    my ($upto, %wanted) = ($1, map { $_ => 1 } grep { length } split /,/, $2);
    # the segment active at lock time is archived shortly after the lock switched it out
    my $deadline = time + 120;
    sleep 1 until archived_upto($upto) || time > $deadline;
    if (!archived_upto($upto)) {
      print $client "ERR segment $upto not archived yet\n";
    } else {
      my $first = first_segments($upto, \%wanted);
      print $client "$_ $first->{$_}\n" for sort { $a <=> $b } keys %$first;
      print $client ".\n";
    }
  } elsif ($cmd eq 'SEED') {
    # nbackup locks through the local server (ISC_USER/ISC_PASSWORD); writes go to the
    # delta file while the main file is copied, and the database is always unlocked again
    unlink $seed_file;
    my $locked = system('nbackup', '-L', $database) == 0;
    my $copied = $locked && system('cp', $database, $seed_file) == 0;
    my $unlocked = !$locked || system('nbackup', '-N', $database) == 0;
    if ($copied && $unlocked) {
      send_file($client, $seed_file);
      print "served seed copy of $database\n";
    } else {
      print $client "ERR seed copy failed\n";
      print "seed copy failed (locked=$locked copied=$copied unlocked=$unlocked)\n";
    }
    unlink $seed_file;
  } else {
    print $client "ERR bad request\n";
  }
  close $client;
}
