#!/usr/bin/perl
# Replication sidecar running on every instance. It serves the local journal archive to
# replicas and seed copies to new replicas, over a minimal line protocol:
#
#   "<token> LIST\n"           -> archived segment names, one per line, then ".\n"
#   "<token> GET <name>\n"     -> "OK <size>\n" + file bytes, or "ERR <reason>\n"
#   "<token> SEED\n"           -> "OK <dbsize> <ctlsize> <kind>\n" + database bytes + control bytes
#   "<token> TXNS <S> <ids>\n" -> "<id> <segment>" for each listed transaction with blocks in
#                                 archived segments <= S (its first such segment), then ".\n"
#   "<token> PLAN <S> <next> <ids>\n" -> "<id> <segment> <begins>" for each listed transaction, and
#                                 each transaction numbered <next> or above, with blocks in archived
#                                 segments <= S: its first such segment, and 1 when that block begins
#                                 the transaction (0: it began in a segment no longer archived)
#   "<token> POSITION\n"       -> replica: "OK <sequence> <offset> <pending>", the replica control
#                                 file position and the number of received segments beyond it (a
#                                 synchronous standby: the last segment archived on the primary,
#                                 which it has every change of);
#                                 primary: "OK primary" (used by planned switchover)
#   "<token> ARCHIVED\n"       -> "<sequence> <age seconds>" for each archived segment, then ".\n"
#   "<token> LINEAGE\n"        -> "<database>.lineage-<P>-<U>" for each failover that promoted this
#                                instance from segment P after the journal archive's segment U
#                                (segments P+1..U are not in its history), then ".\n"
#                                 (the operator compares it with the replicas' POSITION: lag)
#   "<token> RETAIN <S>|none\n" -> "OK": keep archived segments after S (the lowest segment the
#                                 replicas applied, sent by the operator) past the retention age,
#                                 up to SEGMENT_MAX_RETENTION_SECONDS; "none" clears the floor (the
#                                 segments after the offline bootstrap seed are then kept instead)
#   "<token> UPLOADED <S>\n"   -> "OK": every archived segment up to S is in the journal archive
#                                 (object store), sent by the journal archive Job after its upload
#   "<token> FILE <name>\n"     -> "OK <size>\n" + bytes of the backup file <name> in the data
#                                 directory, or "ERR <reason>\n" (physical backups to S3)
#   "<token> STORE <name> <size>\n" + bytes -> "OK\n" once the backup file <name> is written to
#                                 the data directory (physical restores from S3)
#   "<token> REMOVE <name>\n"   -> "OK\n": deletes the backup file <name> from the data directory
#                                 (physical backups to S3, retention of server-side backups)
#   "<token> FILES\n"           -> the backup file names in the data directory, one per line, then ".\n"
#   "<token> HEADER\n"          -> "OK <sequence>": the replication sequence in the database header
#                                 page on disk (planned switchover, once the old primary is in full
#                                 shutdown: Firebird 6 refuses header statistics through the
#                                 service manager then, and gstat reads local files only)
#   "<token> ISOLATION\n"       -> "OK fenced <epoch>" when the isolation check fenced this
#                                 primary (isolation-check.pl), "OK online" otherwise
#   "<token> SYNC <host>|none\n" -> "OK": primary, synchronous replication: writes SYNC_FILE (included
#                                 by replication.conf) with a sync_replica entry for the replica
#                                 <host>, or empties it. Firebird reads it when the database is
#                                 opened, so the sync-standby Job sends it while the database is in
#                                 full shutdown (sync-standby.pl)
#   "<token> SYNCTO\n"          -> "OK <host>" or "OK none": the replica the primary replicates to
#                                 synchronously (from SYNC_FILE: what applies once the database is
#                                 opened; the sync-standby Job changes it only in full shutdown)
#   "<token> STANDBY on\n"      -> "OK": replica, becomes the synchronous standby: the segment
#                                 puller stops applying journal segments (the primary sends every
#                                 change directly), and only records the last archived one
#   "<token> STANDBY off <S>\n" -> "OK": back to journal shipping after segment S (the primary's
#                                 last segment, in full shutdown): the replica control file and the
#                                 puller's position move to S
#   "<token> REJOIN\n"          -> "OK": brings a database fenced by the isolation check back
#                                 online, sent by the operator once it checked that this instance
#                                 still holds the leader Lease ("OK" too when it is not fenced)
#
# FILE, STORE, REMOVE and FILES only handle plain "*.nbk" and "*.fbk" names (no directories), so
# they cannot touch the database, the journal or the replication state. FILE and STORE run in a child process, so a
# large transfer does not hold up replicas and the operator.
#
# The token is the SYSDBA password (ISC_PASSWORD).
#
# The live database is only ever accessed through the local server (isql localhost:): opening
# the file with an independent embedded engine and lock table (gstat or nbackup from this
# sidecar container) is not a supported access pattern. Only files no server has open (seed
# copies) are read with gstat.
#
# SEED depends on the role of this instance (seeds avoid locking the primary: ISSUES.md, issue 2):
#   replica: pause the segment puller, wait until every received segment is applied, then take
#            the copy under the backup lock together with the replica control file. Nothing
#            commits on a replica, so the copy and the control file describe the same point.
#            kind "replica"; the new replica adopts the control file.
#   primary: serve the offline bootstrap seed written when the database was created, while every
#            journal segment after it is still archived. kind "offline".
#            Otherwise, when ALLOW_LIVE_SEED=true (allowLiveSeedFromPrimary, by default), lock the live primary (kind "live"): the
#            new replica then replays the copy's uncommitted transactions found via TXNS.
use strict;
use warnings;
use IO::Socket::INET;

# FILES_ONLY: only the backup file commands (FILE, STORE, REMOVE, FILES), for instances without
# replication (physical backups to and restores from S3, retention of server-side backups)
my $files_only = ($ENV{FILES_ONLY} // '') eq 'true';
sub required { my ($name) = @_; my $v = $ENV{$name} // ''; die "$name is required\n" if $v eq '' && !$files_only; return $v; }
my $dir       = required('ARCHIVE_DIR');
my $database  = $ENV{DATABASE_PATH} or die "DATABASE_PATH is required\n";
my $source    = required('SOURCE_DIR');
my $base      = required('REPLICATION_DIR');
my $primary_file = required('PRIMARY_FILE');
my $self      = $ENV{POD_NAME} // '';
my $token     = $ENV{ISC_PASSWORD} // '';
my $port      = $ENV{SEGMENT_PORT} // 3051;
my $retention = $ENV{SEGMENT_RETENTION_SECONDS} // 86400;
my $max_retention = $ENV{SEGMENT_MAX_RETENTION_SECONDS} // 7 * 86400;
$max_retention = $retention if $max_retention < $retention;
# lowest segment applied by the replicas, as last reported by the operator; kept on the volume
# so a restarted primary does not prune what a stopped replica still needs
my $floor_file = "$base/retain-floor";
# highest segment the journal archive Job has uploaded (only needed with PRUNE_APPLIED)
my $uploaded_file = "$base/uploaded-floor";
# delete segments every replica applied (and, with an archive upload, uploaded) before the
# retention age: the archive is then bounded by the replicas' progress
my $prune_applied = ($ENV{PRUNE_APPLIED} // '') eq 'true';
my $archive_upload = ($ENV{ARCHIVE_UPLOAD} // '') eq 'true';
my $allow_live = ($ENV{ALLOW_LIVE_SEED} // '') eq 'true';
my $seed_file = "$base/seed.copy";
my $bootstrap_seed = "$base/bootstrap-seed.fdb";
my $pause_flag = "$base/.pause-pull";
my $pause_ack  = "$base/.pull-paused";
my $self_fenced = "$base/self-fenced";
# synchronous replication: the primary's sync_replica entry, and the standby's flag and last seen segment
my $sync_file = "$base/sync.conf";
my $standby_flag = "$base/sync-standby";
my $standby_seen = "$base/sync-seen";
my $state_file = $ENV{STATE_FILE} // "$base/.last-pulled";
my $isolation_timeout = $ENV{ISOLATION_TIMEOUT_SECONDS} // 0;
my $name_re   = qr/^[A-Za-z0-9._-]+\.journal-\d+$/;
my $backup_re = qr/^[A-Za-z0-9][A-Za-z0-9._-]*\.(?:nbk|fbk)$/;
(my $data_dir = $database) =~ s{/[^/]*$}{};
$data_dir = '.' if $data_dir eq '';
$| = 1;

my $server = IO::Socket::INET->new(LocalPort => $port, Listen => 16, ReuseAddr => 1, Proto => 'tcp')
  or die "listen on $port: $!\n";
print $files_only ? "backup file server listening on $port, serving $data_dir\n" : "segment server listening on $port, serving $dir\n";

sub slurp { my ($f) = @_; open(my $fh, '<', $f) or return ''; local $/; my $v = <$fh>; close $fh; $v //= ''; $v =~ s/\s+$//; return $v; }

sub is_primary {
  my $primary = slurp($primary_file);
  return $primary eq '' || $primary =~ /^\Q$self\E(\.|$)/;
}

# Replication sequence (HDR_repl_seq clump, 0 without one) from the header page of a database file
# on disk; undef if unreadable. Header layout as in set-repl-seq.pl: u16 page size at 16, u16 ODS
# version at 18, hdr_end and the clumps at 66 / 128 (ODS 13) or 36 / 148 (ODS 14).
sub header_sequence {
  my ($file) = @_;
  my %layout = (13 => [66, 128], 14 => [36, 148]);
  open(my $fh, '<:raw', $file) or return undef;
  my $n = read($fh, my $page, 65536);
  close $fh;
  return undef unless $n && $n >= 4096 && ord(substr($page, 0, 1)) == 1;
  my $ods = unpack('v', substr($page, 18, 2)) & 0x7fff;
  my $layout = $layout{$ods} or return undef;
  my ($end_at, $p) = @$layout;
  my $end = unpack('v', substr($page, $end_at, 2));
  return undef if $end > length($page);
  while ($p < $end) {
    my ($type, $len) = unpack('C C', substr($page, $p, 2));
    last if $type == 0;
    return unpack('Q<', substr($page, $p + 2, 8)) if $type == 11 && $len == 8;
    $p += 2 + $len;
  }
  return 0;
}

sub segments {
  my ($path) = @_;
  $path //= $dir;
  opendir(my $dh, $path) or return ();
  my @names = sort grep { $_ =~ $name_re && -f "$path/$_" } readdir($dh);
  closedir($dh);
  return @names;
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

sub archived_sequences {
  return sort { $a <=> $b } grep { defined } map { segment_sequence("$dir/$_") } segments();
}

# First archived segment <= $upto holding a block of each wanted transaction (and, with $next, of
# every transaction numbered $next or above), and whether that block begins the transaction
# (BLOCK_BEGIN_TRANS): returns { id => [segment, begins] }
sub first_segments {
  my ($upto, $wanted, $next) = @_;   # $wanted: hashref of transaction ids
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
      my ($tra, undef, $flags, $len) = unpack('Q< v v V', $blk);
      if ($tra && !$first{$tra} && ($wanted->{$tra} || (defined $next && $tra >= $next))) {
        $first{$tra} = [$seq, ($flags & 1) ? 1 : 0];
      }
      $pos += 16 + $len;
    }
    close $fh;
  }
  return \%first;
}

# One value from the live database, queried through the local server
sub live_value {
  my ($expression) = @_;
  my $sql = "SET LIST ON; SELECT $expression AS V FROM MON\$DATABASE;";
  $sql =~ s/([\\"`\$])/\\$1/g;   # quoted for the double-quoted shell string below
  open(my $isql, '-|', 'sh', '-c', "echo \"$sql\" | isql -q localhost:$database")
    or return undef;
  my $value;
  while (my $line = <$isql>) { $value = $1 if $line =~ /^V\s+(\S+)/; }
  close $isql;
  return $value;
}

sub live_sql {
  my ($statement) = @_;
  return system('sh', '-c', "echo '$statement' | isql -q -b localhost:$database") == 0;
}

# "Replication sequence" / "Database GUID" from the header page of a database file that no
# server has open (seed copies only)
sub header_field {
  my ($path, $field) = @_;
  open(my $gstat, '-|', 'gstat', '-h', $path) or return undef;
  my $value;
  while (my $line = <$gstat>) {
    $value = $1 if $line =~ /^\s*\Q$field\E:?\s*(\S+)/;
  }
  close $gstat;
  return $value;
}

# Replica control file (ControlFile::DataV1): {char[10], u16 version, u32 txn_count,
# u64 sequence, u32 offset, pad, u64 db_sequence} + txn_count x {u64 tra_id, u64 sequence}
sub read_control {
  my ($path) = @_;
  open(my $fh, '<:raw', $path) or return undef;
  local $/;
  my $data = <$fh>;
  close $fh;
  return undef unless defined $data && length($data) >= 40 && substr($data, 0, 9) eq 'FBREPLCTL';
  my (undef, undef, $count, $seq, $offset) = unpack('a10 v V Q< V', $data);
  return { data => $data, sequence => $seq, offset => $offset, count => $count };
}

sub write_file {
  my ($path, $content) = @_;
  open(my $fh, '>', "$path.tmp") or return 0;
  print $fh $content;
  close $fh or return 0;
  return rename("$path.tmp", $path);
}

# The replica control file (the only {GUID} file in the source directory)
sub control_path {
  opendir(my $dh, $source) or return undef;
  my ($name) = grep { /^\{[0-9A-Fa-f-]+\}$/ } readdir($dh);
  closedir($dh);
  return defined $name ? "$source/$name" : undef;
}

# Standby back to journal shipping after segment $seq: control file at $seq (no transaction in
# progress: the primary is in full shutdown), keeping the database's own sequence (db_sequence,
# checked by the replica server), and the puller's position at segment $seq too
sub standby_off {
  my ($seq) = @_;
  my $path = control_path() or return "no replica control file";
  my $ctl = read_control($path) or return "cannot read $path";
  my $dbseq = unpack('Q<', substr($ctl->{data}, 32, 8));
  write_file($path, pack('a10 v V Q< V x4 Q<', 'FBREPLCTL', 1, 0, $seq, 0, $dbseq)) or return "cannot write $path: $!";
  my $last = slurp($state_file);
  if ($last =~ /^(.*\.journal-)(\d+)$/) {
    write_file($state_file, sprintf("%s%0*d\n", $1, length($2), $seq)) or return "cannot write $state_file: $!";
  }
  unlink $standby_flag, $standby_seen;
  return undef;
}

sub send_seed {
  my ($client, $db, $ctl, $kind) = @_;
  open(my $fh, '<:raw', $db) or return print $client "ERR cannot read seed copy\n";
  my $ctl_size = defined $ctl ? length($ctl) : 0;
  print $client "OK " . (-s $fh) . " $ctl_size $kind\n";
  binmode $client;
  my $buf;
  while (read($fh, $buf, 65536)) { print $client $buf; }
  close $fh;
  print $client $ctl if $ctl_size;
}

sub wait_for {
  my ($seconds, $check) = @_;
  my $deadline = time + $seconds;
  until ($check->()) {
    return 0 if time > $deadline;
    sleep 1;
  }
  return 1;
}

sub locked_copy {
  # the backup lock (what nbackup -L/-N do) is taken through the local server; writes go to the
  # delta file while the main file is copied, and the database is always unlocked again
  my ($on_locked) = @_;
  unlink $seed_file;
  my $locked = live_sql('ALTER DATABASE BEGIN BACKUP;');
  my $copied = $locked && system('cp', $database, $seed_file) == 0;
  $on_locked->() if $copied && $on_locked;
  my $unlocked = !$locked || live_sql('ALTER DATABASE END BACKUP;');
  print "locked copy: locked=$locked copied=$copied unlocked=$unlocked\n";
  return $copied && $unlocked;
}

sub seed_from_replica {
  my ($client) = @_;
  my $guid = live_value('MON$GUID') // '';
  my $control = "$source/$guid";
  unlink $pause_ack;
  if (open(my $flag, '>', $pause_flag)) { close $flag; }
  my $ok = eval {
    wait_for(60, sub { -e $pause_ack }) or die "segment puller did not pause\n";
    # everything received has been applied: no segment beyond the control file position
    wait_for(300, sub {
      my $ctl = read_control($control) or return 0;
      return 0 if $ctl->{offset};
      return !grep { my $s = segment_sequence("$source/$_"); defined $s && $s > $ctl->{sequence} } segments($source);
    }) or die "replica did not finish applying received segments\n";
    my $ctl_data;
    locked_copy(sub { $ctl_data = read_control($control)->{data} }) or die "locked copy failed\n";
    send_seed($client, $seed_file, $ctl_data, 'replica');
    print "served replica seed copy\n";
    1;
  };
  unless ($ok) {
    print $client "ERR $@";
    print "replica seed refused: $@";
  }
  unlink $pause_flag, $seed_file;
}

sub seed_from_primary {
  my ($client) = @_;
  if (-f $bootstrap_seed) {
    my $seed_seq = header_field($bootstrap_seed, 'Replication sequence') // 0;
    my $current = live_value(q{RDB$GET_CONTEXT('SYSTEM', 'REPLICATION_SEQUENCE')}) // 0;
    my @archived = archived_sequences();
    # usable while every segment after the seed is still archived
    if ($current <= $seed_seq || (@archived && $archived[0] <= $seed_seq + 1)) {
      send_seed($client, $bootstrap_seed, undef, 'offline');
      print "served offline bootstrap seed (sequence $seed_seq)\n";
      return;
    }
    print "bootstrap seed (sequence $seed_seq) is older than the archived segments\n";
  }
  if (!$allow_live) {
    print $client "ERR no safe seed source on the primary; add a replica seed source, restart the primary cleanly for a fresh offline seed, or allow live seeds (allowLiveSeedFromPrimary)\n";
    print "seed refused: no usable bootstrap seed and live seeding is not allowed\n";
    return;
  }
  if (locked_copy()) {
    send_seed($client, $seed_file, undef, 'live');
    print "served live seed copy of the primary\n";
  } else {
    print $client "ERR seed copy failed\n";
  }
  unlink $seed_file;
}

# Segments older than the retention age are deleted unless a replica has not applied them yet
# (sequence above the floor); those are kept up to the maximum retention age, so a slow or stopped
# replica can catch up without being re-seeded, and a replica that never returns cannot fill the
# volume.
sub prune {
  my $now = time;
  my $floor = slurp($floor_file);
  $floor = undef unless $floor =~ /^\d+$/;
  my $measured = $floor;   # from the operator, not the bootstrap seed fallback below
  # No replica holds segments back (e.g. a single instance): keep those after the offline bootstrap
  # seed, so a replica added later can still be seeded from it without locking the primary.
  if (!defined $floor && -f $bootstrap_seed) {
    my $seed_seq = slurp("$base/bootstrap-seed.seq");
    $seed_seq = header_field($bootstrap_seed, 'Replication sequence') // 0 unless $seed_seq =~ /^\d+$/;
    $floor = $seed_seq;
  }
  # Early: segments below the floor the operator measured (every replica has pulled and applied
  # them; the floor segment itself may be partly applied) and, when the journal archive uploads
  # them, not above the last upload
  my $applied;
  if ($prune_applied && defined $measured) {
    $applied = $measured - 1;
    if ($archive_upload) {
      my $uploaded = slurp($uploaded_file);
      $applied = $uploaded =~ /^\d+$/ ? ($uploaded < $applied ? $uploaded : $applied) : undef;
    }
  }
  for my $name (segments()) {
    my $mtime = (stat("$dir/$name"))[9];
    next unless defined $mtime;
    my $age = $now - $mtime;
    if (defined $applied) {
      my $seq = segment_sequence("$dir/$name");
      if (defined $seq && $seq <= $applied) { unlink "$dir/$name"; next; }
    }
    next if $age < $retention;
    if ($age < $max_retention && defined $floor) {
      my $seq = segment_sequence("$dir/$name");
      next if !defined $seq || $seq > $floor;
    }
    unlink "$dir/$name";
  }
}

# Streams a data directory nbackup file to the client
sub send_file {
  my ($client, $name) = @_;
  open(my $fh, '<:raw', "$data_dir/$name") or do { print $client "ERR cannot read $name: $!\n"; return };
  print $client "OK " . (-s $fh) . "\n";
  binmode $client;
  my $buf;
  while (read($fh, $buf, 65536)) { print $client $buf or last; }
  close $fh;
}

# Receives <size> bytes into a data directory nbackup file; nothing is left behind on failure
sub store_file {
  my ($client, $name, $size) = @_;
  my $part = "$data_dir/.$name.part";
  open(my $fh, '>:raw', $part) or do { print $client "ERR cannot write $name: $!\n"; return };
  binmode $client;
  my ($got, $buf) = (0, '');
  while ($got < $size) {
    my $want = $size - $got < 65536 ? $size - $got : 65536;
    my $n = read($client, $buf, $want);
    last unless $n;
    print $fh $buf or last;
    $got += $n;
  }
  if (!close($fh) || $got != $size) {
    unlink $part;
    print $client "ERR short write for $name ($got of $size bytes)\n";
    return;
  }
  chmod 0644, $part;
  if (rename($part, "$data_dir/$name")) {
    print $client "OK\n";
  } else {
    unlink $part;
    print $client "ERR rename $name: $!\n";
  }
}

# The isolation check (automatic failover only) runs beside the server, restarted if it exits
my $monitor;
sub start_monitor {
  return if $files_only || $isolation_timeout !~ /^\d+$/ || $isolation_timeout == 0;
  $ENV{SELF_FENCED_FILE} = $self_fenced;
  my $pid = fork;
  if (!defined $pid) { print "cannot start the isolation check: $!\n"; return; }
  if ($pid == 0) {
    close $server;
    exec('perl', "$ENV{SCRIPT_DIR}/isolation-check.pl") or exit 1;
  }
  $monitor = $pid;
}
start_monitor();

my $last_prune = 0;
while (1) {
  if (!$files_only && time - $last_prune > 60) { prune(); $last_prune = time; }
  # reap finished transfers (1 = WNOHANG), and restart the isolation check if it ended
  while ((my $done = waitpid(-1, 1)) > 0) {
    if (defined $monitor && $done == $monitor) { $monitor = undef; sleep 1; start_monitor(); }
  }
  $server->timeout(30);
  my $client = $server->accept or next;
  $client->timeout(600);
  my $line = <$client>;
  if (!defined $line) { close $client; next; }
  $line =~ s/\r?\n$//;
  my ($given, $cmd, $arg) = split / /, $line, 3;
  if (!defined $cmd || $given ne $token) {
    print $client "ERR unauthorized\n";
  } elsif ($files_only && $cmd !~ /^(?:FILE|STORE|REMOVE|FILES)$/) {
    print $client "ERR not available without replication\n";
  } elsif ($cmd eq 'LIST') {
    print $client "$_\n" for segments();
    print $client ".\n";
  } elsif ($cmd eq 'GET' && defined $arg && $arg =~ $name_re && -f "$dir/$arg") {
    open(my $fh, '<:raw', "$dir/$arg") or do { print $client "ERR cannot read\n"; close $client; next };
    print $client "OK " . (-s $fh) . "\n";
    binmode $client;
    my $buf;
    while (read($fh, $buf, 65536)) { print $client $buf; }
    close $fh;
  } elsif ($cmd eq 'TXNS' && defined $arg && $arg =~ /^(\d+) ([\d,]*)$/) {
    my ($upto, %wanted) = ($1, map { $_ => 1 } grep { length } split /,/, $2);
    # the segment active at lock time is archived shortly after the lock switched it out
    if (!wait_for(120, sub { my @s = archived_sequences(); @s && $s[-1] >= $upto })) {
      print $client "ERR segment $upto not archived yet\n";
    } else {
      my $first = first_segments($upto, \%wanted);
      print $client "$_ $first->{$_}[0]\n" for sort { $a <=> $b } keys %$first;
      print $client ".\n";
    }
  } elsif ($cmd eq 'PLAN' && defined $arg && $arg =~ /^(\d+) (\d+) ([\d,]*)$/) {
    my ($upto, $next, %wanted) = ($1, $2, map { $_ => 1 } grep { length } split /,/, $3);
    if (!wait_for(120, sub { my @s = archived_sequences(); @s && $s[-1] >= $upto })) {
      print $client "ERR segment $upto not archived yet\n";
    } else {
      my $first = first_segments($upto, \%wanted, $next);
      print $client "$_ $first->{$_}[0] $first->{$_}[1]\n" for sort { $a <=> $b } keys %$first;
      print $client ".\n";
    }
  } elsif ($cmd eq 'HEADER') {
    my $seq = header_sequence($database);
    print $client (defined $seq ? "OK $seq\n" : "ERR cannot read the header of $database\n");
  } elsif ($cmd eq 'ISOLATION') {
    print $client (-f $self_fenced ? "OK fenced " . (slurp($self_fenced) || 0) . "\n" : "OK online\n");
  } elsif ($cmd eq 'REJOIN') {
    if (!-f $self_fenced) {
      print $client "OK\n";
    } elsif (system('fbsvcmgr', 'localhost:service_mgr', 'action_properties', 'dbname', $database,
                    'prp_online_mode', 'prp_sm_normal') == 0) {
      unlink $self_fenced;
      print "isolation fence lifted by the operator: database online\n";
      print $client "OK\n";
    } else {
      print $client "ERR cannot bring $database online\n";
    }
  } elsif ($cmd eq 'SYNC' && defined $arg && $arg =~ /^(none|[A-Za-z0-9][A-Za-z0-9.-]*)$/) {
    # the server's ISC_USER / ISC_PASSWORD are the credentials: Firebird 4 ignores the sub-section
    # and uses them, Firebird 5 and later read password_env
    my $user = $ENV{ISC_USER} || 'SYSDBA';
    my $content = $arg eq 'none' ? '' :
      "sync_replica = $arg:$database\n{\n  username = $user\n  password_env = ISC_PASSWORD\n}\n";
    if (write_file($sync_file, $content)) {
      print "synchronous replication " . ($arg eq 'none' ? "off" : "to $arg") . " (applied when the database is opened)\n";
      print $client "OK\n";
    } else {
      print $client "ERR cannot write $sync_file: $!\n";
    }
  } elsif ($cmd eq 'SYNCTO') {
    my ($host) = slurp($sync_file) =~ /^sync_replica\s*=\s*([^:\s]+):/m;
    print $client "OK " . ($host // 'none') . "\n";
  } elsif ($cmd eq 'STANDBY' && defined $arg && $arg eq 'on') {
    if (is_primary()) {
      print $client "ERR this instance is the primary\n";
    } elsif (write_file($standby_flag, time . "\n")) {
      print "synchronous standby: journal segments are no longer applied here\n";
      print $client "OK\n";
    } else {
      print $client "ERR cannot write $standby_flag: $!\n";
    }
  } elsif ($cmd eq 'STANDBY' && defined $arg && $arg =~ /^off (\d+)$/) {
    my $seq = $1;
    my $error = -f $standby_flag ? standby_off($seq) : undef;
    if (defined $error) {
      print $client "ERR $error\n";
    } else {
      print "asynchronous replica again: journal shipping continues after segment $seq\n";
      print $client "OK\n";
    }
  } elsif ($cmd eq 'POSITION') {
    if (is_primary()) {
      print $client "OK primary\n";
    } elsif (-f $standby_flag && slurp($standby_seen) =~ /^(\d+)$/) {
      # every change reached this standby synchronously (the puller records the last archived
      # segment only while the primary names it): as far as the archive goes
      print $client "OK $1 0 0\n";
    } else {
      # the control file is the only {GUID} file in the source directory; reading it needs no
      # access to the (possibly shut down) database
      opendir(my $dh, $source);
      my ($ctl_name) = grep { /^\{[0-9A-Fa-f-]+\}$/ } readdir($dh);
      closedir($dh);
      my $ctl = defined $ctl_name ? read_control("$source/$ctl_name") : undef;
      if (!$ctl) {
        print $client "ERR no replica control file\n";
      } else {
        my $pending = grep { my $s = segment_sequence("$source/$_"); defined $s && $s > $ctl->{sequence} } segments($source);
        print $client "OK $ctl->{sequence} $ctl->{offset} $pending\n";
      }
    }
  } elsif ($cmd eq 'LINEAGE') {
    (my $db_name = $database) =~ s{.*/}{};
    if (open(my $fh, '<', "$base/lineage")) {
      while (my $l = <$fh>) { print $client "$db_name.lineage-$1-$2\n" if $l =~ /^(\d+) (\d+)\s*$/; }
      close $fh;
    }
    print $client ".\n";
  } elsif ($cmd eq 'ARCHIVED') {
    # ages are computed here, so the operator's clock does not matter
    my $now = time;
    for my $name (segments()) {
      my $seq = segment_sequence("$dir/$name");
      my $mtime = (stat("$dir/$name"))[9];
      print $client "$seq " . ($now - $mtime) . "\n" if defined $seq && defined $mtime;
    }
    print $client ".\n";
  } elsif ($cmd eq 'UPLOADED' && defined $arg && $arg =~ /^\d+$/) {
    if (open(my $fh, '>', "$uploaded_file.tmp")) {
      print $fh "$arg\n";
      close $fh;
      rename "$uploaded_file.tmp", $uploaded_file;
    }
    print $client "OK\n";
  } elsif ($cmd eq 'RETAIN' && defined $arg && $arg =~ /^(\d+|none)$/) {
    if ($arg eq 'none') {
      unlink $floor_file;
    } else {
      if (open(my $fh, '>', "$floor_file.tmp")) {
        print $fh "$arg\n";
        close $fh;
        rename "$floor_file.tmp", $floor_file;
      }
    }
    print $client "OK\n";
  } elsif (($cmd eq 'FILE' && defined $arg && $arg =~ $backup_re && -f "$data_dir/$arg") ||
           ($cmd eq 'STORE' && defined $arg && $arg =~ /^(\S+) (\d+)$/ && $1 =~ $backup_re)) {
    my $pid = fork;
    if (!defined $pid) {
      print $client "ERR fork: $!\n";
    } elsif ($pid == 0) {
      close $server;
      if ($cmd eq 'FILE') { send_file($client, $arg); } else { my ($n, $size) = split / /, $arg; store_file($client, $n, $size); }
      close $client;
      exit 0;
    }
  } elsif ($cmd eq 'REMOVE' && defined $arg && $arg =~ $backup_re) {
    if (!-e "$data_dir/$arg" || unlink "$data_dir/$arg") { print $client "OK\n"; } else { print $client "ERR remove $arg: $!\n"; }
  } elsif ($cmd eq 'FILES') {
    if (opendir(my $dh, $data_dir)) {
      print $client "$_\n" for sort grep { $_ =~ $backup_re && -f "$data_dir/$_" } readdir($dh);
      closedir($dh);
    }
    print $client ".\n";
  } elsif ($cmd eq 'SEED') {
    is_primary() ? seed_from_primary($client) : seed_from_replica($client);
  } else {
    print $client "ERR bad request\n";
  }
  close $client;
}
