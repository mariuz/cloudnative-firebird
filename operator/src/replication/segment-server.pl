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
#   "<token> NBACKUP <level> <file>\n" -> nbackup into the data directory; on a replica with the
#                                replica control file next to it (see nbackup_here)
#   "<token> PRIMARYSEEN\n"    -> "OK <seconds> <primary host>" since the segment puller last reached
#                                 the primary, or "OK never" (automatic failover of a cut-off primary)
#   "<token> VERSION\n"        -> "OK <engine version>" of the local server (e.g. 5.0.4)
#   "<token> POINTS <S>\n"     -> "<epoch> <length>" recovery points of segment S, then ".\n"
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
#   "<token> SYNC <host>[,<host>...]|none\n" -> "OK": primary, synchronous replication: writes
#                                 SYNC_FILE (included by replication.conf) with a sync_replica entry
#                                 per replica (every commit waits for all of them), or empties it.
#                                 Firebird reads it when the database is opened, so the sync-standby
#                                 Job sends it while the database is in full shutdown
#                                 (sync-standby.pl)
#   "<token> SYNCTO\n"          -> "OK <host>[,<host>...]" or "OK none": the replicas the primary
#                                 replicates to synchronously (from SYNC_FILE: what applies once the
#                                 database is opened; the sync-standby Job changes it only in full
#                                 shutdown)
#   "<token> STANDBY on\n"      -> "OK": replica, becomes the synchronous standby: the segment
#                                 puller stops applying journal segments (the primary sends every
#                                 change directly), and only records the last archived one
#   "<token> STANDBY off <S>\n" -> "OK": back to journal shipping after segment S (the primary's
#                                 last segment, in full shutdown): the replica control file and the
#                                 puller's position move to S
#   "<token> PROMOTE <A>|none\n" -> "OK <S>": planned switchover, promotes this replica in place (no
#                                 restart): after every received segment is applied, a short full
#                                 shutdown sets the replication sequence S (the control file
#                                 position, or the journal archive's last segment A when higher),
#                                 replaces the replication state and the offline bootstrap seed,
#                                 then replica mode none and publication. Marks the instance as the
#                                 primary (the "promoted" file) until its next restart, as the ConfigMap
#                                 reaches the pod's files later. A synchronous standby continues
#                                 after the last segment it saw archived (sync-seen) when higher.
#                                 "ERR ..." leaves it a replica.
#   "<token> REJOIN\n"          -> "OK": brings a database fenced by the isolation check back
#                                 online, sent by the operator once it checked that this instance
#                                 still holds the leader Lease ("OK" too when it is not fenced)
#
# FILE, STORE, REMOVE and FILES only handle plain "*.nbk" and "*.fbk" names (no directories), so
# they cannot touch the database, the journal or the replication state. FILE and STORE run in a child process, so a
# large transfer does not hold up replicas and the operator.
#
# Requests are signed with the SYSDBA password (ISC_PASSWORD), which never crosses the network:
#
#   "SIG1 <epoch> <nonce> <mac> <command>\n", mac = hex HMAC-SHA256("<epoch> <nonce> <command>")
#
# keyed with the password (segment-auth.pl, included in this script). The server accepts it within AUTH_WINDOW_SECONDS of
# its own clock and only once per nonce (replays are refused), and answers a bad or replayed
# signature with "ERR unauthorized (<reason>)". "PING" answers "OK": clients send it signed once
# per server to tell this server from one of an earlier version, which answers a signed request
# with "ERR unauthorized" and gets the legacy form "<password> <command>" instead (rolling update).
# The legacy form is still accepted from clients of earlier versions. Replies and transferred
# bytes are neither encrypted nor signed.
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
#@include segment-auth.pl

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
# how far a signed request's time may be from this server's clock (seconds), and the nonces seen
my $auth_window = $ENV{AUTH_WINDOW_SECONDS} // 300;
my %nonces;
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
# recovery points (journal archive): the primary's journal segments sampled every second
my $recovery_points = ($ENV{RECOVERY_POINTS} // '') eq 'true';
my $journal_dir = $ENV{JOURNAL_DIR} // '';
my $points_dir = "$base/points";
my $seed_file = "$base/seed.copy";
my $bootstrap_seed = "$base/bootstrap-seed.fdb";
my $pause_flag = "$base/.pause-pull";
my $pause_ack  = "$base/.pull-paused";
my $self_fenced = "$base/self-fenced";
# promoted in place (PROMOTE): the primary from now on, whatever the ConfigMap file still says;
# removed when the instance restarts (init-instance.sh)
my $promoted_flag = "$base/promoted";
# synchronous replication: the primary's sync_replica entry, and the standby's flag and last seen segment
my $sync_file = "$base/sync.conf";
my $standby_flag = "$base/sync-standby";
my $standby_seen = "$base/sync-seen";
my $state_file = $ENV{STATE_FILE} // "$base/.last-pulled";
my $isolation_timeout = $ENV{ISOLATION_TIMEOUT_SECONDS} // 0;
my $name_re   = qr/^[A-Za-z0-9._-]+\.journal-\d+$/;
# backups, and the replica control file written next to a replica's nbackup (NBACKUP)
my $backup_re = qr/^[A-Za-z0-9][A-Za-z0-9._-]*\.(?:nbk|fbk|nbk\.ctl)$/;
(my $data_dir = $database) =~ s{/[^/]*$}{};
$data_dir = '.' if $data_dir eq '';
$| = 1;

sub same_string {
  my ($a, $b) = @_;
  return 0 unless length($a) == length($b);
  my $diff = 0;
  $diff |= ord(substr($a, $_, 1)) ^ ord(substr($b, $_, 1)) for 0 .. length($a) - 1;
  return $diff == 0;
}

# The command and argument of a request line, or (undef, undef, reason) when it is not authorized
# (reason '' for a legacy request with a wrong password)
sub authorize {
  my ($line) = @_;
  if ($line =~ /^SIG1 (\d+) ([0-9a-f]{16,64}) ([0-9a-f]{64}) (.+)$/) {
    my ($at, $nonce, $mac, $request) = ($1, $2, $3, $4);
    return (undef, undef, 'signature') unless same_string(hmac_sha256_hex("$at $nonce $request", $token), $mac);
    return (undef, undef, 'clock') if abs(time - $at) > $auth_window;
    return (undef, undef, 'replay') if exists $nonces{$nonce};
    $nonces{$nonce} = $at;
    if (keys(%nonces) > 1000) {
      for (keys %nonces) { delete $nonces{$_} if time - $nonces{$_} > $auth_window; }
    }
    my ($cmd, $arg) = split / /, $request, 2;
    return ($cmd, $arg, undef);
  }
  my ($given, $cmd, $arg) = split / /, $line, 3;
  return (undef, undef, '') if !defined $cmd || $given ne $token;
  return ($cmd, $arg, undef);
}

my $server = IO::Socket::INET->new(LocalPort => $port, Listen => 16, ReuseAddr => 1, Proto => 'tcp')
  or die "listen on $port: $!\n";
print $files_only ? "backup file server listening on $port, serving $data_dir\n" : "segment server listening on $port, serving $dir\n";

sub slurp { my ($f) = @_; open(my $fh, '<', $f) or return ''; local $/; my $v = <$fh>; close $fh; $v //= ''; $v =~ s/\s+$//; return $v; }

sub is_primary {
  return 1 if !$files_only && -e $promoted_flag;
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

# NBACKUP <level> <file>: an nbackup of this instance's database into the data directory. On a
# replica (not the synchronous standby, which receives changes outside the journal) the segment
# puller is paused and every received segment applied first, and the replica control file is
# written next to the backup as <file>.ctl: the position the copy was taken at, which
# point-in-time recovery continues from (pitr-restore.sh). Nothing commits on such a replica
# meanwhile, so the copy and the control file describe the same point. Replies "OK replica <S>"
# (control file written) or "OK".
sub nbackup_here {
  my ($client, $level, $file) = @_;
  unlink "$data_dir/$file.ctl";
  my $replica = !is_primary() && !-e $standby_flag;
  my $ok = eval {
    my $ctl;
    if ($replica) {
      unlink $pause_ack;
      if (open(my $flag, '>', $pause_flag)) { close $flag; }
      wait_for(60, sub { -e $pause_ack }) or die "segment puller did not pause\n";
      my $control = control_path() or die "no replica control file\n";
      wait_for(300, sub {
        my $c = read_control($control) or return 0;
        return 0 if $c->{offset};
        return !grep { my $s = segment_sequence("$source/$_"); defined $s && $s > $c->{sequence} } segments($source);
      }) or die "replica did not finish applying received segments\n";
      $ctl = read_control($control);
    }
    system('fbsvcmgr', 'localhost:service_mgr', 'action_nbak', 'dbname', $database,
      'nbk_file', "$data_dir/$file", 'nbk_level', $level) == 0 or die "nbackup failed\n";
    if ($ctl) {
      open(my $out, '>:raw', "$data_dir/$file.ctl.tmp") or die "write $file.ctl: $!\n";
      print $out $ctl->{data};
      close $out or die "write $file.ctl: $!\n";
      rename("$data_dir/$file.ctl.tmp", "$data_dir/$file.ctl") or die "rename $file.ctl: $!\n";
      print $client "OK replica $ctl->{sequence}\n";
      print "nbackup $file (level $level) taken at replica position $ctl->{sequence}\n";
    } else {
      print $client "OK\n";
      print "nbackup $file (level $level) taken\n";
    }
    1;
  };
  unless ($ok) {
    print $client "ERR $@";
    print "nbackup $file refused: $@";
  }
  unlink $pause_flag if $replica;
}

# PROMOTE: this replica becomes the primary without a restart (planned switchover, once the old
# primary is in full shutdown and this replica applied its last segment). Does what the init
# container's offline promotion does (init-instance.sh), during a short full shutdown: the server
# has the database closed then, so its header can be written. Until the promotion is complete the
# replica control file is kept and any failure restores the replica, so the operator can still
# promote it offline instead.
sub promote_here {
  my ($client, $archived) = @_;
  my $fb = sub { system('fbsvcmgr', 'localhost:service_mgr', 'action_properties', 'dbname', $database, @_) == 0 };
  my ($shut, $writable) = (0, 0);
  my $ok = eval {
    my $mode = live_value('MON$REPLICA_MODE');
    die "cannot read the replica mode\n" unless defined $mode;
    if ($mode == 0) {
      # promoted already (a repeated request)
      my $seq = live_value(q{RDB$GET_CONTEXT('SYSTEM', 'REPLICATION_SEQUENCE')}) // 0;
      write_file($promoted_flag, time . "\n");
      print $client "OK $seq\n";
      print "already promoted (sequence $seq)\n";
      return 1;
    }
    unlink $pause_ack;
    if (open(my $flag, '>', $pause_flag)) { close $flag; }
    wait_for(60, sub { -e $pause_ack }) or die "segment puller did not pause\n";
    my $control = control_path() or die "no replica control file\n";
    wait_for(300, sub {
      my $c = read_control($control) or return 0;
      return 0 if $c->{offset};
      return !grep { my $s = segment_sequence("$source/$_"); defined $s && $s > $c->{sequence} } segments($source);
    }) or die "replica did not finish applying received segments\n";
    my $seq = read_control($control)->{sequence};
    # a synchronous standby has every change up to the last segment archived on the old primary
    # (as the offline promotion: its puller records it, the control file stays where it was)
    my $seen = slurp($standby_seen);
    $seq = $seen if -e $standby_flag && $seen =~ /^\d+$/ && $seen > $seq;
    my $lineage;
    if (defined $archived && $archived > $seq) {
      # as the offline promotion: segments $seq+1..$archived of the archive are not in this lineage
      $lineage = "$seq $archived";
      $seq = $archived;
    }
    $fb->('prp_shutdown_mode', 'prp_sm_full', 'prp_force_shutdown', '0') or die "full shutdown failed\n";
    $shut = 1;
    system('perl', "$ENV{SCRIPT_DIR}/set-repl-seq.pl", $database, $seq) == 0 or die "cannot set the replication sequence\n";
    # the replica's journal and archive (the new journal starts empty) and the offline bootstrap
    # seed new replicas can start from, taken while nothing has the database open
    for my $d ($journal_dir, $dir) {
      next unless $d ne '' && opendir(my $dh, $d);
      unlink map { "$d/$_" } grep { -f "$d/$_" } readdir($dh);
      closedir($dh);
    }
    unlink $bootstrap_seed, "$base/bootstrap-seed.seq";
    system('cp', $database, "$bootstrap_seed.tmp") == 0 or die "cannot copy the bootstrap seed\n";
    $fb->('prp_online_mode', 'prp_sm_normal') or die "cannot bring the database online\n";
    $shut = 0;
    system('gfix', '-replica', 'none', "localhost:$database") == 0 or die "cannot set replica mode none\n";
    $writable = 1;
    system('sh', '-c', "isql -q -b -i \"$ENV{SCRIPT_DIR}/enable-publication.sql\" localhost:$database") == 0
      or die "cannot enable publication\n";
    # complete: the replica's state goes (the new journal continues after $seq)
    write_file($promoted_flag, time . "\n");
    if (defined $lineage && !grep { $_ eq $lineage } split /\n/, slurp("$base/lineage")) {
      if (open(my $fh, '>>', "$base/lineage")) { print $fh "$lineage\n"; close $fh; }
    }
    if (opendir(my $dh, $source)) {
      unlink map { "$source/$_" } grep { -f "$source/$_" } readdir($dh);
      closedir($dh);
    }
    unlink $state_file, $standby_seen, $standby_flag;
    print $client "OK $seq\n";
    print "promoted in place: the journal continues after segment $seq\n";
    # the copy taken in full shutdown becomes what the offline promotion's seed is: online, not a
    # replica, publishing (a file no server has open, as the init container's tools use them);
    # without it new replicas seed from a ready replica
    my $tmp = "$bootstrap_seed.tmp";
    if (system('gfix', '-online', 'normal', $tmp) == 0 && system('gfix', '-replica', 'none', $tmp) == 0 &&
        system('sh', '-c', "isql -q -b -i \"$ENV{SCRIPT_DIR}/enable-publication.sql\" \"$tmp\"") == 0 &&
        write_file("$base/bootstrap-seed.seq", "$seq\n") && rename($tmp, $bootstrap_seed)) {
      print "offline bootstrap seed written (sequence $seq)\n";
    } else {
      unlink $tmp, "$base/bootstrap-seed.seq";
      print "no offline bootstrap seed: the copy could not be prepared\n";
    }
    1;
  };
  unless ($ok) {
    my $err = $@;
    # back to a replica the offline promotion can still take over
    system('gfix', '-replica', 'read_only', "localhost:$database") if $writable;
    unlink "$bootstrap_seed.tmp";
    $fb->('prp_online_mode', 'prp_sm_normal') if $shut;
    print $client "ERR $err";
    print "promotion refused: $err";
  }
  unlink $pause_flag;
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
  # recovery points of segments long gone
  if (opendir(my $ph, $points_dir)) {
    for my $f (grep { /^\d+$/ } readdir($ph)) {
      my $mtime = (stat("$points_dir/$f"))[9];
      unlink "$points_dir/$f" if defined $mtime && $now - $mtime > $max_retention;
    }
    closedir($ph);
  }
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

# Recovery points (RECOVERY_POINTS, with a journal archive): every second the primary's journal
# segments still being written are sampled, and each new length of segment S is appended to
# points/S as "<epoch seconds> <length>". The header length only grows by whole writes (the
# blocks of a commit; verified), so a segment cut at a recorded length (header length set,
# file truncated) is a valid segment ending at that moment: point-in-time recovery cuts the
# segment after its target there (pitr-restore.sh), instead of applying whole segments only.
my $sampler;
sub start_sampler {
  return if $files_only || !$recovery_points || $journal_dir eq '';
  my $pid = fork;
  if (!defined $pid) { print "cannot start the recovery point sampler: $!\n"; return; }
  if ($pid == 0) {
    close $server;
    mkdir $points_dir;
    my %last;
    while (1) {
      if (is_primary() && opendir(my $dh, $journal_dir)) {
        my @names = grep { $_ =~ $name_re } readdir($dh);
        closedir($dh);
        for my $name (@names) {
          open(my $fh, '<:raw', "$journal_dir/$name") or next;
          my $n = read($fh, my $hdr, 48);
          close $fh;
          next unless $n && $n == 48 && substr($hdr, 0, 11) eq 'FBCHANGELOG';
          my (undef, undef, undef, undef, $seq, $length) = unpack('a12 v v a16 Q< Q<', $hdr);
          next if !$seq || $length <= 48 || ($last{$seq} // 0) == $length;
          $last{$seq} = $length;
          if (open(my $out, '>>', "$points_dir/$seq")) { print $out time() . " $length\n"; close $out; }
        }
        # forget segments no longer written
        my %live = map { /journal-0*(\d+)$/ ? ($1 => 1) : () } @names;
        delete $last{$_} for grep { !$live{$_} } keys %last;
      }
      sleep 1;
    }
  }
  $sampler = $pid;
}
start_sampler();

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
    if (defined $sampler && $done == $sampler) { $sampler = undef; sleep 1; start_sampler(); }
  }
  $server->timeout(30);
  my $client = $server->accept or next;
  $client->timeout(600);
  my $line = <$client>;
  if (!defined $line) { close $client; next; }
  $line =~ s/\r?\n$//;
  my ($cmd, $arg, $denied) = authorize($line);
  if (defined $denied) {
    print $client $denied eq '' ? "ERR unauthorized\n" : "ERR unauthorized ($denied)\n";
  } elsif ($cmd eq 'PING') {
    print $client "OK\n";
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
  } elsif ($cmd eq 'SYNC' && defined $arg && $arg =~ /^(none|[A-Za-z0-9][A-Za-z0-9.-]*(?:,[A-Za-z0-9][A-Za-z0-9.-]*)*)$/) {
    # the server's ISC_USER / ISC_PASSWORD are the credentials: Firebird 4 ignores the sub-section
    # and uses them, Firebird 5 and later read password_env
    my $user = $ENV{ISC_USER} || 'SYSDBA';
    my $content = $arg eq 'none' ? '' : join('', map {
      "sync_replica = $_:$database\n{\n  username = $user\n  password_env = ISC_PASSWORD\n}\n"
    } split /,/, $arg);
    if (write_file($sync_file, $content)) {
      print "synchronous replication " . ($arg eq 'none' ? "off" : "to $arg") . " (applied when the database is opened)\n";
      print $client "OK\n";
    } else {
      print $client "ERR cannot write $sync_file: $!\n";
    }
  } elsif ($cmd eq 'SYNCTO') {
    my @hosts = slurp($sync_file) =~ /^sync_replica\s*=\s*([^:\s]+):/mg;
    print $client "OK " . (@hosts ? join(',', @hosts) : 'none') . "\n";
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
  } elsif ($cmd eq 'PRIMARYSEEN') {
    # replica: seconds since the segment puller last reached the primary, and which one
    my ($at, $host) = slurp("$base/primary-seen") =~ /^(\d+) (\S+)$/;
    print $client defined $at ? "OK " . (time - $at) . " $host\n" : "OK never\n";
  } elsif ($cmd eq 'VERSION') {
    my $version = live_value(q{RDB$GET_CONTEXT('SYSTEM', 'ENGINE_VERSION')});
    print $client defined $version && $version =~ /^[\d.]+$/ ? "OK $version\n" : "ERR cannot read the engine version\n";
  } elsif ($cmd eq 'POINTS' && defined $arg && $arg =~ /^(\d+)$/) {
    if (open(my $fh, '<', "$points_dir/$1")) {
      while (my $l = <$fh>) { print $client $l if $l =~ /^\d+ \d+\n$/; }
      close $fh;
    }
    print $client ".\n";
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
  } elsif ($cmd eq 'PROMOTE' && !$files_only && defined $arg && $arg =~ /^(\d+|none)$/) {
    my $archived = $1 eq 'none' ? undef : $1;
    my $pid = fork;
    if (!defined $pid) {
      print $client "ERR fork: $!\n";
    } elsif ($pid == 0) {
      close $server;
      promote_here($client, $archived);
      close $client;
      exit 0;
    }
  } elsif ($cmd eq 'NBACKUP' && !$files_only && defined $arg && $arg =~ /^([0-2]) ([A-Za-z0-9][A-Za-z0-9._-]*\.nbk)$/) {
    my ($level, $file) = ($1, $2);
    my $pid = fork;
    if (!defined $pid) {
      print $client "ERR fork: $!\n";
    } elsif ($pid == 0) {
      close $server;
      nbackup_here($client, $level, $file);
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
