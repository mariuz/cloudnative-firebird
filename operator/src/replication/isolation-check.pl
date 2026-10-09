#!/usr/bin/perl
# Primary isolation check (CloudNativePG's isolationCheck), started by the segment server when
# automatic failover is enabled.
#
# Every CHECK_INTERVAL seconds, while this instance is the primary, it checks whether it can reach
# the Kubernetes API server (a TCP connection to KUBERNETES_SERVICE_HOST:KUBERNETES_SERVICE_PORT)
# or the segment server of any other instance (the addresses of PEERS_SERVICE, the headless
# Service, other than POD_IP). When neither has been reachable for ISOLATION_TIMEOUT_SECONDS, the
# primary is cut off from the rest of the cluster: the operator may be promoting a replica on the
# other side, so the database is put into full shutdown (as a fenced instance) and the marker
# file SELF_FENCED_FILE records it. Clients that can still reach this pod then cannot write to a
# database that is no longer the primary.
#
# The database stays shut down until the operator, having checked that this instance still holds
# the leader Lease, asks the segment server to bring it back online (REJOIN). Being reachable again
# is not enough: a replica may have been promoted meanwhile. Once the database is online again
# (by REJOIN, or by an administrator), the marker is removed and the check starts over.
#
# Without peers or an API server address to check, nothing is ever fenced.
#
# Reaching the API server is not enough on its own: a primary that neither the operator nor any
# replica has reached for CONTACT_TIMEOUT_SECONDS (the segment server records each authenticated
# request in LAST_CONTACT_FILE: the operator's checks at least every reconcile, the replicas'
# pullers every few seconds) is cut off on the other side of a partition, where the operator
# fails it over (a "cut-off primary"). It fences itself too, before the operator, which waits
# longer before it promotes a replica. Only when the headless Service lists other instances: a
# primary without replicas has nothing to be failed over to.
#
# The other instances are found through cluster DNS (the headless Service), which can fail too, for
# example when the DNS servers are on the other side of the partition. The lookup runs in a child
# process with a time limit (DNS_TIMEOUT), and every answer is kept in PEERS_CACHE_FILE. When a
# lookup fails (not "no such name": no answer at all), the check uses the addresses it knew and
# those the operator publishes (PEER_ADDRESSES_FILE, the current pod IPs: a peer that restarted with
# a new address while DNS was down), and the operator's list of ready replicas (SEED_SOURCES_FILE)
# tells whether there are any. A
# primary that nothing has reached is then fenced only when none of the known addresses answers
# either: during a DNS outage alone, the operator and the replicas cannot resolve this primary
# either (so they do not reach it), while it still reaches them, and nothing fails it over.
use strict;
use warnings;
use IO::Select;
use IO::Socket::INET;
use POSIX ();
use Socket qw(getaddrinfo getnameinfo AF_INET SOCK_STREAM NI_NUMERICHOST NIx_NOSERV EAI_NONAME);

my $database     = $ENV{DATABASE_PATH} or die "DATABASE_PATH is required\n";
my $primary_file = $ENV{PRIMARY_FILE} or die "PRIMARY_FILE is required\n";
my $marker       = $ENV{SELF_FENCED_FILE} or die "SELF_FENCED_FILE is required\n";
my $timeout      = $ENV{ISOLATION_TIMEOUT_SECONDS} // 0;
my $contact_timeout = $ENV{CONTACT_TIMEOUT_SECONDS} // 0;
my $contact_file = $ENV{LAST_CONTACT_FILE} // '';
my $interval     = $ENV{CHECK_INTERVAL} // 5;
my $self         = $ENV{POD_NAME} // '';
my $self_ip      = $ENV{POD_IP} // '';
my $peers        = $ENV{PEERS_SERVICE} // '';
my $peer_port    = $ENV{SEGMENT_PORT} // 3051;
my $api_host     = $ENV{KUBERNETES_SERVICE_HOST} // '';
my $api_port     = $ENV{KUBERNETES_SERVICE_PORT} // 443;
my $connect_timeout = $ENV{CONNECT_TIMEOUT} // 3;
my $dns_timeout  = $ENV{DNS_TIMEOUT} // 2;
my $peers_cache  = $ENV{PEERS_CACHE_FILE} // "$marker.peers";
my $seed_sources = $ENV{SEED_SOURCES_FILE} // '';
my $peer_addresses = $ENV{PEER_ADDRESSES_FILE} // '';
my $once         = ($ENV{ONCE} // '') eq 'true';   # tests: one check, then exit
exit 0 unless $timeout =~ /^\d+$/ && $timeout > 0;
$| = 1;

sub slurp { my ($f) = @_; open(my $fh, '<', $f) or return ''; local $/; my $v = <$fh>; close $fh; $v //= ''; $v =~ s/\s+$//; return $v; }

sub is_primary {
  # promoted in place (segment-server.pl PROMOTE) before the ConfigMap file names this instance
  return 1 if $ENV{REPLICATION_DIR} && -e "$ENV{REPLICATION_DIR}/promoted";
  my $primary = slurp($primary_file);
  return $primary eq '' || $primary =~ /^\Q$self\E(\.|$)/;
}

sub reachable {
  my ($host, $port, $timeout) = @_;
  my $sock = IO::Socket::INET->new(PeerHost => $host, PeerPort => $port, Proto => 'tcp', Timeout => $timeout // $connect_timeout)
    or return 0;
  close $sock;
  return 1;
}

# Resolves the headless Service in a child process, so that a lookup DNS never answers (every
# search domain timing out) cannot hold up the check: (1, addresses) when DNS answered, with no
# addresses for "no such name", (0) when it did not answer within DNS_TIMEOUT
sub resolve_peers {
  return (1) if $peers eq '';
  pipe(my $r, my $w) or return (0);
  my $pid = fork;
  return (0) unless defined $pid;
  if ($pid == 0) {
    close $r;
    # tests: a DNS server that fails, or never answers
    if (($ENV{TEST_DNS} // '') eq 'fail') { print $w "failed\n"; close $w; POSIX::_exit(0); }
    sleep 60 if ($ENV{TEST_DNS} // '') eq 'hang';
    my ($err, @res) = getaddrinfo($peers, '', { family => AF_INET, socktype => SOCK_STREAM });
    if ($err) {
      print $w ($err == EAI_NONAME ? "none\n" : "failed\n");
    } else {
      for my $ai (@res) {
        my ($e, $ip) = getnameinfo($ai->{addr}, NI_NUMERICHOST, NIx_NOSERV);
        print $w "$ip\n" unless $e;
      }
      print $w "ok\n";
    }
    close $w;
    POSIX::_exit(0);
  }
  close $w;
  # whole seconds: the image ships perl-base, without Time::HiRes
  my ($out, $select, $deadline) = ('', IO::Select->new($r), time + $dns_timeout);
  while ((my $left = $deadline - time) > 0) {
    last unless $select->can_read($left);
    my $n = sysread($r, my $buf, 4096);
    last unless $n;
    $out .= $buf;
  }
  close $r;
  kill 'KILL', $pid;
  waitpid($pid, 0);
  my @lines = split /\n/, $out;
  my $status = pop(@lines) // '';
  return (1) if $status eq 'none';
  return (0) unless $status eq 'ok';
  my %seen = map { $_ => 1 } grep { $_ ne $self_ip } @lines;
  return (1, sort keys %seen);
}

sub known_peers {
  open(my $fh, '<', $peers_cache) or return ();
  my @ips = grep { /^[0-9.]+$/ } map { s/\s+$//r } <$fh>;
  close $fh;
  return @ips;
}

sub published_peers {
  return () if $peer_addresses eq '';
  return grep { /^[0-9.]+$/ } split /\s+/, slurp($peer_addresses);
}

sub remember_peers {
  my @ips = @_;
  return if join(',', known_peers()) eq join(',', @ips);
  open(my $fh, '>', "$peers_cache.tmp") or return;
  print $fh map { "$_\n" } @ips;
  close $fh;
  rename "$peers_cache.tmp", $peers_cache;
}

# Whether the operator lists ready replicas (the cluster ConfigMap, mounted: no DNS involved)
sub seed_sources_listed { return slurp($seed_sources) =~ /\S/ ? 1 : 0 if $seed_sources ne ''; return 0; }

# The other instances, once per check: (1, addresses) from DNS, or (0, addresses known from the
# last answer) when DNS failed
my ($view, $dns_failing);
sub peer_view {
  return @$view if $view;
  my ($answered, @ips) = resolve_peers();
  if ($answered) {
    print "cluster DNS answers again\n" if $dns_failing;
    $dns_failing = 0;
    remember_peers(@ips);
    $view = [1, @ips];
  } else {
    # the last answer, and the addresses the operator publishes in the cluster ConfigMap (mounted:
    # no DNS involved), which follow peers that restarted with a new address meanwhile
    my %known = map { $_ => 1 } grep { $_ ne $self_ip } (known_peers(), published_peers());
    my @known = sort keys %known;
    print "cluster DNS did not answer for $peers: using the " . scalar(@known) . " peer address(es) known from the last answer and the operator\n" unless $dns_failing;
    $dns_failing = 1;
    $view = [0, @known];
  }
  return @$view;
}

# Whether other instances could be failed over to while nothing reaches this primary
sub cut_off_from_peers {
  my ($answered, @ips) = peer_view();
  return scalar @ips if $answered;
  # DNS failed: only when none of the known peers answers either (not a DNS outage alone); with
  # a short timeout each, as the fence must come before the operator's failover of this primary
  if (@ips) {
    for my $ip (@ips) { return 0 if reachable($ip, $peer_port, 1); }
    return 1;
  }
  return seed_sources_listed();
}

sub connected {
  return 1 if $api_host ne '' && reachable($api_host, $api_port);
  my (undef, @ips) = peer_view();
  for my $ip (@ips) { return 1 if reachable($ip, $peer_port); }
  return 0;
}

# The database state from the local server: "online", "shutdown", or undef (no answer)
sub database_state {
  my $out = `fbsvcmgr localhost:service_mgr action_db_stats dbname '$database' sts_hdr_pages 2>&1`;
  # Firebird 6 refuses header statistics for a database in full shutdown
  return 'shutdown' if $out =~ /^database .* shutdown/m;
  return undef if $?;
  return $out =~ /shutdown/ ? 'shutdown' : 'online';
}

# Seconds since the operator or a replica last reached this instance's segment server, counted
# from when it became the primary at the earliest; undef when the check is off
my $primary_since = time;
sub unreached_for {
  return undef unless $contact_timeout =~ /^\d+$/ && $contact_timeout > 0 && $contact_file ne '';
  my $last = $primary_since;
  my $mtime = (stat $contact_file)[9];
  $last = $mtime if defined $mtime && $mtime > $last;
  return time - $last;
}

sub fence {
  my ($since, $reason) = @_;
  # the marker first: a restarted sidecar must not take a fenced database for an online one
  open(my $fh, '>', "$marker.tmp") or do { print "cannot write $marker: $!\n"; return };
  print $fh time, "\n";
  close $fh;
  rename "$marker.tmp", $marker;
  print "isolated: " . ($reason // "neither the Kubernetes API server nor another instance reachable for " . (time - $since) . "s") . "; fencing the primary (database in full shutdown)\n";
  if (system('fbsvcmgr', 'localhost:service_mgr', 'action_properties', 'dbname', $database,
             'prp_shutdown_mode', 'prp_sm_full', 'prp_force_shutdown', '0') != 0) {
    unlink $marker;
    print "full shutdown failed; retrying on the next check\n";
  }
}

# LAST_CONNECTED (epoch seconds; tests) backdates the last successful check
my $last_ok = ($ENV{LAST_CONNECTED} // '') =~ /^\d+$/ ? $ENV{LAST_CONNECTED} : time;
# LAST_CONTACT_BASE (epoch seconds; tests) backdates when this instance became the primary
$primary_since = $ENV{LAST_CONTACT_BASE} if ($ENV{LAST_CONTACT_BASE} // '') =~ /^\d+$/;
while (1) {
  undef $view;
  # the primary looks its peers up on every check, so that they are known when DNS fails
  peer_view() if is_primary() && !-f $marker;
  my $unreached = unreached_for();
  if (!is_primary()) {
    $last_ok = time;
    $primary_since = time;
  } elsif (-f $marker) {
    # fenced: wait until the database is online again (REJOIN from the operator, or an administrator)
    $last_ok = time;
    if ((database_state() // '') eq 'online') {
      unlink $marker;
      print "database online again: isolation fence lifted\n";
    }
  } elsif (defined $unreached && $unreached >= $contact_timeout && cut_off_from_peers()) {
    fence(time - $unreached, "neither the operator nor any replica has reached this primary for ${unreached}s"
      . ($dns_failing ? ' (cluster DNS failing, and no known peer answers)' : ''));
  } elsif (connected()) {
    $last_ok = time;
  } elsif (time - $last_ok >= $timeout) {
    fence($last_ok);
  } else {
    print "isolated for " . (time - $last_ok) . "s (fencing after ${timeout}s)\n";
  }
  last if $once;
  sleep $interval;
}
