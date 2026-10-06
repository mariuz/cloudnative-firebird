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
use strict;
use warnings;
use IO::Socket::INET;
use Socket qw(getaddrinfo getnameinfo AF_INET SOCK_STREAM NI_NUMERICHOST NIx_NOSERV);

my $database     = $ENV{DATABASE_PATH} or die "DATABASE_PATH is required\n";
my $primary_file = $ENV{PRIMARY_FILE} or die "PRIMARY_FILE is required\n";
my $marker       = $ENV{SELF_FENCED_FILE} or die "SELF_FENCED_FILE is required\n";
my $timeout      = $ENV{ISOLATION_TIMEOUT_SECONDS} // 0;
my $interval     = $ENV{CHECK_INTERVAL} // 5;
my $self         = $ENV{POD_NAME} // '';
my $self_ip      = $ENV{POD_IP} // '';
my $peers        = $ENV{PEERS_SERVICE} // '';
my $peer_port    = $ENV{SEGMENT_PORT} // 3051;
my $api_host     = $ENV{KUBERNETES_SERVICE_HOST} // '';
my $api_port     = $ENV{KUBERNETES_SERVICE_PORT} // 443;
my $connect_timeout = $ENV{CONNECT_TIMEOUT} // 3;
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
  my ($host, $port) = @_;
  my $sock = IO::Socket::INET->new(PeerHost => $host, PeerPort => $port, Proto => 'tcp', Timeout => $connect_timeout)
    or return 0;
  close $sock;
  return 1;
}

# The other instances: every address of the headless Service but this pod's
sub peer_addresses {
  return () if $peers eq '';
  my ($err, @res) = getaddrinfo($peers, '', { family => AF_INET, socktype => SOCK_STREAM });
  return () if $err;
  my %seen;
  for my $ai (@res) {
    my ($e, $ip) = getnameinfo($ai->{addr}, NI_NUMERICHOST, NIx_NOSERV);
    $seen{$ip} = 1 if !$e && $ip ne $self_ip;
  }
  return sort keys %seen;
}

sub connected {
  return 1 if $api_host ne '' && reachable($api_host, $api_port);
  for my $ip (peer_addresses()) { return 1 if reachable($ip, $peer_port); }
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

sub fence {
  my ($since) = @_;
  # the marker first: a restarted sidecar must not take a fenced database for an online one
  open(my $fh, '>', "$marker.tmp") or do { print "cannot write $marker: $!\n"; return };
  print $fh time, "\n";
  close $fh;
  rename "$marker.tmp", $marker;
  print "isolated: neither the Kubernetes API server nor another instance reachable for " . (time - $since) . "s; fencing the primary (database in full shutdown)\n";
  if (system('fbsvcmgr', 'localhost:service_mgr', 'action_properties', 'dbname', $database,
             'prp_shutdown_mode', 'prp_sm_full', 'prp_force_shutdown', '0') != 0) {
    unlink $marker;
    print "full shutdown failed; retrying on the next check\n";
  }
}

# LAST_CONNECTED (epoch seconds; tests) backdates the last successful check
my $last_ok = ($ENV{LAST_CONNECTED} // '') =~ /^\d+$/ ? $ENV{LAST_CONNECTED} : time;
while (1) {
  if (!is_primary()) {
    $last_ok = time;
  } elsif (-f $marker) {
    # fenced: wait until the database is online again (REJOIN from the operator, or an administrator)
    $last_ok = time;
    if ((database_state() // '') eq 'online') {
      unlink $marker;
      print "database online again: isolation fence lifted\n";
    }
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
