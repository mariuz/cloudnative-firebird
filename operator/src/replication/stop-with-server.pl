# SIGTERM for a sidecar that runs as its container's PID 1, which ignores signals it has no
# handler for (Kubernetes then kills it only at the end of the grace period): the sidecar keeps
# working until the pod's Firebird server has stopped (the smart shutdown waits for clients first,
# utils/lifecycle.ts), then exits. Where no server runs that is at once.
our $stopping = 0;
$SIG{TERM} = $SIG{INT} = sub { $stopping = 1 };
sub firebird_stopped {
  my $s = IO::Socket::INET->new(PeerAddr => '127.0.0.1', PeerPort => ($ENV{FIREBIRD_PORT} // 3050), Proto => 'tcp', Timeout => 2);
  return 1 unless $s;
  close $s;
  return 0;
}
# called from the main loop: exits once stopping and the server is gone
sub exit_when_stopped {
  return unless $stopping;
  return unless firebird_stopped();
  print "stopping: the Firebird server has stopped\n";
  exit 0;
}
