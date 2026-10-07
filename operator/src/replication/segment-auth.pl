# Segment server client authentication, included by the scripts that talk to segment servers
# (not shipped on its own). Requests are signed with the SYSDBA password (segment-server.pl):
# the password itself never crosses the network. A server of an earlier version (during a
# rolling update) answers a signed PING with "ERR unauthorized" and gets the legacy form.
# The Firebird image ships perl-base only (no Digest::SHA), so SHA-256 (FIPS 180-4) and HMAC
# (RFC 2104) are implemented here; they only ever hash short request lines.
use IO::Socket::INET;

my @SHA256_K = (
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
  0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
  0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
  0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
  0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
  0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
);

sub sha256_rotr { my ($x, $n) = @_; return (($x >> $n) | ($x << (32 - $n))) & 0xffffffff; }

# SHA-256 of a byte string, as 32 bytes
sub sha256 {
  my ($message) = @_;
  my @h = (0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19);
  my $bits = length($message) * 8;
  $message .= "\x80" . ("\0" x ((55 - length($message)) % 64)) . pack('NN', int($bits / 2**32), $bits % 2**32);
  for (my $offset = 0; $offset < length($message); $offset += 64) {
    my @w = unpack('N16', substr($message, $offset, 64));
    for my $i (16 .. 63) {
      my $s0 = sha256_rotr($w[$i - 15], 7) ^ sha256_rotr($w[$i - 15], 18) ^ ($w[$i - 15] >> 3);
      my $s1 = sha256_rotr($w[$i - 2], 17) ^ sha256_rotr($w[$i - 2], 19) ^ ($w[$i - 2] >> 10);
      $w[$i] = ($w[$i - 16] + $s0 + $w[$i - 7] + $s1) & 0xffffffff;
    }
    my ($a, $b, $c, $d, $e, $f, $g, $hh) = @h;
    for my $i (0 .. 63) {
      my $t1 = ($hh + (sha256_rotr($e, 6) ^ sha256_rotr($e, 11) ^ sha256_rotr($e, 25))
        + (($e & $f) ^ (~$e & 0xffffffff & $g)) + $SHA256_K[$i] + $w[$i]) & 0xffffffff;
      my $t2 = ((sha256_rotr($a, 2) ^ sha256_rotr($a, 13) ^ sha256_rotr($a, 22))
        + (($a & $b) ^ ($a & $c) ^ ($b & $c))) & 0xffffffff;
      ($hh, $g, $f, $e, $d, $c, $b, $a) = ($g, $f, $e, ($d + $t1) & 0xffffffff, $c, $b, $a, ($t1 + $t2) & 0xffffffff);
    }
    my @add = ($a, $b, $c, $d, $e, $f, $g, $hh);
    $h[$_] = ($h[$_] + $add[$_]) & 0xffffffff for 0 .. 7;
  }
  return pack('N8', @h);
}

# HMAC-SHA256 of a message with a key, in hex
sub hmac_sha256_hex {
  my ($message, $key) = @_;
  $key = sha256($key) if length($key) > 64;
  $key .= "\0" x (64 - length($key));
  my $inner = sha256(($key ^ ("\x36" x 64)) . $message);
  return unpack('H*', sha256(($key ^ ("\x5c" x 64)) . $inner));
}

my %segment_auth_signed;   # "host:port" => 1 (signed) or the time it answered as a legacy server

sub segment_auth_nonce {
  my $bytes = '';
  if (open(my $random, '<:raw', '/dev/urandom')) { read($random, $bytes, 16); close $random; }
  $bytes = join('', map { chr(int(rand(256))) } 1 .. 16) if length($bytes) < 16;
  return unpack('H*', $bytes);
}

sub segment_auth_line {
  my ($secret, $request) = @_;
  my ($at, $nonce) = (time, segment_auth_nonce());
  return "SIG1 $at $nonce " . hmac_sha256_hex("$at $nonce $request", $secret) . " $request";
}

# Connects to a segment server and sends one request; returns the socket to read the reply from
sub segment_open {
  my ($host, $port, $secret, $request, $connect_timeout) = @_;
  $connect_timeout //= 10;
  my $key = "$host:$port";
  my $known = $segment_auth_signed{$key};
  # segment TLS (spec.replication.segmentTLS): through the local proxy, which opens mutual TLS to
  # the host named on the first line (segment-tls in the operator image)
  my $proxy = $ENV{SEGMENT_PROXY} // '';
  my $connect = sub {
    if ($proxy =~ /^(.+):(\d+)$/) {
      my $sock = IO::Socket::INET->new(PeerHost => $1, PeerPort => $2, Proto => 'tcp', Timeout => $connect_timeout)
        or die "connect to the segment TLS proxy $proxy: $!\n";
      print $sock "CONNECT $host $port\n";
      return $sock;
    }
    IO::Socket::INET->new(PeerHost => $host, PeerPort => $port, Proto => 'tcp', Timeout => $connect_timeout)
      or die "connect $host:$port: $!\n";
  };
  # tell a current server from one of an earlier version (asked again a minute later)
  if (!defined $known || ($known != 1 && time - $known > 60)) {
    my $probe = $connect->();
    print $probe segment_auth_line($secret, 'PING') . "\n";
    my $reply = <$probe> // '';
    close $probe;
    $reply =~ s/\r?\n$//;
    die "segment server $host:$port: no answer\n" if $reply eq '';
    $known = $segment_auth_signed{$key} = $reply eq 'ERR unauthorized' ? time : 1;
  }
  my $sock = $connect->();
  print $sock ($known == 1 ? segment_auth_line($secret, $request) : "$secret $request") . "\n";
  return $sock;
}
