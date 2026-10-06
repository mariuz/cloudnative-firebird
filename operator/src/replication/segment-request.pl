#!/usr/bin/perl
# One request to a segment server, for shell scripts: prints the reply lines.
# Usage: segment-request.pl <host> <request>; environment: ISC_PASSWORD, SEGMENT_PORT.
use strict;
use warnings;
use IO::Socket::INET;
#@include segment-auth.pl

my ($host, @request) = @ARGV;
die "usage: segment-request.pl <host> <request>\n" unless defined $host && @request;
my $sock = segment_open($host, $ENV{SEGMENT_PORT} || 3051, $ENV{ISC_PASSWORD} // '', join(' ', @request));
print while <$sock>;
close $sock;
