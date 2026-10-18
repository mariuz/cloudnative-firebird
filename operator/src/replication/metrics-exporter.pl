#!/usr/bin/perl
# Custom metrics exporter (CloudNativePG monitoring.customQueriesConfigMap / customQueriesSecret):
# serves GET /metrics in the Prometheus text format. Every scrape reads the queries the operator
# rendered from the cluster's ConfigMaps and Secrets (QUERIES_FILE, a mounted Secret: a changed
# query applies at the next scrape, without a restart), runs each one with isql against the local
# server, and turns the rows into metrics: firebird_<query>_<column>, with the LABEL columns as
# labels. Queries marked primary run only where the database is not a replica.
#
# QUERIES_FILE, one block per query:
#   query <name>
#   primary <0|1>
#   sql <statement; "\n" a newline, "\\" a backslash>
#   metric <column> <GAUGE|COUNTER|LABEL|DISCARD> <description>
#   end
#
# Besides: firebird_up (the database answers), firebird_exporter_query_error and
# firebird_exporter_query_duration_seconds per query.
use strict;
use warnings;
use IO::Socket::INET;
#@include stop-with-server.pl

my $database = $ENV{DATABASE_PATH} or die "DATABASE_PATH is required\n";
my $queries_file = $ENV{QUERIES_FILE} // '/custom-queries/queries';
my $port = $ENV{METRICS_PORT} // 9187;
my $timeout = $ENV{QUERY_TIMEOUT_SECONDS} // 10;
my $isql = $ENV{ISQL} // 'isql';
my $tmp = $ENV{TMPDIR} // '/tmp';
$| = 1;
# sub-second durations where Time::HiRes is installed
my $now = eval { require Time::HiRes; \&Time::HiRes::time } || sub { time };

sub read_queries {
  my @queries;
  open(my $fh, '<', $queries_file) or return ();
  my $q;
  while (my $line = <$fh>) {
    $line =~ s/\r?\n$//;
    if ($line =~ /^query (\S+)$/) {
      $q = { name => $1, primary => 0, sql => '', metrics => [] };
    } elsif (!$q) {
      next;
    } elsif ($line =~ /^primary ([01])$/) {
      $q->{primary} = $1;
    } elsif ($line =~ /^sql (.*)$/) {
      my $sql = $1;
      $sql =~ s/\\(.)/$1 eq 'n' ? "\n" : $1/ge;
      $q->{sql} = $sql;
    } elsif ($line =~ /^metric (\S+) (GAUGE|COUNTER|LABEL|DISCARD) ?(.*)$/) {
      push @{ $q->{metrics} }, { column => $1, usage => $2, description => $3 };
    } elsif ($line eq 'end') {
      push @queries, $q if $q->{sql} ne '';
      undef $q;
    }
  }
  close $fh;
  return @queries;
}

# Runs one statement (isql -b, LIST mode): the rows as hashes of upper-case column names, or
# undef and the error
sub run_sql {
  my ($sql) = @_;
  my $file = "$tmp/metrics-query-$$.sql";
  open(my $out, '>', $file) or return (undef, "cannot write $file: $!");
  $sql =~ s/[;\s]+$//;
  print $out "SET LIST ON;\n$sql;\n";
  close $out;
  my $pid = open(my $in, '-|');
  return (undef, "cannot fork: $!") unless defined $pid;
  if ($pid == 0) {
    open(STDERR, '>&', \*STDOUT);
    exec('timeout', $timeout, $isql, '-q', '-b', '-i', $file, "localhost:$database") or exit 127;
  }
  my @rows;
  my ($row, @text);
  while (my $line = <$in>) {
    $line =~ s/\r?\n$//;
    push @text, $line;
    if ($line =~ /^\s*$/) { push @rows, $row if $row; undef $row; next; }
    if ($line =~ /^(\S+)\s+(.*?)\s*$/) { $row //= {}; $row->{ uc $1 } = $2; }
    elsif ($line =~ /^(\S+)$/) { $row //= {}; $row->{ uc $1 } = ''; }
  }
  push @rows, $row if $row;
  close $in;
  my $status = $?;
  unlink $file;
  if ($status != 0) {
    my $message = join(' ', grep { /\S/ } @text);
    $message = $status >> 8 == 124 ? "timed out after ${timeout}s" : "isql exited with " . ($status >> 8) if $message eq '';
    return (undef, $message);
  }
  return (\@rows, undef);
}

sub label_value {
  my ($v) = @_;
  $v =~ s/\\/\\\\/g;
  $v =~ s/"/\\"/g;
  $v =~ s/\n/\\n/g;
  return $v;
}

sub metric_name { my ($n) = @_; $n = lc $n; $n =~ s/[^a-z0-9_]/_/g; return $n; }

sub numeric { my ($v) = @_; return defined $v && $v =~ /^\s*[-+]?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?\s*$/; }

sub scrape {
  my @lines;
  my ($state, $error) = run_sql('SELECT MON$REPLICA_MODE AS REPLICA_MODE FROM MON$DATABASE');
  my $up = $state && @$state ? 1 : 0;
  my $replica = $up && ($state->[0]{REPLICA_MODE} // '0') ne '0';
  push @lines, '# HELP firebird_up Whether the database answers a query', '# TYPE firebird_up gauge', "firebird_up $up";
  print "the database does not answer: $error\n" if !$up && $error;
  my (@errors, @durations, %families);
  for my $q (read_queries()) {
    next if $q->{primary} && $replica;
    my $start = $now->();
    my ($rows, $err) = $up ? run_sql($q->{sql}) : (undef, 'the database does not answer');
    my $query_label = label_value($q->{name});
    push @durations, "firebird_exporter_query_duration_seconds{query=\"$query_label\"} " . sprintf('%.3f', $now->() - $start);
    push @errors, "firebird_exporter_query_error{query=\"$query_label\"} " . ($rows ? 0 : 1);
    if (!$rows) { print "query $q->{name}: $err\n" if $up; next; }
    my @label_columns = grep { $_->{usage} eq 'LABEL' } @{ $q->{metrics} };
    for my $row (@$rows) {
      my @labels = map {
        my $v = $row->{ uc $_->{column} };
        defined $v && $v ne '<null>' ? metric_name($_->{column}) . '="' . label_value($v) . '"' : ();
      } @label_columns;
      my $labels = @labels ? '{' . join(',', @labels) . '}' : '';
      for my $m (grep { $_->{usage} eq 'GAUGE' || $_->{usage} eq 'COUNTER' } @{ $q->{metrics} }) {
        my $v = $row->{ uc $m->{column} };
        next unless numeric($v);
        my $name = 'firebird_' . metric_name($q->{name}) . '_' . metric_name($m->{column});
        my $family = $families{$name} //= { help => $m->{description}, type => lc $m->{usage}, samples => [] };
        $v =~ s/^\s+|\s+$//g;
        push @{ $family->{samples} }, "$name$labels $v";
      }
    }
  }
  for my $name (sort keys %families) {
    my $f = $families{$name};
    (my $help = $f->{help} ne '' ? $f->{help} : $name) =~ s/\\/\\\\/g;
    $help =~ s/\n/\\n/g;
    push @lines, "# HELP $name $help", "# TYPE $name $f->{type}", @{ $f->{samples} };
  }
  if (@errors) {
    push @lines, '# HELP firebird_exporter_query_error Whether the custom query failed in this scrape',
      '# TYPE firebird_exporter_query_error gauge', @errors,
      '# HELP firebird_exporter_query_duration_seconds How long the custom query took',
      '# TYPE firebird_exporter_query_duration_seconds gauge', @durations;
  }
  return join("\n", @lines) . "\n";
}

my $server = IO::Socket::INET->new(LocalPort => $port, Listen => 16, ReuseAddr => 1, Proto => 'tcp', Timeout => 1)
  or die "cannot listen on $port: $!\n";
print "metrics on :$port/metrics\n";
while (1) {
  exit_when_stopped();
  my $client = $server->accept or next;
  $client->timeout(10);
  my $request = <$client> // '';
  while (my $header = <$client>) { last if $header =~ /^\r?\n$/; }
  my ($status, $type, $body);
  if ($request =~ m{^GET /metrics(?:\?\S*)? HTTP}) {
    ($status, $type, $body) = ('200 OK', 'text/plain; version=0.0.4', scrape());
  } elsif ($request =~ m{^GET /healthz HTTP}) {
    ($status, $type, $body) = ('200 OK', 'text/plain', "ok\n");
  } else {
    ($status, $type, $body) = ('404 Not Found', 'text/plain', "not found\n");
  }
  print $client "HTTP/1.1 $status\r\nContent-Type: $type\r\nContent-Length: " . length($body) . "\r\nConnection: close\r\n\r\n$body";
  close $client;
}
