#!/bin/bash
# Checks the custom metrics exporter (`fb-agent metrics-exporter`) against real Firebird servers,
# with both of its clients: isql (the default) and the agent's own attachment (QUERY_CLIENT=agent,
# monitoring.customQueriesClient: agent). The same queries, a table of every kind of value:
#
#   - integers and NUMERICs are served alike by both;
#   - with the agent's client a boolean is 1 or 0, a timestamp the seconds since the Unix epoch, a
#     text blob a label, and the whole scrape is one attachment;
#   - a broken query is an error in both, and the others still run;
#   - then the time per scrape of each.
#
#   hack/agent/metrics.sh [image ...]      (default: Firebird 4, 5 and the 6 snapshot)
#
# Needs Docker and the fb-agent binary: FB_AGENT_BIN, or a musl build in operator/agent/target.
# Exit status 1 when an image fails.
set -u
here=$(cd "$(dirname "$0")" && pwd)
agent=${FB_AGENT_BIN:-$here/../../operator/agent/target/x86_64-unknown-linux-musl/release/fb-agent}
[ -x "$agent" ] || { echo "no fb-agent binary at $agent (set FB_AGENT_BIN)"; exit 1; }
images=("$@")
[ ${#images[@]} -gt 0 ] || images=(firebirdsql/firebird:4 firebirdsql/firebird:5 firebirdsql/firebird:6-snapshot)
NAME=${NAME:-fbmetrics}
work=$(mktemp -d)
chmod 755 "$work"
cleanup() {
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  docker run --rm -v "$work:/w" --entrypoint sh "${images[0]}" -c 'rm -rf /w/*' >/dev/null 2>&1 || true
  rmdir "$work" 2>/dev/null || true
}
trap cleanup EXIT
cp "$agent" "$work/fb-agent"
cat > "$work/env.sh" <<'ENV'
export ISC_USER=SYSDBA ISC_PASSWORD=masterkey PATH=/opt/firebird/bin:$PATH
export DATABASE_PATH=/data/db.fdb QUERIES_FILE=/w/queries
start_firebird() { su -s /bin/sh firebird -c "/opt/firebird/bin/firebird" & }
# GET /metrics from the exporter on port $1, the body only
scrape() { exec 3<>/dev/tcp/127.0.0.1/$1 && printf 'GET /metrics HTTP/1.0\r\n\r\n' >&3 && sed '1,/^\r$/d' <&3; exec 3<&-; }
ENV
# the queries as the operator renders them (operator/src/utils/custom-queries.ts)
cat > "$work/queries" <<'Q'
query typed
primary 0
sql SELECT K, N, D, F, B, TS, NOTE FROM T
metric k LABEL Key
metric note LABEL A text blob
metric n GAUGE An integer
metric d GAUGE A NUMERIC(9,2)
metric f GAUGE A double
metric b GAUGE A boolean
metric ts GAUGE A timestamp
end
query attachments
primary 0
sql SELECT TRIM(MON$USER) AS USERNAME, COUNT(*) AS TOTAL FROM MON$ATTACHMENTS WHERE MON$SYSTEM_FLAG = 0 GROUP BY 1
metric username LABEL User
metric total GAUGE Attachments per user
end
query broken
primary 0
sql SELECT X AS A FROM NO_SUCH_TABLE
metric a GAUGE
end
Q

status=0
for image in "${images[@]}"; do
  echo "### $image"
  docker rm -f "$NAME" >/dev/null 2>&1
  docker run -d --name "$NAME" -v "$work:/w" --entrypoint bash "$image" -c '. /w/env.sh
    grep -q "^AuthServer = Srp, Legacy_Auth" /opt/firebird/firebird.conf ||
      printf "AuthServer = Srp, Legacy_Auth\nUserManager = Srp, Legacy_UserManager\nWireCrypt = Enabled\n" >> /opt/firebird/firebird.conf
    mkdir -p /data && chown firebird:firebird /data
    start_firebird; sleep 3
    isql -q <<SQL
CREATE DATABASE '\''localhost:/data/db.fdb'\'' USER SYSDBA PASSWORD '\''masterkey'\'';
CREATE TABLE T (K VARCHAR(10), N INTEGER, D NUMERIC(9,2), F DOUBLE PRECISION, B BOOLEAN, TS TIMESTAMP, NOTE BLOB SUB_TYPE TEXT);
INSERT INTO T VALUES ('\''a'\'', 3, 12.50, 0.25, TRUE, '\''1970-01-02 00:00:01'\'', '\''blob label'\'');
COMMIT;
SQL
    METRICS_PORT=9187 /w/fb-agent metrics-exporter > /data/isql.log 2>&1 &
    QUERY_CLIENT=agent METRICS_PORT=9188 /w/fb-agent metrics-exporter > /data/agent.log 2>&1 &
    sleep infinity' >/dev/null
  x() { docker exec "$NAME" bash -c ". /w/env.sh; $1"; }
  for _ in $(seq 1 40); do x 'scrape 9188' 2>/dev/null | grep -qx 'firebird_up 1' && break; sleep 1; done
  failed=0
  ok() { echo "ok: $1"; }
  bad() { echo "FAILED: $1"; failed=1; }
  # has <port> <what> <line regex>
  has() { if grep -Eqx "$3" "$work/m$1"; then ok "$2"; else bad "$2: no line matching $3"; fi; }
  echo "server: $(x 'isql -z </dev/null 2>/dev/null | head -n 1')"
  x 'scrape 9187' > "$work/m9187"
  x 'scrape 9188' > "$work/m9188"
  for port in 9187 9188; do
    client=$([ $port = 9187 ] && echo isql || echo agent)
    has $port "$client: the database answers" 'firebird_up 1'
    has $port "$client: an integer" 'firebird_typed_n\{k="a"[^}]*\} 3'
    has $port "$client: a NUMERIC(9,2) at its scale" 'firebird_typed_d\{k="a"[^}]*\} 12\.50'
    has $port "$client: a double" 'firebird_typed_f\{k="a"[^}]*\} 0\.250*'
    has $port "$client: its own attachment" 'firebird_attachments_total\{username="SYSDBA"\} [1-9][0-9]*'
    has $port "$client: the broken query is an error" 'firebird_exporter_query_error\{query="broken"\} 1'
    has $port "$client: the others are not" 'firebird_exporter_query_error\{query="typed"\} 0'
  done
  # typed values, with the agent's client only
  has 9188 "agent: a boolean is 1" 'firebird_typed_b\{k="a",note="blob label"\} 1'
  has 9188 "agent: a timestamp is the seconds since the Unix epoch" 'firebird_typed_ts\{k="a",note="blob label"\} 86401'
  has 9188 "agent: a text blob is a label" 'firebird_typed_n\{k="a",note="blob label"\} 3'
  has 9188 "agent: one attachment for the whole scrape" 'firebird_attachments_total\{username="SYSDBA"\} 1'
  if grep -q '^firebird_typed_b' "$work/m9187"; then bad "isql: a boolean is served (isql prints <true>)"; else ok "isql: no boolean (isql prints <true>)"; fi
  x 'grep -q "^query broken: " /data/agent.log' && ok "agent: the broken query is logged" || bad "agent log: $(x 'cat /data/agent.log')"
  x 'grep -q "QUERY_CLIENT=agent" /data/agent.log' && ok "agent: says which client it uses" || bad "agent log: $(x 'cat /data/agent.log')"
  # no attachment left between scrapes: only the counting isql's own
  left=$(x 'echo "SET HEADING OFF; SELECT COUNT(*) FROM MON\$ATTACHMENTS WHERE MON\$SYSTEM_FLAG = 0;" | isql -q localhost:/data/db.fdb | tr -d " \n"')
  [ "$left" = 1 ] && ok "no attachment left between scrapes" || bad "$left attachments between scrapes"
  # the time per scrape (three queries), each client
  for port in 9187 9188; do
    ms=$(x "s=\$(date +%s%N); for i in \$(seq 1 20); do scrape $port >/dev/null; done; echo \$(( (\$(date +%s%N) - s) / 20000000 ))")
    echo "$([ $port = 9187 ] && echo isql || echo agent): $ms ms per scrape"
  done
  if [ "$failed" = 1 ]; then
    echo "--- isql"; cat "$work/m9187"; echo "--- agent"; cat "$work/m9188"; x 'tail -n 5 /data/isql.log /data/agent.log; ls -l /data'
    echo "### $image: FAILED"; status=1
  else
    echo "### $image: passed"
  fi
done
exit $status
