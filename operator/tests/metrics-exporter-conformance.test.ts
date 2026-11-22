import { describe, it, expect, afterEach } from 'vitest';
import { ChildProcess, spawn, spawnSync } from 'child_process';
import { chmodSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from 'fs';
import { request } from 'http';
import { connect, createServer, Server } from 'net';
import { tmpdir } from 'os';
import { join } from 'path';
import { agentBin } from './helpers/implementations';

/**
 * The custom metrics exporter of the Rust agent (`fb-agent metrics-exporter`,
 * agent/fb-agent/src/metrics.rs), with a stand-in isql: what it serves and what it logs, the
 * durations aside (as metrics-exporter.pl did, which it replaced; removed in v0.170.0).
 *
 * Needs the fb-agent binary (FB_AGENT_BIN, or a build in agent/target); skipped without it. The
 * "Rust agent" CI job runs this file with it.
 */

/** the queries as the operator renders them (utils/custom-queries.ts) */
const QUERIES = [
  'query attachments',
  'primary 0',
  'sql SELECT MON$USER AS USERNAME, COUNT(*) AS TOTAL\\nFROM MON$ATTACHMENTS GROUP BY 1;  ',
  'metric username LABEL User',
  'metric total GAUGE Attachments\\nper user \\\\ now',
  'end',
  'query transactions',
  'primary 1',
  'sql SELECT MON$NEXT_TRANSACTION AS NEXT_TX, MON$PAGE_SIZE AS PAGE_SIZE FROM MON$DATABASE',
  'metric next_tx COUNTER Next transaction',
  'metric page_size DISCARD',
  'end',
  // values of every kind: numbers isql prints, a null, text where a number is expected, no value
  'query Odd-Values.v2',
  'primary 0',
  'sql SELECT * FROM ODD',
  'metric kind LABEL',
  'metric note LABEL A note',
  'metric amount GAUGE',
  'metric ratio GAUGE A ratio',
  'metric missing GAUGE Never there',
  'end',
  'query broken',
  'primary 0',
  'sql SELECT X AS A FROM NOWHERE',
  'metric a GAUGE',
  'end',
  'query slow',
  'primary 0',
  'sql SELECT SLOW FROM RDB$DATABASE',
  'metric slow GAUGE',
  'end',
  'query silent',
  'primary 0',
  'sql SELECT SILENT FROM RDB$DATABASE',
  'metric silent GAUGE',
  'end',
  // no statement: left out
  'query empty',
  'primary 0',
  'metric a GAUGE',
  'end',
  '',
].join('\n');

describe.skipIf(!agentBin)('the metrics exporter of fb-agent', { timeout: 60_000 }, () => {
  const dir = mkdtempSync(join(tmpdir(), 'metrics-conformance-'));
  const isql = join(dir, 'isql');
  // args: -q -b -i <file> <database>; the database it was asked for is recorded
  writeFileSync(
    isql,
    `#!/bin/sh
echo "$5" >> "${dir}/asked"
sql=$(cat "$4")
case "$sql" in
  *REPLICA_MODE*) [ -z "$DOWN" ] || { echo 'Unable to complete network request to host "localhost".' >&2; exit 1; }
     printf '\\nREPLICA_MODE                    %s\\n' "\${REPLICA:-0}" ;;
  *MON\\$ATTACHMENTS*) printf '\\nUSERNAME                        SYSDBA\\nTOTAL                           2\\n\\nUSERNAME                        APP "x" \\\\ y\\nTOTAL                           5\\n' ;;
  *MON\\$NEXT_TRANSACTION*) printf '\\nNEXT_TX                         1234\\nPAGE_SIZE                       8192\\n' ;;
  *ODD*) printf '\\nKIND                            a\\nNOTE                            <null>\\nAMOUNT                          -12.50\\nRATIO                           1.5e-3\\n\\nKIND                            b\\nNOTE                            two  words\\nAMOUNT                          <null>\\nRATIO                           n/a\\n\\nKIND\\nNOTE                            c\\nAMOUNT                          +.5\\nRATIO                           7.\\n   not a column\\n' ;;
  *SLOW*) sleep 5 ;;
  *SILENT*) exit 3 ;;
  *) echo 'Statement failed, SQLSTATE = 42S02'; echo; echo 'Table unknown' >&2; exit 1 ;;
esac
`,
  );
  chmodSync(isql, 0o755);
  const queries = join(dir, 'queries');
  writeFileSync(queries, QUERIES);
  const children: ChildProcess[] = [];
  const servers: Server[] = [];

  const freePort = () =>
    new Promise<number>((resolve) => {
      const s = createServer();
      s.listen(0, '127.0.0.1', () => {
        const port = (s.address() as { port: number }).port;
        s.close(() => resolve(port));
      });
    });
  const get = (port: number, path: string) =>
    new Promise<{ status: number; type: string; body: string }>((resolve, reject) => {
      const req = request({ host: '127.0.0.1', port, path }, (res) => {
        let body = '';
        res.on('data', (d) => (body += d));
        res.on('end', () => resolve({ status: res.statusCode!, type: String(res.headers['content-type']), body }));
      });
      req.on('error', reject);
      req.end();
    });
  /** the exporter with the environment, on a port of its own */
  const start = async (env: Record<string, string> = {}) => {
    const run = async (command: string, args: string[]) => {
      const port = await freePort();
      const child = spawn(command, args, {
        env: { PATH: process.env.PATH ?? '', DATABASE_PATH: '/db/test.fdb', QUERIES_FILE: queries, METRICS_PORT: String(port), ISQL: isql, TMPDIR: dir, QUERY_TIMEOUT_SECONDS: '1', FIREBIRD_PORT: '1', ...env },
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      children.push(child);
      let log = '';
      child.stdout!.on('data', (d) => (log += d));
      child.stderr!.on('data', (d) => (log += d));
      for (let i = 0; i < 100 && !log.includes('metrics on'); i++) await new Promise((r) => setTimeout(r, 50));
      return { port, child, log: () => log };
    };
    return { agent: await run(agentBin!, ['metrics-exporter']) };
  };
  /** the durations aside */
  const steady = (body: string) => body.replace(/(firebird_exporter_query_duration_seconds\{[^}]*\}) \d+\.\d{3}/g, '$1 T');
  const waitFor = async (check: () => boolean, what: string) => {
    for (let i = 0; i < 100 && !check(); i++) await new Promise((r) => setTimeout(r, 50));
    expect(check(), what).toBe(true);
  };

  afterEach(() => {
    for (const c of children.splice(0)) c.kill('SIGKILL');
    for (const s of servers.splice(0)) s.close();
  });

  it('serve the metrics of the queries', async () => {
    const { agent } = await start();
    const ours = await get(agent.port, '/metrics');
    expect(ours.status).toBe(200);
    expect(ours.type).toMatchInlineSnapshot(`"text/plain; version=0.0.4"`);
    expect(steady(ours.body)).toMatchInlineSnapshot(`
      "# HELP firebird_up Whether the database answers a query
      # TYPE firebird_up gauge
      firebird_up 1
      # HELP firebird_attachments_total Attachments\\\\nper user \\\\\\\\ now
      # TYPE firebird_attachments_total gauge
      firebird_attachments_total{username="SYSDBA"} 2
      firebird_attachments_total{username="APP \\"x\\" \\\\ y"} 5
      # HELP firebird_odd_values_v2_amount firebird_odd_values_v2_amount
      # TYPE firebird_odd_values_v2_amount gauge
      firebird_odd_values_v2_amount{kind="a"} -12.50
      firebird_odd_values_v2_amount{kind="",note="c"} +.5
      # HELP firebird_odd_values_v2_ratio A ratio
      # TYPE firebird_odd_values_v2_ratio gauge
      firebird_odd_values_v2_ratio{kind="a"} 1.5e-3
      firebird_odd_values_v2_ratio{kind="",note="c"} 7.
      # HELP firebird_transactions_next_tx Next transaction
      # TYPE firebird_transactions_next_tx counter
      firebird_transactions_next_tx 1234
      # HELP firebird_exporter_query_error Whether the custom query failed in this scrape
      # TYPE firebird_exporter_query_error gauge
      firebird_exporter_query_error{query="attachments"} 0
      firebird_exporter_query_error{query="transactions"} 0
      firebird_exporter_query_error{query="Odd-Values.v2"} 0
      firebird_exporter_query_error{query="broken"} 1
      firebird_exporter_query_error{query="slow"} 1
      firebird_exporter_query_error{query="silent"} 1
      # HELP firebird_exporter_query_duration_seconds How long the custom query took
      # TYPE firebird_exporter_query_duration_seconds gauge
      firebird_exporter_query_duration_seconds{query="attachments"} T
      firebird_exporter_query_duration_seconds{query="transactions"} T
      firebird_exporter_query_duration_seconds{query="Odd-Values.v2"} T
      firebird_exporter_query_duration_seconds{query="broken"} T
      firebird_exporter_query_duration_seconds{query="slow"} T
      firebird_exporter_query_duration_seconds{query="silent"} T
      "
    `);
    const lines = ours.body.split('\n');
    expect(lines).toContain('firebird_up 1');
    // the description as the operator rendered it, its backslashes escaped for the exposition format
    expect(lines).toContain('# HELP firebird_attachments_total Attachments\\\\nper user \\\\\\\\ now');
    expect(lines).toContain('firebird_attachments_total{username="APP \\"x\\" \\\\ y"} 5');
    expect(lines).toContain('firebird_transactions_next_tx 1234');
    // a name made of what a metric name may have; a null label left out, an empty one kept
    expect(lines).toContain('firebird_odd_values_v2_amount{kind="a"} -12.50');
    expect(lines).toContain('firebird_odd_values_v2_ratio{kind="a"} 1.5e-3');
    expect(lines).toContain('firebird_odd_values_v2_amount{kind="",note="c"} +.5');
    expect(lines).toContain('firebird_odd_values_v2_ratio{kind="",note="c"} 7.');
    // not numbers: no sample
    expect(ours.body).not.toContain('n/a');
    expect(lines.filter((l) => l.startsWith('firebird_odd_values_v2_amount'))).toHaveLength(2);
    expect(ours.body).not.toContain('missing');
    expect(ours.body).not.toContain('page_size');
    expect(ours.body).not.toContain('query="empty"');
    for (const [query, failed] of [['attachments', 0], ['Odd-Values.v2', 0], ['broken', 1], ['slow', 1], ['silent', 1]] as const) {
      expect(lines, query).toContain(`firebird_exporter_query_error{query="${query}"} ${failed}`);
    }
    expect(ours.body).toMatch(/firebird_exporter_query_duration_seconds\{query="slow"\} [01]\.\d{3}\n/);
    // what it says about the queries that failed
    await waitFor(() => agent.log().includes('query silent'), 'the log lines');
    const said = (log: string) => log.split('\n').filter((l) => l.startsWith('query '));
    expect(said(agent.log())).toEqual([
      'query broken: Statement failed, SQLSTATE = 42S02 Table unknown',
      'query slow: timed out after 1s',
      'query silent: isql exited with 3',
    ]);
    // the statement files are removed
    expect(readdirSync(dir).filter((f) => f.startsWith('metrics-query-'))).toEqual([]);
    // the other paths
    const others: Record<string, unknown> = {};
    for (const path of ['/healthz', '/other', '/metricsx', '/metrics/more', '/']) others[path] = await get(agent.port, path);
    expect(others).toMatchInlineSnapshot(`
      {
        "/": {
          "body": "not found
      ",
          "status": 404,
          "type": "text/plain",
        },
        "/healthz": {
          "body": "ok
      ",
          "status": 200,
          "type": "text/plain",
        },
        "/metrics/more": {
          "body": "not found
      ",
          "status": 404,
          "type": "text/plain",
        },
        "/metricsx": {
          "body": "not found
      ",
          "status": 404,
          "type": "text/plain",
        },
        "/other": {
          "body": "not found
      ",
          "status": 404,
          "type": "text/plain",
        },
      }
    `);
    expect((await get(agent.port, '/healthz')).body).toBe('ok\n');
    expect((await get(agent.port, '/other')).status).toBe(404);
    // a scrape with a query string, and a second one: nothing kept from the first
    const again = await get(agent.port, '/metrics?name[]=firebird_up');
    expect(again.status).toBe(200);
    expect(steady(again.body)).toBe(steady(ours.body));
  });

  it('skip the primary-only queries on a replica, and every query when the database does not answer', async () => {
    const replica = await start({ REPLICA: '1' });
    const ours = await get(replica.agent.port, '/metrics');
    expect(steady(ours.body)).toMatchInlineSnapshot(`
      "# HELP firebird_up Whether the database answers a query
      # TYPE firebird_up gauge
      firebird_up 1
      # HELP firebird_attachments_total Attachments\\\\nper user \\\\\\\\ now
      # TYPE firebird_attachments_total gauge
      firebird_attachments_total{username="SYSDBA"} 2
      firebird_attachments_total{username="APP \\"x\\" \\\\ y"} 5
      # HELP firebird_odd_values_v2_amount firebird_odd_values_v2_amount
      # TYPE firebird_odd_values_v2_amount gauge
      firebird_odd_values_v2_amount{kind="a"} -12.50
      firebird_odd_values_v2_amount{kind="",note="c"} +.5
      # HELP firebird_odd_values_v2_ratio A ratio
      # TYPE firebird_odd_values_v2_ratio gauge
      firebird_odd_values_v2_ratio{kind="a"} 1.5e-3
      firebird_odd_values_v2_ratio{kind="",note="c"} 7.
      # HELP firebird_exporter_query_error Whether the custom query failed in this scrape
      # TYPE firebird_exporter_query_error gauge
      firebird_exporter_query_error{query="attachments"} 0
      firebird_exporter_query_error{query="Odd-Values.v2"} 0
      firebird_exporter_query_error{query="broken"} 1
      firebird_exporter_query_error{query="slow"} 1
      firebird_exporter_query_error{query="silent"} 1
      # HELP firebird_exporter_query_duration_seconds How long the custom query took
      # TYPE firebird_exporter_query_duration_seconds gauge
      firebird_exporter_query_duration_seconds{query="attachments"} T
      firebird_exporter_query_duration_seconds{query="Odd-Values.v2"} T
      firebird_exporter_query_duration_seconds{query="broken"} T
      firebird_exporter_query_duration_seconds{query="slow"} T
      firebird_exporter_query_duration_seconds{query="silent"} T
      "
    `);
    expect(ours.body).toContain('firebird_attachments_total{username="SYSDBA"} 2');
    expect(ours.body).not.toContain('next_tx');
    expect(ours.body).not.toContain('query="transactions"');

    const down = await start({ DOWN: '1' });
    const ourDown = await get(down.agent.port, '/metrics');
    expect(steady(ourDown.body)).toMatchInlineSnapshot(`
      "# HELP firebird_up Whether the database answers a query
      # TYPE firebird_up gauge
      firebird_up 0
      # HELP firebird_exporter_query_error Whether the custom query failed in this scrape
      # TYPE firebird_exporter_query_error gauge
      firebird_exporter_query_error{query="attachments"} 1
      firebird_exporter_query_error{query="transactions"} 1
      firebird_exporter_query_error{query="Odd-Values.v2"} 1
      firebird_exporter_query_error{query="broken"} 1
      firebird_exporter_query_error{query="slow"} 1
      firebird_exporter_query_error{query="silent"} 1
      # HELP firebird_exporter_query_duration_seconds How long the custom query took
      # TYPE firebird_exporter_query_duration_seconds gauge
      firebird_exporter_query_duration_seconds{query="attachments"} T
      firebird_exporter_query_duration_seconds{query="transactions"} T
      firebird_exporter_query_duration_seconds{query="Odd-Values.v2"} T
      firebird_exporter_query_duration_seconds{query="broken"} T
      firebird_exporter_query_duration_seconds{query="slow"} T
      firebird_exporter_query_duration_seconds{query="silent"} T
      "
    `);
    expect(ourDown.body).toContain('firebird_up 0');
    expect(ourDown.body).not.toContain('firebird_attachments_total');
    expect(ourDown.body).toContain('firebird_exporter_query_error{query="attachments"} 1');
    await waitFor(() => down.agent.log().includes('does not answer'), 'the log line');
    const said = (log: string) => log.split('\n').filter((l) => l.startsWith('the database') || l.startsWith('query '));
    expect(said(down.agent.log())).toEqual(['the database does not answer: Unable to complete network request to host "localhost".']);
  });

  it('take a changed queries file at the next scrape, and none as no query', async () => {
    const own = join(dir, 'own-queries');
    writeFileSync(own, 'query one\nprimary 0\nsql SELECT 1 AS NEXT_TX FROM MON$NEXT_TRANSACTION\nmetric next_tx GAUGE\nend\n');
    const { agent } = await start({ QUERIES_FILE: own });
    const first = await get(agent.port, '/metrics');
    expect(steady(first.body)).toMatchInlineSnapshot(`
      "# HELP firebird_up Whether the database answers a query
      # TYPE firebird_up gauge
      firebird_up 1
      # HELP firebird_one_next_tx firebird_one_next_tx
      # TYPE firebird_one_next_tx gauge
      firebird_one_next_tx 1234
      # HELP firebird_exporter_query_error Whether the custom query failed in this scrape
      # TYPE firebird_exporter_query_error gauge
      firebird_exporter_query_error{query="one"} 0
      # HELP firebird_exporter_query_duration_seconds How long the custom query took
      # TYPE firebird_exporter_query_duration_seconds gauge
      firebird_exporter_query_duration_seconds{query="one"} T
      "
    `);
    expect(first.body).toContain('firebird_one_next_tx 1234');
    writeFileSync(own, 'query two\nprimary 0\nsql SELECT 1 AS NEXT_TX FROM MON$NEXT_TRANSACTION\nmetric next_tx COUNTER Renamed\nend\n');
    const second = await get(agent.port, '/metrics');
    expect(steady(second.body)).toMatchInlineSnapshot(`
      "# HELP firebird_up Whether the database answers a query
      # TYPE firebird_up gauge
      firebird_up 1
      # HELP firebird_two_next_tx Renamed
      # TYPE firebird_two_next_tx counter
      firebird_two_next_tx 1234
      # HELP firebird_exporter_query_error Whether the custom query failed in this scrape
      # TYPE firebird_exporter_query_error gauge
      firebird_exporter_query_error{query="two"} 0
      # HELP firebird_exporter_query_duration_seconds How long the custom query took
      # TYPE firebird_exporter_query_duration_seconds gauge
      firebird_exporter_query_duration_seconds{query="two"} T
      "
    `);
    expect(second.body).toContain('# TYPE firebird_two_next_tx counter');
    expect(second.body).not.toContain('firebird_one');
    rmSync(own);
    const none = await get(agent.port, '/metrics');
    expect(none.body).toBe('# HELP firebird_up Whether the database answers a query\n# TYPE firebird_up gauge\nfirebird_up 1\n');
  });

  it('ask the local server for the database, and stop once the Firebird server has stopped', async () => {
    // a "Firebird server" that listens: the exporter keeps answering after SIGTERM
    const firebird = createServer((socket) => socket.destroy());
    servers.push(firebird);
    await new Promise<void>((r) => firebird.listen(0, '127.0.0.1', r));
    const { agent } = await start({ FIREBIRD_PORT: String((firebird.address() as { port: number }).port) });
    writeFileSync(join(dir, 'asked'), '');
    await get(agent.port, '/metrics');
    const asked = spawnSync('sort', ['-u', join(dir, 'asked')]).stdout.toString();
    expect(asked).toBe('localhost:/db/test.fdb\n');
    const codes: Record<string, number | null | undefined> = {};
    for (const [name, exporter] of Object.entries({ agent })) {
      exporter.child.once('close', (code) => (codes[name] = code));
      exporter.child.kill('SIGTERM');
    }
    await new Promise((r) => setTimeout(r, 1500));
    expect(codes).toEqual({});
    expect((await get(agent.port, '/healthz')).body).toBe('ok\n');
    await new Promise<void>((r) => firebird.close(() => r()));
    await waitFor(() => codes.agent !== undefined, 'the exit');
    expect(codes).toEqual({ agent: 0 });
    expect(agent.log()).toContain('stopping: the Firebird server has stopped\n');
    // and a request that is not HTTP at all
    const raw = (port: number) =>
      new Promise<string>((resolve) => {
        const socket = connect(port, '127.0.0.1');
        let text = '';
        socket.on('data', (d) => (text += d));
        socket.on('close', () => resolve(text));
        socket.on('error', () => resolve(text));
        socket.write('hello\r\n\r\n');
      });
    const fresh = await start();
    expect(await raw(fresh.agent.port)).toMatchInlineSnapshot(`
      "HTTP/1.1 404 Not Found
      Content-Type: text/plain
      Content-Length: 10
      Connection: close

      not found
      "
    `);
    expect(await raw(fresh.agent.port)).toContain('404 Not Found');
  });

  it('need the database named', async () => {
    const done = spawnSync(agentBin!, ['metrics-exporter'], { env: { PATH: process.env.PATH ?? '' } });
    expect(done.status).not.toBe(0);
    expect(done.stderr.toString()).toContain('DATABASE_PATH is required');
  });

  it('query over the agent\'s own attachment with QUERY_CLIENT=agent, and refuse another client', async () => {
    // no server on FIREBIRD_PORT 1: the database does not answer, every query fails, isql is never run
    rmSync(join(dir, 'asked'), { force: true });
    const { agent } = await start({ QUERY_CLIENT: 'agent' });
    const lines = (await get(agent.port, '/metrics')).body.split('\n');
    expect(lines).toContain('firebird_up 0');
    expect(lines).toContain('firebird_exporter_query_error{query="attachments"} 1');
    expect(readdirSync(dir)).not.toContain('asked');
    expect(agent.log()).toContain("queries over the agent's own attachment (QUERY_CLIENT=agent)");
    expect(agent.log()).toMatch(/the database does not answer: /);
    const refused = spawnSync(agentBin!, ['metrics-exporter'], {
      env: { PATH: process.env.PATH ?? '', DATABASE_PATH: '/db/test.fdb', QUERY_CLIENT: 'odbc' },
    });
    expect(refused.status).not.toBe(0);
    expect(refused.stderr.toString()).toContain('QUERY_CLIENT: isql or agent, not odbc');
  });
});
