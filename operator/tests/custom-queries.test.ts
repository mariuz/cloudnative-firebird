import { describe, it, expect, vi, type Mock } from 'vitest';
import { spawn, ChildProcess } from 'child_process';
import { chmodSync, mkdtempSync, writeFileSync } from 'fs';
import { request } from 'http';
import { createServer } from 'net';
import { tmpdir } from 'os';
import { join } from 'path';
import { KubeConfig, V1StatefulSet } from '@kubernetes/client-node';
import { FirebirdClusterController } from '../src/controllers/firebirdcluster.controller';
import { FirebirdCluster } from '../src/types';
import {
  CUSTOM_QUERIES_ERRORS_ANNOTATION,
  customQueriesErrors,
  mergeCustomQueries,
  parseCustomQueries,
  renderCustomQueries,
} from '../src/utils/custom-queries';
import { UPGRADE_SCRIPTS } from '../src/utils/replication';
import { buildPodMonitor, buildStatefulSet, statefulSetNeedsUpdate } from '../src/utils/resources';
import { validateClusterSpec } from '../src/utils/validation';
import { makeCluster, notFoundError } from './helpers/factories';

const QUERIES = `
attachments:
  query: |
    SELECT MON$USER AS USERNAME, COUNT(*) AS TOTAL
    FROM MON$ATTACHMENTS GROUP BY 1
  metrics:
    - username: { usage: LABEL, description: "User" }
    - total: { usage: gauge, description: "Attachments
        per user" }
transactions:
  query: "SELECT MON$NEXT_TRANSACTION AS NEXT_TX, MON$PAGE_SIZE AS PAGE_SIZE FROM MON$DATABASE"
  primary: true
  metrics:
    - next_tx: { usage: COUNTER, description: "Next transaction" }
    - page_size: { usage: DISCARD }
`;

const withQueries = (monitoring: FirebirdCluster['spec']['monitoring'] = {}) =>
  makeCluster({ monitoring: { customQueriesConfigMap: [{ name: 'fb-queries', key: 'queries.yaml' }], ...monitoring } });
const container = (sts: V1StatefulSet, name: string) => sts.spec!.template.spec!.containers.find((c) => c.name === name);

describe('custom queries', () => {
  it('are parsed from CloudNativePG-style YAML', () => {
    const { queries, errors } = parseCustomQueries(QUERIES, 'ConfigMap q key k');
    expect(errors).toEqual([]);
    expect(queries).toEqual([
      {
        name: 'attachments',
        query: 'SELECT MON$USER AS USERNAME, COUNT(*) AS TOTAL\nFROM MON$ATTACHMENTS GROUP BY 1',
        primary: false,
        metrics: [
          { column: 'username', usage: 'LABEL', description: 'User' },
          { column: 'total', usage: 'GAUGE', description: 'Attachments per user' },
        ],
      },
      {
        name: 'transactions',
        query: 'SELECT MON$NEXT_TRANSACTION AS NEXT_TX, MON$PAGE_SIZE AS PAGE_SIZE FROM MON$DATABASE',
        primary: true,
        metrics: [
          { column: 'next_tx', usage: 'COUNTER', description: 'Next transaction' },
          { column: 'page_size', usage: 'DISCARD', description: '' },
        ],
      },
    ]);
  });

  it('leave out what is invalid and say why', () => {
    expect(parseCustomQueries('a: [', 'S').errors[0]).toMatch(/^S: not valid YAML/);
    expect(parseCustomQueries('- a', 'S').errors).toEqual(['S: expected a map of queries by name']);
    expect(parseCustomQueries('', 'S')).toEqual({ queries: [], errors: [] });
    const { queries, errors } = parseCustomQueries(
      `
"bad-name": { query: "SELECT 1 AS A FROM RDB$DATABASE", metrics: [ { a: { usage: GAUGE } } ] }
noquery: { metrics: [ { a: { usage: GAUGE } } ] }
nometrics: { query: "SELECT 1 AS A FROM RDB$DATABASE" }
badusage: { query: "SELECT 1 AS A FROM RDB$DATABASE", metrics: [ { a: { usage: HISTOGRAM } } ] }
badcolumn: { query: "SELECT 1 AS A FROM RDB$DATABASE", metrics: [ { "a b": { usage: GAUGE } } ] }
labelsonly: { query: "SELECT 1 AS A FROM RDB$DATABASE", metrics: [ { a: { usage: LABEL } } ] }
badprimary: { query: "SELECT 1 AS A FROM RDB$DATABASE", primary: "yes", metrics: [ { a: { usage: GAUGE } } ] }
twokeys: { query: "SELECT 1 AS A FROM RDB$DATABASE", metrics: [ { a: { usage: GAUGE }, b: { usage: GAUGE } } ] }
good: { query: "SELECT 1 AS A FROM RDB$DATABASE", metrics: [ { a: { usage: GAUGE } } ] }
`,
      'S',
    );
    expect(queries.map((q) => q.name)).toEqual(['good']);
    expect(errors).toEqual([
      'S: query bad-name: the name must be letters, digits and underscores',
      'S: query noquery: query must be an SQL statement',
      "S: query nometrics: metrics must list the query's columns",
      'S: query badusage: column a: usage must be one of GAUGE, COUNTER, LABEL, DISCARD',
      'S: query badcolumn: column a b must be a plain column name (an alias of the query)',
      'S: query labelsonly: no GAUGE or COUNTER column, so it gives no metric',
      'S: query badprimary: primary must be true or false',
      'S: query twokeys: each metrics entry maps one column to its usage and description',
    ]);
  });

  it('of a later source replace those of the same name', () => {
    const a = parseCustomQueries('x: { query: "SELECT 1 AS A FROM RDB$DATABASE", metrics: [ { a: { usage: GAUGE } } ] }', 'A').queries;
    const b = parseCustomQueries('x: { query: "SELECT 2 AS A FROM RDB$DATABASE", metrics: [ { a: { usage: GAUGE } } ] }\ny: { query: "SELECT 3 AS B FROM RDB$DATABASE", metrics: [ { b: { usage: GAUGE } } ] }', 'B').queries;
    expect(mergeCustomQueries([a, b]).map((q) => [q.name, q.query])).toEqual([
      ['x', 'SELECT 2 AS A FROM RDB$DATABASE'],
      ['y', 'SELECT 3 AS B FROM RDB$DATABASE'],
    ]);
  });

  it("are rendered in the exporter's line format", () => {
    const { queries } = parseCustomQueries(QUERIES, 'S');
    queries[0].query += " WHERE MON$USER <> 'a\\b'";
    expect(renderCustomQueries(queries)).toBe(
      [
        'query attachments',
        'primary 0',
        "sql SELECT MON$USER AS USERNAME, COUNT(*) AS TOTAL\\nFROM MON$ATTACHMENTS GROUP BY 1 WHERE MON$USER <> 'a\\\\b'",
        'metric username LABEL User',
        'metric total GAUGE Attachments per user',
        'end',
        'query transactions',
        'primary 1',
        'sql SELECT MON$NEXT_TRANSACTION AS NEXT_TX, MON$PAGE_SIZE AS PAGE_SIZE FROM MON$DATABASE',
        'metric next_tx COUNTER Next transaction',
        'metric page_size DISCARD',
        'end',
        '',
      ].join('\n'),
    );
  });

  it('add the metrics exporter sidecar, its volume and a PodMonitor endpoint', () => {
    const plain = buildStatefulSet(makeCluster());
    expect(container(plain, 'metrics-exporter')).toBeUndefined();
    expect(plain.spec!.template.spec!.volumes!.some((v) => v.name === 'custom-queries')).toBe(false);

    const sts = buildStatefulSet(withQueries());
    const exporter = container(sts, 'metrics-exporter')!;
    expect(exporter.image).toBe(container(sts, 'firebird')!.image);
    expect(exporter.command).toEqual(['perl', '-e', UPGRADE_SCRIPTS['metrics-exporter.pl']]);
    expect(exporter.command![2]).toContain('sub exit_when_stopped');
    expect(exporter.ports).toEqual([{ name: 'custom-metrics', containerPort: 9187, protocol: 'TCP' }]);
    expect(exporter.env).toEqual(
      expect.arrayContaining([
        { name: 'ISC_USER', value: 'SYSDBA' },
        { name: 'DATABASE_PATH', value: expect.stringMatching(/^\/var\/lib\/firebird\/data\/.+\.fdb$/) },
        { name: 'QUERIES_FILE', value: '/custom-queries/queries' },
      ]),
    );
    expect(exporter.volumeMounts).toEqual([{ name: 'custom-queries', mountPath: '/custom-queries', readOnly: true }]);
    expect(sts.spec!.template.spec!.volumes).toContainEqual({
      name: 'custom-queries',
      secret: { secretName: 'test-cluster-custom-queries', optional: true, defaultMode: 0o440 },
    });

    const endpoints = (m: Record<string, unknown>) => (m.spec as { podMetricsEndpoints: Array<{ port: string }> }).podMetricsEndpoints.map((e) => e.port);
    expect(endpoints(buildPodMonitor(withQueries({ enablePodMonitor: true })))).toEqual(['custom-metrics']);
    expect(endpoints(buildPodMonitor(withQueries({ enablePodMonitor: true, exporter: { enabled: true } })))).toEqual(['metrics', 'custom-metrics']);
    expect(endpoints(buildPodMonitor(makeCluster({ monitoring: { enablePodMonitor: true } })))).toEqual(['firebird']);
  });

  it('roll the instances only when added or removed, not when a query changes', () => {
    const plain = buildStatefulSet(makeCluster());
    const queries = buildStatefulSet(withQueries());
    expect(statefulSetNeedsUpdate(queries, plain)).toBe(true);
    expect(statefulSetNeedsUpdate(plain, queries)).toBe(true);
    // as stored: the defaults the API server adds to the sidecar
    const stored = JSON.parse(JSON.stringify(queries)) as V1StatefulSet;
    Object.assign(container(stored, 'metrics-exporter')!, { terminationMessagePath: '/dev/termination-log', terminationMessagePolicy: 'File', imagePullPolicy: 'IfNotPresent', resources: {} });
    expect(statefulSetNeedsUpdate(stored, buildStatefulSet(withQueries()))).toBe(false);
  });

  it('references are validated', () => {
    expect(customQueriesErrors(withQueries().spec)).toEqual([]);
    expect(
      customQueriesErrors(
        makeCluster({ monitoring: { customQueriesConfigMap: [{ name: 'Bad_Name', key: 'a/b' }], customQueriesSecret: [{ name: 'ok', key: '' }] } }).spec,
      ),
    ).toEqual([
      'monitoring.customQueriesConfigMap[0].name must be a DNS subdomain name',
      'monitoring.customQueriesConfigMap[0].key must be a ConfigMap or Secret key',
      'monitoring.customQueriesSecret[0].key must be a ConfigMap or Secret key',
    ]);
    expect(() => validateClusterSpec(withQueries({ exporter: { enabled: true, port: 9187 } }))).toThrow(
      "monitoring.exporter.port 9187 is the custom queries' metrics exporter port",
    );
  });
});

describe('the custom queries Secret', () => {
  const mocks = (overrides: Record<string, Mock>) => {
    const calls: Record<string, Mock> = {};
    const fn = (m: string): Mock =>
      (calls[m] ??=
        overrides[m] ??
        (m.startsWith('read') || m.startsWith('get') ? vi.fn().mockRejectedValue(notFoundError) : m.startsWith('list') ? vi.fn().mockResolvedValue({ items: [] }) : vi.fn().mockResolvedValue({})));
    const kubeConfig = new KubeConfig();
    vi.spyOn(kubeConfig, 'makeApiClient').mockReturnValue(new Proxy({}, { get: (_t, p: string) => fn(p) }) as never);
    return { kubeConfig, api: fn };
  };
  const cluster = () =>
    makeCluster({
      monitoring: {
        customQueriesConfigMap: [
          { name: 'fb-queries', key: 'queries.yaml' },
          { name: 'gone', key: 'queries.yaml' },
        ],
        customQueriesSecret: [{ name: 'private-queries', key: 'q.yaml' }],
      },
    });
  const configMaps = vi.fn().mockImplementation(async ({ name }) => (name === 'fb-queries' ? { data: { 'queries.yaml': QUERIES } } : Promise.reject(notFoundError)));
  const secretYaml = 'attachments:\n  query: "SELECT COUNT(*) AS TOTAL FROM MON$ATTACHMENTS"\n  metrics:\n    - total: { usage: GAUGE }\n';

  it('holds the merged queries and reports the missing sources once', async () => {
    const m = mocks({
      readNamespacedConfigMap: configMaps,
      readNamespacedSecret: vi.fn().mockImplementation(async ({ name }) =>
        name === 'private-queries' ? { data: { 'q.yaml': Buffer.from(secretYaml).toString('base64') } } : Promise.reject(notFoundError),
      ),
    });
    await new FirebirdClusterController(m.kubeConfig).reconcile(cluster());
    const created = m.api('createNamespacedSecret').mock.calls.map((c) => c[0].body).find((b) => b.metadata.name === 'test-cluster-custom-queries');
    expect(created.metadata.ownerReferences[0]).toMatchObject({ kind: 'FirebirdCluster', name: 'test-cluster' });
    expect(created.metadata.annotations[CUSTOM_QUERIES_ERRORS_ANNOTATION]).toBe('ConfigMap gone does not exist');
    // the Secret's attachments query replaced the ConfigMap's
    expect(created.stringData.queries).toContain('query attachments\nprimary 0\nsql SELECT COUNT(*) AS TOTAL FROM MON$ATTACHMENTS\nmetric total GAUGE\nend\n');
    expect(created.stringData.queries).toContain('query transactions\nprimary 1\n');
    const events = m.api('createNamespacedEvent').mock.calls.map((c) => c[0].body);
    expect(events.filter((e) => e.reason === 'CustomQueriesInvalid').map((e) => e.message)).toEqual(['custom queries left out: ConfigMap gone does not exist']);

    // unchanged: neither rewritten nor reported again
    const same = mocks({
      readNamespacedConfigMap: configMaps,
      readNamespacedSecret: vi.fn().mockImplementation(async ({ name }) =>
        name === 'private-queries'
          ? { data: { 'q.yaml': Buffer.from(secretYaml).toString('base64') } }
          : name === 'test-cluster-custom-queries'
            ? {
                metadata: { name, resourceVersion: '7', annotations: { [CUSTOM_QUERIES_ERRORS_ANNOTATION]: 'ConfigMap gone does not exist' } },
                data: { queries: Buffer.from(created.stringData.queries).toString('base64') },
              }
            : Promise.reject(notFoundError),
      ),
    });
    await new FirebirdClusterController(same.kubeConfig).reconcile(cluster());
    expect(same.api('replaceNamespacedSecret')).not.toHaveBeenCalled();
    expect(same.api('createNamespacedSecret').mock.calls.some((c) => c[0].body.metadata.name === 'test-cluster-custom-queries')).toBe(false);
    expect(same.api('createNamespacedEvent').mock.calls.some((c) => c[0].body.reason === 'CustomQueriesInvalid')).toBe(false);
  });

  it('is replaced when a query changes and deleted without custom queries', async () => {
    const m = mocks({
      readNamespacedConfigMap: configMaps,
      readNamespacedSecret: vi.fn().mockImplementation(async ({ name }) =>
        name === 'test-cluster-custom-queries' ? { metadata: { name, resourceVersion: '7', annotations: {} }, data: { queries: '' } } : Promise.reject(notFoundError),
      ),
    });
    await new FirebirdClusterController(m.kubeConfig).reconcile(cluster());
    const replaced = m.api('replaceNamespacedSecret').mock.calls[0][0];
    expect(replaced.name).toBe('test-cluster-custom-queries');
    expect(replaced.body.metadata.resourceVersion).toBe('7');
    expect(replaced.body.stringData.queries).toContain('query attachments\n');

    const off = mocks({ readNamespacedSecret: vi.fn().mockResolvedValue({ metadata: { name: 'test-cluster-custom-queries' } }) });
    await new FirebirdClusterController(off.kubeConfig).reconcile(makeCluster());
    expect(off.api('deleteNamespacedSecret').mock.calls.map((c) => c[0].name)).toContain('test-cluster-custom-queries');
  });
});

/** The exporter script against a stand-in isql: the replica mode query, two queries and a failing one */
describe('the metrics exporter', { timeout: 30000 }, () => {
  const dir = mkdtempSync(join(tmpdir(), 'metrics-exporter-'));
  const isql = join(dir, 'isql');
  writeFileSync(
    isql,
    `#!/bin/sh
# args: -q -b -i <file> <database>
sql=$(cat "$4")
case "$sql" in
  *REPLICA_MODE*) printf '\\nREPLICA_MODE                    %s\\n' "\${REPLICA:-0}" ;;
  *MON\\$ATTACHMENTS*) printf '\\nUSERNAME                        SYSDBA\\nTOTAL                           2\\n\\nUSERNAME                        APP "x"\\nTOTAL                           5\\n' ;;
  *MON\\$NEXT_TRANSACTION*) printf '\\nNEXT_TX                         1234\\nPAGE_SIZE                       8192\\n' ;;
  *) echo 'Statement failed, SQLSTATE = 42S02'; echo 'Table unknown'; exit 1 ;;
esac
`,
  );
  chmodSync(isql, 0o755);

  const freePort = () =>
    new Promise<number>((resolve) => {
      const s = createServer();
      s.listen(0, '127.0.0.1', () => {
        const port = (s.address() as { port: number }).port;
        s.close(() => resolve(port));
      });
    });
  const get = (port: number, path: string) =>
    new Promise<{ status: number; body: string }>((resolve, reject) => {
      const req = request({ host: '127.0.0.1', port, path }, (res) => {
        let body = '';
        res.on('data', (d) => (body += d));
        res.on('end', () => resolve({ status: res.statusCode!, body }));
      });
      req.on('error', reject);
      req.end();
    });
  const start = async (env: Record<string, string>) => {
    const queries = join(dir, `queries-${Math.random()}`);
    const { queries: parsed } = parseCustomQueries(QUERIES + 'broken:\n  query: "SELECT X AS A FROM NOWHERE"\n  metrics:\n    - a: { usage: GAUGE }\n', 'S');
    writeFileSync(queries, renderCustomQueries(parsed));
    const port = await freePort();
    const child: ChildProcess = spawn('perl', ['-e', UPGRADE_SCRIPTS['metrics-exporter.pl']], {
      env: { ...process.env, DATABASE_PATH: '/db/test.fdb', QUERIES_FILE: queries, METRICS_PORT: String(port), ISQL: isql, FIREBIRD_PORT: String(await freePort()), ...env },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let log = '';
    child.stdout!.on('data', (d) => (log += d));
    for (let i = 0; i < 50 && !log.includes('metrics on'); i++) await new Promise((r) => setTimeout(r, 100));
    return { port, child, log: () => log };
  };

  it('serves the queries as metrics, with labels, types and errors', async () => {
    const { port, child, log } = await start({});
    try {
      const { status, body } = await get(port, '/metrics');
      expect(status).toBe(200);
      const lines = body.split('\n');
      expect(lines).toContain('firebird_up 1');
      expect(lines).toContain('# HELP firebird_attachments_total Attachments per user');
      expect(lines).toContain('# TYPE firebird_attachments_total gauge');
      expect(lines).toContain('firebird_attachments_total{username="SYSDBA"} 2');
      expect(lines).toContain('firebird_attachments_total{username="APP \\"x\\""} 5');
      expect(lines).toContain('# TYPE firebird_transactions_next_tx counter');
      expect(lines).toContain('firebird_transactions_next_tx 1234');
      expect(body).not.toContain('page_size');
      expect(lines).toContain('firebird_exporter_query_error{query="attachments"} 0');
      expect(lines).toContain('firebird_exporter_query_error{query="broken"} 1');
      expect(body).toMatch(/firebird_exporter_query_duration_seconds\{query="transactions"\} \d+\.\d{3}/);
      expect(log()).toContain('query broken: Statement failed, SQLSTATE = 42S02 Table unknown');
      expect((await get(port, '/healthz')).body).toBe('ok\n');
      expect((await get(port, '/other')).status).toBe(404);
    } finally {
      child.kill('SIGKILL');
    }
  });

  it('runs primary-only queries only where the database is not a replica', async () => {
    const { port, child } = await start({ REPLICA: '1' });
    try {
      const { body } = await get(port, '/metrics');
      expect(body).toContain('firebird_attachments_total{username="SYSDBA"} 2');
      expect(body).not.toContain('firebird_transactions_next_tx');
      expect(body).not.toContain('query="transactions"');
    } finally {
      child.kill('SIGKILL');
    }
  });

  it('reports the database down and skips the queries', async () => {
    const { port, child } = await start({ ISQL: '/bin/false' });
    try {
      const { body } = await get(port, '/metrics');
      expect(body).toContain('firebird_up 0');
      expect(body).not.toContain('firebird_attachments_total');
      expect(body).toContain('firebird_exporter_query_error{query="attachments"} 1');
    } finally {
      child.kill('SIGKILL');
    }
  });
});
