import { describe, it, expect, vi, Mock } from 'vitest';
import { spawn, spawnSync } from 'child_process';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, utimesSync, writeFileSync } from 'fs';
import { createServer } from 'net';
import { tmpdir } from 'os';
import { join } from 'path';
import { KubeConfig, V1Pod } from '@kubernetes/client-node';
import { computeLag, parseArchived, parsePosition, SegmentClient, segmentRetention } from '../src/utils/replication-lag';
import { maxSegmentRetentionHours, REPLICATION_SCRIPTS } from '../src/utils/replication';
import { REPLICATION_LAG_ANNOTATION } from '../src/utils/routing';
import { FirebirdClusterController } from '../src/controllers/firebirdcluster.controller';
import { FirebirdCluster } from '../src/types';
import { metrics } from '../src/utils/metrics';

describe('replication lag computation', () => {
  const archived = parseArchived(['10 300', '12 60', '11 120', '.'].filter((l) => l !== '.'));

  it('parses ARCHIVED and POSITION replies', () => {
    expect(archived.map((s) => s.sequence)).toEqual([10, 11, 12]);
    expect(parsePosition(['OK 11 4096 1'])).toEqual({ sequence: 11, pending: 1 });
    expect(() => parsePosition(['ERR no replica control file'])).toThrow(/no replica control file/);
    expect(() => parsePosition([])).toThrow(/empty/);
    expect(() => parseArchived(['ERR unauthorized'])).toThrow(/unauthorized/);
  });

  it('counts the archived segments after the applied one and the age of the oldest', () => {
    expect(computeLag(archived, 12)).toEqual({ lagSegments: 0, lagSeconds: 0 });
    expect(computeLag(archived, 11)).toEqual({ lagSegments: 1, lagSeconds: 60 });
    expect(computeLag(archived, 9)).toEqual({ lagSegments: 3, lagSeconds: 300 });
    expect(computeLag([], 5)).toEqual({ lagSegments: 0, lagSeconds: 0 });
  });

  it('keeps segments from the lowest applied position, remembering replicas that are not ready', () => {
    const measured = [
      { name: 'db-1', appliedSequence: 40 },
      { name: 'db-2', error: 'connect ECONNREFUSED' },
    ];
    // db-2 cannot be measured now: its last known position holds the floor back
    expect(
      segmentRetention({
        replicaNames: ['db-1', 'db-2'],
        measured,
        previous: { floorSequence: 30, replicas: [{ name: 'db-1', appliedSequence: 35 }, { name: 'db-2', appliedSequence: 31 }] },
      }),
    ).toEqual({ floorSequence: 31, replicas: [{ name: 'db-1', appliedSequence: 40 }, { name: 'db-2', appliedSequence: 31 }] });
    // scaled away or promoted: dropped; never measured (being seeded): does not hold the floor
    expect(
      segmentRetention({
        replicaNames: ['db-1', 'db-3'],
        measured,
        previous: { replicas: [{ name: 'db-2', appliedSequence: 31 }, { name: 'db-0', appliedSequence: 10 }] },
      }),
    ).toEqual({ floorSequence: 40, replicas: [{ name: 'db-1', appliedSequence: 40 }] });
    expect(segmentRetention({ replicaNames: ['db-1'], measured: [] })).toEqual({});
  });

  it('caps unapplied segments at maxSegmentRetentionHours, never below segmentRetentionHours', () => {
    const c = (replication: FirebirdCluster['spec']['replication']): FirebirdCluster => ({
      apiVersion: 'firebird.cloudnative-firebird.io/v1',
      kind: 'FirebirdCluster',
      metadata: { name: 'db' },
      spec: { instances: 2, storage: { size: '1Gi' }, replication },
    });
    expect(maxSegmentRetentionHours(c({ enabled: true }))).toBe(168);
    expect(maxSegmentRetentionHours(c({ enabled: true, maxSegmentRetentionHours: 48 }))).toBe(48);
    expect(maxSegmentRetentionHours(c({ enabled: true, segmentRetentionHours: 200 }))).toBe(200);
    expect(maxSegmentRetentionHours(c({ enabled: true, segmentRetentionHours: 72, maxSegmentRetentionHours: 24 }))).toBe(72);
  });

  it('segmentRequest sends one line and reads the reply up to the terminator', async () => {
    const { segmentRequest } = await vi.importActual<typeof import('../src/utils/replication-lag')>(
      '../src/utils/replication-lag',
    );
    const received: string[] = [];
    const server = createServer((sock) => {
      sock.once('data', (d) => {
        received.push(d.toString());
        sock.end('10 30\n11 5\n.\n');
      });
    });
    await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
    const port = (server.address() as { port: number }).port;
    try {
      expect(await segmentRequest('127.0.0.1', port, 'tok ARCHIVED')).toEqual(['10 30', '11 5']);
      expect(received).toEqual(['tok ARCHIVED\n']);
      await expect(segmentRequest('127.0.0.1', 1, 'tok ARCHIVED', 500)).rejects.toThrow();
    } finally {
      server.close();
    }
  });
});

const hasPerl = spawnSync('perl', ['-v']).status === 0;
describe.skipIf(!hasPerl)('segment-server.pl ARCHIVED', () => {
  it('lists the sequence and age of every archived segment', async () => {
    const root = mkdtempSync(join(tmpdir(), 'segsrv-'));
    for (const d of ['archive', 'source', 'repl']) mkdirSync(join(root, d));
    const segment = (name: string, seq: number, ageSeconds: number) => {
      const header = Buffer.alloc(48);
      header.write('FBCHANGELOG', 0, 'latin1');
      header.writeBigUInt64LE(BigInt(seq), 32);
      header.writeBigUInt64LE(48n, 40);
      const path = join(root, 'archive', name);
      writeFileSync(path, header);
      const t = Date.now() / 1000 - ageSeconds;
      utimesSync(path, t, t);
    };
    segment('mydb.fdb.journal-000000007', 7, 120);
    segment('mydb.fdb.journal-000000008', 8, 30);
    const script = join(root, 'segment-server.pl');
    writeFileSync(script, REPLICATION_SCRIPTS['segment-server.pl']);
    writeFileSync(join(root, 'repl', 'primary'), 'db-0.db-headless\n');
    const port = 20000 + Math.floor(Math.random() * 20000);
    const child = spawn('perl', [script], {
      env: {
        ...process.env,
        ARCHIVE_DIR: join(root, 'archive'),
        DATABASE_PATH: join(root, 'mydb.fdb'),
        SOURCE_DIR: join(root, 'source'),
        REPLICATION_DIR: join(root, 'repl'),
        PRIMARY_FILE: join(root, 'repl', 'primary'),
        POD_NAME: 'db-0',
        ISC_PASSWORD: 'tok',
        SEGMENT_PORT: String(port),
      },
    });
    try {
      await new Promise<void>((resolve, reject) => {
        child.stdout.on('data', (d) => d.toString().includes('listening') && resolve());
        child.once('exit', (code) => reject(new Error(`segment server exited ${code}`)));
      });
      const { segmentRequest } = await vi.importActual<typeof import('../src/utils/replication-lag')>(
        '../src/utils/replication-lag',
      );
      const reply = parseArchived(await segmentRequest('127.0.0.1', port, 'tok ARCHIVED'));
      expect(reply.map((s) => s.sequence)).toEqual([7, 8]);
      expect(reply[0].ageSeconds).toBeGreaterThanOrEqual(119);
      expect(reply[1].ageSeconds).toBeLessThan(60);
      expect(await segmentRequest('127.0.0.1', port, 'wrong ARCHIVED')).toEqual(['ERR unauthorized']);
    } finally {
      child.kill();
    }
  });
});

describe.skipIf(!hasPerl)('segment-server.pl RETAIN and pruning', () => {
  it('keeps segments replicas have not applied past the retention age, up to the maximum', async () => {
    const root = mkdtempSync(join(tmpdir(), 'segsrv-'));
    for (const d of ['archive', 'source', 'repl']) mkdirSync(join(root, d));
    const segment = (seq: number, ageSeconds: number) => {
      const header = Buffer.alloc(48);
      header.write('FBCHANGELOG', 0, 'latin1');
      header.writeBigUInt64LE(BigInt(seq), 32);
      header.writeBigUInt64LE(48n, 40);
      const path = join(root, 'archive', `mydb.fdb.journal-${String(seq).padStart(9, '0')}`);
      writeFileSync(path, header);
      const t = Date.now() / 1000 - ageSeconds;
      utimesSync(path, t, t);
    };
    segment(7, 7200); // past the maximum retention: deleted even though a replica needs it
    segment(8, 120); // past the retention age, applied by every replica: deleted
    segment(9, 120); // past the retention age, not applied by a replica yet: kept
    segment(10, 10); // within the retention age: kept
    // the floor survives a restart of the primary's segment server
    writeFileSync(join(root, 'repl', 'retain-floor'), '8\n');
    writeFileSync(join(root, 'repl', 'primary'), 'db-0.db-headless\n');
    const script = join(root, 'segment-server.pl');
    writeFileSync(script, REPLICATION_SCRIPTS['segment-server.pl']);
    const port = 20000 + Math.floor(Math.random() * 20000);
    const child = spawn('perl', [script], {
      env: {
        ...process.env,
        ARCHIVE_DIR: join(root, 'archive'),
        DATABASE_PATH: join(root, 'mydb.fdb'),
        SOURCE_DIR: join(root, 'source'),
        REPLICATION_DIR: join(root, 'repl'),
        PRIMARY_FILE: join(root, 'repl', 'primary'),
        POD_NAME: 'db-0',
        ISC_PASSWORD: 'tok',
        SEGMENT_PORT: String(port),
        SEGMENT_RETENTION_SECONDS: '60',
        SEGMENT_MAX_RETENTION_SECONDS: '3600',
      },
    });
    try {
      await new Promise<void>((resolve, reject) => {
        child.stdout.on('data', (d) => d.toString().includes('listening') && resolve());
        child.once('exit', (code) => reject(new Error(`segment server exited ${code}`)));
      });
      const { segmentRequest } = await vi.importActual<typeof import('../src/utils/replication-lag')>(
        '../src/utils/replication-lag',
      );
      // the first request is served after the startup prune
      const archived = parseArchived(await segmentRequest('127.0.0.1', port, 'tok ARCHIVED'));
      expect(archived.map((s) => s.sequence)).toEqual([9, 10]);

      const floor = () => readFileSync(join(root, 'repl', 'retain-floor'), 'utf8');
      expect(await segmentRequest('127.0.0.1', port, 'tok RETAIN 9')).toEqual(['OK']);
      expect(floor()).toBe('9\n');
      expect(await segmentRequest('127.0.0.1', port, 'tok RETAIN none')).toEqual(['OK']);
      expect(existsSync(join(root, 'repl', 'retain-floor'))).toBe(false);
      expect(await segmentRequest('127.0.0.1', port, 'tok RETAIN -1')).toEqual(['ERR bad request']);
      expect(await segmentRequest('127.0.0.1', port, 'wrong RETAIN 9')).toEqual(['ERR unauthorized']);
    } finally {
      child.kill();
    }
  });
});

describe.skipIf(!hasPerl)('segment-server.pl keeps the segments after the offline seed', () => {
  it('without a replica floor, the segments after the bootstrap seed are kept, up to the maximum', async () => {
    const root = mkdtempSync(join(tmpdir(), 'segsrv-'));
    for (const d of ['archive', 'source', 'repl']) mkdirSync(join(root, d));
    const segment = (seq: number, ageSeconds: number) => {
      const header = Buffer.alloc(48);
      header.write('FBCHANGELOG', 0, 'latin1');
      header.writeBigUInt64LE(BigInt(seq), 32);
      header.writeBigUInt64LE(48n, 40);
      const path = join(root, 'archive', `mydb.fdb.journal-${String(seq).padStart(9, '0')}`);
      writeFileSync(path, header);
      const t = Date.now() / 1000 - ageSeconds;
      utimesSync(path, t, t);
    };
    segment(7, 120); // up to the seed: not needed by a replica seeded from it
    segment(9, 120); // after the seed: kept
    segment(10, 7200); // past the maximum retention: deleted anyway
    writeFileSync(join(root, 'repl', 'bootstrap-seed.fdb'), 'seed');
    writeFileSync(join(root, 'repl', 'bootstrap-seed.seq'), '8\n');
    writeFileSync(join(root, 'repl', 'primary'), 'db-0.db-headless\n');
    const script = join(root, 'segment-server.pl');
    writeFileSync(script, REPLICATION_SCRIPTS['segment-server.pl']);
    const port = 20000 + Math.floor(Math.random() * 20000);
    const child = spawn('perl', [script], {
      env: {
        ...process.env,
        ARCHIVE_DIR: join(root, 'archive'),
        DATABASE_PATH: join(root, 'mydb.fdb'),
        SOURCE_DIR: join(root, 'source'),
        REPLICATION_DIR: join(root, 'repl'),
        PRIMARY_FILE: join(root, 'repl', 'primary'),
        POD_NAME: 'db-0',
        ISC_PASSWORD: 'tok',
        SEGMENT_PORT: String(port),
        SEGMENT_RETENTION_SECONDS: '60',
        SEGMENT_MAX_RETENTION_SECONDS: '3600',
      },
    });
    try {
      await new Promise<void>((resolve, reject) => {
        child.stdout.on('data', (d) => d.toString().includes('listening') && resolve());
        child.once('exit', (code) => reject(new Error(`segment server exited ${code}`)));
      });
      const { segmentRequest } = await vi.importActual<typeof import('../src/utils/replication-lag')>(
        '../src/utils/replication-lag',
      );
      const archived = parseArchived(await segmentRequest('127.0.0.1', port, 'tok ARCHIVED'));
      expect(archived.map((s) => s.sequence)).toEqual([9]);
    } finally {
      child.kill();
    }
  });
});

describe('replication lag reconciliation', () => {
  const cluster: FirebirdCluster = {
    apiVersion: 'firebird.cloudnative-firebird.io/v1',
    kind: 'FirebirdCluster',
    metadata: { name: 'db', namespace: 'prod', uid: 'c' },
    spec: { instances: 3, storage: { size: '1Gi' }, replication: { enabled: true } },
  };
  const pod = (name: string, annotations: Record<string, string> = {}): V1Pod => ({
    metadata: { name, uid: `u-${name}`, annotations },
    status: { phase: 'Running', conditions: [{ type: 'Ready', status: 'True' }] },
  });

  function setup(pods: V1Pod[], replies: Record<string, string[] | Error>) {
    const notFound = Object.assign(new Error('Not Found'), { code: 404 });
    const api: Record<string, Mock> = {
      listNamespacedPod: vi.fn().mockResolvedValue({ items: pods }),
      readNamespacedLease: vi.fn().mockResolvedValue({ spec: { holderIdentity: 'db-0' } }),
      readNamespacedConfigMap: vi.fn().mockResolvedValue({ data: {} }),
    };
    const calls: Record<string, Mock> = {};
    const fn = (m: string): Mock =>
      (calls[m] ??=
        api[m] ??
        (m.startsWith('read') || m.startsWith('get')
          ? vi.fn().mockRejectedValue(notFound)
          : m.startsWith('list')
            ? vi.fn().mockResolvedValue({ items: [] })
            : vi.fn().mockResolvedValue({})));
    const kubeConfig = new KubeConfig();
    vi.spyOn(kubeConfig, 'makeApiClient').mockReturnValue(new Proxy({}, { get: (_t, p: string) => fn(p) }) as never);
    const client: SegmentClient = vi.fn(async (host: string, _port: number, line: string) => {
      const reply = replies[`${host.split('.')[0]} ${line.split(' ')[1]}`];
      if (reply instanceof Error) throw reply;
      return reply ?? [];
    });
    const status = () => {
      const c = fn('patchNamespacedCustomObjectStatus').mock.calls;
      return c[c.length - 1][0].body[0].value;
    };
    return { controller: new FirebirdClusterController(kubeConfig, client), fn, client, status };
  }

  it('measures each replica, publishes the lag annotation and reports it in status', async () => {
    const s = setup([pod('db-0'), pod('db-1'), pod('db-2', { [REPLICATION_LAG_ANNOTATION]: '99' })], {
      'db-0 ARCHIVED': ['20 400', '21 90', '22 10'],
      'db-1 POSITION': ['OK 22 100 0'],
      'db-2 POSITION': ['OK 20 100 2'],
    });
    await s.controller.reconcile(cluster);
    // the segment servers are reached by their fully qualified names, with the SYSDBA password
    expect(s.client).toHaveBeenCalledWith('db-0.db-headless.prod.svc', 3051, 'masterkey ARCHIVED');
    const annotations = Object.fromEntries(
      s.fn('patchNamespacedPod').mock.calls
        .filter((c) => c[0].body?.metadata?.annotations)
        .map((c) => [c[0].name, c[0].body.metadata.annotations[REPLICATION_LAG_ANNOTATION]]),
    );
    expect(annotations).toEqual({ 'db-1': '0', 'db-2': '90' });
    expect(s.status().replicationStatus).toMatchObject({
      primaryPod: 'db-0',
      lastArchivedSequence: 22,
      replicas: [
        { name: 'db-1', appliedSequence: 22, pendingSegments: 0, lagSegments: 0, lagSeconds: 0 },
        { name: 'db-2', appliedSequence: 20, pendingSegments: 2, lagSegments: 2, lagSeconds: 90 },
      ],
    });
    // and exported as Prometheus metrics
    const exported = metrics.render();
    expect(exported).toContain('firebird_replication_lag_seconds{namespace="prod",cluster="db",pod="db-2"} 90\n');
    expect(exported).toContain('firebird_replication_lag_segments{namespace="prod",cluster="db",pod="db-1"} 0\n');
    expect(exported).toMatch(/firebird_operator_reconciles_total\{namespace="prod",cluster="db",result="success"\} \d+/);
  });

  it('sends the primary the lowest applied segment, remembering replicas that are not ready', async () => {
    const s = setup([pod('db-0'), pod('db-1'), { ...pod('db-2'), status: { phase: 'Running', conditions: [{ type: 'Ready', status: 'False' }] } }], {
      'db-0 ARCHIVED': ['20 400', '21 90', '22 10'],
      'db-0 RETAIN': ['OK'],
      'db-1 POSITION': ['OK 22 100 0'],
    });
    await s.controller.reconcile({
      ...cluster,
      status: { replicationStatus: { segmentRetention: { floorSequence: 18, replicas: [{ name: 'db-1', appliedSequence: 21 }, { name: 'db-2', appliedSequence: 18 }] } } },
    });
    expect(s.client).toHaveBeenCalledWith('db-0.db-headless.prod.svc', 3051, 'masterkey RETAIN 18');
    expect(s.status().replicationStatus.segmentRetention).toEqual({
      floorSequence: 18,
      replicas: [{ name: 'db-1', appliedSequence: 22 }, { name: 'db-2', appliedSequence: 18 }],
    });
  });

  it('keeps the floor when the primary cannot be reached, and clears it without replicas', async () => {
    const previous = { segmentRetention: { floorSequence: 18, replicas: [{ name: 'db-1', appliedSequence: 18 }] } };
    const down = setup([pod('db-0'), pod('db-1')], { 'db-0 ARCHIVED': new Error('connect ECONNREFUSED') });
    await down.controller.reconcile({ ...cluster, spec: { ...cluster.spec, instances: 2 }, status: { replicationStatus: previous } });
    expect(down.status().replicationStatus.segmentRetention).toEqual(previous.segmentRetention);

    const single = setup([pod('db-0')], { 'db-0 RETAIN': ['OK'] });
    await single.controller.reconcile({ ...cluster, spec: { ...cluster.spec, instances: 1 }, status: { replicationStatus: previous } });
    expect(single.client).toHaveBeenCalledWith('db-0.db-headless.prod.svc', 3051, 'masterkey RETAIN none');
    expect(single.status().replicationStatus.segmentRetention).toBeUndefined();
  });

  it('drops the annotation of a replica that cannot be measured', async () => {
    const s = setup([pod('db-0'), pod('db-1', { [REPLICATION_LAG_ANNOTATION]: '5' })], {
      'db-0 ARCHIVED': ['20 5'],
      'db-1 POSITION': new Error('connect ECONNREFUSED'),
    });
    await s.controller.reconcile({ ...cluster, spec: { ...cluster.spec, instances: 2 } });
    const patch = s.fn('patchNamespacedPod').mock.calls.find((c) => c[0].body?.metadata?.annotations);
    expect(patch?.[0]).toMatchObject({ name: 'db-1', body: { metadata: { annotations: { [REPLICATION_LAG_ANNOTATION]: null } } } });
    expect(s.status().replicationStatus.replicas).toEqual([{ name: 'db-1', error: 'connect ECONNREFUSED' }]);
  });

  it('does not patch an unchanged annotation, and skips measuring without replicas', async () => {
    const s = setup([pod('db-0'), pod('db-1', { [REPLICATION_LAG_ANNOTATION]: '0' })], {
      'db-0 ARCHIVED': ['20 5'],
      'db-1 POSITION': ['OK 20 0 0'],
    });
    await s.controller.reconcile({ ...cluster, spec: { ...cluster.spec, instances: 2 } });
    expect(s.fn('patchNamespacedPod').mock.calls.filter((c) => c[0].body?.metadata?.annotations)).toEqual([]);

    const single = setup([pod('db-0')], {});
    await single.controller.reconcile({ ...cluster, spec: { ...cluster.spec, instances: 1 } });
    expect(single.client).not.toHaveBeenCalled();
  });
});

describe.skipIf(!hasPerl)('segment-server.pl pruneAppliedSegments', () => {
  /** Starts the real segment server on young segments 7..11 with the given floor files and env */
  async function archivedAfterStartup(options: { floor?: string; uploaded?: string; env: Record<string, string> }) {
    const root = mkdtempSync(join(tmpdir(), 'segsrv-'));
    for (const d of ['archive', 'source', 'repl']) mkdirSync(join(root, d));
    for (const seq of [7, 8, 9, 10, 11]) {
      const header = Buffer.alloc(48);
      header.write('FBCHANGELOG', 0, 'latin1');
      header.writeBigUInt64LE(BigInt(seq), 32);
      header.writeBigUInt64LE(48n, 40);
      writeFileSync(join(root, 'archive', `mydb.fdb.journal-${String(seq).padStart(9, '0')}`), header);
    }
    if (options.floor) writeFileSync(join(root, 'repl', 'retain-floor'), options.floor);
    if (options.uploaded) writeFileSync(join(root, 'repl', 'uploaded-floor'), options.uploaded);
    writeFileSync(join(root, 'repl', 'primary'), 'db-0.db-headless\n');
    const script = join(root, 'segment-server.pl');
    writeFileSync(script, REPLICATION_SCRIPTS['segment-server.pl']);
    const port = 20000 + Math.floor(Math.random() * 20000);
    const child = spawn('perl', [script], {
      env: {
        ...process.env,
        ARCHIVE_DIR: join(root, 'archive'),
        DATABASE_PATH: join(root, 'mydb.fdb'),
        SOURCE_DIR: join(root, 'source'),
        REPLICATION_DIR: join(root, 'repl'),
        PRIMARY_FILE: join(root, 'repl', 'primary'),
        POD_NAME: 'db-0',
        ISC_PASSWORD: 'tok',
        SEGMENT_PORT: String(port),
        SEGMENT_RETENTION_SECONDS: '86400',
        SEGMENT_MAX_RETENTION_SECONDS: '604800',
        ...options.env,
      },
    });
    try {
      await new Promise<void>((resolve, reject) => {
        child.stdout.on('data', (d) => d.toString().includes('listening') && resolve());
        child.once('exit', (code) => reject(new Error(`segment server exited ${code}`)));
      });
      const { segmentRequest } = await vi.importActual<typeof import('../src/utils/replication-lag')>(
        '../src/utils/replication-lag',
      );
      const sequences = parseArchived(await segmentRequest('127.0.0.1', port, 'tok ARCHIVED')).map((s) => s.sequence);
      expect(await segmentRequest('127.0.0.1', port, 'tok UPLOADED 12')).toEqual(['OK']);
      expect(readFileSync(join(root, 'repl', 'uploaded-floor'), 'utf8')).toBe('12\n');
      expect(await segmentRequest('127.0.0.1', port, 'tok UPLOADED x')).toEqual(['ERR bad request']);
      return sequences;
    } finally {
      child.kill();
    }
  }

  it('deletes young segments below the replicas\' floor that the archive Job has uploaded', async () => {
    // floor 10: segment 10 may be partly applied; uploaded up to 8
    expect(
      await archivedAfterStartup({ floor: '10\n', uploaded: '8\n', env: { PRUNE_APPLIED: 'true', ARCHIVE_UPLOAD: 'true' } }),
    ).toEqual([9, 10, 11]);
    // nothing reported as uploaded yet: nothing goes early
    expect(await archivedAfterStartup({ floor: '10\n', env: { PRUNE_APPLIED: 'true', ARCHIVE_UPLOAD: 'true' } })).toEqual([
      7, 8, 9, 10, 11,
    ]);
  });

  it('without an archive upload only the floor counts; without the option or a floor nothing goes early', async () => {
    expect(await archivedAfterStartup({ floor: '10\n', env: { PRUNE_APPLIED: 'true' } })).toEqual([10, 11]);
    expect(await archivedAfterStartup({ floor: '10\n', env: {} })).toEqual([7, 8, 9, 10, 11]);
    expect(await archivedAfterStartup({ env: { PRUNE_APPLIED: 'true' } })).toEqual([7, 8, 9, 10, 11]);
  });
});
