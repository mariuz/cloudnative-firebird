import { describe, it, expect, vi, Mock } from 'vitest';
import { spawn, spawnSync } from 'child_process';
import { mkdirSync, mkdtempSync, utimesSync, writeFileSync } from 'fs';
import { createServer } from 'net';
import { tmpdir } from 'os';
import { join } from 'path';
import { KubeConfig, V1Pod } from '@kubernetes/client-node';
import { computeLag, parseArchived, parsePosition, SegmentClient } from '../src/utils/replication-lag';
import { REPLICATION_SCRIPTS } from '../src/utils/replication';
import { REPLICATION_LAG_ANNOTATION } from '../src/utils/routing';
import { FirebirdClusterController } from '../src/controllers/firebirdcluster.controller';
import { FirebirdCluster } from '../src/types';

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
