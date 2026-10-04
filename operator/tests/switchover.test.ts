import { describe, it, expect, vi, Mock } from 'vitest';
import { execFileSync, spawn, spawnSync } from 'child_process';
import { chmodSync, mkdtempSync, readFileSync, writeFileSync } from 'fs';
import { createServer } from 'net';
import { tmpdir } from 'os';
import { join } from 'path';
import { KubeConfig, V1Job } from '@kubernetes/client-node';
import { FirebirdClusterController } from '../src/controllers/firebirdcluster.controller';
import { TARGET_PRIMARY_ANNOTATION, buildSwitchoverJob } from '../src/utils/switchover';
import { REPLICATION_SCRIPTS } from '../src/utils/replication';
import { FirebirdCluster, SwitchoverStatus } from '../src/types';

const makeCluster = (target?: string, status?: FirebirdCluster['status']): FirebirdCluster => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdCluster',
  metadata: {
    name: 'db',
    namespace: 'default',
    uid: 'c',
    ...(target ? { annotations: { [TARGET_PRIMARY_ANNOTATION]: target } } : {}),
  },
  spec: { instances: 3, storage: { size: '1Gi' }, replication: { enabled: true } },
  ...(status ? { status } : {}),
});

const pod = (name: string, uid: string, ready = true) => ({
  metadata: { name, uid },
  status: { phase: 'Running', conditions: [{ type: 'Ready', status: ready ? 'True' : 'False' }] },
});

function setup(opts: { pods?: object[]; job?: V1Job; stored?: FirebirdCluster; lease?: string } = {}) {
  const notFound = Object.assign(new Error('Not Found'), { code: 404 });
  const api: Record<string, Mock> = {
    listNamespacedPod: vi.fn().mockResolvedValue({
      items: opts.pods ?? [pod('db-0', 'u0'), pod('db-1', 'u1'), pod('db-2', 'u2')],
    }),
    readNamespacedLease: vi.fn().mockResolvedValue({ spec: { holderIdentity: opts.lease ?? 'db-0' } }),
    getNamespacedCustomObject: opts.stored ? vi.fn().mockResolvedValue(opts.stored) : vi.fn().mockRejectedValue(notFound),
    readNamespacedJob: opts.job ? vi.fn().mockResolvedValue(opts.job) : vi.fn().mockRejectedValue(notFound),
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
  const status = () => {
    const c = fn('patchNamespacedCustomObjectStatus').mock.calls;
    return c[c.length - 1][0].body[0].value;
  };
  const cmData = () => {
    const c = fn('patchNamespacedConfigMap').mock.calls;
    return c.length ? c[c.length - 1][0].body.data : undefined;
  };
  const created = () => fn('createNamespacedJob').mock.calls.map((c) => c[0].body as V1Job);
  return { controller: new FirebirdClusterController(kubeConfig), fn, status, cmData, created };
}

const done = (type: 'Complete' | 'Failed'): V1Job => ({ status: { conditions: [{ type, status: 'True' }] } });

describe('planned switchover', () => {
  it('starts the switchover Job against the current primary, the target and the other ready replicas', async () => {
    const s = setup({ pods: [pod('db-0', 'u0'), pod('db-1', 'u1'), pod('db-2', 'u2'), pod('db-3', 'u3', false)] });
    await s.controller.reconcile({ ...makeCluster('db-1'), spec: { ...makeCluster().spec, instances: 4 } });
    const job = s.created().find((j) => j.metadata?.name === 'db-switchover')!;
    const env = Object.fromEntries(job.spec!.template.spec!.containers[0].env!.map((e) => [e.name, e.value]));
    expect(env.OLD_PRIMARY).toBe('db-0.db-headless');
    expect(env.TARGET).toBe('db-1.db-headless');
    expect(env.REPLICAS).toBe('db-2.db-headless');
    expect(s.status().switchover).toMatchObject({ target: 'db-1', from: 'db-0', phase: 'Stopping' });
  });

  it('does nothing when the target is already the primary', async () => {
    const s = setup();
    await s.controller.reconcile(makeCluster('db-0'));
    expect(s.created().some((j) => j.metadata?.name === 'db-switchover')).toBe(false);
  });

  it('refuses an unknown, unready or fenced target', async () => {
    for (const [target, pods, fenced, message] of [
      ['db-7', undefined, [], /not an instance/],
      ['db-1', [pod('db-0', 'u0'), pod('db-1', 'u1', false)], [], /not ready/],
      ['db-1', undefined, ['db-1'], /fenced/],
    ] as const) {
      const s = setup({ pods: pods as object[] | undefined });
      const cluster = makeCluster(target, { fencedInstances: [...fenced] });
      await s.controller.reconcile(cluster);
      expect(s.status().switchover).toMatchObject({ phase: 'Failed', message: expect.stringMatching(message) });
      expect(s.created().some((j) => j.metadata?.name === 'db-switchover')).toBe(false);
    }
  });

  it('does not retry a failed switchover to the same target', async () => {
    const s = setup();
    const failed: SwitchoverStatus = { target: 'db-1', from: 'db-0', phase: 'Failed', message: 'x' };
    await s.controller.reconcile(makeCluster('db-1', { switchover: failed }));
    expect(s.created().some((j) => j.metadata?.name === 'db-switchover')).toBe(false);
  });

  it('moves the primary once the Job confirmed the replicas caught up', async () => {
    const s = setup({ job: done('Complete'), pods: [pod('db-0', 'u0'), pod('db-1', 'u1'), pod('db-2', 'u2', false)] });
    const stopping: SwitchoverStatus = { target: 'db-1', from: 'db-0', phase: 'Stopping' };
    await s.controller.reconcile(makeCluster('db-1', { switchover: stopping }));

    expect(s.fn('patchNamespacedLease').mock.calls[0][0].body[0]).toEqual({
      op: 'replace',
      path: '/spec/holderIdentity',
      value: 'db-1',
    });
    const data = s.cmData();
    expect(data.primary).toBe('db-1.db-headless');
    expect(data.promote).toBe('db-1 u1\n');
    expect(data.demote).toBe('db-0 u0\n');
    // the unready replica may have missed segments: re-seeded
    expect(data.reseed).toBe('db-2 u2\n');
    const restarted = s.fn('deleteNamespacedPod').mock.calls.map((c) => c[0].name).sort();
    expect(restarted).toEqual(['db-0', 'db-1', 'db-2']);
    // the ConfigMap carries the directives before the pods restart
    expect(s.fn('patchNamespacedConfigMap').mock.invocationCallOrder[0]).toBeLessThan(
      s.fn('deleteNamespacedPod').mock.invocationCallOrder[0],
    );
    expect(s.status().switchover).toMatchObject({ phase: 'Promoting', targetToken: 'u1', fromToken: 'u0' });
  });

  it('brings the old primary back online when the Job fails', async () => {
    const s = setup({ job: done('Failed') });
    await s.controller.reconcile(makeCluster('db-1', { switchover: { target: 'db-1', from: 'db-0', phase: 'Stopping' } }));
    const unfence = s.created().find((j) => j.metadata?.name === 'db-0-fencing');
    expect(unfence?.spec?.template.spec?.containers[0].args?.[0]).toContain('prp_online_mode prp_sm_normal');
    expect(s.status().switchover).toMatchObject({ phase: 'Failed', message: expect.stringContaining('db-0 stays primary') });
    expect(s.fn('patchNamespacedLease')).not.toHaveBeenCalled();
  });

  it('keeps the directives until both instances are ready as new pods, then completes', async () => {
    const promoting: SwitchoverStatus = { target: 'db-1', from: 'db-0', phase: 'Promoting', targetToken: 'u1', fromToken: 'u0' };
    const waiting = setup({ pods: [pod('db-0', 'u0'), pod('db-1', 'new1'), pod('db-2', 'u2')] });
    await waiting.controller.reconcile(makeCluster('db-1', { switchover: promoting }));
    expect(waiting.cmData().promote).toBe('db-1 u1\n');
    expect(waiting.cmData().primary).toBe('db-1.db-headless');
    expect(waiting.status().switchover.phase).toBe('Promoting');

    const finished = setup({ pods: [pod('db-0', 'new0'), pod('db-1', 'new1'), pod('db-2', 'u2')] });
    await finished.controller.reconcile(makeCluster('db-1', { switchover: promoting }));
    expect(finished.cmData().promote).toBe('');
    expect(finished.cmData().demote).toBe('');
    expect(finished.status().switchover).toMatchObject({ phase: 'Completed' });
  });

  it('acts on the stored switchover state, not on a stale copy of the cluster', async () => {
    // a reconcile started with an old watch copy (Stopping) after the move to Promoting was stored
    const stale = makeCluster('db-1', { switchover: { target: 'db-1', from: 'db-0', phase: 'Stopping' } });
    const stored = makeCluster('db-1', {
      switchover: { target: 'db-1', from: 'db-0', phase: 'Promoting', targetToken: 'u1', fromToken: 'u0' },
    });
    const s = setup({ stored, lease: 'db-1' });
    await s.controller.reconcile(stale);
    expect(s.created().some((j) => j.metadata?.name === 'db-switchover')).toBe(false);
    expect(s.cmData().promote).toBe('db-1 u1\n');
    expect(s.cmData().primary).toBe('db-1.db-headless');
  });

  it('stores each phase before acting on it', async () => {
    const s = setup({ job: done('Complete') });
    await s.controller.reconcile(makeCluster('db-1', { switchover: { target: 'db-1', from: 'db-0', phase: 'Stopping' } }));
    const stored = s.fn('patchNamespacedCustomObjectStatus').mock.calls.findIndex(
      (c) => c[0].body[0].path === '/status/switchover' && c[0].body[0].value.phase === 'Promoting',
    );
    expect(stored).toBeGreaterThanOrEqual(0);
    const storedOrder = s.fn('patchNamespacedCustomObjectStatus').mock.invocationCallOrder[stored];
    expect(storedOrder).toBeLessThan(s.fn('deleteNamespacedJob').mock.invocationCallOrder[0]);
    expect(storedOrder).toBeLessThan(s.fn('patchNamespacedLease').mock.invocationCallOrder[0]);
  });

  it('resumes Promoting idempotently: moves the Lease and restarts pods that still run as before', async () => {
    const promoting: SwitchoverStatus = { target: 'db-1', from: 'db-0', phase: 'Promoting', targetToken: 'u1', fromToken: 'u0' };
    // the Lease was not moved and db-0 was not restarted yet (an earlier reconcile failed midway)
    const s = setup({ pods: [pod('db-0', 'u0'), pod('db-1', 'new1'), pod('db-2', 'u2')] });
    await s.controller.reconcile(makeCluster('db-1', { switchover: promoting }));
    expect(s.fn('patchNamespacedLease')).toHaveBeenCalled();
    expect(s.fn('deleteNamespacedPod').mock.calls.map((c) => c[0].name)).toEqual(['db-0']);
  });

  it('only considers instance pods, not Job pods of the cluster', async () => {
    const jobPod = { ...pod('db-switchover-abcde', 'j1', false) };
    const s = setup({ job: done('Complete'), pods: [pod('db-0', 'u0'), pod('db-1', 'u1'), pod('db-2', 'u2'), jobPod] });
    await s.controller.reconcile(makeCluster('db-1', { switchover: { target: 'db-1', from: 'db-0', phase: 'Stopping' } }));
    expect(s.cmData().reseed).toBe('');
    expect(s.fn('listNamespacedPod').mock.calls[0][0].labelSelector).toContain('app.kubernetes.io/component=database');
  });

  it('needs replication', async () => {
    const s = setup();
    const cluster = makeCluster('db-1');
    cluster.spec.replication = { enabled: false };
    await s.controller.reconcile(cluster);
    expect(s.created().some((j) => j.metadata?.name === 'db-switchover')).toBe(false);
  });

  it('runs switchover.pl with the cluster scripts', () => {
    const job = buildSwitchoverJob(makeCluster(), { from: 'db-0', target: 'db-2', replicas: [] });
    expect(job.spec?.backoffLimit).toBe(0);
    expect(job.spec?.template.spec?.containers[0].command).toEqual(['perl', '/etc/firebird-operator/switchover.pl']);
    expect(job.spec?.template.spec?.volumes).toEqual([{ name: 'cluster-config', configMap: { name: 'db-config' } }]);
  });
});

describe('set-repl-seq.pl', () => {
  const hasPerl = spawnSync('perl', ['-v']).status === 0;

  // header page: pag_type 1, page size at 16, ODS version at 18 (Firebird flag 0x8000), then
  // hdr_end and the clumps: at 66 and from 128 in ODS 13, at 36 and from 148 in ODS 14
  const LAYOUT: Record<number, [number, number]> = { 13: [66, 128], 14: [36, 148] };
  const header = (clumps: Buffer, ods = 13) => {
    const [endAt, start] = LAYOUT[ods] ?? [66, 128];
    const page = Buffer.alloc(8192);
    page[0] = 1;
    page.writeUInt16LE(8192, 16);
    page.writeUInt16LE(0x8000 | ods, 18);
    clumps.copy(page, start);
    page.writeUInt16LE(start + clumps.length, endAt);
    page[start + clumps.length] = 0;
    return page;
  };

  it('adds the replication sequence clump after the existing ones', () => {
    if (!hasPerl) return;
    const dir = mkdtempSync(join(tmpdir(), 'fb-seq-'));
    const script = join(dir, 'set-repl-seq.pl');
    writeFileSync(script, REPLICATION_SCRIPTS['set-repl-seq.pl']);
    const guid = Buffer.concat([Buffer.from([10, 16]), Buffer.alloc(16, 0xab)]);
    const db = join(dir, 'db.fdb');
    writeFileSync(db, Buffer.concat([header(guid), Buffer.alloc(8192)]));

    execFileSync('perl', [script, db, '63'], { stdio: 'pipe' });
    const page = readFileSync(db);
    expect(page.readUInt16LE(66)).toBe(128 + 18 + 10);
    expect(page.subarray(128, 146)).toEqual(guid);
    expect(page[146]).toBe(11);
    expect(page[147]).toBe(8);
    expect(page.readBigUInt64LE(148)).toBe(63n);
    expect(page[156]).toBe(0);
    expect(page.length).toBe(16384);

    // an existing clump is replaced in place
    execFileSync('perl', [script, db, '64'], { stdio: 'pipe' });
    const again = readFileSync(db);
    expect(again.readUInt16LE(66)).toBe(156);
    expect(again.readBigUInt64LE(148)).toBe(64n);
  });

  it('uses the ODS 14 (Firebird 6) header layout', () => {
    if (!hasPerl) return;
    const dir = mkdtempSync(join(tmpdir(), 'fb-seq-'));
    const script = join(dir, 'set-repl-seq.pl');
    writeFileSync(script, REPLICATION_SCRIPTS['set-repl-seq.pl']);
    const db = join(dir, 'db.fdb');
    writeFileSync(db, header(Buffer.alloc(0), 14));
    execFileSync('perl', [script, db, '7'], { stdio: 'pipe' });
    const page = readFileSync(db);
    expect(page.readUInt16LE(36)).toBe(148 + 10);
    expect([page[148], page[149]]).toEqual([11, 8]);
    expect(page.readBigUInt64LE(150)).toBe(7n);
    expect(page[158]).toBe(0);
    // the ODS 13 fields are left alone (in ODS 14 they hold transaction counters)
    expect(page.readUInt16LE(66)).toBe(0);
    expect(page.subarray(128, 148)).toEqual(Buffer.alloc(20));
  });

  it('refuses an unknown on-disk structure, and files that are not a database header', () => {
    if (!hasPerl) return;
    const dir = mkdtempSync(join(tmpdir(), 'fb-seq-'));
    const script = join(dir, 'set-repl-seq.pl');
    writeFileSync(script, REPLICATION_SCRIPTS['set-repl-seq.pl']);
    const future = join(dir, 'future.fdb');
    writeFileSync(future, header(Buffer.alloc(0), 15));
    const r = spawnSync('perl', [script, future, '1'], { encoding: 'utf8' });
    expect(r.status).not.toBe(0);
    expect(r.stderr).toContain('unsupported on-disk structure 15');
    expect(readFileSync(future)).toEqual(header(Buffer.alloc(0), 15));
  });

  it('refuses files that are not a database header', () => {
    if (!hasPerl) return;
    const dir = mkdtempSync(join(tmpdir(), 'fb-seq-'));
    const script = join(dir, 'set-repl-seq.pl');
    writeFileSync(script, REPLICATION_SCRIPTS['set-repl-seq.pl']);
    const bogus = join(dir, 'x');
    writeFileSync(bogus, Buffer.alloc(8192));
    expect(spawnSync('perl', [script, bogus, '1']).status).not.toBe(0);
  });
});

describe('switchover.pl with Firebird 6', () => {
  const hasPerl = spawnSync('perl', ['-v']).status === 0;

  it('reads the final sequence through the segment server when header statistics are refused', async () => {
    if (!hasPerl) return;
    const dir = mkdtempSync(join(tmpdir(), 'fb-sw6-'));
    // Firebird 6 answers header statistics of a database in full shutdown with an error
    const fbsvcmgr = join(dir, 'fbsvcmgr');
    writeFileSync(fbsvcmgr, '#!/bin/sh\necho "$*" >> "$(dirname "$0")/calls"\necho "database /var/lib/firebird/data/mydb.fdb shutdown"\nexit 1\n');
    chmodSync(fbsvcmgr, 0o755);
    const requests: string[] = [];
    const server = createServer((sock) => {
      sock.once('data', (buf) => {
        const cmd = buf.toString().trim().split(' ')[1];
        requests.push(cmd);
        if (cmd === 'HEADER') sock.end('OK 53\n');
        else if (cmd === 'LIST') sock.end('mydb.fdb_GUID.journal-000000053\n.\n');
        else if (cmd === 'POSITION') sock.end('OK 53 0 0\n');
        else sock.end('ERR unknown\n');
      });
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const port = (server.address() as { port: number }).port;
    const script = join(dir, 'switchover.pl');
    writeFileSync(script, REPLICATION_SCRIPTS['switchover.pl']);
    const out = await new Promise<{ code: number | null; text: string }>((resolve) => {
      const child = spawn('perl', [script], {
        env: {
          ...process.env,
          PATH: `${dir}:${process.env.PATH}`,
          OLD_PRIMARY: '127.0.0.1',
          TARGET: '127.0.0.1',
          DATABASE_PATH: '/var/lib/firebird/data/mydb.fdb',
          ISC_PASSWORD: 'tok',
          SEGMENT_PORT: String(port),
          TIMEOUT_SECONDS: '10',
        },
      });
      let text = '';
      child.stdout.on('data', (d) => (text += d));
      child.stderr.on('data', (d) => (text += d));
      child.on('exit', (code) => resolve({ code, text }));
    });
    server.close();
    expect(out.code).toBe(0);
    // already in full shutdown: no second shutdown request
    expect(out.text).toContain('127.0.0.1 already shut down');
    expect(readFileSync(join(dir, 'calls'), 'utf8')).not.toContain('action_properties');
    expect(out.text).toContain('final replication sequence of 127.0.0.1: 53');
    expect(out.text).toContain('switchover ready: promote 127.0.0.1 after segment 53');
    expect(requests).toEqual(['HEADER', 'POSITION']);
  });
});
