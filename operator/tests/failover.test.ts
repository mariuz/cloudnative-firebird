import { describe, it, expect, vi, Mock } from 'vitest';
import { KubeConfig, V1Job, loadYaml } from '@kubernetes/client-node';
import { readFileSync } from 'fs';
import { join } from 'path';
import { FirebirdClusterController } from '../src/controllers/firebirdcluster.controller';
import {
  buildFailoverJob,
  effectiveFailoverDelaySeconds,
  parseElection,
  TARGET_PRIMARY_ANNOTATION,
} from '../src/utils/switchover';
import { buildReplicationContainers } from '../src/utils/replication';
import { validateClusterSpec } from '../src/utils/validation';
import { FirebirdCluster, SwitchoverStatus } from '../src/types';

const makeCluster = (status?: FirebirdCluster['status'], failover: object = { enabled: true, delaySeconds: 30 }): FirebirdCluster => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdCluster',
  metadata: { name: 'db', namespace: 'default', uid: 'c' },
  spec: { instances: 3, storage: { size: '1Gi' }, replication: { enabled: true, failover } },
  ...(status ? { status } : {}),
});

const pod = (name: string, uid: string, ready = true) => ({
  metadata: { name, uid },
  status: { phase: 'Running', conditions: [{ type: 'Ready', status: ready ? 'True' : 'False' }] },
});

const longAgo = new Date(Date.now() - 120_000).toISOString();

function setup(opts: { pods?: object[]; job?: V1Job; jobPods?: object[]; archivePods?: object[]; segment?: Mock } = {}) {
  const notFound = Object.assign(new Error('Not Found'), { code: 404 });
  const api: Record<string, Mock> = {
    listNamespacedPod: vi.fn().mockImplementation(({ labelSelector }: { labelSelector: string }) =>
      Promise.resolve({
        items: labelSelector.startsWith('job-name=')
          ? (opts.jobPods ?? [])
          : labelSelector.includes('journal-archive')
            ? (opts.archivePods ?? [])
          : (opts.pods ?? [pod('db-0', 'u0', false), pod('db-1', 'u1'), pod('db-2', 'u2')]),
      }),
    ),
    readNamespacedLease: vi.fn().mockResolvedValue({ spec: { holderIdentity: 'db-0' } }),
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
  // segment servers: unreachable unless a test answers for them
  const segment = opts.segment ?? vi.fn().mockRejectedValue(new Error('unreachable'));
  return { controller: new FirebirdClusterController(kubeConfig, segment), fn, status, cmData, created, segment };
}

const done = (type: 'Complete' | 'Failed'): V1Job => ({ status: { conditions: [{ type, status: 'True' }] } });
const electionPod = (message: string) => ({
  metadata: { name: 'db-failover-x' },
  status: { containerStatuses: [{ state: { terminated: { exitCode: 0, message } } }] },
});

describe('automatic failover', () => {
  it("promotes after every segment the journal archive may hold", async () => {
    const electing: SwitchoverStatus = { kind: 'failover', target: '', from: 'db-0', phase: 'Electing' };
    const archivePod = (message: string, exitCode = 0) => ({
      metadata: { name: `db-journal-archive-${message}` },
      status: { initContainerStatuses: [{ name: 'fetch-segments', state: { terminated: { exitCode, message } } }] },
    });
    const s = setup({
      job: done('Complete'),
      jobPods: [electionPod('target=db-2.db-headless sequence=41 positions=db-1.db-headless:40,db-2.db-headless:41')],
      archivePods: [archivePod('listed=57'), archivePod('listed=99', 1), archivePod('listed=55')],
    });
    const cluster = makeCluster({ switchover: electing, journalArchiveSequence: 50 });
    cluster.spec.replication!.journalArchiveS3 = { bucket: 'b' };
    await s.controller.reconcile(cluster);
    expect(s.cmData().promote).toBe('db-2 u2 57\n');
    expect(s.status().journalArchiveSequence).toBe(57);
  });


  it('records when the primary became unavailable and waits for the delay', async () => {
    const s = setup();
    await s.controller.reconcile(makeCluster());
    expect(s.status().primaryNotReadySince).toBeDefined();
    expect(s.created().some((j) => j.metadata?.name === 'db-failover')).toBe(false);
  });

  it('elects among the ready replicas once the primary has been unavailable for the delay', async () => {
    const s = setup();
    await s.controller.reconcile(makeCluster({ primaryNotReadySince: longAgo }));
    const job = s.created().find((j) => j.metadata?.name === 'db-failover')!;
    const env = Object.fromEntries(job.spec!.template.spec!.containers[0].env!.map((e) => [e.name, e.value]));
    expect(env.CANDIDATES).toBe('db-1.db-headless db-2.db-headless');
    expect(s.status().switchover).toMatchObject({ kind: 'failover', from: 'db-0', phase: 'Electing' });
  });

  it('is off unless enabled, and never fails over a fenced primary', async () => {
    const off = setup();
    await off.controller.reconcile(makeCluster({ primaryNotReadySince: longAgo }, { enabled: false }));
    expect(off.created().some((j) => j.metadata?.name === 'db-failover')).toBe(false);
    expect(off.status().primaryNotReadySince).toBeUndefined();

    const fenced = setup();
    await fenced.controller.reconcile(makeCluster({ primaryNotReadySince: longAgo, fencedInstances: ['db-0'] }));
    expect(fenced.created().some((j) => j.metadata?.name === 'db-failover')).toBe(false);
    expect(fenced.status().primaryNotReadySince).toBeUndefined();
  });

  it('clears the timer when the primary is ready', async () => {
    const s = setup({ pods: [pod('db-0', 'u0'), pod('db-1', 'u1'), pod('db-2', 'u2')] });
    await s.controller.reconcile(makeCluster({ primaryNotReadySince: longAgo }));
    expect(s.status().primaryNotReadySince).toBeUndefined();
    expect(s.created().some((j) => j.metadata?.name === 'db-failover')).toBe(false);
  });

  it('promotes the elected replica and re-seeds the old primary and replicas behind it', async () => {
    const electing: SwitchoverStatus = { kind: 'failover', target: '', from: 'db-0', phase: 'Electing' };
    const s = setup({
      job: done('Complete'),
      jobPods: [electionPod('target=db-2.db-headless sequence=41 positions=db-1.db-headless:40,db-2.db-headless:41')],
    });
    await s.controller.reconcile(makeCluster({ switchover: electing }));

    expect(s.fn('patchNamespacedLease').mock.calls[0][0].body[0].value).toBe('db-2');
    const data = s.cmData();
    expect(data.primary).toBe('db-2.db-headless');
    expect(data.promote).toBe('db-2 u2\n');
    expect(data.demote).toBe('');
    expect(data.reseed).toBe('db-0 u0\ndb-1 u1\n');
    expect(s.fn('deleteNamespacedPod').mock.calls.map((c) => c[0].name).sort()).toEqual(['db-0', 'db-1', 'db-2']);
    // the targetPrimary annotation follows, so it cannot switch back to the failed primary
    expect(s.fn('patchNamespacedCustomObject').mock.calls[0][0].body).toEqual({
      metadata: { annotations: { [TARGET_PRIMARY_ANNOTATION]: 'db-2' } },
    });
    expect(s.status().switchover).toMatchObject({ kind: 'failover', target: 'db-2', phase: 'Promoting' });
  });

  it('keeps replicas that applied as much as the elected one', async () => {
    const electing: SwitchoverStatus = { kind: 'failover', target: '', from: 'db-0', phase: 'Electing' };
    const s = setup({
      job: done('Complete'),
      jobPods: [electionPod('target=db-1.db-headless sequence=41 positions=db-1.db-headless:41,db-2.db-headless:41')],
    });
    await s.controller.reconcile(makeCluster({ switchover: electing }));
    expect(s.cmData().reseed).toBe('db-0 u0\n');
  });

  it('cancels the election when the primary recovers', async () => {
    const electing: SwitchoverStatus = { kind: 'failover', target: '', from: 'db-0', phase: 'Electing' };
    const s = setup({ pods: [pod('db-0', 'u0'), pod('db-1', 'u1'), pod('db-2', 'u2')] });
    await s.controller.reconcile(makeCluster({ switchover: electing }));
    expect(s.status().switchover).toMatchObject({ phase: 'Failed', message: expect.stringContaining('recovered') });
    expect(s.fn('patchNamespacedLease')).not.toHaveBeenCalled();
  });

  it('completes once the promoted replica is ready, without waiting for the old primary', async () => {
    const promoting: SwitchoverStatus = {
      kind: 'failover', target: 'db-2', from: 'db-0', phase: 'Promoting', targetToken: 'u2', reseed: { 'db-0': 'u0' },
    };
    // the old primary's node is gone: no pod
    const s = setup({ pods: [pod('db-1', 'u1'), pod('db-2', 'new2')] });
    await s.controller.reconcile(makeCluster({ switchover: promoting }));
    expect(s.status().switchover).toMatchObject({ phase: 'Completed' });
  });

  it('parses the election result and validates the delay', () => {
    expect(parseElection('target=db-1.db-headless sequence=7 positions=db-1.db-headless:7,db-2.db-headless:5')).toEqual({
      target: 'db-1.db-headless',
      sequence: 7,
      positions: { 'db-1.db-headless': 7, 'db-2.db-headless': 5 },
    });
    expect(parseElection('garbage')).toBeUndefined();
    expect(() => validateClusterSpec(makeCluster(undefined, { enabled: true, delaySeconds: 0 }))).toThrow(/delaySeconds/);
    const job = buildFailoverJob(makeCluster(), ['db-1']);
    expect(job.spec?.template.spec?.containers[0].command).toEqual(['perl', '/etc/firebird-operator/failover.pl']);
  });
});

describe('primary isolation check', () => {
  const answering = (isolation: string) =>
    vi.fn().mockImplementation((_host: string, _port: number, line: string) =>
      Promise.resolve(line.endsWith(' ISOLATION') ? [isolation] : line.endsWith(' REJOIN') ? ['OK'] : ['ERR bad request']),
    );

  it('brings a primary that fenced itself back online while it still holds the Lease', async () => {
    const segment = answering('OK fenced 1700000000');
    const s = setup({ segment });
    await s.controller.reconcile(makeCluster({ primaryNotReadySince: longAgo }));
    const lines = segment.mock.calls.map((c) => [c[0], c[2]]);
    expect(lines).toContainEqual(['db-0.db-headless.default.svc', 'masterkey ISOLATION']);
    expect(lines).toContainEqual(['db-0.db-headless.default.svc', 'masterkey REJOIN']);
    // no failover: nothing was promoted while it was isolated
    expect(s.created().some((j) => j.metadata?.name === 'db-failover')).toBe(false);
  });

  it('fails over a primary that is unavailable but not fenced by the isolation check', async () => {
    const segment = answering('OK online');
    const s = setup({ segment });
    await s.controller.reconcile(makeCluster({ primaryNotReadySince: longAgo }));
    expect(segment.mock.calls.map((c) => c[2])).not.toContain('masterkey REJOIN');
    expect(s.created().some((j) => j.metadata?.name === 'db-failover')).toBe(true);
  });

  it('never rejoins during a failover, nor with the check disabled', async () => {
    const segment = answering('OK fenced 1700000000');
    const electing: SwitchoverStatus = { kind: 'failover', target: '', from: 'db-0', phase: 'Electing', startTime: longAgo };
    const s = setup({ segment, job: {} });
    await s.controller.reconcile(makeCluster({ primaryNotReadySince: longAgo, switchover: electing }));
    expect(segment).not.toHaveBeenCalled();

    const off = setup({ segment: answering('OK fenced 1700000000') });
    await off.controller.reconcile(
      makeCluster({ primaryNotReadySince: longAgo }, { enabled: true, delaySeconds: 30, isolationCheck: { enabled: false } }),
    );
    expect(off.segment).not.toHaveBeenCalled();
    expect(off.created().some((j) => j.metadata?.name === 'db-failover')).toBe(true);
  });

  it('waits at least until an isolated primary has fenced itself before failing over', async () => {
    const cluster = makeCluster(
      { primaryNotReadySince: new Date(Date.now() - 65_000).toISOString() },
      { enabled: true, delaySeconds: 30, isolationCheck: { timeoutSeconds: 60 } },
    );
    expect(effectiveFailoverDelaySeconds(cluster)).toBe(70);
    const s = setup();
    await s.controller.reconcile(cluster);
    expect(s.created().some((j) => j.metadata?.name === 'db-failover')).toBe(false);
    // defaults: a 20 s timeout fits in the 30 s delay
    expect(effectiveFailoverDelaySeconds(makeCluster())).toBe(30);
    expect(effectiveFailoverDelaySeconds(makeCluster(undefined, { enabled: true, isolationCheck: { enabled: false }, delaySeconds: 5 }))).toBe(5);
  });

  it('configures the check on the segment server with automatic failover only', () => {
    const env = (failover: object) =>
      buildReplicationContainers(makeCluster(undefined, failover), {
        image: 'fb',
        databasePath: '/var/lib/firebird/data/db.fdb',
        dataDir: '/var/lib/firebird/data',
        credentials: [],
      }).sidecars[0].env!;
    const names = (failover: object) => env(failover).map((e) => e.name);
    expect(env({ enabled: true })).toEqual(
      expect.arrayContaining([
        { name: 'ISOLATION_TIMEOUT_SECONDS', value: '20' },
        { name: 'POD_IP', valueFrom: { fieldRef: { fieldPath: 'status.podIP' } } },
        { name: 'PEERS_SERVICE', value: 'db-headless' },
      ]),
    );
    expect(env({ enabled: true, isolationCheck: { timeoutSeconds: 45 } })).toContainEqual({ name: 'ISOLATION_TIMEOUT_SECONDS', value: '45' });
    expect(names({ enabled: false })).not.toContain('ISOLATION_TIMEOUT_SECONDS');
    expect(names({ enabled: true, isolationCheck: { enabled: false } })).not.toContain('POD_IP');
  });

  it('validates the timeout', () => {
    const spec = (timeoutSeconds: number) => makeCluster(undefined, { enabled: true, isolationCheck: { timeoutSeconds } });
    expect(() => validateClusterSpec(spec(4))).toThrow(/isolationCheck.timeoutSeconds/);
    expect(() => validateClusterSpec(spec(5))).not.toThrow();
  });
});

describe('synchronous replication and failover, switchover, re-seeding', () => {
  const syncCluster = (status: FirebirdCluster['status'], annotations: Record<string, string> = {}): FirebirdCluster => ({
    ...makeCluster(status),
    metadata: { name: 'db', namespace: 'default', uid: 'c', annotations },
    spec: {
      instances: 3,
      storage: { size: '1Gi' },
      replication: { enabled: true, mode: 'sync', failover: { enabled: true, delaySeconds: 30, isolationCheck: { enabled: false } } },
    },
  });
  const attached = { standby: 'db-1', primary: 'db-0', phase: 'Attached' as const, time: longAgo };

  it('promotes the attached standby without an election, and re-seeds the other instances', async () => {
    const s = setup();
    await s.controller.reconcile(syncCluster({ primaryNotReadySince: longAgo, synchronous: attached }));
    expect(s.created().some((j) => j.metadata?.name === 'db-failover')).toBe(false);
    const promoting = s.fn('patchNamespacedCustomObjectStatus').mock.calls
      .map((c) => c[0].body[0])
      .find((op) => op.path === '/status/switchover')?.value;
    expect(promoting).toMatchObject({ kind: 'failover', from: 'db-0', target: 'db-1', phase: 'Promoting', targetToken: 'u1' });
    expect(Object.keys(promoting.reseed).sort()).toEqual(['db-0', 'db-2']);
    expect(promoting.message).toContain('no transaction lost');
  });

  it('does not fail over while a sync-standby Job holds the primary in full shutdown', async () => {
    const attaching = { standby: 'db-1', primary: 'db-0', phase: 'Attaching' as const, time: new Date(Date.now() - 60_000).toISOString() };
    const s = setup({ job: {} });
    await s.controller.reconcile(syncCluster({ primaryNotReadySince: longAgo, synchronous: attaching }));
    expect(s.created().some((j) => j.metadata?.name === 'db-failover')).toBe(false);
    expect(s.status().primaryNotReadySince).toBeUndefined();
    // a Job that has held it for longer than the grace no longer prevents a failover
    const stuck = setup({ job: {} });
    await stuck.controller.reconcile(
      syncCluster({ primaryNotReadySince: longAgo, synchronous: { ...attaching, time: new Date(Date.now() - 400_000).toISOString() } }),
    );
    expect(stuck.created().some((j) => j.metadata?.name === 'db-failover')).toBe(true);
  });

  it('with several standbys, promotes the lowest-ordinal ready one', async () => {
    const both = { standby: 'db-2', primary: 'db-0', standbys: ['db-1', 'db-2'], phase: 'Attached' as const, time: longAgo };
    const promotion = async (pods: object[], synchronous: object) => {
      const s = setup({ pods });
      await s.controller.reconcile(syncCluster({ primaryNotReadySince: longAgo, synchronous } as never));
      return s.fn('patchNamespacedCustomObjectStatus').mock.calls.map((c) => c[0].body[0]).find((op) => op.path === '/status/switchover')?.value;
    };
    expect(await promotion([pod('db-0', 'u0', false), pod('db-1', 'u1'), pod('db-2', 'u2')], both)).toMatchObject({ target: 'db-1' });
    // db-1 not ready: db-2, which has every committed transaction too
    expect(await promotion([pod('db-0', 'u0', false), pod('db-1', 'u1', false), pod('db-2', 'u2')], both)).toMatchObject({ target: 'db-2' });
    // not the one being detached
    const p = await promotion([pod('db-0', 'u0', false), pod('db-1', 'u1', false), pod('db-2', 'u2')], { ...both, phase: 'Detaching' });
    expect(p?.target ?? '').not.toBe('db-2');
  });

  it('on Firebird 4 elects instead of promoting the standby (it may lack commits)', async () => {
    const segment = vi.fn().mockImplementation(async (_h: string, _p: number, line: string) => (line.endsWith(' VERSION') ? ['OK 4.0.7'] : []));
    const ready = [pod('db-0', 'u0'), pod('db-1', 'u1'), pod('db-2', 'u2')];
    const s = setup({ pods: ready, segment });
    // learnt while the primary is ready
    await s.controller.reconcile(syncCluster({ synchronous: attached }));
    expect(segment).toHaveBeenCalledWith(expect.stringContaining('db-0.db-headless'), 3051, expect.stringMatching(/ VERSION$/));
    s.fn('listNamespacedPod').mockImplementation(({ labelSelector }: { labelSelector: string }) =>
      Promise.resolve({ items: labelSelector.startsWith('job-name=') ? [] : [pod('db-0', 'u0', false), pod('db-1', 'u1'), pod('db-2', 'u2')] }),
    );
    await s.controller.reconcile(syncCluster({ primaryNotReadySince: longAgo, synchronous: attached }));
    expect(s.created().some((j) => j.metadata?.name === 'db-failover')).toBe(true);
  });

  it('elects as before when the standby is not ready', async () => {
    const s = setup({ pods: [pod('db-0', 'u0', false), pod('db-1', 'u1', false), pod('db-2', 'u2')] });
    await s.controller.reconcile(syncCluster({ primaryNotReadySince: longAgo, synchronous: attached }));
    const job = s.created().find((j) => j.metadata?.name === 'db-failover')!;
    expect(job.spec!.template.spec!.containers[0].env!.find((e) => e.name === 'CANDIDATES')?.value).toBe('db-2.db-headless');
  });

  it('detaches the standby before a planned switchover', async () => {
    const s = setup({ pods: [pod('db-0', 'u0'), pod('db-1', 'u1'), pod('db-2', 'u2')] });
    await s.controller.reconcile(syncCluster({ synchronous: attached }, { [TARGET_PRIMARY_ANNOTATION]: 'db-2' }));
    expect(s.created().some((j) => j.metadata?.name === 'db-switchover')).toBe(false);
    const job = s.created().find((j) => j.metadata?.name === 'db-sync-standby')!;
    expect(job.metadata?.annotations).toMatchObject({ 'firebird.cloudnative-firebird.io/sync-action': 'detach' });
  });

  it('holds a re-seed of the standby until it is detached', async () => {
    const annotated = { ...pod('db-1', 'u1'), metadata: { name: 'db-1', uid: 'u1', annotations: { 'firebird.cloudnative-firebird.io/reseed': 'true' } } };
    const s = setup({ pods: [pod('db-0', 'u0'), annotated, pod('db-2', 'u2')] });
    await s.controller.reconcile(syncCluster({ synchronous: attached }));
    expect(s.fn('deleteNamespacedPod').mock.calls.map((c) => c[0].name)).not.toContain('db-1');
    const job = s.created().find((j) => j.metadata?.name === 'db-sync-standby')!;
    expect(job.metadata?.annotations).toMatchObject({ 'firebird.cloudnative-firebird.io/sync-action': 'detach' });
    // once detached, the re-seed goes ahead
    const detached = setup({ pods: [pod('db-0', 'u0'), annotated, pod('db-2', 'u2')] });
    await detached.controller.reconcile(syncCluster({ synchronous: { ...attached, phase: 'Detached' } }));
    expect(detached.fn('deleteNamespacedPod').mock.calls.map((c) => c[0].name)).toContain('db-1');
  });
});

describe('status written by the operator', () => {
  type Schema = { properties?: Record<string, Schema>; items?: Schema; additionalProperties?: unknown; 'x-kubernetes-preserve-unknown-fields'?: boolean };
  const crd = loadYaml(readFileSync(join(__dirname, '..', '..', 'config', 'crds', 'firebirdcluster.yaml'), 'utf8')) as {
    spec: { versions: Array<{ schema: { openAPIV3Schema: Schema } }> };
  };
  const statusSchema = crd.spec.versions[0].schema.openAPIV3Schema.properties!.status;
  /** Paths in value the schema does not declare: the API server would prune them */
  const undeclared = (value: unknown, schema: Schema, path: string): string[] => {
    if (Array.isArray(value)) return schema.items ? value.flatMap((v, i) => undeclared(v, schema.items!, `${path}[${i}]`)) : [];
    if (value === null || typeof value !== 'object') return [];
    if (schema['x-kubernetes-preserve-unknown-fields'] || (schema.additionalProperties && !schema.properties)) return [];
    return Object.entries(value).flatMap(([k, v]) =>
      schema.properties?.[k] ? undeclared(v, schema.properties[k], `${path}.${k}`) : [`${path}.${k}`],
    );
  };

  it('declares every status field in the CRD (undeclared ones are pruned)', async () => {
    const s = setup({ pods: [pod('db-0', 'u0'), pod('db-1', 'u1'), pod('db-2', 'u2')] });
    const cluster: FirebirdCluster = {
      ...makeCluster({
        synchronous: { standby: 'db-1', primary: 'db-0', phase: 'Attached', time: longAgo, unavailableSince: longAgo, retryAfter: longAgo },
      }),
      spec: { instances: 3, storage: { size: '1Gi' }, replication: { enabled: true, mode: 'sync', failover: { enabled: true } } },
    };
    await s.controller.reconcile(cluster);
    const written = s.status();
    expect(written.synchronous).toBeDefined();
    expect(undeclared(written, statusSchema, 'status')).toEqual([]);
  });
});
