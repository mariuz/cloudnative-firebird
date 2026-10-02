import { describe, it, expect, vi, Mock } from 'vitest';
import { KubeConfig, V1Job } from '@kubernetes/client-node';
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

function setup(opts: { pods?: object[]; job?: V1Job; jobPods?: object[]; segment?: Mock } = {}) {
  const notFound = Object.assign(new Error('Not Found'), { code: 404 });
  const api: Record<string, Mock> = {
    listNamespacedPod: vi.fn().mockImplementation(({ labelSelector }: { labelSelector: string }) =>
      Promise.resolve({
        items: labelSelector.startsWith('job-name=')
          ? (opts.jobPods ?? [])
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
