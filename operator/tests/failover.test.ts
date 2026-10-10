import { describe, it, expect, vi, Mock } from 'vitest';
import { KubeConfig, V1Job, loadYaml } from '@kubernetes/client-node';
import { readFileSync } from 'fs';
import { join } from 'path';
import { FirebirdClusterController } from '../src/controllers/firebirdcluster.controller';
import {
  buildFailoverJob,
  effectiveFailoverDelaySeconds,
  parseElection,
  primaryCutOff,
  TARGET_PRIMARY_ANNOTATION,
} from '../src/utils/switchover';
import { buildReplicationContainers } from '../src/utils/replication';
import { validateClusterSpec } from '../src/utils/validation';
import { FirebirdCluster, SwitchoverStatus } from '../src/types';
import { metrics } from '../src/utils/metrics';

const makeCluster = (status?: FirebirdCluster['status'], failover: object = { enabled: true, delaySeconds: 30 }): FirebirdCluster => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdCluster',
  // as the controller's first reconcile recorded it for a cluster that existed before v0.87.0
  metadata: { name: 'db', namespace: 'default', uid: 'c', annotations: { 'firebird.cloudnative-firebird.io/primary-lease': 'pinned' } },
  spec: { instances: 3, storage: { size: '1Gi' }, segmentTLS: { enabled: false }, replication: { enabled: true, failover } },
  ...(status ? { status } : {}),
});

const pod = (name: string, uid: string, ready = true) => ({
  metadata: { name, uid },
  status: { phase: 'Running', conditions: [{ type: 'Ready', status: ready ? 'True' : 'False' }] },
});

const longAgo = new Date(Date.now() - 120_000).toISOString();

function setup(opts: { pods?: object[]; job?: V1Job; jobs?: Record<string, V1Job>; jobPods?: object[]; archivePods?: object[]; segment?: Mock; lease?: object } = {}) {
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
    readNamespacedLease: vi.fn().mockResolvedValue(opts.lease ?? { spec: { holderIdentity: 'db-0' } }),
    readNamespacedJob: opts.jobs
      ? vi.fn().mockImplementation(({ name }: { name: string }) => (opts.jobs![name] ? Promise.resolve(opts.jobs![name]) : Promise.reject(notFound)))
      : opts.job
        ? vi.fn().mockResolvedValue(opts.job)
        : vi.fn().mockRejectedValue(notFound),
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
      jobs: { 'db-failover': done('Complete') },
      jobPods: [electionPod('target=db-2.db-headless sequence=41 positions=db-1.db-headless:40,db-2.db-headless:41')],
    });
    await s.controller.reconcile(makeCluster({ switchover: electing }));

    expect(s.fn('patchNamespacedLease').mock.calls[0][0].body[0].value).toBe('db-2');
    const data = s.cmData();
    expect(data.primary).toBe('db-2.db-headless');
    expect(data.promote).toBe('db-2 u2\n');
    expect(data.demote).toBe('');
    expect(data.reseed).toBe('db-0 u0\ndb-1 u1\n');
    // the elected replica keeps running: a Job promotes it in place once the primary moved
    expect(s.fn('deleteNamespacedPod').mock.calls.map((c) => c[0].name).sort()).toEqual(['db-0', 'db-1']);
    const promote = s.created().find((j) => j.metadata?.name === 'db-promote')!;
    expect(promote.spec!.template.spec!.containers[0].command![2]).toContain('segment-request.pl "$TARGET" PROMOTE none');
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

  it('keeps the target running when the promote Job promoted it in place, and restarts it otherwise', async () => {
    const promoting: SwitchoverStatus = {
      kind: 'failover', target: 'db-2', from: 'db-0', phase: 'Promoting', targetToken: 'u2', reseed: { 'db-0': 'u0' },
    };
    const promoteJob = (reply: string, type: 'Complete' | 'Failed' = 'Complete'): V1Job => ({
      metadata: { annotations: { [TARGET_PRIMARY_ANNOTATION]: 'db-2' } },
      ...done(type),
    });
    const promotePod = (message: string) => ({
      metadata: { name: 'db-promote-x' },
      status: { containerStatuses: [{ state: { terminated: { exitCode: 0, message } } }] },
    });
    const pods = [pod('db-0', 'new0', false), pod('db-1', 'u1'), pod('db-2', 'u2')];

    const promoted = setup({ pods, job: promoteJob('OK 41'), jobPods: [promotePod('OK 41\n')] });
    await promoted.controller.reconcile(makeCluster({ switchover: promoting }));
    expect(promoted.status().switchover).toMatchObject({ phase: 'Promoting', promotedInPlace: true, message: expect.stringContaining('in place (OK 41)') });
    expect(promoted.fn('deleteNamespacedPod').mock.calls.map((c) => c[0].name)).not.toContain('db-2');
    // then complete as soon as the target is ready, with its pod
    const done2 = setup({ pods });
    await done2.controller.reconcile(makeCluster({ switchover: { ...promoting, promotedInPlace: true } }));
    expect(done2.status().switchover).toMatchObject({ phase: 'Completed' });

    const refused = setup({ pods, job: promoteJob('ERR'), jobPods: [promotePod('ERR this instance is a synchronous standby\n')] });
    await refused.controller.reconcile(makeCluster({ switchover: promoting }));
    expect(refused.status().switchover).toMatchObject({ promotedInPlace: false, message: expect.stringContaining('restarting it') });
    expect(refused.fn('deleteNamespacedPod').mock.calls.map((c) => c[0].name)).toContain('db-2');
    expect(refused.cmData().promote).toBe('db-2 u2\n');
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

describe('the primary Lease as a promotion mutex', () => {
  const withLease = { enabled: true, delaySeconds: 30, primaryLease: { enabled: true } };
  const electing: SwitchoverStatus = { kind: 'failover', target: '', from: 'db-0', phase: 'Electing' };
  const elected = () => ({
    jobs: { 'db-failover': done('Complete') },
    jobPods: [electionPod('target=db-2.db-headless sequence=41 positions=db-1.db-headless:40,db-2.db-headless:41')],
  });
  const renewed = (secondsAgo: number) => ({
    spec: { holderIdentity: 'db-0', leaseDurationSeconds: 15, renewTime: new Date(Date.now() - secondsAgo * 1000).toISOString() },
  });

  it('does not promote the elected replica while the old primary still renews its Lease', async () => {
    const s = setup({ ...elected(), lease: renewed(3) });
    await s.controller.reconcile(makeCluster({ switchover: electing }, withLease));
    expect(s.fn('patchNamespacedLease')).not.toHaveBeenCalled();
    expect(s.created().some((j) => j.metadata?.name === 'db-promote')).toBe(false);
    expect(s.status().switchover).toMatchObject({ phase: 'Electing', message: expect.stringMatching(/waiting for the Lease of db-0 to expire/) });
    expect(s.fn('createNamespacedEvent').mock.calls.map((c) => c[0].body.reason)).toContain('PrimaryLeaseHeld');
  });

  it('promotes once the Lease expired, taking the Lease over with the version it read, or without the primary Lease', async () => {
    const s = setup({ ...elected(), lease: { metadata: { resourceVersion: '41' }, ...renewed(16) } });
    await s.controller.reconcile(makeCluster({ switchover: electing }, withLease));
    const taken = s.fn('replaceNamespacedLease').mock.calls[0][0].body;
    expect(taken.metadata.resourceVersion).toBe('41');
    expect(taken.spec.holderIdentity).toBe('db-2');
    expect(s.status().switchover).toMatchObject({ kind: 'failover', target: 'db-2', phase: 'Promoting' });
    // the old primary renewed it between the read and the take-over (a conflict): looked at again
    const conflict = setup({ ...elected(), lease: renewed(16) });
    conflict.fn('replaceNamespacedLease').mockRejectedValue(Object.assign(new Error('Conflict'), { code: 409 }));
    await conflict.controller.reconcile(makeCluster({ switchover: electing }, withLease));
    expect(conflict.status().switchover).toMatchObject({ phase: 'Electing' });
    // without it a fresh renewal time (the operator's, when it created the Lease) never waits
    const off = setup({ ...elected(), lease: renewed(3) });
    await off.controller.reconcile(makeCluster({ switchover: electing }));
    expect(off.status().switchover).toMatchObject({ phase: 'Promoting' });
  });

  it('leaves a primary fenced over its Lease to its holder, and still lifts the isolation check\'s fences', async () => {
    const answering = (isolation: string) =>
      vi.fn().mockImplementation((_host: string, _port: number, line: string) =>
        Promise.resolve(line.endsWith(' ISOLATION') ? [isolation] : line.endsWith(' REJOIN') ? ['OK'] : ['ERR bad request']),
      );
    const s = setup({ segment: answering('OK fenced 1700000000 lease'), lease: renewed(60) });
    await s.controller.reconcile(makeCluster({ primaryNotReadySince: longAgo }, withLease));
    expect(s.segment.mock.calls.map((c) => c[2])).not.toContain('masterkey REJOIN');
    // it fails over instead
    expect(s.created().some((j) => j.metadata?.name === 'db-failover')).toBe(true);
    // the isolation check's fence (nothing reached the primary): rejoined as before, no failover
    const isolated = setup({ segment: answering('OK fenced 1700000000'), lease: renewed(60) });
    await isolated.controller.reconcile(makeCluster({ primaryNotReadySince: longAgo }, withLease));
    expect(isolated.segment.mock.calls.map((c) => c[2])).toContain('masterkey REJOIN');
    expect(isolated.created().some((j) => j.metadata?.name === 'db-failover')).toBe(false);
  });

  it('manages the Role and RoleBinding that let the instances renew the Lease', async () => {
    const s = setup();
    await s.controller.reconcile(makeCluster(undefined, withLease));
    const role = s.fn('createNamespacedRole').mock.calls[0][0].body;
    expect(role.metadata.name).toBe('db-primary-lease');
    expect(role.rules).toEqual([{ apiGroups: ['coordination.k8s.io'], resources: ['leases'], resourceNames: ['db-lease'], verbs: ['get', 'update', 'patch'] }]);
    const binding = s.fn('createNamespacedRoleBinding').mock.calls[0][0].body;
    expect(binding.subjects).toEqual([{ kind: 'ServiceAccount', name: 'default', namespace: 'default' }]);
    expect(binding.roleRef).toMatchObject({ kind: 'Role', name: 'db-primary-lease' });
    // removed again once the primary Lease is off and the Role still exists
    const off = setup();
    off.fn('readNamespacedRole').mockResolvedValue({ metadata: { name: 'db-primary-lease' } });
    await off.controller.reconcile(makeCluster());
    expect(off.fn('deleteNamespacedRole')).toHaveBeenCalled();
    expect(off.fn('deleteNamespacedRoleBinding')).toHaveBeenCalled();
    const never = setup();
    await never.controller.reconcile(makeCluster());
    expect(never.fn('deleteNamespacedRole')).not.toHaveBeenCalled();
  });

  it('exposes the age of the Lease', async () => {
    const s = setup({ lease: renewed(4) });
    await s.controller.reconcile(makeCluster(undefined, withLease));
    expect(metrics.render()).toMatch(/firebird_cluster_primary_lease_age_seconds\{namespace="default",cluster="db"\} 4/);
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
    const contact = (contactTimeoutSeconds: number) =>
      makeCluster(undefined, { enabled: true, isolationCheck: { contactTimeoutSeconds } });
    expect(() => validateClusterSpec(contact(44))).toThrow(/contactTimeoutSeconds/);
    expect(() => validateClusterSpec(contact(45))).not.toThrow();
  });

  it('also fences a primary nothing reaches any more, unless fenceWhenUnreached is off', () => {
    const env = (failover: object) =>
      buildReplicationContainers(makeCluster(undefined, failover), {
        image: 'fb',
        databasePath: '/var/lib/firebird/data/db.fdb',
        dataDir: '/var/lib/firebird/data',
        credentials: [],
      }).sidecars[0].env!;
    expect(env({ enabled: true })).toContainEqual({ name: 'CONTACT_TIMEOUT_SECONDS', value: '60' });
    expect(env({ enabled: true, isolationCheck: { contactTimeoutSeconds: 120 } })).toContainEqual({
      name: 'CONTACT_TIMEOUT_SECONDS',
      value: '120',
    });
    const names = (failover: object) => env(failover).map((e) => e.name);
    expect(names({ enabled: true, isolationCheck: { fenceWhenUnreached: false } })).not.toContain('CONTACT_TIMEOUT_SECONDS');
    expect(names({ enabled: true, isolationCheck: { enabled: false } })).not.toContain('CONTACT_TIMEOUT_SECONDS');
    expect(names({ enabled: false })).not.toContain('CONTACT_TIMEOUT_SECONDS');
  });

  it('waits longer before failing over a cut-off primary, until it has fenced itself', () => {
    // nothing reached it for 60s by then: every replica lost it 30s in, at most one 5s pull after
    // it was last reached, plus the margin for the fence
    expect(effectiveFailoverDelaySeconds(makeCluster(), true)).toBe(45);
    expect(effectiveFailoverDelaySeconds(makeCluster(), false)).toBe(30);
    expect(effectiveFailoverDelaySeconds(makeCluster(undefined, { enabled: true, isolationCheck: { contactTimeoutSeconds: 120 } }), true)).toBe(105);
    expect(effectiveFailoverDelaySeconds(makeCluster(undefined, { enabled: true, isolationCheck: { fenceWhenUnreached: false } }), true)).toBe(30);
    expect(effectiveFailoverDelaySeconds(makeCluster(undefined, { enabled: true, delaySeconds: 90 }), true)).toBe(90);
  });
});

describe('failover of a cut-off primary', () => {
  const allReady = () => [pod('db-0', 'u0'), pod('db-1', 'u1'), pod('db-2', 'u2')];
  // the primary's segment server is unreachable; replicas answer PRIMARYSEEN as given
  const seen = (replies: Record<string, string>) =>
    vi.fn().mockImplementation(async (host: string, _p: number, line: string) => {
      const pod = host.split('.')[0];
      if (line.endsWith(' PRIMARYSEEN') && replies[pod] !== undefined) return [replies[pod]];
      throw new Error('unreachable');
    });

  it('decides from what the replicas last saw of the primary', () => {
    const cut = (operatorReached: boolean, ...replies: (string | undefined)[]) =>
      primaryCutOff({ primaryHost: 'db-0.db-headless', operatorReached, replicas: replies.map((reply, i) => ({ pod: `db-${i + 1}`, reply })) });
    expect(cut(false, 'OK 45 db-0.db-headless', 'OK never')).toMatch(/cut off.*db-1 45s ago, db-2/);
    expect(cut(false, 'OK 45 db-0.db-headless', undefined)).toBeDefined();
    // a contact with another primary does not count as reaching this one
    expect(cut(false, 'OK 2 db-1.db-headless')).toBeDefined();
    expect(cut(true, 'OK 45 db-0.db-headless')).toBeUndefined();
    expect(cut(false, 'OK 45 db-0.db-headless', 'OK 3 db-0.db-headless')).toBeUndefined();
    expect(cut(false, undefined, undefined)).toBeUndefined();
    expect(cut(false, 'ERR bad request')).toBeUndefined();
    expect(cut(false, 'OK 29 db-0.db-headless')).toBeUndefined();
  });

  it('fails over a ready primary that neither the operator nor any replica reaches', async () => {
    const s = setup({ pods: allReady(), segment: seen({ 'db-1': 'OK 45 db-0.db-headless', 'db-2': 'OK never' }) });
    await s.controller.reconcile(makeCluster());
    expect(s.status().primaryNotReadySince).toBeDefined();
    const events = s.fn('createNamespacedEvent').mock.calls.map((c) => c[0].body.message as string);
    expect(events.some((m) => m.includes('primary db-0 ready but cut off'))).toBe(true);

    const later = setup({ pods: allReady(), segment: seen({ 'db-1': 'OK 45 db-0.db-headless', 'db-2': 'OK 60 db-0.db-headless' }) });
    await later.controller.reconcile(makeCluster({ primaryNotReadySince: longAgo }));
    const job = later.created().find((j) => j.metadata?.name === 'db-failover')!;
    const env = Object.fromEntries(job.spec!.template.spec!.containers[0].env!.map((e) => [e.name, e.value]));
    expect(env.CANDIDATES).toBe('db-1.db-headless db-2.db-headless');
  });

  it('fails a cut-off primary over only once it has fenced itself (45s by default)', async () => {
    const lost = () => seen({ 'db-1': 'OK 45 db-0.db-headless', 'db-2': 'OK 60 db-0.db-headless' });
    const fortySecondsAgo = new Date(Date.now() - 40_000).toISOString();
    const early = setup({ pods: allReady(), segment: lost() });
    await early.controller.reconcile(makeCluster({ primaryNotReadySince: fortySecondsAgo }));
    expect(early.created().some((j) => j.metadata?.name === 'db-failover')).toBe(false);
    expect(early.status().primaryNotReadySince).toBe(fortySecondsAgo);

    const first = setup({ pods: allReady(), segment: lost() });
    await first.controller.reconcile(makeCluster());
    const events = first.fn('createNamespacedEvent').mock.calls.map((c) => c[0].body.message as string);
    expect(events.some((m) => m.includes('ready but cut off') && m.includes('failover in 45s'))).toBe(true);

    // without the fence of a primary nothing reaches, the failover delay applies as before
    const off = setup({ pods: allReady(), segment: lost() });
    await off.controller.reconcile(
      makeCluster({ primaryNotReadySince: fortySecondsAgo }, { enabled: true, delaySeconds: 30, isolationCheck: { fenceWhenUnreached: false } }),
    );
    expect(off.created().some((j) => j.metadata?.name === 'db-failover')).toBe(true);
  });

  it('keeps the primary while one replica still reaches it, or the operator does', async () => {
    const fresh = setup({ pods: allReady(), segment: seen({ 'db-1': 'OK 45 db-0.db-headless', 'db-2': 'OK 2 db-0.db-headless' }) });
    await fresh.controller.reconcile(makeCluster({ primaryNotReadySince: longAgo }));
    expect(fresh.status().primaryNotReadySince).toBeUndefined();
    expect(fresh.created().some((j) => j.metadata?.name === 'db-failover')).toBe(false);

    const reached = vi.fn().mockImplementation(async (_h: string, _p: number, line: string) =>
      line.endsWith(' ISOLATION') ? ['OK online'] : ['OK never'],
    );
    const s = setup({ pods: allReady(), segment: reached });
    await s.controller.reconcile(makeCluster({ primaryNotReadySince: longAgo }));
    expect(s.created().some((j) => j.metadata?.name === 'db-failover')).toBe(false);
    expect(reached.mock.calls.map((c) => c[2])).not.toContain('masterkey PRIMARYSEEN');

    // not with automatic failover off
    const off = setup({ pods: allReady(), segment: seen({ 'db-1': 'OK never', 'db-2': 'OK never' }) });
    await off.controller.reconcile(makeCluster({ primaryNotReadySince: longAgo }, { enabled: false }));
    expect(off.segment.mock.calls.map((c) => c[2])).not.toContain('masterkey PRIMARYSEEN');
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
    // then promoted in place too (its position is the last segment it saw archived)
    expect(promoting.promotedInPlace).toBeUndefined();
    const next = setup();
    await next.controller.reconcile(syncCluster({ switchover: promoting, synchronous: attached }));
    expect(next.created().some((j) => j.metadata?.name === 'db-promote')).toBe(true);
    expect(next.fn('deleteNamespacedPod').mock.calls.map((c) => c[0].name)).not.toContain('db-1');
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
