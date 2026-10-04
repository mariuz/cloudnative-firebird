import { describe, it, expect, vi, Mock } from 'vitest';
import { CoreV1Api, KubeConfig, V1Job, V1Pod } from '@kubernetes/client-node';
import { EVENT_AGGREGATION_MS, EVENT_SOURCE, EventRecorder } from '../src/utils/events';
import { FirebirdClusterController } from '../src/controllers/firebirdcluster.controller';
import { FirebirdBackupController } from '../src/controllers/backup.controller';
import { TARGET_PRIMARY_ANNOTATION } from '../src/utils/switchover';
import { REVISION_LABEL } from '../src/utils/rolling-update';
import { buildStatefulSet } from '../src/utils/resources';
import { FirebirdBackup, FirebirdCluster } from '../src/types';

const object = {
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdCluster',
  metadata: { name: 'db', namespace: 'prod', uid: 'c-uid' },
};

function recorder(start = Date.parse('2026-09-28T10:00:00Z')) {
  let now = start;
  const api = {
    createNamespacedEvent: vi.fn().mockImplementation(({ body }) => Promise.resolve(body)),
    patchNamespacedEvent: vi.fn().mockResolvedValue({}),
  };
  const events = new EventRecorder(api as unknown as CoreV1Api, () => now);
  return { api, events, advance: (ms: number) => (now += ms) };
}

describe('EventRecorder', () => {
  it('creates an event referencing the object', async () => {
    const { api, events } = recorder();
    await events.record(object, 'Warning', 'FailoverStarted', 'primary db-0 unavailable');
    const { namespace, body } = api.createNamespacedEvent.mock.calls[0][0];
    expect(namespace).toBe('prod');
    expect(body).toMatchObject({
      involvedObject: { apiVersion: object.apiVersion, kind: 'FirebirdCluster', name: 'db', namespace: 'prod', uid: 'c-uid' },
      type: 'Warning',
      reason: 'FailoverStarted',
      message: 'primary db-0 unavailable',
      source: { component: EVENT_SOURCE },
      count: 1,
    });
    expect(body.metadata.name).toMatch(/^db\.[0-9a-f]+$/);
  });

  it('aggregates a repeated event into a count, and starts a new one after the aggregation window', async () => {
    const { api, events, advance } = recorder();
    await events.record(object, 'Warning', 'ReconcileFailed', 'boom');
    advance(1000);
    await events.record(object, 'Warning', 'ReconcileFailed', 'boom');
    expect(api.createNamespacedEvent).toHaveBeenCalledTimes(1);
    const name = api.createNamespacedEvent.mock.calls[0][0].body.metadata.name;
    expect(api.patchNamespacedEvent.mock.calls[0][0]).toMatchObject({ name, namespace: 'prod', body: { count: 2 } });

    // a different message is a different event
    await events.record(object, 'Warning', 'ReconcileFailed', 'other');
    expect(api.createNamespacedEvent).toHaveBeenCalledTimes(2);

    advance(EVENT_AGGREGATION_MS);
    await events.record(object, 'Warning', 'ReconcileFailed', 'boom');
    expect(api.createNamespacedEvent).toHaveBeenCalledTimes(3);
  });

  it('creates a new event when the aggregated one is gone, and never throws', async () => {
    const { api, events } = recorder();
    await events.record(object, 'Normal', 'X', 'y');
    api.patchNamespacedEvent.mockRejectedValue(Object.assign(new Error('gone'), { code: 404 }));
    await events.record(object, 'Normal', 'X', 'y');
    expect(api.createNamespacedEvent).toHaveBeenCalledTimes(2);

    api.createNamespacedEvent.mockRejectedValue(Object.assign(new Error('forbidden'), { code: 403 }));
    await expect(events.record(object, 'Normal', 'Z', 'z')).resolves.toBeUndefined();
  });

  it('truncates long messages', async () => {
    const { api, events } = recorder();
    await events.record(object, 'Warning', 'ReconcileFailed', 'x'.repeat(5000));
    expect(api.createNamespacedEvent.mock.calls[0][0].body.message).toHaveLength(1024);
  });
});

const makeCluster = (spec: Partial<FirebirdCluster['spec']> = {}, meta: Partial<FirebirdCluster['metadata']> = {}): FirebirdCluster => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdCluster',
  metadata: { name: 'db', namespace: 'default', uid: 'c', ...meta },
  spec: { instances: 3, storage: { size: '1Gi' }, replication: { enabled: true }, ...spec },
});

const pod = (name: string, opts: { ready?: boolean; revision?: string } = {}): V1Pod => ({
  metadata: { name, uid: `u-${name}`, labels: { [REVISION_LABEL]: opts.revision ?? 'db-new' } },
  status: { phase: 'Running', conditions: [{ type: 'Ready', status: opts.ready === false ? 'False' : 'True' }] },
});

function clusterSetup(pods: V1Pod[], overrides: Record<string, Mock> = {}) {
  const notFound = Object.assign(new Error('Not Found'), { code: 404 });
  const statefulSet = {
    ...buildStatefulSet(makeCluster()),
    metadata: { name: 'db', generation: 1 },
    status: { updateRevision: 'db-new', observedGeneration: 1 },
  };
  const api: Record<string, Mock> = {
    listNamespacedPod: vi.fn().mockResolvedValue({ items: pods }),
    readNamespacedLease: vi.fn().mockResolvedValue({ spec: { holderIdentity: 'db-0' } }),
    readNamespacedConfigMap: vi.fn().mockResolvedValue({ data: {} }),
    readNamespacedStatefulSet: vi.fn().mockResolvedValue(statefulSet),
    patchNamespacedStatefulSet: vi.fn().mockResolvedValue(statefulSet),
    createNamespacedEvent: vi.fn().mockImplementation(({ body }) => Promise.resolve(body)),
    ...overrides,
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
  const events = () =>
    fn('createNamespacedEvent').mock.calls.map((c) => ({
      type: c[0].body.type,
      reason: c[0].body.reason,
      message: c[0].body.message,
      kind: c[0].body.involvedObject.kind,
    }));
  return { controller: new FirebirdClusterController(kubeConfig), fn, events };
}

describe('cluster events', () => {
  it('records the start of a planned switchover', async () => {
    const s = clusterSetup([pod('db-0'), pod('db-1'), pod('db-2')]);
    await s.controller.reconcile(makeCluster({}, { annotations: { [TARGET_PRIMARY_ANNOTATION]: 'db-1' } }));
    expect(s.events()).toContainEqual({
      type: 'Normal',
      reason: 'SwitchoverStarted',
      message: 'stopping writes on the primary',
      kind: 'FirebirdCluster',
    });
  });

  it('warns once that the TLS certificate settings are ignored', async () => {
    const s = clusterSetup([pod('db-0'), pod('db-1'), pod('db-2')]);
    const cluster = makeCluster({ tls: { enabled: true, issuerRef: { name: 'letsencrypt' } } });
    await s.controller.reconcile(cluster);
    await s.controller.reconcile(cluster);
    expect(s.events().filter((e) => e.reason === 'TLSCertificateIgnored')).toEqual([
      expect.objectContaining({ type: 'Warning', message: expect.stringContaining('tls.secretName and tls.issuerRef are ignored') }),
    ]);
    // nothing for cert-manager
    expect(s.fn('createNamespacedCustomObject').mock.calls.filter((c) => c[0].plural === 'certificates')).toEqual([]);
  });

  it('records a refused switchover as a warning', async () => {
    const s = clusterSetup([pod('db-0'), pod('db-1', { ready: false }), pod('db-2')]);
    await s.controller.reconcile(makeCluster({}, { annotations: { [TARGET_PRIMARY_ANNOTATION]: 'db-1' } }));
    expect(s.events()).toContainEqual(expect.objectContaining({ type: 'Warning', reason: 'SwitchoverFailed', message: 'db-1 is not ready' }));
  });

  it('warns when the primary becomes unavailable and when the failover starts', async () => {
    const failover = { enabled: true, delaySeconds: 30 };
    const pods = [pod('db-0', { ready: false }), pod('db-1'), pod('db-2')];
    const first = clusterSetup(pods);
    await first.controller.reconcile(makeCluster({ replication: { enabled: true, failover } }));
    expect(first.events()).toContainEqual(expect.objectContaining({ type: 'Warning', reason: 'PrimaryNotReady' }));

    const later = clusterSetup(pods);
    await later.controller.reconcile({
      ...makeCluster({ replication: { enabled: true, failover } }),
      status: { primaryNotReadySince: new Date(Date.now() - 60_000).toISOString() },
    });
    const reasons = later.events().map((e) => e.reason);
    expect(reasons).toContain('FailoverStarted');
    expect(reasons).not.toContain('PrimaryNotReady'); // only when it starts
  });

  it('records the rolling update steps and its completion', async () => {
    const s = clusterSetup([pod('db-0', { revision: 'db-old' }), pod('db-1', { revision: 'db-old' }), pod('db-2', { revision: 'db-old' })]);
    await s.controller.reconcile(makeCluster());
    expect(s.events()).toContainEqual(
      expect.objectContaining({ type: 'Normal', reason: 'RollingUpdate', message: 'restarting replica db-2 on revision db-new' }),
    );

    const done = clusterSetup([pod('db-0'), pod('db-1'), pod('db-2')]);
    await done.controller.reconcile({
      ...makeCluster(),
      status: { rollingUpdate: { revision: 'db-new', outdatedInstances: ['db-0'], message: '' } },
    });
    expect(done.events()).toContainEqual(expect.objectContaining({ reason: 'RollingUpdateCompleted' }));
  });

  it('records fencing once it is applied', async () => {
    const complete: V1Job = { status: { conditions: [{ type: 'Complete', status: 'True' }] } };
    const s = clusterSetup([pod('db-0'), pod('db-1'), pod('db-2')], {
      readNamespacedJob: vi.fn().mockImplementation(({ name }) =>
        name === 'db-1-fencing'
          ? Promise.resolve({ ...complete, metadata: { labels: { 'firebird.cloudnative-firebird.io/fencing-action': 'fence' } } })
          : Promise.reject(Object.assign(new Error('nf'), { code: 404 })),
      ),
    });
    await s.controller.reconcile(
      makeCluster({}, { annotations: { 'firebird.cloudnative-firebird.io/fencedInstances': '["db-1"]' } }),
    );
    expect(s.events()).toContainEqual(expect.objectContaining({ type: 'Normal', reason: 'InstanceFenced' }));
  });

  it('records a failed reconcile as a warning', async () => {
    const s = clusterSetup([pod('db-0')], {
      readNamespacedStatefulSet: vi.fn().mockRejectedValue(Object.assign(new Error('nf'), { code: 404 })),
      createNamespacedStatefulSet: vi.fn().mockRejectedValue(new Error('admission webhook denied the request')),
    });
    await expect(s.controller.reconcile(makeCluster())).rejects.toThrow();
    expect(s.events()).toContainEqual(
      expect.objectContaining({ type: 'Warning', reason: 'ReconcileFailed', message: 'admission webhook denied the request' }),
    );
  });
});

describe('backup events', () => {
  it('records the start and the completion of a backup on the FirebirdBackup', async () => {
    const notFound = Object.assign(new Error('Not Found'), { code: 404 });
    const cluster = makeCluster({ replication: undefined });
    const api: Record<string, Mock> = {
      getNamespacedCustomObject: vi.fn().mockResolvedValue(cluster),
      readNamespacedLease: vi.fn().mockResolvedValue({ spec: { holderIdentity: 'db-0' } }),
      readNamespacedJob: vi.fn().mockRejectedValue(notFound),
      createNamespacedJob: vi.fn().mockImplementation(({ body }) => Promise.resolve(body)),
      createNamespacedEvent: vi.fn().mockImplementation(({ body }) => Promise.resolve(body)),
    };
    const kubeConfig = new KubeConfig();
    vi.spyOn(kubeConfig, 'makeApiClient').mockReturnValue(
      new Proxy({}, { get: (_t, p: string) => (api[p] ??= vi.fn().mockResolvedValue({})) }) as never,
    );
    const controller = new FirebirdBackupController(kubeConfig);
    const backup: FirebirdBackup = {
      apiVersion: 'firebird.cloudnative-firebird.io/v1',
      kind: 'FirebirdBackup',
      metadata: { name: 'nightly', namespace: 'default', uid: 'b' },
      spec: { clusterName: 'db' },
    };
    await controller.reconcileBackup(backup);
    api.readNamespacedJob.mockResolvedValue({ status: { conditions: [{ type: 'Complete', status: 'True' }] } });
    await controller.reconcileBackup({ ...backup, status: { phase: 'Running' } });
    const recorded = api.createNamespacedEvent.mock.calls.map((c) => c[0].body);
    expect(recorded.map((e) => e.reason)).toEqual(['BackupStarted', 'BackupCompleted']);
    expect(recorded[0].involvedObject).toMatchObject({ kind: 'FirebirdBackup', name: 'nightly', uid: 'b' });
    expect(recorded[1].message).toBe('backup stored at /var/lib/firebird/data/backup-nightly.fbk');
  });
});
