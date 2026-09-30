import { describe, it, expect, vi, Mock } from 'vitest';
import { KubeConfig, V1Pod, V1StatefulSet } from '@kubernetes/client-node';
import { FirebirdClusterController } from '../src/controllers/firebirdcluster.controller';
import { REVISION_LABEL, planRollingUpdate } from '../src/utils/rolling-update';
import { buildStatefulSet, statefulSetNeedsUpdate } from '../src/utils/resources';
import { TARGET_PRIMARY_ANNOTATION } from '../src/utils/switchover';
import { validateClusterSpec } from '../src/utils/validation';
import { FirebirdCluster } from '../src/types';

const makeCluster = (spec: Partial<FirebirdCluster['spec']> = {}): FirebirdCluster => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdCluster',
  metadata: { name: 'db', namespace: 'default', uid: 'c' },
  spec: { instances: 3, storage: { size: '1Gi' }, replication: { enabled: true }, ...spec },
});

const pod = (name: string, revision: string, opts: { ready?: boolean; deleting?: boolean } = {}): V1Pod => ({
  metadata: {
    name,
    uid: `u-${name}`,
    labels: { [REVISION_LABEL]: revision },
    ...(opts.deleting ? { deletionTimestamp: new Date() } : {}),
  },
  status: { phase: 'Running', conditions: [{ type: 'Ready', status: opts.ready === false ? 'False' : 'True' }] },
});

const sts = (revision = 'db-new', observed = 2): V1StatefulSet => ({
  metadata: { name: 'db', generation: 2 },
  status: { replicas: 3, updateRevision: revision, currentRevision: 'db-old', observedGeneration: observed },
});

const plan = (pods: V1Pod[], opts: Partial<Parameters<typeof planRollingUpdate>[0]> = {}) =>
  planRollingUpdate({
    cluster: makeCluster(),
    statefulSet: sts(),
    pods,
    primaryPod: 'db-0',
    fenced: [],
    ...opts,
  });

describe('rolling update planning', () => {
  it('restarts outdated replicas first, highest ordinal first', () => {
    const p = plan([pod('db-0', 'db-old'), pod('db-1', 'db-old'), pod('db-2', 'db-old')]);
    expect(p).toMatchObject({ restart: 'db-2', outdated: ['db-0', 'db-1', 'db-2'], revision: 'db-new' });
    expect(plan([pod('db-0', 'db-old'), pod('db-1', 'db-old'), pod('db-2', 'db-new')])?.restart).toBe('db-1');
  });

  it('restarts the primary first when replication was just enabled (it seeds the replicas)', () => {
    const withContainers = (p: V1Pod, names: string[]) => ({ ...p, spec: { containers: names.map((n) => ({ name: n })) } });
    const plain = [pod('db-0', 'db-old'), pod('db-1', 'db-old'), pod('db-2', 'db-old')].map((p) => withContainers(p, ['firebird']));
    expect(plan(plain)).toMatchObject({ restart: 'db-0' });
    expect(plan(plain)?.message).toContain('replication enabled');
    // once the primary runs with replication, the replicas follow as usual
    const next = [withContainers(pod('db-0', 'db-new'), ['firebird', 'segment-server', 'segment-puller']), ...plain.slice(1)];
    expect(plan(next)?.restart).toBe('db-2');
  });

  it('restarts the primary last', () => {
    const p = plan([pod('db-0', 'db-old'), pod('db-1', 'db-new'), pod('db-2', 'db-new')]);
    expect(p).toMatchObject({ restart: 'db-0' });
    // the primary is not ordinal 0 after a switchover
    const q = plan([pod('db-0', 'db-old'), pod('db-1', 'db-old'), pod('db-2', 'db-old')], { primaryPod: 'db-2' });
    expect(q?.restart).toBe('db-1');
  });

  it('waits until every instance is ready and none is terminating', () => {
    const notReady = plan([pod('db-0', 'db-old'), pod('db-1', 'db-new', { ready: false }), pod('db-2', 'db-new')]);
    expect(notReady?.restart).toBeUndefined();
    expect(notReady?.message).toContain('waiting for all instances');
    const terminating = plan([pod('db-0', 'db-old'), pod('db-1', 'db-old', { deleting: true }), pod('db-2', 'db-new')]);
    expect(terminating?.restart).toBeUndefined();
    const missing = plan([pod('db-0', 'db-old'), pod('db-1', 'db-old')]);
    expect(missing?.restart).toBeUndefined();
  });

  it('does nothing during a switchover, failover or re-seed', () => {
    const p = plan([pod('db-0', 'db-old'), pod('db-1', 'db-old'), pod('db-2', 'db-old')], { busy: 'failover in progress' });
    expect(p?.restart).toBeUndefined();
    expect(p?.message).toBe('waiting: failover in progress');
  });

  it('waits until the StatefulSet controller has observed the latest template', () => {
    expect(plan([pod('db-0', 'db-old')], { statefulSet: sts('db-new', 1) })).toBeUndefined();
  });

  it('reports nothing to do when every instance runs the update revision', () => {
    const p = plan([pod('db-0', 'db-new'), pod('db-1', 'db-new'), pod('db-2', 'db-new')]);
    expect(p?.outdated).toEqual([]);
    expect(p?.restart).toBeUndefined();
  });

  it('skips fenced instances and never restarts a fenced primary', () => {
    const fencedReplica = plan([pod('db-0', 'db-old'), pod('db-1', 'db-new'), pod('db-2', 'db-old', { ready: false })], {
      fenced: ['db-2'],
    });
    expect(fencedReplica?.restart).toBe('db-0');
    const fencedPrimary = plan([pod('db-0', 'db-old', { ready: false }), pod('db-1', 'db-new'), pod('db-2', 'db-new')], {
      fenced: ['db-0'],
    });
    expect(fencedPrimary?.restart).toBeUndefined();
    expect(fencedPrimary?.message).toContain('fenced');
  });

  it('waits for the user with primaryUpdateStrategy: supervised', () => {
    const p = plan([pod('db-0', 'db-old'), pod('db-1', 'db-new'), pod('db-2', 'db-new')], {
      cluster: makeCluster({ primaryUpdateStrategy: 'supervised' }),
    });
    expect(p?.restart).toBeUndefined();
    expect(p?.switchoverTo).toBeUndefined();
    expect(p?.message).toContain('supervised');
    // replicas are still updated automatically
    expect(
      plan([pod('db-0', 'db-old'), pod('db-1', 'db-old'), pod('db-2', 'db-new')], {
        cluster: makeCluster({ primaryUpdateStrategy: 'supervised' }),
      })?.restart,
    ).toBe('db-1');
  });

  it('switches over to an updated replica with primaryUpdateMethod: switchover', () => {
    const cluster = makeCluster({ primaryUpdateMethod: 'switchover' });
    const pods = [pod('db-0', 'db-old'), pod('db-1', 'db-new'), pod('db-2', 'db-new')];
    expect(plan(pods, { cluster })).toMatchObject({ switchoverTo: 'db-1' });
    expect(plan(pods, { cluster })?.restart).toBeUndefined();
    // a failed switchover to that replica is not retried: the next one, then a restart
    const failed = { target: 'db-1', from: 'db-0', phase: 'Failed' as const };
    expect(plan(pods, { cluster, lastSwitchover: failed })?.switchoverTo).toBe('db-2');
    const onlyOne = [pod('db-0', 'db-old'), pod('db-1', 'db-new')];
    expect(plan(onlyOne, { cluster: { ...cluster, spec: { ...cluster.spec, instances: 2 } }, lastSwitchover: failed })).toMatchObject({
      restart: 'db-0',
    });
  });

  it('leaves clusters without replication to the StatefulSet controller', () => {
    expect(plan([pod('db-0', 'db-old')], { cluster: makeCluster({ replication: undefined, instances: 1 }) })).toBeUndefined();
    expect(plan([pod('db-0', 'db-old')], { cluster: makeCluster({ hibernated: true }) })).toBeUndefined();
  });
});

describe('StatefulSet update strategy', () => {
  it('uses OnDelete with replication and RollingUpdate otherwise', () => {
    expect(buildStatefulSet(makeCluster()).spec?.updateStrategy).toEqual({ type: 'OnDelete' });
    expect(buildStatefulSet(makeCluster({ replication: undefined })).spec?.updateStrategy).toEqual({
      type: 'RollingUpdate',
    });
  });

  it('updates an existing StatefulSet whose strategy differs', () => {
    const desired = buildStatefulSet(makeCluster());
    const existing = JSON.parse(JSON.stringify(desired)) as V1StatefulSet;
    expect(statefulSetNeedsUpdate(existing, desired)).toBe(false);
    existing.spec!.updateStrategy = { type: 'RollingUpdate', rollingUpdate: { partition: 0 } };
    expect(statefulSetNeedsUpdate(existing, desired)).toBe(true);
    delete existing.spec!.updateStrategy; // defaulted to RollingUpdate by older operator versions
    expect(statefulSetNeedsUpdate(existing, desired)).toBe(true);
  });

  it('validates primaryUpdateStrategy and primaryUpdateMethod', () => {
    expect(() => validateClusterSpec(makeCluster({ primaryUpdateStrategy: 'manual' as never }))).toThrow(
      /primaryUpdateStrategy/,
    );
    expect(() => validateClusterSpec(makeCluster({ primaryUpdateMethod: 'recreate' as never }))).toThrow(
      /primaryUpdateMethod/,
    );
    expect(() =>
      validateClusterSpec(makeCluster({ primaryUpdateStrategy: 'supervised', primaryUpdateMethod: 'switchover' })),
    ).not.toThrow();
  });
});

function setup(pods: V1Pod[], opts: { existing?: V1StatefulSet; patched?: V1StatefulSet } = {}) {
  const notFound = Object.assign(new Error('Not Found'), { code: 404 });
  const existing = opts.existing ?? { ...buildStatefulSet(makeCluster()), ...sts() };
  const api: Record<string, Mock> = {
    listNamespacedPod: vi.fn().mockResolvedValue({ items: pods }),
    readNamespacedLease: vi.fn().mockResolvedValue({ spec: { holderIdentity: 'db-0' } }),
    readNamespacedConfigMap: vi.fn().mockResolvedValue({ data: {} }),
    readNamespacedStatefulSet: vi.fn().mockResolvedValue(existing),
    patchNamespacedStatefulSet: vi.fn().mockResolvedValue(opts.patched ?? existing),
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
  return { controller: new FirebirdClusterController(kubeConfig), fn, status };
}

describe('rolling update reconciliation', () => {
  it('restarts one outdated replica and reports the update in status', async () => {
    const s = setup([pod('db-0', 'db-old'), pod('db-1', 'db-old'), pod('db-2', 'db-old')]);
    await s.controller.reconcile(makeCluster());
    expect(s.fn('deleteNamespacedPod').mock.calls).toEqual([[{ name: 'db-2', namespace: 'default' }]]);
    expect(s.status().rollingUpdate).toMatchObject({ revision: 'db-new', outdatedInstances: ['db-0', 'db-1', 'db-2'] });
  });

  it('restarts the primary once the replicas are updated, and clears the status when done', async () => {
    const s = setup([pod('db-0', 'db-old'), pod('db-1', 'db-new'), pod('db-2', 'db-new')]);
    await s.controller.reconcile(makeCluster());
    expect(s.fn('deleteNamespacedPod')).toHaveBeenCalledWith({ name: 'db-0', namespace: 'default' });

    const done = setup([pod('db-0', 'db-new'), pod('db-1', 'db-new'), pod('db-2', 'db-new')]);
    await done.controller.reconcile({ ...makeCluster(), status: { rollingUpdate: { revision: 'db-new', outdatedInstances: ['db-0'], message: '' } } });
    expect(done.fn('deleteNamespacedPod')).not.toHaveBeenCalled();
    expect(done.status().rollingUpdate).toBeUndefined();
  });

  it('records the primary restart, and automatic failover waits for the restarted primary', async () => {
    const s = setup([pod('db-0', 'db-old'), pod('db-1', 'db-new'), pod('db-2', 'db-new')]);
    await s.controller.reconcile(makeCluster());
    const restarted = s.status().rollingUpdate;
    expect(restarted.primaryRestart).toMatchObject({ pod: 'db-0', uid: 'u-db-0' });

    // the new primary pod is starting: no election, even past failover.delaySeconds
    const failover = { enabled: true, delaySeconds: 1 };
    const cluster = (rollingUpdate: object) => ({
      ...makeCluster({ replication: { enabled: true, failover } }),
      status: { primaryNotReadySince: new Date(Date.now() - 60_000).toISOString(), rollingUpdate },
    });
    const starting = setup([pod('db-0', 'db-new', { ready: false }), pod('db-1', 'db-new'), pod('db-2', 'db-new')]);
    await starting.controller.reconcile(cluster(restarted) as FirebirdCluster);
    expect(starting.fn('createNamespacedJob')).not.toHaveBeenCalled();
    expect(starting.status().rollingUpdate.primaryRestart).toMatchObject({ pod: 'db-0' });
    expect(starting.status().primaryNotReadySince).toBeUndefined();

    // once the grace period is over a primary that does not come back is failed over
    const expired = { ...restarted, primaryRestart: { ...restarted.primaryRestart, time: new Date(Date.now() - 600_000).toISOString() } };
    const stuck = setup([pod('db-0', 'db-new', { ready: false }), pod('db-1', 'db-new'), pod('db-2', 'db-new')]);
    await stuck.controller.reconcile(cluster(expired) as FirebirdCluster);
    expect(stuck.fn('createNamespacedJob').mock.calls.map((c) => c[0].body.metadata.name)).toContain('db-failover');

    // the restarted primary is ready: the update is finished
    const recreated = pod('db-0', 'db-new');
    recreated.metadata!.uid = 'u-db-0-new';
    const back = setup([recreated, pod('db-1', 'db-new'), pod('db-2', 'db-new')]);
    await back.controller.reconcile(cluster(restarted) as FirebirdCluster);
    expect(back.status().rollingUpdate).toBeUndefined();
  });

  it('keeps the primary restart when a reconcile starts from a stale copy of the cluster', async () => {
    const s = setup([pod('db-0', 'db-old'), pod('db-1', 'db-new'), pod('db-2', 'db-new')]);
    await s.controller.reconcile(makeCluster());
    const restarted = s.status().rollingUpdate;
    const failover = { enabled: true, delaySeconds: 1 };
    const spec = makeCluster({ replication: { enabled: true, failover } });
    // the stored status has the restart; the watch copy this reconcile starts from predates it
    const stored = { ...spec, status: { rollingUpdate: restarted } };
    const stale = setup([pod('db-0', 'db-new', { ready: false }), pod('db-1', 'db-new'), pod('db-2', 'db-new')]);
    stale.fn('getNamespacedCustomObject').mockResolvedValue(stored);
    await stale.controller.reconcile({ ...spec, status: { rollingUpdate: { revision: 'db-new', outdatedInstances: ['db-0'], message: '' } } } as FirebirdCluster);
    expect(stale.status().rollingUpdate?.primaryRestart).toMatchObject({ pod: 'db-0' });
    expect(stale.status().primaryNotReadySince).toBeUndefined();
    expect(stale.fn('createNamespacedJob')).not.toHaveBeenCalled();
  });

  it('requests a switchover through the targetPrimary annotation', async () => {
    const s = setup([pod('db-0', 'db-old'), pod('db-1', 'db-new'), pod('db-2', 'db-new')]);
    await s.controller.reconcile(makeCluster({ primaryUpdateMethod: 'switchover' }));
    expect(s.fn('deleteNamespacedPod')).not.toHaveBeenCalled();
    expect(s.fn('patchNamespacedCustomObject')).toHaveBeenCalledWith(
      expect.objectContaining({ body: { metadata: { annotations: { [TARGET_PRIMARY_ANNOTATION]: 'db-1' } } } }),
      expect.anything(),
    );
  });

  it('clears rollingUpdate settings when switching an existing StatefulSet to OnDelete', async () => {
    const existing = { ...buildStatefulSet(makeCluster({ replication: undefined })), ...sts() };
    existing.spec!.updateStrategy = { type: 'RollingUpdate', rollingUpdate: { partition: 0 } };
    const s = setup([pod('db-0', 'db-new'), pod('db-1', 'db-new'), pod('db-2', 'db-new')], { existing });
    await s.controller.reconcile(makeCluster());
    const body = s.fn('patchNamespacedStatefulSet').mock.calls[0][0].body;
    expect(body.spec.updateStrategy).toEqual({ type: 'OnDelete', rollingUpdate: null });
  });

  it('restarts nothing while the StatefulSet controller catches up with a template change', async () => {
    const s = setup([pod('db-0', 'db-old'), pod('db-1', 'db-old'), pod('db-2', 'db-old')], {
      patched: { ...buildStatefulSet(makeCluster()), ...sts('db-old', 1) },
      existing: { ...buildStatefulSet(makeCluster({ imageName: 'firebirdsql/firebird:5.0.3' })), ...sts('db-old', 1) },
    });
    await s.controller.reconcile(makeCluster());
    expect(s.fn('patchNamespacedStatefulSet')).toHaveBeenCalled();
    expect(s.fn('deleteNamespacedPod')).not.toHaveBeenCalled();
  });
});
