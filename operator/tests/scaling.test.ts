import { describe, it, expect, vi, Mock } from 'vitest';
import { KubeConfig, V1StatefulSet } from '@kubernetes/client-node';
import { FirebirdClusterController } from '../src/controllers/firebirdcluster.controller';
import { TARGET_PRIMARY_ANNOTATION } from '../src/utils/switchover';
import { scaleDownHold } from '../src/utils/scaling';
import { FirebirdCluster } from '../src/types';

const cluster = (instances: number, extra: Partial<FirebirdCluster['spec']> = {}, annotations: Record<string, string> = {}, status?: FirebirdCluster['status']): FirebirdCluster => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdCluster',
  metadata: { name: 'db', namespace: 'default', uid: 'c', annotations },
  spec: { instances, storage: { size: '1Gi' }, replication: { enabled: true }, ...extra },
  ...(status ? { status } : {}),
});

const pod = (name: string, ready = true) => ({
  metadata: { name, uid: `u-${name}` },
  status: { phase: 'Running', conditions: [{ type: 'Ready', status: ready ? 'True' : 'False' }] },
});

describe('scaling down below the primary (scaleDownHold)', () => {
  const pods = [pod('db-0'), pod('db-1'), pod('db-2')] as never[];

  it('keeps the primary running and switches over to the lowest-ordinal ready replica that stays', () => {
    expect(scaleDownHold(cluster(1), 'db-2', pods)).toEqual({
      replicas: 3,
      switchoverTo: 'db-0',
      message: 'scaling down to 1 removes the primary db-2: switching over to db-0 first',
    });
    expect(scaleDownHold(cluster(2), 'db-2', pods)?.switchoverTo).toBe('db-0');
    // db-0 not ready: db-1 stays and takes over
    expect(scaleDownHold(cluster(2), 'db-2', [pod('db-0', false), pod('db-1'), pod('db-2')] as never[])?.switchoverTo).toBe('db-1');
    // no ready replica that stays: waits, says so
    expect(scaleDownHold(cluster(1), 'db-2', [pod('db-0', false), pod('db-2')] as never[])).toEqual({
      replicas: 3,
      switchoverTo: undefined,
      message: 'scaling down to 1 removes the primary db-2: waiting for a ready replica below ordinal 1 to switch over to',
    });
  });

  it('is no hold when the primary stays, without replication, or hibernated', () => {
    expect(scaleDownHold(cluster(2), 'db-0', pods)).toBeUndefined();
    expect(scaleDownHold(cluster(3), 'db-2', pods)).toBeUndefined();
    expect(scaleDownHold(cluster(1, { replication: { enabled: false } }), 'db-2', pods)).toBeUndefined();
    expect(scaleDownHold(cluster(1, { hibernated: true }), 'db-2', pods)).toBeUndefined();
  });

  it('waits for a switchover or failover in flight whose instances go', () => {
    expect(scaleDownHold(cluster(1), 'db-2', pods, { from: 'db-2', target: 'db-0', phase: 'Stopping' })).toEqual({
      replicas: 3,
      message: 'scaling down to 1 waits for the switchover in progress',
    });
    // promoted already (the primary moved), the old primary still being demoted: it stays until then
    expect(scaleDownHold(cluster(1), 'db-0', pods, { from: 'db-2', target: 'db-0', phase: 'Promoting' })?.replicas).toBe(3);
    expect(scaleDownHold(cluster(1), 'db-0', pods, { from: 'db-2', target: 'db-0', phase: 'Completed' })).toBeUndefined();
    expect(scaleDownHold(cluster(1), 'db-1', pods, { from: 'db-2', target: 'db-1', phase: 'Promoting', kind: 'failover' })?.message).toBe('scaling down to 1 waits for the failover in progress');
  });
});

describe('FirebirdClusterController – scaling down below the primary', () => {
  const notFound = Object.assign(new Error('Not Found'), { code: 404 });
  function setup(opts: { lease: string; pods?: object[]; stored?: FirebirdCluster; statefulSet?: V1StatefulSet }) {
    const api: Record<string, Mock> = {
      listNamespacedPod: vi.fn().mockImplementation(({ labelSelector }: { labelSelector?: string }) =>
        Promise.resolve({ items: labelSelector?.startsWith('job-name=') ? [] : (opts.pods ?? [pod('db-0'), pod('db-1'), pod('db-2')]) }),
      ),
      readNamespacedLease: vi.fn().mockResolvedValue({ spec: { holderIdentity: opts.lease } }),
      getNamespacedCustomObject: opts.stored ? vi.fn().mockResolvedValue(opts.stored) : vi.fn().mockRejectedValue(notFound),
      readNamespacedStatefulSet: opts.statefulSet ? vi.fn().mockResolvedValue(opts.statefulSet) : vi.fn().mockRejectedValue(notFound),
      readNamespacedConfigMap: vi.fn().mockResolvedValue({ data: {} }),
      // a created StatefulSet reports its pods ready at once
      createNamespacedStatefulSet: vi.fn().mockImplementation((req: { body: V1StatefulSet }) => Promise.resolve({ ...req.body, status: { readyReplicas: req.body.spec?.replicas ?? 0 } })),
      patchNamespacedStatefulSet: vi.fn().mockImplementation((req: { body: V1StatefulSet }) => Promise.resolve({ ...req.body, status: { readyReplicas: req.body.spec?.replicas ?? 0 } })),
    };
    const calls: Record<string, Mock> = {};
    const fn = (m: string): Mock =>
      (calls[m] ??=
        api[m] ??
        (m.startsWith('read') || m.startsWith('get')
          ? vi.fn().mockRejectedValue(notFound)
          : m.startsWith('list')
            ? vi.fn().mockResolvedValue({ items: [] })
            : vi.fn().mockImplementation((req: { body?: unknown }) => Promise.resolve(req?.body ?? {}))));
    const kubeConfig = new KubeConfig();
    vi.spyOn(kubeConfig, 'makeApiClient').mockReturnValue(new Proxy({}, { get: (_t, p: string) => fn(p) }) as never);
    const status = () => fn('patchNamespacedCustomObjectStatus').mock.calls.map((c) => c[0].body[0].value).at(-1);
    const annotation = () => fn('patchNamespacedCustomObject').mock.calls.map((c) => c[0].body?.metadata?.annotations?.[TARGET_PRIMARY_ANNOTATION]).filter(Boolean);
    const events = () => fn('createNamespacedEvent').mock.calls.map((c) => c[0].body.reason);
    return { controller: new FirebirdClusterController(kubeConfig, vi.fn().mockRejectedValue(new Error('unreachable'))), fn, status, annotation, events };
  }

  it('keeps the StatefulSet at the primary\'s ordinal, requests the switchover once and says so', async () => {
    const s = setup({ lease: 'db-2' });
    await s.controller.reconcile(cluster(1));
    const created = s.fn('createNamespacedStatefulSet').mock.calls[0][0].body as V1StatefulSet;
    expect(created.spec?.replicas).toBe(3);
    expect(s.annotation()).toEqual(['db-0']);
    expect(s.events()).toContain('ScaleDownSwitchover');
    expect(s.status()).toMatchObject({ phase: 'Updating', phaseReason: 'Scaling down: scaling down to 1 removes the primary db-2: switching over to db-0 first', instances: 1 });

    // requested already and the switchover running: no second request, the StatefulSet stays
    const running = cluster(1, {}, { [TARGET_PRIMARY_ANNOTATION]: 'db-0' }, { switchover: { from: 'db-2', target: 'db-0', phase: 'Stopping' } });
    const r = setup({ lease: 'db-2', stored: running, statefulSet: { metadata: { name: 'db' }, spec: { replicas: 3, podManagementPolicy: 'Parallel', template: { spec: { containers: [{ name: 'firebird' }] } } }, status: { readyReplicas: 3 } } });
    await r.controller.reconcile(running);
    expect(r.annotation()).toEqual([]);
    expect(r.status().phaseReason).toBe('Scaling down: scaling down to 1 waits for the switchover in progress');
  });

  it('shrinks once the primary is below spec.instances', async () => {
    const s = setup({ lease: 'db-0' });
    await s.controller.reconcile(cluster(1));
    expect((s.fn('createNamespacedStatefulSet').mock.calls[0][0].body as V1StatefulSet).spec?.replicas).toBe(1);
    expect(s.annotation()).toEqual([]);
    expect(s.status().phaseReason).not.toContain('Scaling down');
  });
});
