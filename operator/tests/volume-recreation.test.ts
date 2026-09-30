import { describe, it, expect, vi, type Mock } from 'vitest';
import { KubeConfig, V1PersistentVolumeClaim, V1Pod } from '@kubernetes/client-node';
import { planVolumeRecreation } from '../src/utils/volume-recreation';
import { FirebirdClusterController } from '../src/controllers/firebirdcluster.controller';
import { FirebirdCluster } from '../src/types';

const cluster = (recreatingVolumes?: Array<{ pod: string; claimUid: string }>, instances = 3): FirebirdCluster => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdCluster',
  metadata: { name: 'db', namespace: 'default', uid: 'c' },
  spec: { instances, storage: { size: '1Gi' }, replication: { enabled: true } },
  ...(recreatingVolumes ? { status: { recreatingVolumes } } : {}),
});
const pod = (name: string, opts: { ready?: boolean; reseed?: string; deleting?: boolean } = {}): V1Pod => ({
  metadata: {
    name,
    uid: `pod-${name}`,
    annotations: opts.reseed ? { 'firebird.cloudnative-firebird.io/reseed': opts.reseed } : {},
    ...(opts.deleting ? { deletionTimestamp: new Date() } : {}),
  },
  status: { phase: 'Running', conditions: [{ type: 'Ready', status: opts.ready === false ? 'False' : 'True' }] },
});
const claim = (pod: string, uid: string, deleting = false): V1PersistentVolumeClaim => ({
  metadata: { name: `firebird-data-${pod}`, uid, ...(deleting ? { deletionTimestamp: new Date() } : {}) },
});

describe('planVolumeRecreation', () => {
  const base = { primaryPod: 'db-0', replication: true };

  it('starts on an annotated replica: deletes its claim and its pod', () => {
    const plan = planVolumeRecreation({
      ...base,
      cluster: cluster(),
      pods: [pod('db-0'), pod('db-1'), pod('db-2', { reseed: 'volume' })],
      claims: [claim('db-0', 'c0'), claim('db-1', 'c1'), claim('db-2', 'c2')],
    });
    expect(plan).toMatchObject({
      recreating: [{ pod: 'db-2', claimUid: 'c2' }],
      deleteClaims: ['db-2'],
      deletePods: ['db-2'],
      started: ['db-2'],
    });
  });

  it('refuses the primary, a scaled-away instance and a cluster without replication', () => {
    const pods = [pod('db-0', { reseed: 'volume' }), pod('db-3', { reseed: 'volume' })];
    const claims = [claim('db-0', 'c0'), claim('db-3', 'c3')];
    const plan = planVolumeRecreation({ ...base, cluster: cluster(), pods, claims });
    expect(plan.recreating).toEqual([]);
    expect(plan.ignored.map((i) => i.pod)).toEqual(['db-0', 'db-3']);
    expect(plan.ignored[0].reason).toMatch(/primary/);
    const standalone = planVolumeRecreation({ ...base, replication: false, cluster: cluster(), pods: [pod('db-1', { reseed: 'volume' })], claims: [claim('db-1', 'c1')] });
    expect(standalone.ignored[0].reason).toMatch(/empty database/);
    // plain re-seeding (reseed=true) is not a volume request
    expect(planVolumeRecreation({ ...base, cluster: cluster(), pods: [pod('db-1', { reseed: 'true' })], claims: [claim('db-1', 'c1')] }).started).toEqual([]);
  });

  it('deletes the pod again while the old claim terminates, and while no claim exists', () => {
    const entry = [{ pod: 'db-2', claimUid: 'c2' }];
    // recreated by the StatefulSet against the terminating claim
    const terminating = planVolumeRecreation({
      ...base,
      cluster: cluster(entry),
      pods: [pod('db-2', { ready: false })],
      claims: [claim('db-2', 'c2', true)],
    });
    expect(terminating).toMatchObject({ recreating: entry, deleteClaims: [], deletePods: ['db-2'] });
    // the claim is gone, the pod waits for a claim only a new pod gets
    const gone = planVolumeRecreation({ ...base, cluster: cluster(entry), pods: [pod('db-2', { ready: false })], claims: [] });
    expect(gone).toMatchObject({ recreating: entry, deletePods: ['db-2'] });
    // a pod already being deleted is left alone
    expect(planVolumeRecreation({ ...base, cluster: cluster(entry), pods: [pod('db-2', { deleting: true })], claims: [] }).deletePods).toEqual([]);
  });

  it('waits on the new claim until the replica is ready, then completes', () => {
    const entry = [{ pod: 'db-2', claimUid: 'c2' }];
    const seeding = planVolumeRecreation({ ...base, cluster: cluster(entry), pods: [pod('db-2', { ready: false })], claims: [claim('db-2', 'new')] });
    expect(seeding).toMatchObject({ recreating: entry, deletePods: [], deleteClaims: [], completed: [] });
    const done = planVolumeRecreation({ ...base, cluster: cluster(entry), pods: [pod('db-2')], claims: [claim('db-2', 'new')] });
    expect(done).toMatchObject({ recreating: [], completed: ['db-2'] });
    // scaled away meanwhile: dropped
    expect(planVolumeRecreation({ ...base, cluster: cluster(entry, 2), pods: [], claims: [] }).recreating).toEqual([]);
  });
});

describe('FirebirdClusterController – volume re-creation', () => {
  function setup(pods: V1Pod[], claims: V1PersistentVolumeClaim[]) {
    const notFound = Object.assign(new Error('Not Found'), { code: 404 });
    const api: Record<string, Mock> = {
      listNamespacedPod: vi.fn().mockResolvedValue({ items: pods }),
      listNamespacedPersistentVolumeClaim: vi.fn().mockResolvedValue({ items: claims }),
      readNamespacedLease: vi.fn().mockResolvedValue({ spec: { holderIdentity: 'db-0' } }),
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
    const statuses = () => fn('patchNamespacedCustomObjectStatus').mock.calls.map((c) => c[0].body[0].value);
    return { controller: new FirebirdClusterController(kubeConfig), fn, statuses };
  }

  it('records the request before deleting the claim and the pod, and reports it until done', async () => {
    const s = setup([pod('db-0'), pod('db-1'), pod('db-2', { reseed: 'volume' })], [claim('db-2', 'c2')]);
    await s.controller.reconcile(cluster());
    const deleteClaim = s.fn('deleteNamespacedPersistentVolumeClaim');
    expect(deleteClaim).toHaveBeenCalledWith({ name: 'firebird-data-db-2', namespace: 'default' });
    expect(s.fn('deleteNamespacedPod')).toHaveBeenCalledWith({ name: 'db-2', namespace: 'default' });
    // persisted before the first deletion
    const firstWithRequest = s.fn('patchNamespacedCustomObjectStatus').mock.invocationCallOrder.find(
      (_order, i) => s.statuses()[i]?.recreatingVolumes?.length,
    );
    expect(firstWithRequest).toBeLessThan(deleteClaim.mock.invocationCallOrder[0]);
    expect(s.statuses().at(-1)?.recreatingVolumes).toEqual([{ pod: 'db-2', claimUid: 'c2' }]);
  });
});
