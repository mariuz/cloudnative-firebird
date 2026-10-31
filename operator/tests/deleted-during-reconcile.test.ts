import { describe, it, expect, vi, type Mock } from 'vitest';
import { KubeConfig } from '@kubernetes/client-node';
import { FirebirdClusterController } from '../src/controllers/firebirdcluster.controller';
import { makeCluster, notFoundError } from './helpers/factories';

/** API clients whose calls default to: read/get → 404, list → empty, others → {} */
function mockApi(overrides: Record<string, Mock> = {}) {
  const calls: Record<string, Mock> = {};
  const fn = (method: string): Mock =>
    (calls[method] ??=
      overrides[method] ??
      (method.startsWith('read') || method.startsWith('get')
        ? vi.fn().mockRejectedValue(notFoundError)
        : method.startsWith('list')
          ? vi.fn().mockResolvedValue({ items: [] })
          : vi.fn().mockResolvedValue({})));
  const kubeConfig = new KubeConfig();
  vi.spyOn(kubeConfig, 'makeApiClient').mockReturnValue(new Proxy({}, { get: (_t, p: string) => fn(p) }) as never);
  return { kubeConfig, api: fn };
}

describe('a cluster deleted during its reconcile', () => {
  // the StatefulSet existed when read, and was garbage-collected before the patch
  const gone = () => ({
    readNamespacedStatefulSet: vi.fn().mockResolvedValue({ metadata: { name: 'test-cluster' }, spec: { replicas: 9, podManagementPolicy: 'Parallel' }, status: {} }),
    patchNamespacedStatefulSet: vi.fn().mockRejectedValue(notFoundError),
  });

  it('stops without an error, an event or a Degraded status', async () => {
    const { kubeConfig, api } = mockApi(gone());
    await expect(new FirebirdClusterController(kubeConfig).reconcile(makeCluster())).resolves.toBeUndefined();
    expect(api('patchNamespacedStatefulSet')).toHaveBeenCalled();
    expect(api('getNamespacedCustomObject')).toHaveBeenCalledWith(expect.objectContaining({ name: 'test-cluster', plural: 'firebirdclusters' }));
    expect(api('createNamespacedEvent')).not.toHaveBeenCalled();
    const statuses = api('patchNamespacedCustomObjectStatus').mock.calls.map((c) => JSON.stringify(c[0].body));
    expect(statuses.some((b) => b.includes('Degraded'))).toBe(false);
  });

  it('still fails when the cluster exists', async () => {
    const { kubeConfig, api } = mockApi({ ...gone(), getNamespacedCustomObject: vi.fn().mockResolvedValue({}) });
    await expect(new FirebirdClusterController(kubeConfig).reconcile(makeCluster())).rejects.toThrow();
    const statuses = api('patchNamespacedCustomObjectStatus').mock.calls.map((c) => JSON.stringify(c[0].body));
    expect(statuses.some((b) => b.includes('Degraded'))).toBe(true);
  });
});

describe('a cluster deleted with orphan propagation during its reconcile', () => {
  it('never puts itself back as an owner in its update patches', async () => {
    const cluster = makeCluster();
    // read while the cluster still owned it; orphan propagation removes the owner before the
    // patch, which must leave the owners as they are (a merge patch sets the whole list)
    const owner = { apiVersion: 'firebird.cloudnative-firebird.io/v1', kind: 'FirebirdCluster', name: 'test-cluster', uid: cluster.metadata.uid!, controller: true };
    const owned = (kind: string) => ({ apiVersion: 'v1', kind, metadata: { name: 'test-cluster', ownerReferences: [owner], labels: {} }, spec: { replicas: 9 }, data: { stale: 'x' } });
    const { kubeConfig, api } = mockApi({
      readNamespacedStatefulSet: vi.fn().mockResolvedValue({ ...owned('StatefulSet'), spec: { replicas: 9, podManagementPolicy: 'Parallel' }, status: {} }),
      readNamespacedConfigMap: vi.fn().mockResolvedValue(owned('ConfigMap')),
      getNamespacedCustomObject: vi.fn().mockResolvedValue({}),
    });
    await new FirebirdClusterController(kubeConfig).reconcile(cluster);
    const patches = ['patchNamespacedStatefulSet', 'patchNamespacedConfigMap'].flatMap((m) => api(m).mock.calls.map((c) => c[0].body));
    expect(api('patchNamespacedStatefulSet')).toHaveBeenCalled();
    for (const body of patches) expect(JSON.parse(JSON.stringify(body)).metadata?.ownerReferences).toBeUndefined();
  });
});

describe('a cluster the watch saw deleted or replaced (markGone)', () => {
  it('gets nothing created from a reconcile of its copy, and no failure recorded', async () => {
    const cluster = makeCluster();
    const { kubeConfig, api } = mockApi();
    const controller = new FirebirdClusterController(kubeConfig);
    controller.markGone(cluster.metadata.uid);
    await expect(controller.reconcile(cluster)).resolves.toBeUndefined();
    for (const m of ['createNamespacedStatefulSet', 'createNamespacedService', 'createNamespacedConfigMap', 'createNamespacedLease']) {
      expect(api(m)).not.toHaveBeenCalled();
    }
    expect(api('createNamespacedEvent')).not.toHaveBeenCalled();
  });

  it('leaves another cluster of the name (another uid) alone', async () => {
    const { kubeConfig, api } = mockApi();
    const controller = new FirebirdClusterController(kubeConfig);
    controller.markGone('old-uid');
    await controller.reconcile(makeCluster());
    expect(api('createNamespacedStatefulSet')).toHaveBeenCalled();
  });
});
