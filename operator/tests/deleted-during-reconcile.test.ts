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
