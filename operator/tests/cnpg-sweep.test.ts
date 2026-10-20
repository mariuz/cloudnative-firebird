import { describe, it, expect, vi, type Mock } from 'vitest';
import { KubeConfig } from '@kubernetes/client-node';
import { FirebirdClusterController } from '../src/controllers/firebirdcluster.controller';
import { buildPodMonitor } from '../src/utils/resources';
import { clusterEndpoints } from '../src/utils/managed-services';
import { makeCluster, notFoundError } from './helpers/factories';

/**
 * CloudNativePG spec fields adopted in the 1.30 sweep (docs/cloudnative-pg-review.md):
 * description, enablePDB, the PodMonitor relabelings, and status.instanceNames / writeService /
 * readService.
 */

function mocks(overrides: Record<string, Mock> = {}) {
  const calls: Record<string, Mock> = {};
  const fn = (m: string): Mock =>
    (calls[m] ??=
      overrides[m] ??
      (m.startsWith('read') || m.startsWith('get')
        ? vi.fn().mockRejectedValue(notFoundError)
        : m.startsWith('list')
          ? vi.fn().mockResolvedValue({ items: [] })
          : vi.fn().mockResolvedValue({})));
  const kubeConfig = new KubeConfig();
  vi.spyOn(kubeConfig, 'makeApiClient').mockReturnValue(new Proxy({}, { get: (_t, p: string) => fn(p) }) as never);
  const statuses = () => fn('patchNamespacedCustomObjectStatus').mock.calls.map((c) => c[0].body[0].value);
  return { controller: new FirebirdClusterController(kubeConfig), fn, statuses };
}

describe('the PodMonitor relabelings (podMonitorRelabelings, podMonitorMetricRelabelings)', () => {
  it('go onto every endpoint', () => {
    const relabelings = [{ sourceLabels: ['__meta_kubernetes_pod_node_name'], targetLabel: 'node' }];
    const metricRelabelings = [{ sourceLabels: ['__name__'], regex: 'firebird_exporter_.*', action: 'drop' }];
    const cluster = makeCluster({
      monitoring: {
        enablePodMonitor: true,
        exporter: { enabled: true },
        customQueriesConfigMap: [{ name: 'q', key: 'queries' }],
        podMonitorRelabelings: relabelings,
        podMonitorMetricRelabelings: metricRelabelings,
      },
    });
    const pm = buildPodMonitor(cluster) as { spec: { podMetricsEndpoints: Array<Record<string, unknown>> } };
    expect(pm.spec.podMetricsEndpoints.map((e) => e.port)).toEqual(['metrics', 'custom-metrics']);
    for (const endpoint of pm.spec.podMetricsEndpoints) {
      expect(endpoint.relabelings).toEqual(relabelings);
      expect(endpoint.metricRelabelings).toEqual(metricRelabelings);
    }
    // none set, or empty: the endpoints stay as they were
    const plain = buildPodMonitor(makeCluster({ monitoring: { enablePodMonitor: true, podMonitorRelabelings: [] } })) as { spec: { podMetricsEndpoints: Array<Record<string, unknown>> } };
    expect(plain.spec.podMetricsEndpoints).toEqual([{ port: 'firebird', path: '/metrics', interval: '30s' }]);
  });
});

describe('status.instanceNames, writeService and readService', () => {
  it('name the pods and the Services clients use', () => {
    expect(clusterEndpoints(makeCluster({ instances: 2 }))).toEqual({
      instanceNames: ['test-cluster-0', 'test-cluster-1'],
      writeService: 'test-cluster',
      readService: 'test-cluster',
    });
    expect(clusterEndpoints(makeCluster({ instances: 3, replication: { enabled: true } }))).toEqual({
      instanceNames: ['test-cluster-0', 'test-cluster-1', 'test-cluster-2'],
      writeService: 'test-cluster',
      readService: 'test-cluster-replica',
    });
    // the read-only Service turned off: reads go to the primary's
    expect(clusterEndpoints(makeCluster({ instances: 3, replication: { enabled: true }, managed: { services: { disabledDefaultServices: ['ro'] } } })).readService).toBe('test-cluster');
  });

  it('are written with the cluster status', async () => {
    const m = mocks();
    await m.controller.reconcile(makeCluster({ instances: 2 }));
    expect(m.statuses().at(-1)).toMatchObject({ instanceNames: ['test-cluster-0', 'test-cluster-1'], writeService: 'test-cluster', readService: 'test-cluster' });
    const hibernated = mocks();
    await hibernated.controller.reconcile(makeCluster({ instances: 2, hibernated: true }));
    expect(hibernated.statuses().at(-1)).toMatchObject({ phase: 'Hibernated', instanceNames: ['test-cluster-0', 'test-cluster-1'], writeService: 'test-cluster' });
  });
});

describe('spec.enablePDB', () => {
  it('false removes the PodDisruptionBudget of a cluster of several instances, unset keeps it', async () => {
    const withPdb = mocks({ readNamespacedPodDisruptionBudget: vi.fn().mockResolvedValue({ metadata: { name: 'test-cluster-pdb' }, spec: {} }) });
    await withPdb.controller.reconcile(makeCluster({ instances: 3, enablePDB: false }));
    expect(withPdb.fn('deleteNamespacedPodDisruptionBudget')).toHaveBeenCalledWith({ name: 'test-cluster-pdb', namespace: 'default' });
    expect(withPdb.fn('createNamespacedPodDisruptionBudget')).not.toHaveBeenCalled();

    const without = mocks();
    await without.controller.reconcile(makeCluster({ instances: 3, enablePDB: false }));
    expect(without.fn('createNamespacedPodDisruptionBudget')).not.toHaveBeenCalled();

    const kept = mocks();
    await kept.controller.reconcile(makeCluster({ instances: 3 }));
    expect(kept.fn('createNamespacedPodDisruptionBudget')).toHaveBeenCalled();
    const explicit = mocks();
    await explicit.controller.reconcile(makeCluster({ instances: 3, enablePDB: true }));
    expect(explicit.fn('createNamespacedPodDisruptionBudget')).toHaveBeenCalled();
  });
});
