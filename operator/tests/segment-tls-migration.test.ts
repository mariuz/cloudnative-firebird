import { describe, it, expect, vi, afterEach, type Mock } from 'vitest';
import { KubeConfig, V1Pod } from '@kubernetes/client-node';
import { FirebirdClusterController } from '../src/controllers/firebirdcluster.controller';
import { setServerVersion } from '../src/utils/segment-tls-default';
import { MIGRATION_ANNOTATION, migrationMode, migrationStep } from '../src/utils/segment-tls-migration';
import { FirebirdCluster } from '../src/types';
import { makeCluster, notFoundError } from './helpers/factories';

/** An idle three-instance cluster on plain segment shipping */
const idle = (annotation?: string, overrides: Partial<FirebirdCluster['spec']> = {}): FirebirdCluster => {
  const cluster = makeCluster({ instances: 3, replication: { enabled: true }, ...overrides });
  if (annotation) cluster.metadata.annotations = { [MIGRATION_ANNOTATION]: annotation };
  cluster.status = { phase: 'Running', readyInstances: 3 };
  return cluster;
};
const pod = (name: string, proxy: boolean, ready = true): V1Pod => ({
  metadata: { name },
  spec: { initContainers: proxy ? [{ name: 'segment-tls' }] : [], containers: [] },
  status: { phase: 'Running', conditions: [{ type: 'Ready', status: ready ? 'True' : 'False' }] },
});

describe('moving existing clusters to segment TLS', () => {
  it('reads the operator setting', () => {
    expect(migrationMode({})).toBe('off');
    expect(migrationMode({ SEGMENT_TLS_MIGRATE: 'pinned' })).toBe('pinned');
    expect(migrationMode({ SEGMENT_TLS_MIGRATE: 'ALL' })).toBe('all');
    expect(migrationMode({ SEGMENT_TLS_MIGRATE: 'true' })).toBe('off');
  });

  it('starts only with the setting, on clusters the operator pinned (or all of them), never against the owner', () => {
    expect(migrationStep(idle('pinned'), 'off', true)).toEqual({ action: 'none', reason: 'off' });
    expect(migrationStep(idle('pinned'), 'pinned', true)).toEqual({ action: 'start' });
    // false chosen by the owner, or pinned before the annotation existed: only with "all"
    expect(migrationStep(idle(), 'pinned', true).action).toBe('none');
    expect(migrationStep(idle(), 'all', true)).toEqual({ action: 'start' });
    // opted out, already done, or not off
    expect(migrationStep(idle('skip'), 'all', true).action).toBe('none');
    expect(migrationStep(idle('done'), 'all', true).action).toBe('none');
    expect(migrationStep(idle('pinned', { segmentTLS: { enabled: true } }), 'all', true).action).toBe('none');
  });

  it('waits for native sidecars, an idle cluster, and no other cluster migrating', () => {
    expect(migrationStep(idle('pinned'), 'pinned', false).reason).toMatch(/native sidecars/);
    expect(migrationStep(idle('pinned'), 'pinned', undefined).action).toBe('none');
    const busy = (status: FirebirdCluster['status']) => {
      const c = idle('pinned');
      c.status = { ...c.status, ...status };
      return migrationStep(c, 'pinned', true).action;
    };
    expect(busy({ phase: 'Updating' })).toBe('none');
    expect(busy({ readyInstances: 2 })).toBe('none');
    expect(busy({ rollingUpdate: { revision: 'r2' } as never })).toBe('none');
    expect(busy({ switchover: { kind: 'failover', phase: 'Promoting' } as never })).toBe('none');
    expect(busy({ switchover: { kind: 'failover', phase: 'Completed' } as never })).toBe('start');
    expect(busy({ fencedInstances: ['test-cluster-1'] })).toBe('none');
    expect(migrationStep(idle('pinned', { hibernated: true }), 'pinned', true).action).toBe('none');
    const other = idle('in-progress');
    other.metadata.name = 'other';
    expect(migrationStep(idle('pinned'), 'pinned', true, [], [other])).toEqual({ action: 'none', reason: 'default/other is migrating' });
    // itself in the list does not count
    expect(migrationStep(idle('pinned'), 'pinned', true, [], [idle('pinned')]).action).toBe('start');
  });

  it('finishes once every instance runs the proxy, and steps back when the owner turns it off again', () => {
    const migrating = idle('in-progress', { segmentTLS: { enabled: true } });
    const pods = [pod('test-cluster-0', false), pod('test-cluster-1', true), pod('test-cluster-2', true)];
    // continues even with the setting off: it was started
    expect(migrationStep(migrating, 'off', true, pods).action).toBe('none');
    pods[0] = pod('test-cluster-0', true, false);
    expect(migrationStep(migrating, 'off', true, pods).action).toBe('none');
    pods[0] = pod('test-cluster-0', true);
    expect(migrationStep(migrating, 'off', true, pods)).toEqual({ action: 'finish' });
    expect(migrationStep(idle('in-progress', { segmentTLS: { enabled: false } }), 'all', true, pods)).toEqual({ action: 'abandon' });
  });

  describe('in the controller', () => {
    afterEach(() => {
      setServerVersion(undefined);
      vi.unstubAllEnvs();
    });

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

    it('switches segment TLS on and marks the migration in one patch', async () => {
      setServerVersion({ major: 1, minor: 31 });
      vi.stubEnv('SEGMENT_TLS_MIGRATE', 'pinned');
      // the cluster is gone once the StatefulSet is patched: the reconcile ends right after
      const { kubeConfig, api } = mockApi({
        getNamespacedCustomObject: vi.fn().mockResolvedValueOnce({}).mockRejectedValue(notFoundError),
        readNamespacedStatefulSet: vi.fn().mockResolvedValue({ metadata: { name: 'test-cluster' }, spec: { replicas: 9, podManagementPolicy: 'Parallel' }, status: {} }),
        patchNamespacedStatefulSet: vi.fn().mockRejectedValue(notFoundError),
      });
      const cluster = idle('pinned');
      await new FirebirdClusterController(kubeConfig).reconcile(cluster);
      const patches = api('patchNamespacedCustomObject').mock.calls.map((c) => c[0].body).filter((b) => b.spec);
      expect(patches).toEqual([
        { metadata: { annotations: { [MIGRATION_ANNOTATION]: 'in-progress' } }, spec: { segmentTLS: { enabled: true } } },
      ]);
      expect(cluster.spec.segmentTLS).toEqual({ enabled: true });
      const reasons = api('createNamespacedEvent').mock.calls.map((c) => c[0].body.reason);
      expect(reasons).toContain('SegmentTLSMigrationStarted');
    });
  });
});
