import { describe, it, expect, vi, afterEach, type Mock } from 'vitest';
import { KubeConfig, V1Pod } from '@kubernetes/client-node';
import { FirebirdClusterController } from '../src/controllers/firebirdcluster.controller';
import { setServerVersion } from '../src/utils/segment-tls-default';
import { PRIMARY_LEASE_ANNOTATION, primaryLeaseEnabled } from '../src/utils/primary-lease';
import { primaryLeaseMigrationMode, primaryLeaseMigrationStep } from '../src/utils/primary-lease-migration';
import { FirebirdCluster } from '../src/types';
import { makeCluster, notFoundError } from './helpers/factories';

/** An idle two-instance cluster with automatic failover, pinned to the operator-moved Lease */
const idle = (annotation = 'pinned', overrides: Partial<FirebirdCluster['spec']> = {}): FirebirdCluster => {
  const cluster = makeCluster({ instances: 2, replication: { enabled: true, failover: { enabled: true } }, ...overrides });
  cluster.metadata.annotations = { [PRIMARY_LEASE_ANNOTATION]: annotation };
  cluster.status = { phase: 'Running', readyInstances: 2 };
  return cluster;
};
const pod = (name: string, holder: boolean, ready = true): V1Pod => ({
  metadata: { name },
  spec: { initContainers: holder ? [{ name: 'lease-holder' }] : [], containers: [] },
  status: { phase: 'Running', conditions: [{ type: 'Ready', status: ready ? 'True' : 'False' }] },
});

describe('moving pinned clusters to the primary Lease', () => {
  it('reads the operator setting', () => {
    expect(primaryLeaseMigrationMode({})).toBe('off');
    expect(primaryLeaseMigrationMode({ PRIMARY_LEASE_MIGRATE: 'pinned' })).toBe('pinned');
    expect(primaryLeaseMigrationMode({ PRIMARY_LEASE_MIGRATE: 'true' })).toBe('off');
  });

  it('starts only with the setting, on pinned clusters, never against the owner', () => {
    expect(primaryLeaseMigrationStep(idle(), 'off', true)).toEqual({ action: 'none', reason: 'off' });
    expect(primaryLeaseMigrationStep(idle(), 'pinned', true)).toEqual({ action: 'start' });
    for (const value of ['enabled', 'done', 'skip']) expect(primaryLeaseMigrationStep(idle(value), 'pinned', true).action).toBe('none');
    expect(primaryLeaseMigrationStep(idle('pinned', { replication: { enabled: true, failover: { enabled: true, primaryLease: { enabled: false } } } }), 'pinned', true).reason).toBe('the owner decided');
    // the annotation makes the Lease effective while it migrates and once done
    expect(primaryLeaseEnabled(idle('in-progress'))).toBe(true);
    expect(primaryLeaseEnabled(idle('done'))).toBe(true);
    expect(primaryLeaseEnabled(idle('pinned'))).toBe(false);
  });

  it('waits for native sidecars, an idle cluster, and no other cluster migrating', () => {
    expect(primaryLeaseMigrationStep(idle(), 'pinned', false).reason).toMatch(/native sidecars/);
    const busy = (status: FirebirdCluster['status']) => {
      const c = idle();
      c.status = { ...c.status, ...status };
      return primaryLeaseMigrationStep(c, 'pinned', true).action;
    };
    expect(busy({ phase: 'Updating' })).toBe('none');
    expect(busy({ readyInstances: 1 })).toBe('none');
    expect(busy({ rollingUpdate: { revision: 'r2' } as never })).toBe('none');
    expect(busy({ switchover: { kind: 'failover', phase: 'Promoting' } as never })).toBe('none');
    expect(busy({ switchover: { kind: 'failover', phase: 'Completed' } as never })).toBe('start');
    expect(busy({ fencedInstances: ['test-cluster-1'] })).toBe('none');
    expect(primaryLeaseMigrationStep(idle('pinned', { hibernated: true }), 'pinned', true).action).toBe('none');
    const other = idle('in-progress');
    other.metadata.name = 'other';
    expect(primaryLeaseMigrationStep(idle(), 'pinned', true, [], [other])).toEqual({ action: 'none', reason: 'default/other is migrating' });
    expect(primaryLeaseMigrationStep(idle(), 'pinned', true, [], [idle()]).action).toBe('start');
  });

  it('finishes once every instance runs the sidecar (at once without failover), and steps back when the owner decides', () => {
    const migrating = idle('in-progress');
    const pods = [pod('test-cluster-0', false), pod('test-cluster-1', true)];
    expect(primaryLeaseMigrationStep(migrating, 'off', true, pods).action).toBe('none');
    pods[0] = pod('test-cluster-0', true, false);
    expect(primaryLeaseMigrationStep(migrating, 'off', true, pods).action).toBe('none');
    pods[0] = pod('test-cluster-0', true);
    expect(primaryLeaseMigrationStep(migrating, 'off', true, pods)).toEqual({ action: 'finish' });
    // no failover: no sidecar to wait for
    const quiet = idle('in-progress', { replication: { enabled: true } });
    expect(primaryLeaseMigrationStep(quiet, 'pinned', true, [pod('test-cluster-0', false), pod('test-cluster-1', false)])).toEqual({ action: 'finish' });
    const decided = idle('in-progress', { replication: { enabled: true, failover: { enabled: true, primaryLease: { enabled: true } } } });
    expect(primaryLeaseMigrationStep(decided, 'pinned', true, pods)).toEqual({ action: 'abandon' });
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

    it('marks the migration and adds the sidecar and the Role in the same reconcile', async () => {
      setServerVersion({ major: 1, minor: 31 });
      vi.stubEnv('PRIMARY_LEASE_MIGRATE', 'pinned');
      const { kubeConfig, api } = mockApi({
        getNamespacedCustomObject: vi.fn().mockResolvedValue({}),
        readNamespacedStatefulSet: vi.fn().mockResolvedValue({ metadata: { name: 'test-cluster' }, spec: { replicas: 2, podManagementPolicy: 'Parallel', template: { spec: { containers: [{ name: 'firebird' }] } } }, status: {} }),
      });
      const cluster = idle();
      await new FirebirdClusterController(kubeConfig).reconcile(cluster);
      const annotations = api('patchNamespacedCustomObject').mock.calls.map((c) => c[0].body?.metadata?.annotations?.[PRIMARY_LEASE_ANNOTATION]).filter(Boolean);
      expect(annotations).toEqual(['in-progress']);
      expect(primaryLeaseEnabled(cluster)).toBe(true);
      const reasons = api('createNamespacedEvent').mock.calls.map((c) => c[0].body.reason);
      expect(reasons).toContain('PrimaryLeaseMigrationStarted');
      expect(api('createNamespacedRole')).toHaveBeenCalled();
      const patched = api('patchNamespacedStatefulSet').mock.calls[0][0].body;
      expect(patched.spec.template.spec.initContainers.map((c: { name: string }) => c.name)).toContain('lease-holder');
    });

    it('does nothing without the setting', async () => {
      setServerVersion({ major: 1, minor: 31 });
      const { kubeConfig, api } = mockApi({ getNamespacedCustomObject: vi.fn().mockResolvedValue({}) });
      await new FirebirdClusterController(kubeConfig).reconcile(idle());
      expect(api('patchNamespacedCustomObject').mock.calls.map((c) => c[0].body?.metadata?.annotations?.[PRIMARY_LEASE_ANNOTATION]).filter(Boolean)).toEqual([]);
      expect(api('createNamespacedRole')).not.toHaveBeenCalled();
    });
  });
});
