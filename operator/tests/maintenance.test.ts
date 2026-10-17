import { describe, it, expect, vi, type Mock } from 'vitest';
import { KubeConfig, V1Pod } from '@kubernetes/client-node';
import { FirebirdClusterController } from '../src/controllers/firebirdcluster.controller';
import { maintenanceRecreatesVolumes, maintenanceVolumeRecreations } from '../src/utils/maintenance';
import { makeCluster, notFoundError } from './helpers/factories';

const now = new Date('2026-10-17T12:00:00Z');
const pod = (name: string, options: { pendingSince?: string; reason?: string; annotations?: Record<string, string> } = {}): V1Pod => ({
  metadata: {
    name,
    annotations: options.annotations ?? {},
    labels: { 'firebird.cloudnative-firebird.io/cluster': 'test-cluster', 'app.kubernetes.io/component': 'database' },
  },
  status: options.pendingSince
    ? {
        phase: 'Pending',
        conditions: [{ type: 'PodScheduled', status: 'False', reason: options.reason ?? 'Unschedulable', lastTransitionTime: new Date(options.pendingSince) }],
      }
    : { phase: 'Running', conditions: [{ type: 'Ready', status: 'True' }] },
});
const windowCluster = (reusePVC?: boolean, replication = true) =>
  makeCluster({ instances: 3, replication: { enabled: replication }, nodeMaintenanceWindow: { inProgress: true, ...(reusePVC !== undefined ? { reusePVC } : {}) } });

describe('maintenance window with reusePVC: false', () => {
  it('applies to open windows of replication clusters that do not reuse volumes', () => {
    expect(maintenanceRecreatesVolumes(windowCluster(false))).toBe(true);
    expect(maintenanceRecreatesVolumes(windowCluster())).toBe(false);
    expect(maintenanceRecreatesVolumes(windowCluster(true))).toBe(false);
    expect(maintenanceRecreatesVolumes(windowCluster(false, false))).toBe(false);
    expect(maintenanceRecreatesVolumes(makeCluster({ replication: { enabled: true }, nodeMaintenanceWindow: { inProgress: false, reusePVC: false } }))).toBe(false);
  });

  it('re-creates the volumes of replicas unschedulable for the grace period only', () => {
    const pods = [
      pod('test-cluster-0', { pendingSince: '2026-10-17T11:58:00Z' }), // the primary
      pod('test-cluster-1', { pendingSince: '2026-10-17T11:59:00Z' }), // unschedulable for 60s
      pod('test-cluster-2', { pendingSince: '2026-10-17T11:59:50Z' }), // for 10s only
      pod('test-cluster-3', { pendingSince: '2026-10-17T11:58:00Z', reason: 'SchedulerError' }),
      pod('test-cluster-4'),
      pod('test-cluster-5', { pendingSince: '2026-10-17T11:58:00Z', annotations: { 'firebird.cloudnative-firebird.io/reseed': 'volume' } }),
      pod('test-cluster-6', { pendingSince: '2026-10-17T11:58:00Z' }), // being re-created already
    ];
    const cluster = { ...windowCluster(false), status: { recreatingVolumes: [{ pod: 'test-cluster-6', claimUid: 'u' }] } };
    expect(maintenanceVolumeRecreations(cluster, 'test-cluster-0', pods, now)).toEqual(['test-cluster-1']);
    expect(maintenanceVolumeRecreations(windowCluster(), 'test-cluster-0', pods, now)).toEqual([]);
  });
});

/** API clients whose calls default to: read → 404, list → empty, others → {} */
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

describe('cluster reconcile in a maintenance window with reusePVC: false', () => {
  it('asks for the volume re-creation of an unschedulable replica, and reports it', async () => {
    vi.useFakeTimers({ now, toFake: ['Date'] });
    try {
      const { kubeConfig, api } = mockApi({
        listNamespacedPod: vi.fn().mockResolvedValue({ items: [pod('test-cluster-0'), pod('test-cluster-1', { pendingSince: '2026-10-17T11:59:00Z' })] }),
      });
      await new FirebirdClusterController(kubeConfig).reconcile(windowCluster(false));
      const patches = api('patchNamespacedPod').mock.calls.map((c) => c[0]).filter((p) => !Array.isArray(p.body));
      expect(patches).toEqual([
        { name: 'test-cluster-1', namespace: 'default', body: { metadata: { annotations: { 'firebird.cloudnative-firebird.io/reseed': 'volume' } } } },
      ]);
      expect(api('createNamespacedEvent').mock.calls.map((c) => c[0].body.reason)).toContain('MaintenanceVolumeRecreation');
    } finally {
      vi.useRealTimers();
    }
  });
});
