import { describe, it, expect, vi, type Mock } from 'vitest';
import { KubeConfig, V1PersistentVolumeClaim, V1Pod } from '@kubernetes/client-node';
import { FirebirdClusterController } from '../src/controllers/firebirdcluster.controller';
import { FirebirdCluster } from '../src/types';
import { clusterLabels } from '../src/utils/resources';
import { READ_ROUTABLE_LABEL, REPLICATION_LAG_ANNOTATION, ROLE_LABEL } from '../src/utils/routing';
import { makeCluster, notFoundError } from './helpers/factories';

type Overrides = Record<string, Mock>;

/**
 * Build a KubeConfig whose API clients default every call:
 * read/get → 404, list → empty, create/patch/delete → {}. Individual methods can be overridden.
 */
function makeMockKubeConfig(overrides: Overrides = {}) {
  const calls: Record<string, Mock> = {};
  const fn = (method: string): Mock => {
    if (!calls[method]) {
      calls[method] =
        overrides[method] ??
        (method.startsWith('read') || method.startsWith('get')
          ? vi.fn().mockRejectedValue(notFoundError)
          : method.startsWith('list')
            ? vi.fn().mockResolvedValue({ items: [] })
            : vi.fn().mockResolvedValue({}));
    }
    return calls[method];
  };
  const api = new Proxy({}, { get: (_t, prop: string) => fn(prop) });
  const kubeConfig = new KubeConfig();
  vi.spyOn(kubeConfig, 'makeApiClient').mockReturnValue(api as never);
  return { kubeConfig, api: fn };
}

const existingStatefulSet = (cluster: FirebirdCluster, size = '1Gi') => ({
  metadata: { name: cluster.metadata.name },
  spec: {
    replicas: cluster.spec.instances,
    template: { spec: { containers: [{ name: 'firebird', image: 'old:image' }] } },
    volumeClaimTemplates: [
      { metadata: { name: 'firebird-data' }, spec: { resources: { requests: { storage: size } } } },
    ],
  },
  status: { readyReplicas: cluster.spec.instances },
});

const pvc = (name: string, size: string, capacity = size): V1PersistentVolumeClaim => ({
  metadata: { name },
  spec: { resources: { requests: { storage: size } } },
  status: { capacity: { storage: capacity } },
});

const statusPatches = (api: (m: string) => Mock) =>
  api('patchNamespacedCustomObjectStatus').mock.calls.map(
    (c) => (c[0] as { body: Array<{ value: Record<string, unknown> }> }).body[0].value,
  );

describe('FirebirdClusterController – volume expansion', () => {
  const cluster = makeCluster({ instances: 2, storage: { size: '5Gi' } });

  it('patches PVCs whose request is below spec.storage.size', async () => {
    const { kubeConfig, api } = makeMockKubeConfig({
      readNamespacedStatefulSet: vi.fn().mockResolvedValue(existingStatefulSet(cluster)),
      listNamespacedPersistentVolumeClaim: vi.fn().mockResolvedValue({
        items: [
          pvc('firebird-data-test-cluster-0', '1Gi'),
          pvc('firebird-data-test-cluster-1', '5Gi'),
          pvc('unrelated-claim', '1Gi'),
        ],
      }),
    });

    await new FirebirdClusterController(kubeConfig).reconcile(cluster);

    expect(api('listNamespacedPersistentVolumeClaim')).toHaveBeenCalledWith({
      namespace: 'default',
      labelSelector: 'firebird.cloudnative-firebird.io/cluster=test-cluster',
    });
    expect(api('patchNamespacedPersistentVolumeClaim')).toHaveBeenCalledTimes(1);
    expect(api('patchNamespacedPersistentVolumeClaim')).toHaveBeenCalledWith({
      name: 'firebird-data-test-cluster-0',
      namespace: 'default',
      body: [{ op: 'replace', path: '/spec/resources/requests/storage', value: '5Gi' }],
    });

    const final = statusPatches(api).at(-1)!;
    expect(final.volumes).toEqual([
      expect.objectContaining({ name: 'firebird-data-test-cluster-0', state: 'Resizing', requestedSize: '5Gi' }),
      expect.objectContaining({ name: 'firebird-data-test-cluster-1', state: 'Ready' }),
    ]);
  });

  it('keeps the existing immutable volumeClaimTemplates when patching the StatefulSet', async () => {
    const { kubeConfig, api } = makeMockKubeConfig({
      readNamespacedStatefulSet: vi.fn().mockResolvedValue(existingStatefulSet(cluster, '1Gi')),
      patchNamespacedStatefulSet: vi.fn().mockImplementation(async ({ body }) => body),
    });

    await new FirebirdClusterController(kubeConfig).reconcile(cluster);

    const body = api('patchNamespacedStatefulSet').mock.calls[0][0].body;
    expect(body.spec.volumeClaimTemplates[0].spec.resources.requests.storage).toBe('1Gi');
  });

  it('records ResizeFailed when the API server rejects the expansion', async () => {
    const { kubeConfig, api } = makeMockKubeConfig({
      readNamespacedStatefulSet: vi.fn().mockResolvedValue(existingStatefulSet(cluster)),
      listNamespacedPersistentVolumeClaim: vi.fn().mockResolvedValue({
        items: [pvc('firebird-data-test-cluster-0', '1Gi')],
      }),
      patchNamespacedPersistentVolumeClaim: vi
        .fn()
        .mockRejectedValue(new Error('storageclass does not support resize')),
    });

    await new FirebirdClusterController(kubeConfig).reconcile(cluster);

    const final = statusPatches(api).at(-1)!;
    expect(final.phase).not.toBe('Degraded');
    expect(final.volumes).toEqual([
      {
        name: 'firebird-data-test-cluster-0',
        requestedSize: '1Gi',
        capacity: '1Gi',
        state: 'ResizeFailed',
        message: 'storageclass does not support resize',
      },
    ]);
  });

  it('never shrinks a PVC', async () => {
    const small = makeCluster({ instances: 1, storage: { size: '1Gi' } });
    const { kubeConfig, api } = makeMockKubeConfig({
      readNamespacedStatefulSet: vi.fn().mockResolvedValue(existingStatefulSet(small, '1Gi')),
      listNamespacedPersistentVolumeClaim: vi.fn().mockResolvedValue({
        items: [pvc('firebird-data-test-cluster-0', '10Gi')],
      }),
    });

    await new FirebirdClusterController(kubeConfig).reconcile(small);

    expect(api('patchNamespacedPersistentVolumeClaim')).not.toHaveBeenCalled();
    expect(statusPatches(api).at(-1)!.volumes).toEqual([
      expect.objectContaining({ state: 'ShrinkRejected' }),
    ]);
  });

  it('skips PVC expansion when the StatefulSet is newly created', async () => {
    const { kubeConfig, api } = makeMockKubeConfig();

    await new FirebirdClusterController(kubeConfig).reconcile(cluster);

    expect(api('listNamespacedPersistentVolumeClaim')).not.toHaveBeenCalled();
  });
});

describe('FirebirdClusterController – smart read-only routing', () => {
  const routedCluster = makeCluster({
    instances: 3,
    replication: { enabled: true, readOnlyRouting: { enabled: true, maxLagSeconds: 10 } },
  });

  const pod = (name: string, lag?: string, labels: Record<string, string> = {}): V1Pod => ({
    metadata: {
      name,
      labels: { ...clusterLabels('test-cluster'), ...labels },
      ...(lag ? { annotations: { [REPLICATION_LAG_ANNOTATION]: lag } } : {}),
    },
    status: { phase: 'Running', conditions: [{ type: 'Ready', status: 'True' }] },
  });

  it('labels pods by role and lag-aware read-routability', async () => {
    const { kubeConfig, api } = makeMockKubeConfig({
      listNamespacedPod: vi.fn().mockResolvedValue({
        items: [
          pod('test-cluster-0'),
          pod('test-cluster-1', '2'),
          pod('test-cluster-2', '60', { [ROLE_LABEL]: 'replica', [READ_ROUTABLE_LABEL]: 'false' }),
        ],
      }),
    });

    await new FirebirdClusterController(kubeConfig).reconcile(routedCluster);

    const patched = Object.fromEntries(
      api('patchNamespacedPod').mock.calls.map((c) => [c[0].name, c[0].body]),
    );
    expect(Object.keys(patched).sort()).toEqual(['test-cluster-0', 'test-cluster-1']);
    expect(patched['test-cluster-0']).toContainEqual(
      expect.objectContaining({ path: '/metadata/labels/firebird.cloudnative-firebird.io~1role', value: 'primary' }),
    );
    expect(patched['test-cluster-1']).toContainEqual(
      expect.objectContaining({ path: '/metadata/labels/firebird.cloudnative-firebird.io~1read-routable', value: 'true' }),
    );

    const replication = statusPatches(api).at(-1)!.replicationStatus as Record<string, unknown>;
    expect(replication.primaryPod).toBe('test-cluster-0');
    expect(replication.readRoutablePods).toEqual(['test-cluster-1']);
    expect(replication.laggingReplicas).toEqual(['test-cluster-2']);
  });

  it('uses the leader lease holder as primary', async () => {
    const { kubeConfig, api } = makeMockKubeConfig({
      readNamespacedLease: vi.fn().mockResolvedValue({ spec: { holderIdentity: 'test-cluster-1' } }),
      listNamespacedPod: vi.fn().mockResolvedValue({ items: [pod('test-cluster-0'), pod('test-cluster-1')] }),
    });

    await new FirebirdClusterController(kubeConfig).reconcile(routedCluster);

    const replication = statusPatches(api).at(-1)!.replicationStatus as Record<string, unknown>;
    expect(replication.primaryPod).toBe('test-cluster-1');
    expect(replication.readRoutablePods).toEqual(['test-cluster-0']);
  });

  it('creates services with role-aware selectors', async () => {
    const { kubeConfig, api } = makeMockKubeConfig();

    await new FirebirdClusterController(kubeConfig).reconcile(routedCluster);

    const selectors = Object.fromEntries(
      api('createNamespacedService').mock.calls.map((c) => [c[0].body.metadata.name, c[0].body.spec.selector]),
    );
    expect(selectors['test-cluster'][ROLE_LABEL]).toBe('primary');
    expect(selectors['test-cluster-replica'][READ_ROUTABLE_LABEL]).toBe('true');
    expect(selectors['test-cluster-headless'][ROLE_LABEL]).toBeUndefined();
  });

  it('patches existing service selectors when routing is enabled', async () => {
    const { kubeConfig, api } = makeMockKubeConfig({
      readNamespacedService: vi.fn().mockResolvedValue({ spec: { selector: clusterLabels('test-cluster') } }),
    });

    await new FirebirdClusterController(kubeConfig).reconcile(routedCluster);

    const patched = api('patchNamespacedService').mock.calls.map((c) => c[0].name).sort();
    expect(patched).toEqual(['test-cluster', 'test-cluster-replica']);
  });

  it('leaves pods and selectors untouched when routing is disabled', async () => {
    const { kubeConfig, api } = makeMockKubeConfig({
      readNamespacedService: vi.fn().mockResolvedValue({ spec: { selector: clusterLabels('test-cluster') } }),
    });

    await new FirebirdClusterController(kubeConfig).reconcile(
      makeCluster({ instances: 2, replication: { enabled: true } }),
    );

    expect(api('listNamespacedPod')).not.toHaveBeenCalled();
    expect(api('patchNamespacedPod')).not.toHaveBeenCalled();
    expect(api('patchNamespacedService')).not.toHaveBeenCalled();
  });
});
