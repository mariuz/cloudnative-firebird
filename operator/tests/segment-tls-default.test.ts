import { describe, it, expect, vi, afterEach, type Mock } from 'vitest';
import { KubeConfig } from '@kubernetes/client-node';
import { FirebirdClusterController } from '../src/controllers/firebirdcluster.controller';
import { createAdmissionValidator } from '../src/utils/admission';
import { nativeSidecarsSupported, parseServerVersion, segmentTlsDefault, segmentTlsRequired, setServerVersion } from '../src/utils/segment-tls-default';
import { metrics } from '../src/utils/metrics';
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

/** A cluster as it was created by the user: no segmentTLS */
const undecided = () => {
  const cluster = makeCluster();
  delete cluster.spec.segmentTLS;
  return cluster;
};

const specPatches = (api: (m: string) => Mock) =>
  api('patchNamespacedCustomObject').mock.calls.map((c) => c[0].body).filter((b) => b.spec);

afterEach(() => {
  setServerVersion(undefined);
  vi.unstubAllEnvs();
});

describe('the segment TLS default', () => {
  it('follows the Kubernetes version (native sidecars from 1.29) unless the operator is told otherwise', () => {
    expect(parseServerVersion('1', '29+')).toEqual({ major: 1, minor: 29 });
    expect(parseServerVersion('1', '')).toBeUndefined();
    setServerVersion(undefined);
    expect(nativeSidecarsSupported()).toBeUndefined();
    expect(segmentTlsDefault({})).toBe(false);
    setServerVersion({ major: 1, minor: 28 });
    expect(segmentTlsDefault({})).toBe(false);
    expect(segmentTlsDefault({ SEGMENT_TLS_DEFAULT: 'true' })).toBe(true);
    setServerVersion({ major: 1, minor: 31 });
    expect(segmentTlsDefault({})).toBe(true);
    expect(segmentTlsDefault({ SEGMENT_TLS_DEFAULT: 'auto' })).toBe(true);
    expect(segmentTlsDefault({ SEGMENT_TLS_DEFAULT: 'false' })).toBe(false);
  });

  it('is written into a new cluster\'s spec before its StatefulSet is created', async () => {
    setServerVersion({ major: 1, minor: 31 });
    const { kubeConfig, api } = mockApi({ getNamespacedCustomObject: vi.fn().mockResolvedValue({}) });
    const cluster = undecided();
    await new FirebirdClusterController(kubeConfig).reconcile(cluster);
    expect(specPatches(api)).toEqual([{ spec: { segmentTLS: { enabled: true } } }]);
    expect(cluster.spec.segmentTLS).toEqual({ enabled: true });
    // the StatefulSet created in the same reconcile runs the proxy
    const sts = api('createNamespacedStatefulSet').mock.calls[0][0].body;
    expect(sts.spec.template.spec.initContainers[0].name).toBe('segment-tls');
    const reasons = api('createNamespacedEvent').mock.calls.map((c) => c[0].body.reason);
    expect(reasons).toContain('SegmentTLSDefaulted');
  });

  it('pins an existing cluster (one with a StatefulSet) to plain segment shipping', async () => {
    setServerVersion({ major: 1, minor: 31 });
    // the cluster exists when the reconcile starts and is gone once it patches the StatefulSet,
    // which ends the reconcile quietly right after the default was written
    const { kubeConfig, api } = mockApi({
      getNamespacedCustomObject: vi.fn().mockResolvedValueOnce({}).mockRejectedValue(notFoundError),
      readNamespacedStatefulSet: vi.fn().mockResolvedValue({ metadata: { name: 'test-cluster' }, spec: { replicas: 9, podManagementPolicy: 'Parallel' }, status: {} }),
      patchNamespacedStatefulSet: vi.fn().mockRejectedValue(notFoundError),
    });
    const cluster = undecided();
    cluster.status = { phase: 'Running' };
    await new FirebirdClusterController(kubeConfig).reconcile(cluster);
    // marked as pinned by the operator, so SEGMENT_TLS_MIGRATE=pinned can move it later
    expect(specPatches(api)).toEqual([
      { metadata: { annotations: { 'firebird.cloudnative-firebird.io/segment-tls-migration': 'pinned' } }, spec: { segmentTLS: { enabled: false } } },
    ]);
  });

  it('leaves a cluster that has a value alone', async () => {
    setServerVersion({ major: 1, minor: 31 });
    const { kubeConfig, api } = mockApi({ getNamespacedCustomObject: vi.fn().mockResolvedValue({}) });
    await new FirebirdClusterController(kubeConfig).reconcile(makeCluster({ segmentTLS: { enabled: false } }));
    expect(specPatches(api)).toEqual([]);
  });

  it('warns at admission when segment TLS is on and Kubernetes has no native sidecars', async () => {
    const validate = createAdmissionValidator({ secret: async () => undefined, cluster: async () => undefined } as never);
    const request = (enabled: boolean) => ({
      uid: '1',
      kind: { group: 'firebird.cloudnative-firebird.io', version: 'v1', kind: 'FirebirdCluster' },
      operation: 'CREATE',
      namespace: 'default',
      object: makeCluster({ segmentTLS: { enabled } }),
    });
    setServerVersion({ major: 1, minor: 28 });
    expect((await validate(request(true) as never)).warnings ?? []).toContainEqual(expect.stringMatching(/Kubernetes 1\.29/));
    expect((await validate(request(false) as never)).warnings ?? []).not.toContainEqual(expect.stringMatching(/Kubernetes 1\.29/));
    setServerVersion({ major: 1, minor: 30 });
    expect((await validate(request(true) as never)).warnings ?? []).not.toContainEqual(expect.stringMatching(/Kubernetes 1\.29/));
  });

  describe('with SEGMENT_TLS_REQUIRED', () => {
    const validate = createAdmissionValidator({ secret: async () => undefined, cluster: async () => undefined } as never);
    const request = (operation: 'CREATE' | 'UPDATE', enabled: boolean | undefined, before?: boolean) => ({
      uid: '1',
      kind: { group: 'firebird.cloudnative-firebird.io', version: 'v1', kind: 'FirebirdCluster' },
      operation,
      namespace: 'default',
      object: enabled === undefined ? undecided() : makeCluster({ segmentTLS: { enabled } }),
      ...(operation === 'UPDATE' ? { oldObject: makeCluster({ segmentTLS: { enabled: before ?? true }, instances: 2 }) } : {}),
    });

    it('reads the setting and makes segment TLS the default whatever the Kubernetes version', () => {
      expect(segmentTlsRequired({})).toBe(false);
      expect(segmentTlsRequired({ SEGMENT_TLS_REQUIRED: 'true' })).toBe(true);
      setServerVersion({ major: 1, minor: 28 });
      expect(segmentTlsDefault({ SEGMENT_TLS_REQUIRED: 'true', SEGMENT_TLS_DEFAULT: 'false' })).toBe(true);
    });

    it('refuses a cluster created with segment TLS off, or switched off; existing plain clusters may still change', async () => {
      vi.stubEnv('SEGMENT_TLS_REQUIRED', 'true');
      setServerVersion({ major: 1, minor: 31 });
      expect((await validate(request('CREATE', false) as never)).denied).toMatch(/requires segment TLS/);
      expect((await validate(request('CREATE', true) as never)).denied).toBeUndefined();
      expect((await validate(request('CREATE', undefined) as never)).denied).toBeUndefined();
      // turning it off
      expect((await validate(request('UPDATE', false, true) as never)).denied).toMatch(/requires segment TLS/);
      // a plain cluster (pinned on upgrade) scaled: not refused
      expect((await validate(request('UPDATE', false, false) as never)).denied).toBeUndefined();
      // without the setting nothing is refused
      vi.stubEnv('SEGMENT_TLS_REQUIRED', '');
      expect((await validate(request('CREATE', false) as never)).denied).toBeUndefined();
    });

    it('reports a plain cluster with a condition, an event and the metric, without restarting it', async () => {
      vi.stubEnv('SEGMENT_TLS_REQUIRED', 'true');
      setServerVersion({ major: 1, minor: 31 });
      const { kubeConfig, api } = mockApi({ getNamespacedCustomObject: vi.fn().mockResolvedValue({}) });
      await new FirebirdClusterController(kubeConfig).reconcile(makeCluster({ segmentTLS: { enabled: false } }));
      expect(specPatches(api)).toEqual([]);
      const statuses = api('patchNamespacedCustomObjectStatus').mock.calls.map((c) => c[0].body[0].value);
      const condition = statuses.at(-1).conditions.find((c: { type: string }) => c.type === 'SegmentTLS');
      expect(condition).toMatchObject({ status: 'False', reason: 'PlainSegmentShipping' });
      expect(api('createNamespacedEvent').mock.calls.map((c) => c[0].body.reason)).toContain('SegmentTLSRequired');
      expect(metrics.render()).toContain('firebird_cluster_segment_tls{namespace="default",cluster="test-cluster"} 0');
      // a cluster on segment TLS: no condition
      const on = mockApi({ getNamespacedCustomObject: vi.fn().mockResolvedValue({}) });
      await new FirebirdClusterController(on.kubeConfig).reconcile(makeCluster({ segmentTLS: { enabled: true } }));
      const last = on.api('patchNamespacedCustomObjectStatus').mock.calls.map((c) => c[0].body[0].value).at(-1);
      expect(last.conditions.some((c: { type: string }) => c.type === 'SegmentTLS')).toBe(false);
      expect(metrics.render()).toContain('firebird_cluster_segment_tls{namespace="default",cluster="test-cluster"} 1');
    });
  });
});
