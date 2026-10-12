import { describe, it, expect, vi, type Mock } from 'vitest';
import { KubeConfig, V1Service } from '@kubernetes/client-node';
import { FirebirdClusterController } from '../src/controllers/firebirdcluster.controller';
import { FirebirdCluster, ManagedService } from '../src/types';
import {
  buildManagedService,
  MANAGED_SERVICE_LABEL,
  managedServiceDiffers,
  managedServiceSelector,
  managedServicesErrors,
} from '../src/utils/managed-services';
import { validateClusterSpec } from '../src/utils/validation';
import { makeCluster, notFoundError } from './helpers/factories';

const ROLE = 'firebird.cloudnative-firebird.io/role';
const ROUTABLE = 'firebird.cloudnative-firebird.io/read-routable';
const lb: ManagedService = {
  selectorType: 'rw',
  serviceTemplate: {
    metadata: { name: 'db-lb', labels: { team: 'a' }, annotations: { 'lb.example.com/internal': 'true' } },
    spec: { type: 'LoadBalancer', loadBalancerSourceRanges: ['10.0.0.0/8'], ports: [{ port: 3051, nodePort: 30051 }] },
  },
};
const replicated = (managed?: FirebirdCluster['spec']['managed'], routing = false) =>
  makeCluster({ instances: 2, replication: { enabled: true, ...(routing ? { readOnlyRouting: { enabled: true } } : {}) }, ...(managed ? { managed } : {}) });

describe('managed services', () => {
  it('selects the primary, the replicas or every instance', () => {
    expect(managedServiceSelector(replicated(), 'rw')).toMatchObject({ [ROLE]: 'primary' });
    expect(managedServiceSelector(replicated(), 'ro')).toMatchObject({ [ROLE]: 'replica' });
    expect(managedServiceSelector(replicated(undefined, true), 'ro')).toMatchObject({ [ROUTABLE]: 'true' });
    expect(managedServiceSelector(replicated(), 'r')).not.toHaveProperty(ROLE);
    // without replication every instance is a primary of its own
    expect(managedServiceSelector(makeCluster(), 'rw')).not.toHaveProperty(ROLE);
  });

  it('validates the services', () => {
    expect(managedServicesErrors(replicated({ services: { disabledDefaultServices: ['ro'], additional: [lb] } }))).toEqual([]);
    const errors = managedServicesErrors(
      makeCluster({
        managed: {
          services: {
            disabledDefaultServices: ['rw' as never],
            additional: [
              { selectorType: 'ro', serviceTemplate: { metadata: { name: 'test-cluster-replica' } } },
              { selectorType: 'r', serviceTemplate: { metadata: { name: 'Bad_Name' } } },
              { selectorType: 'r', serviceTemplate: { metadata: { name: 'dup' }, spec: { selector: { a: 'b' } } } },
              { selectorType: 'r', updateStrategy: 'recreate' as never, serviceTemplate: { metadata: { name: 'dup' }, spec: { type: 'ExternalName' } } },
            ],
          },
        },
      }),
    );
    expect(errors.join('\n')).toMatch(/only "ro"/);
    expect(errors.join('\n')).toMatch(/selectorType ro needs replication/);
    expect(errors.join('\n')).toMatch(/test-cluster-replica is a default Service/);
    expect(errors.join('\n')).toMatch(/must be a Service name/);
    expect(errors.join('\n')).toMatch(/selector is set by the operator/);
    expect(errors.join('\n')).toMatch(/dup is listed twice/);
    expect(errors.join('\n')).toMatch(/updateStrategy/);
    expect(errors.join('\n')).toMatch(/spec.type must be ClusterIP, NodePort or LoadBalancer/);
    expect(() => validateClusterSpec(makeCluster({ managed: { services: { additional: [{ ...lb, selectorType: 'ro' }] } } }))).toThrow(/needs replication/);
  });

  it('builds a Service from the template, with the operator\'s selector and port', () => {
    const service = buildManagedService(replicated(), lb);
    expect(service.metadata).toMatchObject({
      name: 'db-lb',
      labels: { team: 'a', [MANAGED_SERVICE_LABEL]: 'rw', 'firebird.cloudnative-firebird.io/cluster': 'test-cluster' },
      annotations: { 'lb.example.com/internal': 'true' },
    });
    expect(service.metadata?.ownerReferences?.[0]).toMatchObject({ kind: 'FirebirdCluster', uid: 'test-uid-1234' });
    expect(service.spec).toMatchObject({
      type: 'LoadBalancer',
      loadBalancerSourceRanges: ['10.0.0.0/8'],
      selector: expect.objectContaining({ [ROLE]: 'primary' }),
      ports: [{ name: 'firebird', port: 3051, targetPort: 3050, protocol: 'TCP', nodePort: 30051 }],
    });
    expect(buildManagedService(replicated(), { selectorType: 'r', serviceTemplate: { metadata: { name: 'all' } } }).spec).toMatchObject({
      type: 'ClusterIP',
      ports: [{ port: 3050 }],
    });
  });

  it('compares only what the operator sets', () => {
    const desired = buildManagedService(replicated(), lb);
    // what the API server returns: filled-in fields and labels of others
    const existing: V1Service = {
      metadata: { ...desired.metadata, labels: { ...desired.metadata!.labels, other: 'x' }, annotations: { ...desired.metadata!.annotations, 'cloud/added': 'y' } },
      spec: { ...desired.spec, clusterIP: '10.96.1.2', sessionAffinity: 'None', ports: [{ ...desired.spec!.ports![0], targetPort: '3050' as never }] },
    };
    expect(managedServiceDiffers(existing, desired)).toBe(false);
    expect(managedServiceDiffers({ ...existing, metadata: { ...existing.metadata, annotations: {} } }, desired)).toBe(true);
    expect(managedServiceDiffers({ ...existing, spec: { ...existing.spec, type: 'ClusterIP' } }, desired)).toBe(true);
    expect(managedServiceDiffers({ ...existing, spec: { ...existing.spec, selector: {} } }, desired)).toBe(true);
    expect(managedServiceDiffers({ ...existing, spec: { ...existing.spec, ports: [{ port: 3051, nodePort: 31000, targetPort: 3050 as never }] } }, desired)).toBe(true);
    // a nodePort the API server allocated is not a difference
    const plain = buildManagedService(replicated(), { selectorType: 'r', serviceTemplate: { metadata: { name: 'all' }, spec: { type: 'NodePort' } } });
    expect(managedServiceDiffers({ ...plain, spec: { ...plain.spec, ports: [{ ...plain.spec!.ports![0], nodePort: 31234 }] } }, plain)).toBe(false);
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

describe('cluster reconcile with managed services', () => {
  const owner = { kind: 'FirebirdCluster', name: 'test-cluster', uid: 'test-uid-1234' };
  const lastStatus = (api: (m: string) => Mock) => api('patchNamespacedCustomObjectStatus').mock.calls.at(-1)?.[0].body[0].value;
  const createdServices = (api: (m: string) => Mock) => api('createNamespacedService').mock.calls.map((c) => c[0].body as V1Service);

  it('creates the managed Services, records them, and leaves out a disabled replica Service', async () => {
    const { kubeConfig, api } = mockApi();
    await new FirebirdClusterController(kubeConfig).reconcile(
      replicated({ services: { disabledDefaultServices: ['ro'], additional: [lb, { selectorType: 'r', serviceTemplate: { metadata: { name: 'all' } } }] } }),
    );
    const names = createdServices(api).map((s) => s.metadata?.name);
    expect(names).toEqual(expect.arrayContaining(['test-cluster', 'test-cluster-headless', 'db-lb', 'all']));
    expect(names).not.toContain('test-cluster-replica');
    expect(lastStatus(api).managedServices).toEqual(['all', 'db-lb']);
  });

  it('patches or replaces a changed Service, deletes one no longer listed, and leaves another owner\'s alone', async () => {
    const cluster = {
      ...replicated({
        services: {
          additional: [
            lb,
            { selectorType: 'r' as const, updateStrategy: 'replace' as const, serviceTemplate: { metadata: { name: 'all' }, spec: { type: 'NodePort' } } },
            { selectorType: 'r' as const, serviceTemplate: { metadata: { name: 'taken' } } },
          ],
        },
      }),
      status: { phase: 'Running' as const, managedServices: ['all', 'db-lb', 'gone'] },
    };
    const existing: Record<string, V1Service> = {
      'db-lb': { metadata: { name: 'db-lb', ownerReferences: [owner], labels: {} }, spec: { type: 'ClusterIP', ports: [{ port: 3051 }] } },
      all: { metadata: { name: 'all', ownerReferences: [owner], labels: {} }, spec: { type: 'ClusterIP', ports: [{ port: 3050 }] } },
      taken: { metadata: { name: 'taken', ownerReferences: [{ ...owner, uid: 'someone-else' }] }, spec: {} },
      gone: { metadata: { name: 'gone', ownerReferences: [owner], labels: { [MANAGED_SERVICE_LABEL]: 'r' } }, spec: {} },
    };
    const { kubeConfig, api } = mockApi({
      getNamespacedCustomObject: vi.fn().mockResolvedValue(cluster),
      readNamespacedService: vi.fn().mockImplementation(({ name }) => (existing[name] ? Promise.resolve(existing[name]) : Promise.reject(notFoundError))),
    });
    await new FirebirdClusterController(kubeConfig).reconcile(cluster);
    const patched = api('patchNamespacedService').mock.calls.map((c) => c[0]);
    expect(patched.find((p) => p.name === 'db-lb')?.body.spec).toMatchObject({ type: 'LoadBalancer' });
    const deleted = api('deleteNamespacedService').mock.calls.map((c) => c[0].name);
    expect(deleted).toEqual(expect.arrayContaining(['all', 'gone']));
    expect(createdServices(api).map((s) => s.metadata?.name)).toContain('all');
    expect(deleted).not.toContain('taken');
    expect(api('createNamespacedEvent').mock.calls.map((c) => c[0].body.reason)).toContain('ManagedServiceConflict');
    expect(lastStatus(api).managedServices).toEqual(['all', 'db-lb', 'taken']);
  });

  it('keeps the role label on the instances for rw and ro without read-only routing', async () => {
    const pod = (name: string, role?: string) => ({ metadata: { name, labels: role ? { [ROLE]: role } : {} }, status: { phase: 'Running' } });
    const { kubeConfig, api } = mockApi({
      listNamespacedPod: vi.fn().mockResolvedValue({ items: [pod('test-cluster-0', 'primary'), pod('test-cluster-1')] }),
    });
    await new FirebirdClusterController(kubeConfig).reconcile(replicated());
    const patches = api('patchNamespacedPod').mock.calls.map((c) => c[0]).filter((p) => p.body?.metadata?.labels?.[ROLE]);
    expect(patches).toEqual([expect.objectContaining({ name: 'test-cluster-1', body: { metadata: { labels: { [ROLE]: 'replica' } } } })]);
  });

  it('applies a changed serviceType and serviceAnnotations to the default Services', async () => {
    const cluster = { ...makeCluster({ serviceType: 'LoadBalancer', serviceAnnotations: { a: 'b' } }), status: { phase: 'Running' as const } };
    const { kubeConfig, api } = mockApi({
      getNamespacedCustomObject: vi.fn().mockResolvedValue(cluster),
      readNamespacedService: vi.fn().mockImplementation(({ name }) =>
        Promise.resolve({ metadata: { name, annotations: { kept: 'x' } }, spec: { type: 'ClusterIP', selector: {} } }),
      ),
    });
    await new FirebirdClusterController(kubeConfig).reconcile(cluster);
    const settings = api('patchNamespacedService').mock.calls.map((c) => c[0]).find((p) => p.name === 'test-cluster' && p.body?.spec?.type);
    expect(settings?.body).toEqual({ metadata: { annotations: { a: 'b' } }, spec: { type: 'LoadBalancer' } });
  });
});
