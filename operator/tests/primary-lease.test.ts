import { describe, it, expect, vi, beforeAll } from 'vitest';
import { makeCluster } from './helpers/factories';
import { buildStatefulSet } from '../src/utils/resources';
import { setOperatorImage } from '../src/utils/operator-image';
import { leaseExpired, leaseAgeSeconds, primaryLeaseEnabled, primaryLeaseDurationSeconds, primaryLeaseDefault, PRIMARY_LEASE_ANNOTATION } from '../src/utils/primary-lease';
import { setServerVersion } from '../src/utils/segment-tls-default';
import { FirebirdClusterController } from '../src/controllers/firebirdcluster.controller';
import { KubeConfig, type V1Container, type V1PodSpec } from '@kubernetes/client-node';
import { notFoundError } from './helpers/factories';
import type { FirebirdCluster } from '../src/types';
import { afterEach, type Mock } from 'vitest';
import { validateClusterSpec } from '../src/utils/validation';

const env = (c: V1Container | undefined) => Object.fromEntries((c?.env ?? []).map((e) => [e.name, e.value ?? JSON.stringify(e.valueFrom)]));
const podSpec = (cluster = makeCluster()) => buildStatefulSet(cluster).spec!.template.spec as V1PodSpec;
const named = (spec: V1PodSpec, name: string) => [...(spec.initContainers ?? []), ...spec.containers].find((c) => c.name === name);
const withLease = (more: object = {}) =>
  makeCluster({ instances: 3, replication: { enabled: true, failover: { enabled: true, primaryLease: { enabled: true } } }, ...more });

describe('the primary Lease', () => {
  beforeAll(() => setOperatorImage('registry.example/cloudnative-firebird:1.2.3'));

  it('is held only with automatic failover and primaryLease.enabled, or the recorded default', () => {
    expect(primaryLeaseEnabled(makeCluster())).toBe(false);
    expect(primaryLeaseEnabled(makeCluster({ replication: { enabled: true, failover: { enabled: true } } }))).toBe(false);
    expect(primaryLeaseEnabled(makeCluster({ replication: { enabled: true, failover: { enabled: false, primaryLease: { enabled: true } } } }))).toBe(false);
    expect(primaryLeaseEnabled(withLease())).toBe(true);
    // the default recorded on the first reconcile (v0.87.0), unless the owner decided
    const recorded = (value: string, primaryLease?: object) => {
      const c = makeCluster({ replication: { enabled: true, failover: { enabled: true, ...(primaryLease ? { primaryLease } : {}) } } });
      c.metadata.annotations = { [PRIMARY_LEASE_ANNOTATION]: value };
      return c;
    };
    expect(primaryLeaseEnabled(recorded('enabled'))).toBe(true);
    expect(primaryLeaseEnabled(recorded('pinned'))).toBe(false);
    expect(primaryLeaseEnabled(recorded('enabled', { enabled: false }))).toBe(false);
    expect(primaryLeaseEnabled(recorded('pinned', { enabled: true }))).toBe(true);
    const noFailover = makeCluster({ replication: { enabled: true } });
    noFailover.metadata.annotations = { [PRIMARY_LEASE_ANNOTATION]: 'enabled' };
    expect(primaryLeaseEnabled(noFailover)).toBe(false);
    expect(primaryLeaseDurationSeconds(withLease())).toBe(15);
    expect(() => validateClusterSpec(makeCluster({ replication: { enabled: true, failover: { enabled: true, primaryLease: { enabled: true, durationSeconds: 3 } } } }))).toThrow(/durationSeconds/);
    expect(() => validateClusterSpec(withLease())).not.toThrow();
  });

  it('tells an expired Lease from a renewed one', () => {
    const now = Date.parse('2026-10-10T10:00:00Z');
    const lease = (secondsAgo: number, duration?: number, holder = 'db-0') => ({
      spec: { holderIdentity: holder, renewTime: new Date(now - secondsAgo * 1000).toISOString(), ...(duration ? { leaseDurationSeconds: duration } : {}) },
    });
    expect(leaseExpired(undefined, 15, now)).toBe(true);
    expect(leaseExpired({ spec: {} }, 15, now)).toBe(true);
    expect(leaseExpired(lease(3, 15), 15, now)).toBe(false);
    expect(leaseExpired(lease(15, 15), 15, now)).toBe(true);
    // the holder's own duration wins over the cluster's; without it the cluster's applies
    expect(leaseExpired(lease(20, 30), 15, now)).toBe(false);
    expect(leaseExpired(lease(20), 15, now)).toBe(true);
    expect(leaseExpired(lease(3, 15, ''), 15, now)).toBe(true);
    expect(leaseAgeSeconds(lease(7), now)).toBe(7);
    expect(leaseAgeSeconds({ spec: { holderIdentity: 'db-0' } }, now)).toBeUndefined();
  });

  it('runs the lease-holder sidecar with its token, the cluster ConfigMap and the data directory', () => {
    const spec = podSpec(withLease());
    const holder = spec.initContainers!.find((c) => c.name === 'lease-holder')!;
    expect(holder).toMatchObject({ image: 'registry.example/cloudnative-firebird:1.2.3', restartPolicy: 'Always', command: ['/app/fb-agent', 'lease-holder'] });
    expect(holder.resources?.requests).toEqual({ cpu: '10m', memory: '8Mi' });
    expect(env(holder)).toMatchObject({
      LEASE_NAME: 'test-cluster-lease',
      LEASE_DURATION_SECONDS: '15',
      PRIMARY_FILE: '/etc/firebird-operator/primary',
      TOKEN_DIR: '/var/run/primary-lease',
      SEGMENT_SERVER: '127.0.0.1:3051',
      ISC_PASSWORD: 'masterkey',
    });
    expect(env(holder).REPLICATION_DIR).toMatch(/^\/var\/lib\/firebird\/data\//);
    expect(holder.securityContext).toMatchObject({ runAsNonRoot: true, runAsUser: 65532, readOnlyRootFilesystem: true });
    expect(holder.volumeMounts).toEqual(
      expect.arrayContaining([
        { name: 'primary-lease-token', mountPath: '/var/run/primary-lease', readOnly: true },
        { name: 'cluster-config', mountPath: '/etc/firebird-operator', readOnly: true },
        { name: 'firebird-data', mountPath: '/var/lib/firebird/data', readOnly: true },
      ]),
    );
    const token = spec.volumes!.find((v) => v.name === 'primary-lease-token')!;
    expect(token.projected!.sources!.map((s) => Object.keys(s)[0])).toEqual(['serviceAccountToken', 'configMap']);
    expect(token.projected!.sources![0].serviceAccountToken).toMatchObject({ path: 'token', expirationSeconds: 3600 });
    // with segment TLS: the segment server's plain listener on localhost, so no certificates
    const tls = podSpec(withLease({ segmentTLS: { enabled: true } }));
    expect(env(named(tls, 'lease-holder')).SEGMENT_SERVER).toBe('127.0.0.1:3061');
    expect(env(named(tls, 'lease-holder')).SEGMENT_TLS_DIR).toBeUndefined();
    expect(tls.initContainers![0].name).toBe('lease-holder');
    // nothing without it
    const plain = podSpec(makeCluster({ instances: 3, replication: { enabled: true, failover: { enabled: true } } }));
    expect(named(plain, 'lease-holder')).toBeUndefined();
    expect(plain.volumes!.some((v) => v.name === 'primary-lease-token')).toBe(false);
  });
});

// The holder's decisions (renewing, fencing, re-acquiring, the renew deadline, the pod's role)
// are the unit tests of the agent's lease.rs, and its requests tests/lease-holder-conformance.test.ts.

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

describe('the primary Lease default', () => {
  afterEach(() => {
    setServerVersion(undefined);
    vi.unstubAllEnvs();
  });
  const annotationPatches = (api: (m: string) => Mock) =>
    api('patchNamespacedCustomObject')
      .mock.calls.map((c) => c[0].body?.metadata?.annotations?.[PRIMARY_LEASE_ANNOTATION])
      .filter(Boolean);

  it('follows PRIMARY_LEASE_DEFAULT: auto means native sidecars', () => {
    setServerVersion(undefined);
    expect(primaryLeaseDefault({})).toBe(false);
    setServerVersion({ major: 1, minor: 28 });
    expect(primaryLeaseDefault({})).toBe(false);
    expect(primaryLeaseDefault({ PRIMARY_LEASE_DEFAULT: 'true' })).toBe(true);
    setServerVersion({ major: 1, minor: 31 });
    expect(primaryLeaseDefault({})).toBe(true);
    expect(primaryLeaseDefault({ PRIMARY_LEASE_DEFAULT: 'false' })).toBe(false);
  });

  /** A cluster as the user created it: nothing recorded yet */
  const undecided = (spec: Partial<FirebirdCluster['spec']>) => {
    const cluster = makeCluster(spec);
    delete cluster.metadata.annotations;
    return cluster;
  };

  it('is recorded on a new cluster\'s first reconcile, with an event, and applies once failover is on', async () => {
    setServerVersion({ major: 1, minor: 31 });
    const { kubeConfig, api } = mockApi({ getNamespacedCustomObject: vi.fn().mockResolvedValue({}) });
    const cluster = undecided({ instances: 3, replication: { enabled: true, failover: { enabled: true } } });
    await new FirebirdClusterController(kubeConfig).reconcile(cluster);
    expect(annotationPatches(api)).toEqual(['enabled']);
    expect(cluster.metadata.annotations?.[PRIMARY_LEASE_ANNOTATION]).toBe('enabled');
    expect(api('createNamespacedEvent').mock.calls.map((c) => c[0].body.reason)).toContain('PrimaryLeaseDefaulted');
    // the StatefulSet created in the same reconcile runs the sidecar, and the Role exists
    const sts = api('createNamespacedStatefulSet').mock.calls[0][0].body;
    expect(sts.spec.template.spec.initContainers.map((c: V1Container) => c.name)).toContain('lease-holder');
    expect(api('createNamespacedRole')).toHaveBeenCalled();
    // recorded once: a second reconcile leaves it alone
    await new FirebirdClusterController(kubeConfig).reconcile(cluster);
    expect(annotationPatches(api)).toEqual(['enabled']);
  });

  it('follows what a leftover StatefulSet runs: a cluster recreated over what a deleted one left keeps it', async () => {
    setServerVersion({ major: 1, minor: 31 });
    // the StatefulSet the deleted cluster ran: segment TLS, the Lease holder and the anti-affinity
    const spec = { instances: 3, replication: { enabled: true, failover: { enabled: true } } } as const;
    const deleted = makeCluster({ ...spec, segmentTLS: { enabled: true } });
    deleted.metadata.annotations = { [PRIMARY_LEASE_ANNOTATION]: 'enabled', 'firebird.cloudnative-firebird.io/pod-anti-affinity': 'enabled' };
    // as the API server returns it: its own field order, and the deleted cluster's owner gone
    const left = JSON.parse(JSON.stringify(buildStatefulSet(deleted)));
    delete left.metadata.ownerReferences;
    left.spec.template.spec.affinity = JSON.parse(JSON.stringify(left.spec.template.spec.affinity), (_k, v) =>
      v && typeof v === 'object' && !Array.isArray(v) ? Object.fromEntries(Object.entries(v).reverse()) : v,
    );
    const { kubeConfig, api } = mockApi({
      getNamespacedCustomObject: vi.fn().mockResolvedValue({}),
      readNamespacedStatefulSet: vi.fn().mockResolvedValue(left),
    });
    const cluster = undecided(spec);
    // nothing decided yet, segment TLS included (the factory records it as a first reconcile would)
    delete cluster.spec.segmentTLS;
    await new FirebirdClusterController(kubeConfig).reconcile(cluster);
    expect(cluster.spec.segmentTLS?.enabled).toBe(true);
    expect(cluster.metadata.annotations?.[PRIMARY_LEASE_ANNOTATION]).toBe('enabled');
    expect(cluster.metadata.annotations?.['firebird.cloudnative-firebird.io/pod-anti-affinity']).toBe('enabled');
    // not pinned for a migration: nothing to move
    expect(cluster.metadata.annotations?.['firebird.cloudnative-firebird.io/segment-tls-migration']).toBeUndefined();
    const messages = api('createNamespacedEvent').mock.calls.map((c) => c[0].body.message as string);
    expect(messages).toContain('spec.segmentTLS.enabled set to true (the StatefulSet this cluster found runs segment TLS)');
    expect(messages.some((m) => m.includes('the StatefulSet this cluster found runs the Lease holder'))).toBe(true);
    expect(messages.some((m) => m.includes('pod anti-affinity): true (the StatefulSet this cluster found has it'))).toBe(true);
  });

  it('pins a cluster that already has a StatefulSet, and records "pinned" when the default is off', async () => {
    setServerVersion({ major: 1, minor: 31 });
    const { kubeConfig, api } = mockApi({
      getNamespacedCustomObject: vi.fn().mockResolvedValue({}),
      readNamespacedStatefulSet: vi.fn().mockResolvedValue({ metadata: { name: 'test-cluster' }, spec: { replicas: 3, podManagementPolicy: 'Parallel' }, status: {} }),
    });
    const cluster = undecided({ instances: 3, replication: { enabled: true, failover: { enabled: true } } });
    await new FirebirdClusterController(kubeConfig).reconcile(cluster);
    expect(annotationPatches(api)).toEqual(['pinned']);
    expect(primaryLeaseEnabled(cluster)).toBe(false);
    expect(api('createNamespacedRole')).not.toHaveBeenCalled();
    vi.stubEnv('PRIMARY_LEASE_DEFAULT', 'false');
    const off = mockApi({ getNamespacedCustomObject: vi.fn().mockResolvedValue({}) });
    const fresh = undecided({ instances: 3, replication: { enabled: true, failover: { enabled: true } } });
    await new FirebirdClusterController(off.kubeConfig).reconcile(fresh);
    expect(annotationPatches(off.api)).toEqual(['pinned']);
  });
});
