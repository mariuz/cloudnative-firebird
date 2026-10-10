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
import { LeaseHolder, renewIntervalSeconds, isPrimary, type DatabaseState } from '../src/lease-holder';
import { mkdtempSync, writeFileSync, mkdirSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';

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
    expect(holder).toMatchObject({ image: 'registry.example/cloudnative-firebird:1.2.3', restartPolicy: 'Always', command: ['node', 'dist/lease-holder.js'] });
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
    // the segment server behind the TLS proxy
    const tls = podSpec(withLease({ segmentTLS: { enabled: true } }));
    expect(env(named(tls, 'lease-holder')).SEGMENT_SERVER).toBe('127.0.0.1:3061');
    expect(tls.initContainers![0].name).toBe('segment-tls');
    // nothing without it
    const plain = podSpec(makeCluster({ instances: 3, replication: { enabled: true, failover: { enabled: true } } }));
    expect(named(plain, 'lease-holder')).toBeUndefined();
    expect(plain.volumes!.some((v) => v.name === 'primary-lease-token')).toBe(false);
  });
});

describe('the lease holder', () => {
  function holder(overrides: { primary?: boolean; state?: DatabaseState; lease?: object | undefined | (() => Promise<object | undefined>) } = {}) {
    let now = 1_000_000;
    const log: string[] = [];
    const fence = vi.fn().mockResolvedValue(undefined);
    const rejoin = vi.fn().mockResolvedValue(undefined);
    const renew = vi.fn().mockResolvedValue(undefined);
    const state = { primary: overrides.primary ?? true, db: (overrides.state ?? 'online') as DatabaseState, lease: overrides.lease ?? { spec: { holderIdentity: 'db-0' } } };
    const h = new LeaseHolder({
      self: 'db-0',
      durationSeconds: 15,
      isPrimary: () => state.primary,
      databaseState: async () => state.db,
      api: { read: async () => (typeof state.lease === 'function' ? state.lease() : state.lease), renew },
      fence,
      rejoin,
      now: () => now,
      log: (m) => log.push(m),
    });
    return { h, state, fence, rejoin, renew, log, advance: (s: number) => (now += s * 1000) };
  }

  it('renews the Lease while the pod is the primary and its database online, and never otherwise', async () => {
    const t = holder();
    expect(await t.h.tick()).toMatch(/^holding the Lease as the primary/);
    expect(t.renew).toHaveBeenCalledWith({ spec: { holderIdentity: 'db-0' } }, 'db-0', 15);
    t.state.db = 'shutdown';
    expect(await t.h.tick()).toBe('database shutdown: not renewing the Lease');
    t.state.db = undefined;
    expect(await t.h.tick()).toBe('database not answering: not renewing the Lease');
    t.state.db = 'online';
    t.state.primary = false;
    expect(await t.h.tick()).toBe('not the primary: not holding the Lease');
    expect(t.renew).toHaveBeenCalledTimes(1);
    expect(t.fence).not.toHaveBeenCalled();
    // an empty Lease (nobody holds it) is acquired
    t.state.primary = true;
    t.state.lease = { spec: {} };
    expect(await t.h.tick()).toMatch(/acquired/);
    expect(t.renew).toHaveBeenCalledTimes(2);
    expect(renewIntervalSeconds(15)).toBe(5);
    expect(renewIntervalSeconds(5)).toBe(1);
  });

  it('fences the database when the Lease names another instance', async () => {
    const t = holder({ lease: { spec: { holderIdentity: 'db-2', renewTime: new Date().toISOString() } } });
    expect(await t.h.tick()).toBe('fenced: the Lease is held by db-2');
    expect(t.fence).toHaveBeenCalledWith('the Lease is held by db-2');
    expect(t.renew).not.toHaveBeenCalled();
    // fenced: nothing more while another instance holds the Lease
    t.state.db = 'fenced';
    expect(await t.h.tick()).toBe('fenced; the Lease is held by db-2');
    expect(t.fence).toHaveBeenCalledTimes(1);
    expect(t.rejoin).not.toHaveBeenCalled();
  });

  it('re-acquires its Lease and brings a fenced database back online, unless a failover took the Lease over', async () => {
    const t = holder({ state: 'fenced', lease: { metadata: { resourceVersion: '7' }, spec: { holderIdentity: 'db-0' } } });
    expect(await t.h.tick()).toBe('re-acquired the Lease: database back online');
    expect(t.renew).toHaveBeenCalledWith({ metadata: { resourceVersion: '7' }, spec: { holderIdentity: 'db-0' } }, 'db-0', 15);
    expect(t.rejoin).toHaveBeenCalledTimes(1);
    t.state.db = 'online';
    expect(await t.h.tick()).toMatch(/^holding the Lease/);
    // the API server still unreachable: it stays fenced
    const cut = holder({ state: 'fenced', lease: async () => { throw new Error('timed out'); } });
    expect(await cut.h.tick()).toBe('fenced; cannot read the Lease (timed out)');
    expect(cut.rejoin).not.toHaveBeenCalled();
    // the operator took the Lease over between the read and the renewal (a conflict): it stays fenced
    const taken = holder({ state: 'fenced' });
    taken.renew.mockRejectedValueOnce(new Error('409 conflict'));
    expect(await taken.h.tick()).toBe('fenced; cannot re-acquire the Lease (409 conflict)');
    expect(taken.rejoin).not.toHaveBeenCalled();
    // an expired Lease nobody holds is acquired too; one held by a replica promoted meanwhile is not
    const free = holder({ state: 'fenced', lease: { spec: {} } });
    expect(await free.h.tick()).toBe('re-acquired the Lease: database back online');
    // a database shut down for another reason (a Job) is left alone
    const job = holder({ state: 'shutdown' });
    expect(await job.h.tick()).toBe('database shutdown: not renewing the Lease');
    expect(job.rejoin).not.toHaveBeenCalled();
    // one the isolation check fenced (nothing reached this primary) is the operator's to lift
    const isolated = holder({ state: 'isolated' });
    expect(await isolated.h.tick()).toBe('database isolated: not renewing the Lease');
    expect(isolated.rejoin).not.toHaveBeenCalled();
    expect(isolated.renew).not.toHaveBeenCalled();
    // a failed rejoin is retried on the next tick
    const r = holder({ state: 'fenced' });
    r.rejoin.mockRejectedValueOnce(new Error('ERR cannot bring online'));
    expect(await r.h.tick()).toMatch(/cannot bring the database back online/);
    expect(await r.h.tick()).toBe('re-acquired the Lease: database back online');
  });

  it('fences the database once the Lease could not be renewed for its duration', async () => {
    const t = holder();
    await t.h.tick();
    t.state.lease = async () => {
      throw new Error('connect ETIMEDOUT');
    };
    t.advance(5);
    expect(await t.h.tick()).toBe('cannot read the Lease (connect ETIMEDOUT): fencing in 10s');
    t.advance(5);
    expect(await t.h.tick()).toBe('cannot read the Lease (connect ETIMEDOUT): fencing in 5s');
    expect(t.fence).not.toHaveBeenCalled();
    t.advance(5);
    expect(await t.h.tick()).toBe('fenced: the Lease could not be renewed');
    expect(t.fence).toHaveBeenCalledWith('cannot renew the Lease: the API server has not answered for 15s');
    // a failing renewal counts the same way
    const r = holder();
    r.renew.mockRejectedValue(new Error('403'));
    expect(await r.h.tick()).toBe('cannot renew the Lease (403): fencing in 15s');
    r.advance(15);
    expect(await r.h.tick()).toBe('fenced: the Lease could not be renewed');
    // a fence that fails is retried on the next tick
    const f = holder({ lease: { spec: { holderIdentity: 'db-1' } } });
    f.fence.mockRejectedValueOnce(new Error('no reply'));
    await f.h.tick();
    expect(f.log).toContain('cannot fence the database (no reply); retrying');
    await f.h.tick();
    expect(f.fence).toHaveBeenCalledTimes(2);
  });

  it('reads the pod role from the ConfigMap file or the promoted marker', () => {
    const dir = mkdtempSync(join(tmpdir(), 'lease-holder-'));
    const primaryFile = join(dir, 'primary');
    const e = { POD_NAME: 'db-1', PRIMARY_FILE: primaryFile, REPLICATION_DIR: join(dir, 'repl') };
    expect(isPrimary(e)).toBe(false);
    writeFileSync(primaryFile, 'db-0.db-headless\n');
    expect(isPrimary(e)).toBe(false);
    writeFileSync(primaryFile, 'db-1.db-headless\n');
    expect(isPrimary(e)).toBe(true);
    writeFileSync(primaryFile, 'db-0.db-headless\n');
    mkdirSync(join(dir, 'repl'));
    writeFileSync(join(dir, 'repl', 'promoted'), '1\n');
    expect(isPrimary(e)).toBe(true);
  });
});

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
