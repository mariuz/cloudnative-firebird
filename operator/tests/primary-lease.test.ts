import { describe, it, expect, vi, beforeAll } from 'vitest';
import { V1Container, V1PodSpec } from '@kubernetes/client-node';
import { makeCluster } from './helpers/factories';
import { buildStatefulSet } from '../src/utils/resources';
import { setOperatorImage } from '../src/utils/operator-image';
import { leaseExpired, leaseAgeSeconds, primaryLeaseEnabled, primaryLeaseDurationSeconds } from '../src/utils/primary-lease';
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

  it('is held only with automatic failover and primaryLease.enabled', () => {
    expect(primaryLeaseEnabled(makeCluster())).toBe(false);
    expect(primaryLeaseEnabled(makeCluster({ replication: { enabled: true, failover: { enabled: true } } }))).toBe(false);
    expect(primaryLeaseEnabled(makeCluster({ replication: { enabled: true, failover: { enabled: false, primaryLease: { enabled: true } } } }))).toBe(false);
    expect(primaryLeaseEnabled(withLease())).toBe(true);
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
    const renew = vi.fn().mockResolvedValue(undefined);
    const state = { primary: overrides.primary ?? true, db: (overrides.state ?? 'online') as DatabaseState, lease: overrides.lease ?? { spec: { holderIdentity: 'db-0' } } };
    const h = new LeaseHolder({
      self: 'db-0',
      durationSeconds: 15,
      isPrimary: () => state.primary,
      databaseState: async () => state.db,
      api: { read: async () => (typeof state.lease === 'function' ? state.lease() : state.lease), renew },
      fence,
      now: () => now,
      log: (m) => log.push(m),
    });
    return { h, state, fence, renew, log, advance: (s: number) => (now += s * 1000) };
  }

  it('renews the Lease while the pod is the primary and its database online, and never otherwise', async () => {
    const t = holder();
    expect(await t.h.tick()).toMatch(/^holding the Lease as the primary/);
    expect(t.renew).toHaveBeenCalledWith('db-0', 15);
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
    // fenced: the database is down, nothing more until the operator brings it back
    t.state.db = 'shutdown';
    expect(await t.h.tick()).toBe('database shutdown: not renewing the Lease');
    expect(t.fence).toHaveBeenCalledTimes(1);
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
