import { describe, it, expect, vi, afterEach } from 'vitest';
import { V1Lease } from '@kubernetes/client-node';
import { LeaderElector, LeaseApi } from '../src/utils/leader-election';

/** An in-memory Lease with the API server's optimistic concurrency (resourceVersion) */
function memoryLease() {
  let stored: V1Lease | undefined;
  let version = 0;
  const conflict = () => Object.assign(new Error('Conflict'), { code: 409 });
  const api: LeaseApi & { stored: () => V1Lease | undefined } = {
    stored: () => stored,
    read: async () => (stored ? JSON.parse(JSON.stringify(stored)) : undefined),
    create: async (lease) => {
      if (stored) throw conflict();
      stored = { ...lease, metadata: { ...lease.metadata, resourceVersion: String(++version) } };
      return stored;
    },
    replace: async (lease) => {
      if (!stored || lease.metadata?.resourceVersion !== stored.metadata?.resourceVersion) throw conflict();
      stored = { ...lease, metadata: { ...lease.metadata, resourceVersion: String(++version) } };
      return stored;
    },
  };
  return api;
}

const clock = (start = 1_000_000) => {
  let t = start;
  return { now: () => t, advance: (ms: number) => (t += ms) };
};

function elector(lease: LeaseApi, identity: string, now: () => number) {
  const events: string[] = [];
  const e = new LeaderElector(
    lease,
    identity,
    { onStartedLeading: () => void events.push('started'), onStoppedLeading: () => void events.push('stopped') },
    { now },
    'op',
    'ns',
  );
  return { e, events };
}

describe('leader election', () => {
  afterEach(() => vi.useRealTimers());

  it('one replica holds the Lease; the other takes over once it stops renewing', async () => {
    const lease = memoryLease();
    const t = clock();
    const a = elector(lease, 'op-a', t.now);
    const b = elector(lease, 'op-b', t.now);
    expect(await a.e.tryAcquireOrRenew()).toBe(true);
    expect(a.events).toEqual(['started']);
    expect(await b.e.tryAcquireOrRenew()).toBe(false);
    // a renews: b keeps waiting, however long
    for (let i = 0; i < 20; i++) {
      t.advance(2000);
      expect(await a.e.tryAcquireOrRenew()).toBe(true);
      expect(await b.e.tryAcquireOrRenew()).toBe(false);
    }
    expect(lease.stored()?.spec?.holderIdentity).toBe('op-a');
    // a stops renewing: b takes over after the lease duration, measured on b's own clock
    t.advance(14_000);
    expect(await b.e.tryAcquireOrRenew()).toBe(false);
    t.advance(2_000);
    expect(await b.e.tryAcquireOrRenew()).toBe(true);
    expect(b.events).toEqual(['started']);
    expect(lease.stored()?.spec).toMatchObject({ holderIdentity: 'op-b', leaseTransitions: 1 });
    // a comes back: it sees b holding a fresh Lease and stands down
    expect(await a.e.tryAcquireOrRenew()).toBe(false);
    expect(a.events).toEqual(['started', 'stopped']);
  });

  it('ignores the clocks of other replicas (renew times far in the past or future)', async () => {
    const lease = memoryLease();
    const skewed = clock(5_000_000_000); // the holder's clock is far ahead
    const local = clock(1_000_000);
    const a = elector(lease, 'op-a', skewed.now);
    const b = elector(lease, 'op-b', local.now);
    await a.e.tryAcquireOrRenew();
    expect(await b.e.tryAcquireOrRenew()).toBe(false);
    local.advance(5_000);
    skewed.advance(5_000);
    await a.e.tryAcquireOrRenew();
    expect(await b.e.tryAcquireOrRenew()).toBe(false);
  });

  it('loses a race to write the Lease without becoming leader', async () => {
    const lease = memoryLease();
    const t = clock();
    const a = elector(lease, 'op-a', t.now);
    const b = elector(lease, 'op-b', t.now);
    // both read an empty Lease, a creates it first
    const read = lease.read;
    lease.read = async () => undefined;
    expect(await a.e.tryAcquireOrRenew()).toBe(true);
    expect(await b.e.tryAcquireOrRenew()).toBe(false);
    lease.read = read;
    expect(b.e.isLeader).toBe(false);
    expect(lease.stored()?.spec?.holderIdentity).toBe('op-a');
  });

  it('releases the Lease on shutdown, so the standby takes over at once', async () => {
    const lease = memoryLease();
    const t = clock();
    const a = elector(lease, 'op-a', t.now);
    const b = elector(lease, 'op-b', t.now);
    await a.e.tryAcquireOrRenew();
    await b.e.tryAcquireOrRenew();
    await a.e.stop();
    expect(lease.stored()?.spec?.holderIdentity).toBe('');
    expect(await b.e.tryAcquireOrRenew()).toBe(true);
    expect(lease.stored()?.spec).toMatchObject({ holderIdentity: 'op-b', leaseTransitions: 1 });
  });

  it('stops leading when it cannot renew within the deadline', async () => {
    vi.useFakeTimers();
    const lease = memoryLease();
    const a = elector(lease, 'op-a', () => Date.now());
    a.e.start();
    await vi.advanceTimersByTimeAsync(10);
    expect(a.events).toEqual(['started']);
    // the API server becomes unreachable
    lease.read = async () => {
      throw new Error('connection refused');
    };
    await vi.advanceTimersByTimeAsync(9_000);
    expect(a.events).toEqual(['started']);
    await vi.advanceTimersByTimeAsync(3_000);
    expect(a.events).toEqual(['started', 'stopped']);
    expect(a.e.isLeader).toBe(false);
  });
});
