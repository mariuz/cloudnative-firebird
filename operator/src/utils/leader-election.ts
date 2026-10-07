import { CoordinationV1Api, KubeConfig, V1Lease, V1MicroTime } from '@kubernetes/client-node';
import { logger } from './logger';

/**
 * Leader election for running several operator replicas (client-go's leaderelection, which
 * CloudNativePG uses): the replica holding the Lease reconciles, the others wait and take over
 * when it stops renewing. Every replica serves the admission webhook meanwhile.
 *
 * A Lease is considered expired when its record (holder and renew time) has not changed for
 * leaseDurationSeconds as seen by this replica's own clock, so clock skew between nodes does not
 * matter. The leader renews every retryPeriodMs; if it cannot renew within renewDeadlineMs it has
 * lost the lease (another replica may take over once leaseDurationSeconds passed) and must stop
 * reconciling at once. A clean shutdown releases the Lease so a standby takes over immediately.
 */

export const OPERATOR_LEASE = 'cloudnative-firebird-operator';
export const LEASE_DURATION_SECONDS = 15;
export const RENEW_DEADLINE_MS = 10_000;
export const RETRY_PERIOD_MS = 2_000;

export interface LeaseApi {
  read(): Promise<V1Lease | undefined>;
  create(lease: V1Lease): Promise<V1Lease>;
  /** Replaces the Lease; fails with a conflict when its resourceVersion changed meanwhile */
  replace(lease: V1Lease): Promise<V1Lease>;
}

function isNotFound(err: unknown): boolean {
  const e = err as { code?: number; statusCode?: number };
  return e?.code === 404 || e?.statusCode === 404;
}

export function apiLease(kubeConfig: KubeConfig, namespace: string, name = OPERATOR_LEASE): LeaseApi {
  const api = kubeConfig.makeApiClient(CoordinationV1Api);
  return {
    read: async () => {
      try {
        return await api.readNamespacedLease({ name, namespace });
      } catch (err) {
        if (isNotFound(err)) return undefined;
        throw err;
      }
    },
    create: (body) => api.createNamespacedLease({ namespace, body }),
    replace: (body) => api.replaceNamespacedLease({ name, namespace, body }),
  };
}

export class LeaderElector {
  private leading = false;
  private stopped = false;
  private timer?: NodeJS.Timeout;
  /** The Lease record last seen, and when (this replica's clock) it last changed */
  private observed?: { record: string; at: number };
  private lastRenew = 0;

  constructor(
    private readonly lease: LeaseApi,
    private readonly identity: string,
    private readonly callbacks: { onStartedLeading: () => void | Promise<void>; onStoppedLeading: () => void },
    private readonly options: { leaseDurationSeconds?: number; renewDeadlineMs?: number; retryPeriodMs?: number; now?: () => number } = {},
    private readonly name = OPERATOR_LEASE,
    private readonly namespace = '',
  ) {}

  get isLeader(): boolean {
    return this.leading;
  }

  private now(): number {
    return (this.options.now ?? Date.now)();
  }

  /** Starts trying to acquire the Lease, then renews it while leading */
  start(): void {
    const tick = async () => {
      if (this.stopped) return;
      try {
        await this.tryAcquireOrRenew();
      } catch (err) {
        logger.warn({ err }, 'Leader election: Lease update failed');
      }
      if (this.leading && this.now() - this.lastRenew > (this.options.renewDeadlineMs ?? RENEW_DEADLINE_MS)) {
        logger.error({ identity: this.identity }, 'Leader election: could not renew the Lease in time; leadership lost');
        this.leading = false;
        this.callbacks.onStoppedLeading();
        return;
      }
      if (!this.stopped) this.timer = setTimeout(() => void tick(), this.options.retryPeriodMs ?? RETRY_PERIOD_MS);
    };
    void tick();
  }

  /** One election round: returns whether this replica holds the Lease afterwards */
  async tryAcquireOrRenew(): Promise<boolean> {
    const now = this.now();
    const durationSeconds = this.options.leaseDurationSeconds ?? LEASE_DURATION_SECONDS;
    const current = await this.lease.read();
    const spec = current?.spec ?? {};
    const record = `${spec.holderIdentity ?? ''}|${spec.renewTime ? new Date(spec.renewTime as unknown as string).toISOString() : ''}|${spec.leaseTransitions ?? 0}`;
    if (!this.observed || this.observed.record !== record) this.observed = { record, at: now };
    const holder = spec.holderIdentity ?? '';
    const expired = holder === '' || now - this.observed.at >= durationSeconds * 1000;
    if (current && holder !== this.identity && !expired) {
      if (this.leading) {
        // another replica took over (this one failed to renew in time)
        this.leading = false;
        this.callbacks.onStoppedLeading();
      }
      return false;
    }
    const time = new V1MicroTime(now);
    const taking = holder !== this.identity;
    const desired: V1Lease = {
      metadata: { name: this.name, namespace: this.namespace || current?.metadata?.namespace, resourceVersion: current?.metadata?.resourceVersion },
      spec: {
        holderIdentity: this.identity,
        leaseDurationSeconds: durationSeconds,
        renewTime: time,
        acquireTime: taking ? time : spec.acquireTime,
        // as client-go: every change of holder of an existing Lease counts (a release included)
        leaseTransitions: (spec.leaseTransitions ?? 0) + (taking && current ? 1 : 0),
      },
    };
    try {
      if (current) await this.lease.replace(desired);
      else await this.lease.create(desired);
    } catch (err) {
      const code = (err as { code?: number }).code;
      if (code === 409) return this.leading; // someone else wrote it first: read again next round
      throw err;
    }
    this.lastRenew = now;
    if (!this.leading) {
      this.leading = true;
      logger.info({ identity: this.identity, previous: holder || undefined }, 'Leader election: this replica is the leader');
      await this.callbacks.onStartedLeading();
    }
    return true;
  }

  /** Stops renewing; a leader releases the Lease so a standby takes over without waiting */
  async stop(): Promise<void> {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
    if (!this.leading) return;
    this.leading = false;
    try {
      const current = await this.lease.read();
      if (current?.spec?.holderIdentity === this.identity) {
        await this.lease.replace({
          metadata: { ...current.metadata },
          spec: { ...current.spec, holderIdentity: '', leaseDurationSeconds: 1 },
        });
        logger.info('Leader election: Lease released');
      }
    } catch (err) {
      logger.warn({ err }, 'Leader election: could not release the Lease');
    }
  }
}
