import { CoordinationV1Api, KubeConfig, V1Lease, V1MicroTime } from '@kubernetes/client-node';
import { existsSync, readFileSync } from 'fs';
import { join } from 'path';
import { segmentRequest } from './utils/replication-lag';

/**
 * The primary Lease holder: a native sidecar of every instance pod (utils/primary-lease.ts) that
 * keeps the cluster Lease alive while its pod is the primary, as CloudNativePG's instance manager
 * does with the primary Lease.
 *
 * Every LEASE_DURATION_SECONDS / 3 it checks whether this pod is the primary (the cluster
 * ConfigMap's primary entry, or the "promoted" marker of an instance promoted in place) and
 * whether its database is online (the segment server's STATE). While both hold it renews the
 * Lease (renewTime, with itself as the holder); a database shut down stops the renewals, so the
 * Lease expires and the operator may fail over.
 *
 * It fences the database (the segment server's FENCE: the isolation check's full shutdown, lifted
 * by the operator with REJOIN once it has checked that this instance still holds the Lease) when
 * the Lease names another instance (the operator promoted a replica) or when it could not renew
 * it for the Lease's duration (it cannot reach the API server: the operator may be promoting a
 * replica on the other side). A database fenced this way comes back online once this pod holds
 * the Lease again: the holder re-acquires it (with the version it read, so a failover that took
 * the Lease over meanwhile wins) and asks the segment server to bring the database back
 * (REJOIN). A database the isolation check fenced (nothing reached this primary, or neither the
 * API server nor a peer did) is left to the operator, which lifts it once it reaches the primary
 * again while it still holds the Lease, as without the primary Lease: a primary that still
 * reaches the API server but nothing else must not take writes again on its own.
 *
 * Environment: POD_NAME, POD_NAMESPACE, LEASE_NAME, LEASE_DURATION_SECONDS, PRIMARY_FILE,
 * REPLICATION_DIR, TOKEN_DIR (the projected ServiceAccount token and the API server's CA),
 * KUBERNETES_SERVICE_HOST / PORT, SEGMENT_SERVER (host:port of this pod's segment server),
 * ISC_PASSWORD (the requests to it are signed with it).
 */

/** online, shut down, fenced by this holder, isolated (fenced by the isolation check: the operator lifts that), or not answering */
export type DatabaseState = 'online' | 'shutdown' | 'fenced' | 'isolated' | undefined;

export interface LeaseApi {
  /** The Lease, or undefined when it does not exist */
  read(): Promise<V1Lease | undefined>;
  /**
   * Renews the Lease that was read, for this holder, with the version that was read: a write that
   * landed meanwhile (the operator moving it) makes it fail
   */
  renew(lease: V1Lease, holder: string, durationSeconds: number): Promise<void>;
}

export interface LeaseHolderOptions {
  self: string;
  durationSeconds: number;
  isPrimary: () => boolean;
  databaseState: () => Promise<DatabaseState>;
  api: LeaseApi;
  fence: (reason: string) => Promise<void>;
  /** Brings a fenced database back online (the segment server's REJOIN) */
  rejoin: () => Promise<void>;
  now?: () => number;
  log?: (message: string) => void;
}

/** One holder: its ticks decide what to do from the pod's role, the database's state and the Lease */
export class LeaseHolder {
  /** When the Lease was last renewed, or this pod started holding it; undefined while not holding */
  private lastRenewed: number | undefined;
  private lastAction = '';

  constructor(private readonly options: LeaseHolderOptions) {}

  private get now(): number {
    return (this.options.now ?? Date.now)();
  }

  private say(action: string): string {
    if (action !== this.lastAction) {
      this.options.log?.(action);
      this.lastAction = action;
    }
    return action;
  }

  async tick(): Promise<string> {
    const { self, durationSeconds, api } = this.options;
    if (!this.options.isPrimary()) {
      this.lastRenewed = undefined;
      return this.say('not the primary: not holding the Lease');
    }
    const state = await this.options.databaseState();
    if (state === 'fenced') {
      // fenced (by this holder, or the isolation check): back online once this pod holds the
      // Lease again, which the operator leaves to it unless a failover took the Lease over
      this.lastRenewed = undefined;
      let lease: V1Lease | undefined;
      try {
        lease = await api.read();
      } catch (err) {
        return this.say(`fenced; cannot read the Lease (${(err as Error).message})`);
      }
      const holder = lease?.spec?.holderIdentity ?? '';
      if (!lease || (holder && holder !== self)) return this.say(`fenced; the Lease is held by ${holder || 'nobody'}`);
      try {
        await api.renew(lease, self, durationSeconds);
      } catch (err) {
        return this.say(`fenced; cannot re-acquire the Lease (${(err as Error).message})`);
      }
      try {
        await this.options.rejoin();
      } catch (err) {
        return this.say(`re-acquired the Lease; cannot bring the database back online (${(err as Error).message}); retrying`);
      }
      this.lastRenewed = this.now;
      return this.say('re-acquired the Lease: database back online');
    }
    if (state !== 'online') {
      // down (shut down by a Job, or not answering yet): the Lease expires on its own
      this.lastRenewed = undefined;
      return this.say(`database ${state ?? 'not answering'}: not renewing the Lease`);
    }
    this.lastRenewed ??= this.now;
    let lease: V1Lease | undefined;
    try {
      lease = await api.read();
    } catch (err) {
      const since = Math.round((this.now - this.lastRenewed) / 1000);
      if (since >= durationSeconds) {
        await this.fence(`cannot renew the Lease: the API server has not answered for ${since}s`);
        return this.say('fenced: the Lease could not be renewed');
      }
      return this.say(`cannot read the Lease (${(err as Error).message}): fencing in ${durationSeconds - since}s`);
    }
    const holder = lease?.spec?.holderIdentity ?? '';
    if (!lease) return this.say('the Lease does not exist yet');
    if (holder && holder !== self) {
      // the operator moved the Lease: a replica is being promoted, this database must not take writes
      await this.fence(`the Lease is held by ${holder}`);
      return this.say(`fenced: the Lease is held by ${holder}`);
    }
    try {
      await api.renew(lease, self, durationSeconds);
      this.lastRenewed = this.now;
      return this.say(`holding the Lease ${this.options.self === holder ? '' : '(acquired) '}as the primary, renewed every ${renewIntervalSeconds(durationSeconds)}s`);
    } catch (err) {
      const since = Math.round((this.now - this.lastRenewed) / 1000);
      if (since >= durationSeconds) {
        await this.fence(`cannot renew the Lease: ${(err as Error).message} (for ${since}s)`);
        return this.say('fenced: the Lease could not be renewed');
      }
      return this.say(`cannot renew the Lease (${(err as Error).message}): fencing in ${durationSeconds - since}s`);
    }
  }

  private async fence(reason: string): Promise<void> {
    this.lastRenewed = undefined;
    try {
      await this.options.fence(reason);
    } catch (err) {
      this.options.log?.(`cannot fence the database (${(err as Error).message}); retrying`);
    }
  }
}

export function renewIntervalSeconds(durationSeconds: number): number {
  return Math.max(1, Math.floor(durationSeconds / 3));
}

/** How long an API request may take: a primary cut off from the API server must notice within the Lease's duration */
export const API_TIMEOUT_MS = 5_000;

function withTimeout<T>(promise: Promise<T>, what: string, ms = API_TIMEOUT_MS): Promise<T> {
  let timer: NodeJS.Timeout;
  const timeout = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => reject(new Error(`${what} timed out after ${ms / 1000}s`)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

/** The Lease API through the pod's projected ServiceAccount token (re-read: it rotates) */
export function kubernetesLeaseApi(env: NodeJS.ProcessEnv, name: string, namespace: string): LeaseApi {
  const dir = env.TOKEN_DIR ?? '/var/run/primary-lease';
  const client = () => {
    const kubeConfig = new KubeConfig();
    kubeConfig.loadFromOptions({
      clusters: [{ name: 'cluster', server: `https://${env.KUBERNETES_SERVICE_HOST}:${env.KUBERNETES_SERVICE_PORT ?? 443}`, caFile: join(dir, 'ca.crt') }],
      users: [{ name: 'pod', token: readFileSync(join(dir, 'token'), 'utf8').trim() }],
      contexts: [{ name: 'pod', cluster: 'cluster', user: 'pod' }],
      currentContext: 'pod',
    });
    return kubeConfig.makeApiClient(CoordinationV1Api);
  };
  return {
    async read() {
      try {
        return await withTimeout(client().readNamespacedLease({ name, namespace }), 'reading the Lease');
      } catch (err) {
        if ((err as { code?: number })?.code === 404) return undefined;
        throw err;
      }
    },
    async renew(lease, holder, durationSeconds) {
      // with the resourceVersion that was read: a conflict (409) means the operator moved it meanwhile
      await withTimeout(
        client().replaceNamespacedLease({
          name,
          namespace,
          body: { ...lease, spec: { ...lease.spec, holderIdentity: holder, leaseDurationSeconds: durationSeconds, renewTime: new V1MicroTime() } },
        }),
        'renewing the Lease',
      );
    },
  };
}

/** Whether this pod is the primary: promoted in place (the marker), or named by the ConfigMap */
export function isPrimary(env: NodeJS.ProcessEnv): boolean {
  if (env.REPLICATION_DIR && existsSync(join(env.REPLICATION_DIR, 'promoted'))) return true;
  let primary = '';
  try {
    primary = readFileSync(env.PRIMARY_FILE ?? '', 'utf8').trim();
  } catch {
    return false;
  }
  return primary === '' || primary === env.POD_NAME || primary.startsWith(`${env.POD_NAME}.`);
}

/** The segment server of this pod, with requests signed by the SYSDBA password */
export function segmentServer(env: NodeJS.ProcessEnv): {
  state: () => Promise<DatabaseState>;
  fence: (reason: string) => Promise<void>;
  rejoin: () => Promise<void>;
} {
  const [host, port] = (env.SEGMENT_SERVER ?? '127.0.0.1:3051').split(':');
  const ask = (request: string) => segmentRequest(host, Number(port), `${env.ISC_PASSWORD ?? ''} ${request}`, 10_000);
  return {
    async state() {
      try {
        const [reply] = await ask('STATE');
        if (reply === 'OK online') return 'online';
        if (reply !== 'OK shutdown') return undefined;
        // shut down: by this holder's fence (the marker says "lease"), by the isolation check (the
        // operator lifts that once it reaches this primary again), or something else (a Job)
        const [isolation] = await ask('ISOLATION');
        if (!isolation?.startsWith('OK fenced')) return 'shutdown';
        return isolation.endsWith(' lease') ? 'fenced' : 'isolated';
      } catch {
        return undefined;
      }
    },
    async fence(reason) {
      const [reply] = await ask(`FENCE ${reason.replace(/[^\w .:,()/-]/g, ' ').slice(0, 200)}`);
      if (!reply?.startsWith('OK')) throw new Error(reply ?? 'no reply');
    },
    async rejoin() {
      const [reply] = await ask('REJOIN');
      if (reply !== 'OK') throw new Error(reply ?? 'no reply');
    },
  };
}

export async function main(env = process.env): Promise<never> {
  const self = env.POD_NAME ?? '';
  const namespace = env.POD_NAMESPACE ?? 'default';
  const name = env.LEASE_NAME ?? '';
  const durationSeconds = Number(env.LEASE_DURATION_SECONDS) || 15;
  if (!self || !name) throw new Error('lease holder: POD_NAME and LEASE_NAME are required');
  const server = segmentServer(env);
  const holder = new LeaseHolder({
    self,
    durationSeconds,
    isPrimary: () => isPrimary(env),
    databaseState: server.state,
    api: kubernetesLeaseApi(env, name, namespace),
    fence: server.fence,
    rejoin: server.rejoin,
    log: (message) => console.log(`lease holder: ${message}`),
  });
  console.log(`lease holder: ${self} watching Lease ${namespace}/${name} (duration ${durationSeconds}s)`);
  for (;;) {
    try {
      await holder.tick();
    } catch (err) {
      console.log(`lease holder: ${(err as Error).message}`);
    }
    await new Promise((resolve) => setTimeout(resolve, renewIntervalSeconds(durationSeconds) * 1000));
  }
}

if (require.main === module) {
  main().catch((err) => {
    console.error((err as Error).message);
    process.exit(1);
  });
}
