import { V1Job, V1Pod } from '@kubernetes/client-node';
import { API_GROUP, DEFAULT_FIREBIRD_IMAGE, FirebirdCluster, SynchronousStatus } from '../types';
import { clusterLabels, databaseName, FIREBIRD_DATA_DIR, superuserClientEnv, jobPodSpec } from './resources';
import { OPERATOR_CONFIG_DIR, SEGMENT_PORT, instanceHost } from './replication';
import { REPLICATION_LAG_ANNOTATION, isPodReady } from './routing';

/**
 * Synchronous replication (replication.mode sync).
 *
 * Firebird replicates synchronously to the databases listed as sync_replica: every change is
 * applied on the replica before the primary's commit completes. With report_errors and
 * disable_on_error off (set in replication.conf for mode sync), a commit fails when the replica
 * cannot be reached, so the replica never misses a committed transaction; once it is back, the
 * primary reconnects on its own.
 *
 * synchronous.number replicas (default 1), the synchronous standbys, are attached: Firebird applies
 * each commit on every one of them (verified: with one down, commits fail). They are attached and
 * detached one at a time. A standby must not apply the journal as
 * well (it would apply every change twice), so the switch happens with the primary briefly in
 * full shutdown at the end of its last segment (sync-standby.pl): the standby stops applying
 * segments, the primary's segment server writes the sync_replica entry to the file
 * replication.conf includes, and Firebird reads it when the database is opened again. Detaching
 * repositions the standby's replica control file at the primary's last segment, so it continues
 * from the journal without re-seeding (or re-seeds it, if it cannot be reached).
 *
 * The standby is detached before a planned switchover, a re-seed or fencing of the standby, or
 * when it leaves the cluster; with dataDurability preferred also when it has not been ready for
 * standbyUnavailableSeconds. After a failover the attached standby is promoted (it has every
 * committed transaction), and the other replicas are re-seeded from it.
 *
 * status.synchronous: standby / phase / time / message describe the last attach or detach (phase
 * Attaching or Detaching while its Job runs), standbys every standby attached to the primary.
 */

/** Major version from which Firebird fails a commit that a sync_replica could not apply */
export const SYNC_MIN_FIREBIRD_MAJOR = 5;

/** The unsupported reason for an engine version ("4.0.7"), undefined when it is supported or unknown */
/**
 * The sync-standby Job's termination message: the outcome on its first line ("attached",
 * "detached", "detached unreachable", "failed clean", "failed") and, from v0.85.0, how long
 * writes were stopped on a second line ("paused 3s"), rendered for the status message
 */
export function parseJobOutcome(message: string | undefined): { outcome: string; paused: string } {
  const [first = '', ...rest] = (message ?? '').trim().split('\n');
  const seconds = rest.join('\n').match(/^paused (\d+)s$/m)?.[1];
  return { outcome: first.trim(), paused: seconds === undefined ? '' : ` (writes paused ${seconds}s)` };
}

export function syncUnsupportedReason(engineVersion?: string): string | undefined {
  const major = Number(engineVersion?.split('.')[0]);
  if (!Number.isFinite(major) || major >= SYNC_MIN_FIREBIRD_MAJOR) return undefined;
  return (
    `Firebird ${engineVersion} commits while a synchronous replica is unreachable (the replica misses the ` +
    `transaction): synchronous replication needs Firebird ${SYNC_MIN_FIREBIRD_MAJOR} or later; replicating asynchronously`
  );
}

/** Standbys attached to the status's primary (statuses written before standbys: the one standby) */
export function attachedStandbys(status?: SynchronousStatus): string[] {
  if (!status) return [];
  if (status.standbys) return status.standbys;
  return status.phase === 'Attached' || status.phase === 'Detaching' ? [status.standby] : [];
}

/** Instances in synchronous replication with the status's primary: attached, or being attached */
export function synchronousMembers(status?: SynchronousStatus): string[] {
  if (!status) return [];
  const members = attachedStandbys(status);
  return status.phase === 'Attaching' && !members.includes(status.standby) ? [...members, status.standby] : members;
}

/** synchronous.number, at most the replicas there are */
export function synchronousNumber(cluster: FirebirdCluster): number {
  return Math.max(1, Math.min(cluster.spec.replication?.synchronous?.number ?? 1, cluster.spec.instances - 1));
}

export const DEFAULT_STANDBY_UNAVAILABLE_SECONDS = 30;
/** How long a failed sync-standby Job blocks the next attempt */
export const SYNC_RETRY_SECONDS = 60;
/** A replica is attached only when its measured lag is at most this (the primary is shut down meanwhile) */
export const SYNC_ATTACH_MAX_LAG_SECONDS = 30;

export type SyncAction = 'attach' | 'detach';

export function syncStandbyJobName(cluster: FirebirdCluster): string {
  return `${cluster.metadata.name}-sync-standby`;
}

export function synchronousMode(cluster: FirebirdCluster): boolean {
  return Boolean(cluster.spec.replication?.enabled && cluster.spec.replication.mode === 'sync');
}

/** Builds the Job that attaches or detaches the synchronous standby (sync-standby.pl) */
export function buildSyncStandbyJob(
  cluster: FirebirdCluster,
  action: SyncAction,
  primary: string,
  standby: string,
  others: string[] = [],
): V1Job {
  const { name, namespace = 'default', uid } = cluster.metadata;
  const labels = { ...clusterLabels(name), 'app.kubernetes.io/component': 'sync-standby' };
  return {
    apiVersion: 'batch/v1',
    kind: 'Job',
    metadata: {
      name: syncStandbyJobName(cluster),
      namespace,
      labels,
      annotations: { [`${API_GROUP}/sync-action`]: action, [`${API_GROUP}/sync-standby`]: standby },
      ownerReferences: [
        { apiVersion: `${API_GROUP}/v1`, kind: 'FirebirdCluster', name, uid: uid ?? '', controller: true, blockOwnerDeletion: true },
      ],
    },
    spec: {
      // a retry finds the primary shut down by the first attempt and continues (or brings it back)
      backoffLimit: 1,
      ttlSecondsAfterFinished: 3600,
      template: {
        metadata: { labels },
        spec: jobPodSpec(cluster, {
          restartPolicy: 'Never',
          containers: [
            {
              name: 'sync-standby',
              image: cluster.spec.imageName ?? DEFAULT_FIREBIRD_IMAGE,
              command: ['perl', `${OPERATOR_CONFIG_DIR}/sync-standby.pl`],
              env: [
                ...superuserClientEnv(cluster),
                { name: 'ACTION', value: action },
                { name: 'PRIMARY', value: instanceHost(cluster, primary) },
                { name: 'STANDBY', value: instanceHost(cluster, standby) },
                // the standbys that stay attached
                { name: 'OTHERS', value: others.map((pod) => instanceHost(cluster, pod)).join(',') },
                { name: 'DATABASE_PATH', value: `${FIREBIRD_DATA_DIR}/${databaseName(cluster)}` },
                { name: 'SEGMENT_PORT', value: String(SEGMENT_PORT) },
                { name: 'TIMEOUT_SECONDS', value: '120' },
              ],
              volumeMounts: [{ name: 'cluster-config', mountPath: OPERATOR_CONFIG_DIR, readOnly: true }],
            },
          ],
          volumes: [{ name: 'cluster-config', configMap: { name: `${name}-config` } }],
        }),
      },
    },
  };
}

export interface SyncPlanInput {
  cluster: FirebirdCluster;
  primaryPod: string;
  pods: V1Pod[];
  /** The stored state */
  status?: SynchronousStatus;
  /** The sync-standby Job, if any, and its outcome (termination message of its last pod) once finished */
  job?: V1Job;
  jobOutcome?: string;
  /** A switchover, failover or re-seed is under way or wanted: no new attach, and the standby is detached */
  busy?: string;
  /** Instances fenced (applied) or to be fenced (annotation) */
  fenced: string[];
  /** Instances with a re-seed request */
  reseeding: string[];
  /**
   * Why synchronous replication cannot be trusted on the primary's server, if it cannot: Firebird 4
   * does not fail a commit while a sync_replica is unreachable (it logs the error and commits; the
   * replica never receives the transaction, verified on 4.0.7). Nothing is attached, and attached
   * standbys are detached.
   */
  unsupported?: string;
  /** Why no standby may be attached yet (e.g. the primary's engine version is not known yet); attached ones stay */
  attachBlocked?: string;
  /**
   * The replica the rolling update restarts next and last (the only outdated replica left): it is
   * not attached, and an attached standby is handed over to another replica (or, with
   * dataDurability preferred, detached) before its restart
   */
  rollingTarget?: string;
  now: number;
}

export type SyncStep =
  /** nothing to do; status is what to store (undefined: no standby) */
  | { kind: 'none'; status?: SynchronousStatus }
  | { kind: 'start'; action: SyncAction; standby: string; status: SynchronousStatus }
  /** the Job finished: store status, delete the Job, and re-seed reseed if set */
  | { kind: 'finished'; status: SynchronousStatus; reseed?: string; event: string };

const withoutRetry = (status: SynchronousStatus): SynchronousStatus => {
  const copy = { ...status };
  delete copy.retryAfter;
  return copy;
};

const conditionTrue = (job: V1Job, type: string) =>
  (job.status?.conditions ?? []).some((c) => c.type === type && c.status === 'True');

/** Why the attached standby must be detached, if it must; unavailableSince: since when it has not been ready */
export function detachReason(input: SyncPlanInput, standby: string, unavailableSince?: string): string | undefined {
  const { cluster, now } = input;
  if (!synchronousMode(cluster)) return 'synchronous replication is off';
  if (input.unsupported) return input.unsupported;
  const ordinal = Number(standby.slice(cluster.metadata.name.length + 1));
  if (!(ordinal < cluster.spec.instances)) return `${standby} is being removed (instances)`;
  if (input.fenced.includes(standby)) return `${standby} is fenced`;
  if (input.reseeding.includes(standby)) return `${standby} is being re-seeded`;
  if (input.busy) return input.busy;
  if (standby === input.rollingTarget && handoverForUpdate(input)) {
    return `${standby} is restarted by the rolling update`;
  }
  if (cluster.spec.replication?.synchronous?.dataDurability === 'preferred' && unavailableSince) {
    const limit = (cluster.spec.replication.synchronous.standbyUnavailableSeconds ?? DEFAULT_STANDBY_UNAVAILABLE_SECONDS) * 1000;
    const down = now - Date.parse(unavailableSince);
    if (down >= limit) return `${standby} has not been ready for ${Math.round(down / 1000)}s (dataDurability preferred)`;
  }
  return undefined;
}

/**
 * Whether the standby the rolling update restarts next is detached first: with dataDurability
 * preferred always (writes continue asynchronously during its restart instead of waiting for it),
 * with required when another replica can take over as the standby, or when detachForUpdates allows
 * commits without a standby during the restart (otherwise it is restarted attached, and writes
 * wait until it is ready again)
 */
export function handoverForUpdate(input: SyncPlanInput): boolean {
  if (!input.rollingTarget) return false;
  const synchronous = input.cluster.spec.replication?.synchronous;
  if (synchronous?.dataDurability === 'preferred' || synchronous?.detachForUpdates) return true;
  return chooseStandby(input) !== undefined;
}

/**
 * The replica to attach: ready, not fenced or re-seeding, caught up, not about to be restarted by
 * the rolling update; the lowest ordinal first
 */
export function chooseStandby(input: SyncPlanInput, exclude: string[] = attachedStandbys(input.status)): string | undefined {
  const { cluster, pods, primaryPod } = input;
  const name = cluster.metadata.name;
  return pods
    .filter((p) => {
      const pod = p.metadata?.name ?? '';
      const lag = Number(p.metadata?.annotations?.[REPLICATION_LAG_ANNOTATION]);
      return (
        pod !== primaryPod &&
        !exclude.includes(pod) &&
        Number(pod.slice(name.length + 1)) < cluster.spec.instances &&
        isPodReady(p) &&
        !p.metadata?.deletionTimestamp &&
        !input.fenced.includes(pod) &&
        !input.reseeding.includes(pod) &&
        pod !== input.rollingTarget &&
        Number.isFinite(lag) &&
        lag <= SYNC_ATTACH_MAX_LAG_SECONDS
      );
    })
    .map((p) => p.metadata!.name!)
    .sort((a, b) => Number(a.slice(name.length + 1)) - Number(b.slice(name.length + 1)))[0];
}

/** Decides the next synchronous replication step */
export function planSynchronous(input: SyncPlanInput): SyncStep {
  const { cluster, primaryPod, pods, status, job, now } = input;
  const at = new Date(now).toISOString();
  const primary = pods.find((p) => p.metadata?.name === primaryPod);
  const primaryReady = Boolean(primary && isPodReady(primary));
  const before = attachedStandbys(status);
  const without = (pod: string) => before.filter((s) => s !== pod);

  // a Job in progress or finished
  if (status && (status.phase === 'Attaching' || status.phase === 'Detaching')) {
    if (!job) {
      return {
        kind: 'finished',
        status: { ...status, standbys: before, phase: 'Failed', message: `the sync-standby Job disappeared`, time: at },
        event: 'SyncStandbyFailed',
      };
    }
    if (conditionTrue(job, 'Failed') && status.phase === 'Detaching') {
      // the primary may still replicate to the standby: attached as before, retried later
      return {
        kind: 'finished',
        status: {
          ...status,
          standbys: before,
          phase: 'Attached',
          message: `detaching ${status.standby} failed (see the Job logs); retried`,
          time: at,
          retryAfter: new Date(now + SYNC_RETRY_SECONDS * 1000).toISOString(),
        },
        event: 'SyncStandbyFailed',
      };
    }
    if (conditionTrue(job, 'Failed')) {
      // an attach that may have stopped the standby's journal shipping, or left it with
      // synchronous changes beyond its position, is not undone: the standby is re-seeded
      const reseed = status.phase === 'Attaching' && parseJobOutcome(input.jobOutcome).outcome !== 'failed clean';
      return {
        kind: 'finished',
        status: {
          ...status,
          standbys: without(status.standby),
          phase: 'Failed',
          message: `${status.phase === 'Attaching' ? 'attaching' : 'detaching'} ${status.standby} failed (see the Job logs)` +
            (reseed ? `; ${status.standby} is re-seeded` : ''),
          time: at,
        },
        reseed: reseed ? status.standby : undefined,
        event: 'SyncStandbyFailed',
      };
    }
    if (!conditionTrue(job, 'Complete')) return { kind: 'none', status };
    const { outcome, paused } = parseJobOutcome(input.jobOutcome);
    if (status.phase === 'Attaching' && outcome === 'attached') {
      return {
        kind: 'finished',
        status: {
          ...withoutRetry(status),
          standbys: [...without(status.standby), status.standby],
          phase: 'Attached',
          message: `${status.standby} is a synchronous standby of ${status.primary}${paused}`,
          time: at,
        },
        event: 'SyncStandbyAttached',
      };
    }
    if (status.phase === 'Detaching' && outcome.startsWith('detached')) {
      const unreachable = outcome === 'detached unreachable';
      const rest = without(status.standby);
      return {
        kind: 'finished',
        status: {
          ...status,
          standbys: rest,
          phase: rest.length > 0 ? 'Attached' : 'Detached',
          message: (unreachable
            ? `${status.standby} detached while unreachable: it is re-seeded`
            : `${status.standby} detached; it continues from the journal`) + paused,
          time: at,
        },
        reseed: unreachable ? status.standby : undefined,
        event: 'SyncStandbyDetached',
      };
    }
    return {
      kind: 'finished',
      status: { ...status, standbys: without(status.standby), phase: 'Failed', message: `unexpected sync-standby Job outcome: ${outcome || 'none'}`, time: at },
      event: 'SyncStandbyFailed',
    };
  }

  // the primary moved (failover, or a switchover after the detach): nothing attached to it
  const current = status && status.primary === primaryPod ? status : undefined;
  const attached = current ? attachedStandbys(current) : [];

  if (current && attached.length > 0) {
    // since when each attached standby has not been ready (statuses before v0.61.0: unavailableSince)
    const unavailable: Record<string, string> = {};
    for (const standby of attached) {
      const pod = pods.find((p) => p.metadata?.name === standby);
      if (pod && isPodReady(pod)) continue;
      unavailable[standby] =
        current.unavailable?.[standby] ?? (standby === current.standby ? current.unavailableSince : undefined) ?? at;
    }
    const kept: SynchronousStatus = { ...current, standbys: attached };
    delete kept.unavailableSince;
    delete kept.unavailable;
    if (Object.keys(unavailable).length > 0) kept.unavailable = unavailable;
    if (!primaryReady) return { kind: 'none', status: kept }; // a primary restart keeps its standbys; failover handles a lost one
    const retrying = Boolean(current.retryAfter && now < Date.parse(current.retryAfter));
    // more standbys than synchronous.number: the highest ordinal goes first
    const excess = synchronousMode(cluster) && attached.length > synchronousNumber(cluster)
      ? [...attached].sort((a, b) => ordinalOf(cluster, b) - ordinalOf(cluster, a))[0]
      : undefined;
    for (const standby of excess ? [excess, ...attached.filter((s) => s !== excess)] : attached) {
      const reason = standby === excess && !detachReason(input, standby, unavailable[standby])
        ? `more synchronous standbys than synchronous.number (${synchronousNumber(cluster)})`
        : detachReason(input, standby, unavailable[standby]);
      if (!reason || retrying) continue;
      const next: SynchronousStatus = { ...withoutRetry(kept), standby, phase: 'Detaching', message: `detaching ${standby}: ${reason}`, time: at };
      return { kind: 'start', action: 'detach', standby, status: next };
    }
    if (retrying || !synchronousMode(cluster) || input.unsupported || input.attachBlocked || attached.length >= synchronousNumber(cluster) || input.busy || input.fenced.includes(primaryPod)) {
      return { kind: 'none', status: kept };
    }
    // one more standby
    if (current.phase === 'Failed' && now - Date.parse(current.time) < SYNC_RETRY_SECONDS * 1000) return { kind: 'none', status: kept };
    const standby = chooseStandby(input, attached);
    if (!standby) return { kind: 'none', status: kept };
    return {
      kind: 'start',
      action: 'attach',
      standby,
      status: { ...withoutRetry(kept), standby, phase: 'Attaching', message: `attaching ${standby} as a synchronous standby`, time: at },
    };
  }

  // nothing attached: attach a standby when possible
  if (synchronousMode(cluster) && input.unsupported) {
    const message = input.unsupported;
    const unchanged = current?.phase === 'Failed' && current.message === message;
    return {
      kind: 'none',
      status: { standby: current?.standby ?? '', primary: primaryPod, standbys: [], phase: 'Failed', message, time: unchanged ? current.time : at },
    };
  }
  const keep: SynchronousStatus | undefined =
    synchronousMode(cluster) && (current?.phase === 'Failed' || current?.phase === 'Detached')
      ? { ...current, standbys: [] }
      : undefined;
  if (keep) {
    delete keep.unavailableSince;
    delete keep.unavailable;
  }
  if (!synchronousMode(cluster) || !primaryReady || input.busy || input.attachBlocked || input.fenced.includes(primaryPod)) {
    return { kind: 'none', status: keep };
  }
  if (keep?.phase === 'Failed' && now - Date.parse(keep.time) < SYNC_RETRY_SECONDS * 1000) return { kind: 'none', status: keep };
  const standby = chooseStandby(input, []);
  if (!standby) return { kind: 'none', status: keep };
  return {
    kind: 'start',
    action: 'attach',
    standby,
    status: { standby, primary: primaryPod, standbys: [], phase: 'Attaching', message: `attaching ${standby} as a synchronous standby`, time: at },
  };
}

const ordinalOf = (cluster: FirebirdCluster, pod: string) => Number(pod.slice(cluster.metadata.name.length + 1));
