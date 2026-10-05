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
 * One replica, the synchronous standby, is attached at a time. It must not apply the journal as
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
 */

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
export function buildSyncStandbyJob(cluster: FirebirdCluster, action: SyncAction, primary: string, standby: string): V1Job {
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
 * with required only when another replica can take over as the standby (otherwise it is restarted
 * attached, and writes wait until it is ready again)
 */
export function handoverForUpdate(input: SyncPlanInput): boolean {
  if (!input.rollingTarget) return false;
  if (input.cluster.spec.replication?.synchronous?.dataDurability === 'preferred') return true;
  return chooseStandby(input) !== undefined;
}

/**
 * The replica to attach: ready, not fenced or re-seeding, caught up, not about to be restarted by
 * the rolling update; the lowest ordinal first
 */
export function chooseStandby(input: SyncPlanInput): string | undefined {
  const { cluster, pods, primaryPod } = input;
  const name = cluster.metadata.name;
  return pods
    .filter((p) => {
      const pod = p.metadata?.name ?? '';
      const lag = Number(p.metadata?.annotations?.[REPLICATION_LAG_ANNOTATION]);
      return (
        pod !== primaryPod &&
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

  // a Job in progress or finished
  if (status && (status.phase === 'Attaching' || status.phase === 'Detaching')) {
    if (!job) {
      return { kind: 'finished', status: { ...status, phase: 'Failed', message: `the sync-standby Job disappeared`, time: at }, event: 'SyncStandbyFailed' };
    }
    if (conditionTrue(job, 'Failed') && status.phase === 'Detaching') {
      // the primary may still replicate to the standby: attached as before, retried later
      return {
        kind: 'finished',
        status: {
          ...status,
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
      const reseed = status.phase === 'Attaching' && (input.jobOutcome ?? '').trim() !== 'failed clean';
      return {
        kind: 'finished',
        status: {
          ...status,
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
    const outcome = (input.jobOutcome ?? '').trim();
    if (status.phase === 'Attaching' && outcome === 'attached') {
      return {
        kind: 'finished',
        status: { ...withoutRetry(status), phase: 'Attached', message: `${status.standby} is the synchronous standby of ${status.primary}`, time: at },
        event: 'SyncStandbyAttached',
      };
    }
    if (status.phase === 'Detaching' && outcome.startsWith('detached')) {
      const unreachable = outcome === 'detached unreachable';
      return {
        kind: 'finished',
        status: {
          ...status,
          phase: 'Detached',
          message: unreachable
            ? `${status.standby} detached while unreachable: it is re-seeded`
            : `${status.standby} detached; it continues from the journal`,
          time: at,
        },
        reseed: unreachable ? status.standby : undefined,
        event: 'SyncStandbyDetached',
      };
    }
    return { kind: 'finished', status: { ...status, phase: 'Failed', message: `unexpected sync-standby Job outcome: ${outcome || 'none'}`, time: at }, event: 'SyncStandbyFailed' };
  }

  // the primary moved (failover, or a switchover after the detach): no standby attached to it
  const current = status && status.primary === primaryPod ? status : undefined;

  if (current?.phase === 'Attached') {
    const standbyPod = pods.find((p) => p.metadata?.name === current.standby);
    const standbyReady = Boolean(standbyPod && isPodReady(standbyPod));
    const unavailableSince = standbyReady ? undefined : (current.unavailableSince ?? at);
    const attached: SynchronousStatus = { ...current, unavailableSince };
    if (!unavailableSince) delete attached.unavailableSince;
    if (!primaryReady) return { kind: 'none', status: attached }; // a primary restart keeps its standby; failover handles a lost one
    const reason = detachReason(input, current.standby, unavailableSince);
    if (!reason || (current.retryAfter && now < Date.parse(current.retryAfter))) return { kind: 'none', status: attached };
    return {
      kind: 'start',
      action: 'detach',
      standby: current.standby,
      status: { ...withoutRetry(attached), phase: 'Detaching', message: `detaching ${current.standby}: ${reason}`, time: at },
    };
  }

  // nothing attached: attach a standby when possible
  const keep = synchronousMode(cluster) && (current?.phase === 'Failed' || current?.phase === 'Detached') ? current : undefined;
  if (!synchronousMode(cluster) || !primaryReady || input.busy || input.fenced.includes(primaryPod)) {
    return { kind: 'none', status: keep };
  }
  if (keep?.phase === 'Failed' && now - Date.parse(keep.time) < SYNC_RETRY_SECONDS * 1000) return { kind: 'none', status: keep };
  const standby = chooseStandby(input);
  if (!standby) return { kind: 'none', status: keep };
  return {
    kind: 'start',
    action: 'attach',
    standby,
    status: { standby, primary: primaryPod, phase: 'Attaching', message: `attaching ${standby} as the synchronous standby`, time: at },
  };
}
