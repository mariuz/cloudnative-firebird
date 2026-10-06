import { V1Job } from '@kubernetes/client-node';
import { API_GROUP, DEFAULT_FIREBIRD_IMAGE, FirebirdCluster } from '../types';
import { clusterLabels, databaseName, FIREBIRD_DATA_DIR, superuserClientEnv, jobPodSpec } from './resources';
import { OPERATOR_CONFIG_DIR, SEGMENT_PORT, instanceHost, isolationCheckTimeoutSeconds } from './replication';

/**
 * Planned switchover, requested with the targetPrimary annotation (CloudNativePG's
 * `kubectl cnpg promote`, which sets status.targetPrimary):
 *
 * 1. Stopping: a Job (switchover.pl) shuts the old primary down (no more writes), reads its last
 *    replication sequence S and waits until segment S is archived and every ready replica, the
 *    target included, has applied it.
 * 2. Promoting: the operator moves the leader Lease and the ConfigMap `primary` entry to the
 *    target and restarts the target and the old primary with "promote" and "demote" directives.
 *    Their init containers apply them offline: the target gets replication sequence S (so its
 *    journal continues at S + 1 and the other replicas keep applying without re-seeding),
 *    replica mode none and publication; the old primary becomes a read-only replica positioned
 *    after S. Replicas that were not ready are re-seeded.
 * 3. Completed once both restarted pods are ready. A failed Job brings the old primary back online.
 */
export const TARGET_PRIMARY_ANNOTATION = `${API_GROUP}/targetPrimary`;

/** How long the switchover Job waits for the last segment to be archived and applied */
export const SWITCHOVER_TIMEOUT_SECONDS = 300;

export function switchoverJobName(cluster: FirebirdCluster): string {
  return `${cluster.metadata.name}-switchover`;
}

export function buildSwitchoverJob(
  cluster: FirebirdCluster,
  options: { from: string; target: string; replicas: string[] },
): V1Job {
  const { name, namespace = 'default', uid } = cluster.metadata;
  const labels = { ...clusterLabels(name), 'app.kubernetes.io/component': 'switchover' };
  return {
    apiVersion: 'batch/v1',
    kind: 'Job',
    metadata: {
      name: switchoverJobName(cluster),
      namespace,
      labels,
      annotations: { [TARGET_PRIMARY_ANNOTATION]: options.target },
      ownerReferences: [
        { apiVersion: `${API_GROUP}/v1`, kind: 'FirebirdCluster', name, uid: uid ?? '', controller: true, blockOwnerDeletion: true },
      ],
    },
    spec: {
      backoffLimit: 0,
      ttlSecondsAfterFinished: 3600,
      template: {
        metadata: { labels },
        spec: jobPodSpec(cluster, {
          restartPolicy: 'Never',
          containers: [
            {
              name: 'switchover',
              image: cluster.spec.imageName ?? DEFAULT_FIREBIRD_IMAGE,
              command: ['perl', `${OPERATOR_CONFIG_DIR}/switchover.pl`],
              env: [
                ...superuserClientEnv(cluster),
                { name: 'OLD_PRIMARY', value: instanceHost(cluster, options.from) },
                { name: 'TARGET', value: instanceHost(cluster, options.target) },
                { name: 'REPLICAS', value: options.replicas.map((p) => instanceHost(cluster, p)).join(' ') },
                { name: 'DATABASE_PATH', value: `${FIREBIRD_DATA_DIR}/${databaseName(cluster)}` },
                { name: 'SEGMENT_PORT', value: String(SEGMENT_PORT) },
                { name: 'TIMEOUT_SECONDS', value: String(SWITCHOVER_TIMEOUT_SECONDS) },
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

/** How long the election waits for replicas to apply the segments they already received */
export const FAILOVER_SETTLE_SECONDS = 60;
export const DEFAULT_FAILOVER_DELAY_SECONDS = 30;
/** Time the isolation check may need beyond its timeout to fence (check interval and timeouts) */
export const ISOLATION_FENCE_MARGIN_SECONDS = 10;

/**
 * How long the primary must be unavailable before a failover starts: failover.delaySeconds, but
 * never less than an isolated primary needs to fence itself, so a promoted replica cannot
 * coexist with a primary still accepting writes on the other side of a partition.
 */
export function effectiveFailoverDelaySeconds(cluster: FirebirdCluster): number {
  const delay = cluster.spec.replication?.failover?.delaySeconds ?? DEFAULT_FAILOVER_DELAY_SECONDS;
  const isolation = isolationCheckTimeoutSeconds(cluster);
  return isolation === undefined ? delay : Math.max(delay, isolation + ISOLATION_FENCE_MARGIN_SECONDS);
}

/** How long every replica must have lost a ready primary before it counts as unavailable */
export const PRIMARY_CUT_OFF_SECONDS = 30;

/** What a replica's segment server answered to PRIMARYSEEN (reply undefined: not reachable) */
export interface PrimaryContact {
  pod: string;
  reply?: string;
}

/**
 * Whether a primary whose pod is ready is cut off all the same: the operator cannot reach its
 * segment server, and every ready replica it can reach has not reached this primary for at least
 * PRIMARY_CUT_OFF_SECONDS (PRIMARYSEEN; "never", or a contact with another primary, counts as
 * lost). At least one replica must say so. Returns why, or undefined.
 */
export function primaryCutOff(options: {
  primaryHost: string;
  operatorReached: boolean;
  replicas: PrimaryContact[];
  thresholdSeconds?: number;
}): string | undefined {
  const { primaryHost, operatorReached, replicas } = options;
  const threshold = options.thresholdSeconds ?? PRIMARY_CUT_OFF_SECONDS;
  if (operatorReached) return undefined;
  const answered = replicas.filter((r) => r.reply !== undefined);
  if (answered.length === 0) return undefined;
  const lost = answered.map((r) => {
    const m = /^OK (\d+) (\S+)$/.exec(r.reply!.trim());
    if (r.reply!.trim() === 'OK never') return { pod: r.pod, lost: true, age: undefined };
    if (!m) return { pod: r.pod, lost: false, age: undefined };
    const age = Number(m[1]);
    const samePrimary = m[2] === primaryHost || m[2].startsWith(`${primaryHost}.`);
    return { pod: r.pod, lost: !samePrimary || age >= threshold, age: samePrimary ? age : undefined };
  });
  if (!lost.every((r) => r.lost)) return undefined;
  const ages = lost.map((r) => `${r.pod}${r.age === undefined ? '' : ` ${r.age}s ago`}`).join(', ');
  return `ready but cut off: the operator cannot reach it, and no replica has reached it for ${threshold}s or more (${ages})`;
}

export function failoverJobName(cluster: FirebirdCluster): string {
  return `${cluster.metadata.name}-failover`;
}

/**
 * Election Job for an automatic failover: reports the most advanced ready replica in its
 * termination message (failover.pl). It changes nothing, so it can be discarded if the primary
 * recovers.
 */
export function buildFailoverJob(cluster: FirebirdCluster, candidates: string[]): V1Job {
  const { name, namespace = 'default', uid } = cluster.metadata;
  const labels = { ...clusterLabels(name), 'app.kubernetes.io/component': 'failover' };
  return {
    apiVersion: 'batch/v1',
    kind: 'Job',
    metadata: {
      name: failoverJobName(cluster),
      namespace,
      labels,
      ownerReferences: [
        { apiVersion: `${API_GROUP}/v1`, kind: 'FirebirdCluster', name, uid: uid ?? '', controller: true, blockOwnerDeletion: true },
      ],
    },
    spec: {
      backoffLimit: 0,
      ttlSecondsAfterFinished: 3600,
      template: {
        metadata: { labels },
        spec: jobPodSpec(cluster, {
          restartPolicy: 'Never',
          containers: [
            {
              name: 'failover',
              image: cluster.spec.imageName ?? DEFAULT_FIREBIRD_IMAGE,
              command: ['perl', `${OPERATOR_CONFIG_DIR}/failover.pl`],
              env: [
                ...superuserClientEnv(cluster),
                { name: 'CANDIDATES', value: candidates.map((p) => instanceHost(cluster, p)).join(' ') },
                { name: 'SEGMENT_PORT', value: String(SEGMENT_PORT) },
                { name: 'SETTLE_SECONDS', value: String(FAILOVER_SETTLE_SECONDS) },
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

/** Parses the election result ("target=<host> sequence=<S> positions=<host>:<seq>,...") */
export function parseElection(message: string): { target: string; sequence: number; positions: Record<string, number> } | undefined {
  const m = /target=(\S+) sequence=(\d+) positions=(\S*)/.exec(message ?? '');
  if (!m) return undefined;
  const positions: Record<string, number> = {};
  for (const entry of m[3].split(',').filter(Boolean)) {
    const i = entry.lastIndexOf(':');
    positions[entry.slice(0, i)] = Number(entry.slice(i + 1));
  }
  return { target: m[1], sequence: Number(m[2]), positions };
}
