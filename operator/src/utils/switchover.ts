import { V1Job } from '@kubernetes/client-node';
import { API_GROUP, DEFAULT_FIREBIRD_IMAGE, FirebirdCluster } from '../types';
import { clusterLabels, databaseName, FIREBIRD_DATA_DIR, superuserClientEnv } from './resources';
import { OPERATOR_CONFIG_DIR, SEGMENT_PORT, instanceHost } from './replication';

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
        spec: {
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
        },
      },
    },
  };
}

/** How long the election waits for replicas to apply the segments they already received */
export const FAILOVER_SETTLE_SECONDS = 60;
export const DEFAULT_FAILOVER_DELAY_SECONDS = 30;

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
        spec: {
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
        },
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
