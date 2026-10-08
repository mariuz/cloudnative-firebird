import { V1Pod } from '@kubernetes/client-node';
import { API_GROUP, FirebirdCluster } from '../types';
import { isPodReady } from './routing';
import { SEGMENT_TLS_CONTAINER } from './segment-tls-pods';

/**
 * Moving existing clusters to segment TLS (SEGMENT_TLS_MIGRATE), one at a time.
 *
 * Clusters created before v0.77.0 keep plain segment shipping: the operator pins them to
 * `segmentTLS.enabled: false` (segment-tls-default.ts) and, since v0.79.0, marks them with the
 * segment-tls-migration annotation "pinned". With SEGMENT_TLS_MIGRATE the operator switches them
 * on itself, through the same rolling update as an owner's switch (no replica lag, v0.76.0):
 *
 * - `pinned`: the clusters it pinned (the annotation), never one whose owner chose false;
 * - `all`: every cluster with `enabled: false` (also those pinned by v0.77.0 or v0.78.0, before the
 *   annotation), unless the owner opted out;
 * - anything else (default): none.
 *
 * The annotation records the migration: "in-progress" while the instances restart, "done" once they
 * all run the proxy, "skip" when an owner opts the cluster out (or turned segment TLS off again
 * during its migration). A cluster is switched only when it is idle: running, every instance
 * ready, no rolling update, switchover or fencing, and no other cluster migrating.
 */

export const MIGRATION_ANNOTATION = `${API_GROUP}/segment-tls-migration`;

export type MigrationMode = 'off' | 'pinned' | 'all';

export function migrationMode(env = process.env): MigrationMode {
  const value = (env.SEGMENT_TLS_MIGRATE ?? '').trim().toLowerCase();
  return value === 'pinned' || value === 'all' ? value : 'off';
}

export type MigrationStep =
  | { action: 'start' }
  | { action: 'finish' }
  | { action: 'abandon' }
  | { action: 'none'; reason?: string };

const annotation = (cluster: FirebirdCluster) => cluster.metadata.annotations?.[MIGRATION_ANNOTATION];
const key = (cluster: FirebirdCluster) => `${cluster.metadata.namespace ?? 'default'}/${cluster.metadata.name}`;

export function migrationInProgress(cluster: FirebirdCluster): boolean {
  return annotation(cluster) === 'in-progress';
}

/**
 * Why the cluster cannot be switched now, or undefined when it is a candidate (the operator's
 * setting, the owner's choice, the Kubernetes version and the cluster's state; not the others)
 */
export function notMigrating(cluster: FirebirdCluster, mode: MigrationMode, nativeSidecars: boolean | undefined): string | undefined {
  if (mode === 'off') return 'off';
  if (cluster.spec.segmentTLS?.enabled !== false) return 'segment TLS is not off';
  const marked = annotation(cluster);
  if (marked === 'skip' || marked === 'done') return `annotation ${marked}`;
  if (mode === 'pinned' && marked !== 'pinned') return 'not pinned by the operator';
  if (nativeSidecars !== true) return 'Kubernetes without native sidecars (1.29 or later)';
  if (cluster.spec.hibernated || cluster.spec.suspended) return 'hibernated or suspended';
  const status = cluster.status ?? {};
  if (status.phase !== 'Running') return `phase ${status.phase ?? 'unknown'}`;
  if (status.rollingUpdate) return 'rolling update in progress';
  if (status.switchover && !['Completed', 'Failed'].includes(status.switchover.phase)) return 'switchover in progress';
  if ((status.fencedInstances ?? []).length > 0) return 'fenced instances';
  if ((status.readyInstances ?? 0) < cluster.spec.instances) return 'not every instance is ready';
  return undefined;
}

/**
 * The next migration step for a cluster: `pods` are its instance pods (needed while it migrates),
 * `others` every cluster (needed to start one)
 */
export function migrationStep(
  cluster: FirebirdCluster,
  mode: MigrationMode,
  nativeSidecars: boolean | undefined,
  pods: V1Pod[] = [],
  others: FirebirdCluster[] = [],
): MigrationStep {
  if (migrationInProgress(cluster)) {
    if (cluster.spec.segmentTLS?.enabled !== true) return { action: 'abandon' };
    const switched =
      !cluster.status?.rollingUpdate &&
      pods.length >= cluster.spec.instances &&
      pods.every((p) => (p.spec?.initContainers ?? []).some((c) => c.name === SEGMENT_TLS_CONTAINER) && isPodReady(p));
    return switched ? { action: 'finish' } : { action: 'none', reason: 'instances restarting' };
  }
  const reason = notMigrating(cluster, mode, nativeSidecars);
  if (reason) return { action: 'none', reason };
  const busy = others.find((o) => key(o) !== key(cluster) && migrationInProgress(o));
  if (busy) return { action: 'none', reason: `${key(busy)} is migrating` };
  return { action: 'start' };
}
