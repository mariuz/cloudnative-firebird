import { API_GROUP, FirebirdCluster, RECONCILIATION_DISABLED_ANNOTATION, reconciliationDisabled } from '../types';

/**
 * Pausing the reconciliation of a cluster for manual maintenance: spec.suspended, the
 * reconciliationLoop annotation set to disabled (CloudNativePG's cnpg.io/reconciliationLoop; kubectl
 * firebird reconcile off), or reconciliationDisabled set to "true" as on the other resources
 * (CloudNativePG 1.29 cnpg.io/reconciliationDisabled). The annotations need no spec change. While
 * paused the operator changes nothing of the cluster: no rolling update, switchover, failover,
 * re-seed or resource update. What already runs keeps running (the instances, their Services, the
 * scheduled backup and maintenance CronJobs).
 */

export const RECONCILIATION_LOOP_ANNOTATION = `${API_GROUP}/reconciliationLoop`;

/** Why the cluster's reconciliation is paused, or undefined when it is not */
export function reconciliationPaused(cluster: FirebirdCluster): string | undefined {
  if (cluster.spec.suspended) return 'Reconciliation suspended by spec.suspended';
  const loop = cluster.metadata.annotations?.[RECONCILIATION_LOOP_ANNOTATION];
  if (loop?.trim().toLowerCase() === 'disabled') return `Reconciliation disabled by the ${RECONCILIATION_LOOP_ANNOTATION} annotation`;
  if (reconciliationDisabled(cluster)) return `Reconciliation disabled by the ${RECONCILIATION_DISABLED_ANNOTATION} annotation`;
  return undefined;
}
