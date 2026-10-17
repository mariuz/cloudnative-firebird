import { V1Pod } from '@kubernetes/client-node';
import { FirebirdCluster } from '../types';
import { RESEED_ANNOTATION } from './replication';
import { RESEED_VOLUME } from './volume-recreation';

/**
 * Node maintenance window with reusePVC: false (CloudNativePG's nodeMaintenanceWindow.reusePVC):
 * a replica whose pod cannot be scheduled while the window is open (its volume bound to a drained
 * node, local storage) gets a new volume elsewhere. The operator's volume re-creation does it
 * (the reseed=volume annotation, utils/volume-recreation.ts): the claim and the pod are deleted
 * and the new instance is seeded again from the primary. Only replicas of a replication cluster:
 * a primary fails over first, and an instance without replication would come back empty.
 */

/** How long a replica pod stays unschedulable before its volume is re-created */
export const UNSCHEDULABLE_GRACE_SECONDS = 30;

/** Whether the window is open with volumes not reused */
export function maintenanceRecreatesVolumes(cluster: FirebirdCluster): boolean {
  const window = cluster.spec.nodeMaintenanceWindow;
  return window?.inProgress === true && window.reusePVC === false && cluster.spec.replication?.enabled === true && !cluster.spec.hibernated;
}

/** When a pod became unschedulable (PodScheduled False, Unschedulable), if it is */
function unschedulableSince(pod: V1Pod): Date | undefined {
  if (pod.status?.phase !== 'Pending' || pod.metadata?.deletionTimestamp) return undefined;
  const scheduled = pod.status.conditions?.find((c) => c.type === 'PodScheduled');
  if (scheduled?.status !== 'False' || scheduled.reason !== 'Unschedulable') return undefined;
  return scheduled.lastTransitionTime ? new Date(scheduled.lastTransitionTime) : undefined;
}

/**
 * The replica pods whose volume the window re-creates now: unschedulable for the grace period,
 * not the primary, not re-created already and not asked for already
 */
export function maintenanceVolumeRecreations(cluster: FirebirdCluster, primaryPod: string, pods: V1Pod[], now: Date): string[] {
  if (!maintenanceRecreatesVolumes(cluster)) return [];
  const recreating = new Set((cluster.status?.recreatingVolumes ?? []).map((r) => r.pod));
  return pods
    .filter((pod) => {
      const name = pod.metadata?.name ?? '';
      if (!name || name === primaryPod || recreating.has(name)) return false;
      if (pod.metadata?.annotations?.[RESEED_ANNOTATION] === RESEED_VOLUME) return false;
      const since = unschedulableSince(pod);
      return since !== undefined && now.getTime() - since.getTime() >= UNSCHEDULABLE_GRACE_SECONDS * 1000;
    })
    .map((pod) => pod.metadata!.name!)
    .sort();
}
