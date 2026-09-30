import { V1PersistentVolumeClaim, V1Pod } from '@kubernetes/client-node';
import { FirebirdCluster, VolumeRecreationStatus } from '../types';
import { RESEED_ANNOTATION } from './replication';
import { isPodReady } from './routing';

/**
 * Re-creating a replica's volume (`firebird.cloudnative-firebird.io/reseed=volume` on the pod), for
 * a volume that is lost or unusable, e.g. local storage on a node that is gone. Plain re-seeding
 * keeps the PVC.
 *
 * The StatefulSet recreates a deleted pod against its existing claim, even while that claim is
 * terminating, and only creates a claim together with a new pod. So the operator deletes the claim
 * and the pod, then deletes the pod again as long as the old claim still exists or no claim exists
 * (the pod waits for a claim that will never come). Once the StatefulSet has created a new claim,
 * the pod starts on an empty volume and its replication init seeds it like a new replica; the
 * request is done when that pod is ready. Users (FirebirdUser) are re-applied because the volume
 * changed. Only replicas of a replication cluster: a primary or a standalone instance would start
 * with an empty database.
 */

/** Pod annotation value (of RESEED_ANNOTATION) asking for a new volume */
export const RESEED_VOLUME = 'volume';

/** Name of an instance's data PVC (StatefulSet volumeClaimTemplate "firebird-data") */
export const dataClaimName = (pod: string): string => `firebird-data-${pod}`;

export interface VolumeRecreationPlan {
  /** Requests still in progress (status.recreatingVolumes) */
  recreating: VolumeRecreationStatus[];
  /** Claims to delete (a new request, or an old claim not yet marked for deletion) */
  deleteClaims: string[];
  /** Pods to delete so the StatefulSet recreates them, and their claim once the old one is gone */
  deletePods: string[];
  started: string[];
  completed: string[];
  /** Annotated pods that cannot be re-created, with the reason */
  ignored: Array<{ pod: string; reason: string }>;
}

export function planVolumeRecreation(options: {
  cluster: FirebirdCluster;
  primaryPod: string;
  pods: V1Pod[];
  claims: V1PersistentVolumeClaim[];
  replication: boolean;
}): VolumeRecreationPlan {
  const { cluster, primaryPod, pods, claims, replication } = options;
  const { name } = cluster.metadata;
  const plan: VolumeRecreationPlan = { recreating: [], deleteClaims: [], deletePods: [], started: [], completed: [], ignored: [] };
  const podOf = (pod: string) => pods.find((p) => p.metadata?.name === pod);
  const claimOf = (pod: string) => claims.find((c) => c.metadata?.name === dataClaimName(pod));
  const inCluster = (pod: string) => Number(pod.slice(name.length + 1)) < cluster.spec.instances;

  for (const entry of cluster.status?.recreatingVolumes ?? []) {
    if (!inCluster(entry.pod)) continue; // scaled away
    const claim = claimOf(entry.pod);
    const pod = podOf(entry.pod);
    if (claim && claim.metadata?.uid !== entry.claimUid) {
      // the StatefulSet created the new claim with the pod: done once the replica is seeded
      if (pod && isPodReady(pod)) {
        plan.completed.push(entry.pod);
        continue;
      }
    } else {
      if (claim && !claim.metadata?.deletionTimestamp) plan.deleteClaims.push(entry.pod);
      if (pod && !pod.metadata?.deletionTimestamp) plan.deletePods.push(entry.pod);
    }
    plan.recreating.push(entry);
  }

  for (const pod of pods) {
    const podName = pod.metadata?.name ?? '';
    if (pod.metadata?.annotations?.[RESEED_ANNOTATION] !== RESEED_VOLUME || pod.metadata.deletionTimestamp) continue;
    if (plan.recreating.some((e) => e.pod === podName) || plan.completed.includes(podName)) continue;
    const reason = !replication
      ? 'replication is not enabled: the instance would start with an empty database'
      : podName === primaryPod
        ? 'the primary cannot be re-created (switch over first)'
        : cluster.spec.hibernated
          ? 'the cluster is hibernated'
          : !inCluster(podName)
            ? 'the instance is scaled away'
            : !claimOf(podName)?.metadata?.uid
              ? 'the instance has no volume claim'
              : undefined;
    if (reason) {
      plan.ignored.push({ pod: podName, reason });
      continue;
    }
    plan.recreating.push({ pod: podName, claimUid: claimOf(podName)!.metadata!.uid! });
    plan.deleteClaims.push(podName);
    plan.deletePods.push(podName);
    plan.started.push(podName);
  }
  plan.recreating.sort((a, b) => a.pod.localeCompare(b.pod, undefined, { numeric: true }));
  return plan;
}
