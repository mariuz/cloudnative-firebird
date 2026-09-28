import { V1Pod, V1StatefulSet } from '@kubernetes/client-node';
import { FirebirdCluster, SwitchoverStatus } from '../types';
import { replicationEnabled } from './replication';
import { isPodReady } from './routing';

/**
 * Rolling updates with the primary last (CloudNativePG `primaryUpdateStrategy` /
 * `primaryUpdateMethod`).
 *
 * With replication the StatefulSet uses the OnDelete update strategy, so a template change
 * (image, resources, configuration) restarts no pod by itself. The operator restarts the outdated
 * replicas one at a time, each only once every instance is ready again, and the primary last:
 *
 * - `primaryUpdateMethod: restart` (default): the primary pod is restarted in place.
 * - `primaryUpdateMethod: switchover`: an updated replica is promoted through the targetPrimary
 *   annotation; the demoted primary restarts on the new revision. Falls back to a restart when no
 *   updated replica is ready or the last switchover to it failed.
 * - `primaryUpdateStrategy: supervised`: the primary is left alone until the user switches over
 *   (targetPrimary annotation) or deletes its pod.
 *
 * Fenced instances are not restarted; they are updated once unfenced.
 */

/**
 * How long automatic failover leaves a primary alone after the rolling update restarted it
 * (at least failover.delaySeconds): a planned restart must not turn into a lossy failover.
 */
export const PRIMARY_RESTART_GRACE_SECONDS = 300;

/** Pod label set by the StatefulSet controller with the revision the pod was created from */
export const REVISION_LABEL = 'controller-revision-hash';

/** Whether the operator (rather than the StatefulSet controller) rolls the instance pods */
export function operatorRollsPods(cluster: FirebirdCluster): boolean {
  return replicationEnabled(cluster);
}

export interface RollingUpdatePlan {
  /** StatefulSet revision the pods are updated to */
  revision: string;
  /** Instance pods not created from that revision */
  outdated: string[];
  /** Pod to delete now (the StatefulSet recreates it from the new revision) */
  restart?: string;
  /** Replica to promote through the targetPrimary annotation */
  switchoverTo?: string;
  message: string;
}

const ordinal = (pod: string) => Number(pod.slice(pod.lastIndexOf('-') + 1));

/**
 * Decides the next step of a rolling update. Returns undefined when the operator does not roll the
 * pods or the StatefulSet controller has not observed the latest template yet.
 */
export function planRollingUpdate(options: {
  cluster: FirebirdCluster;
  statefulSet: V1StatefulSet;
  pods: V1Pod[];
  primaryPod: string;
  fenced: string[];
  /** A switchover, failover or re-seed is in progress: restart nothing */
  busy?: string;
  lastSwitchover?: SwitchoverStatus;
}): RollingUpdatePlan | undefined {
  const { cluster, statefulSet, pods, primaryPod, fenced, busy, lastSwitchover } = options;
  if (!operatorRollsPods(cluster) || cluster.spec.hibernated) return undefined;
  const revision = statefulSet.status?.updateRevision;
  const generation = statefulSet.metadata?.generation ?? 0;
  if (!revision || (statefulSet.status?.observedGeneration ?? 0) < generation) return undefined;

  const name = (p: V1Pod) => p.metadata?.name ?? '';
  const outdated = pods
    .filter((p) => p.metadata?.labels?.[REVISION_LABEL] !== revision)
    .map(name)
    .sort((a, b) => ordinal(a) - ordinal(b));
  const plan: RollingUpdatePlan = { revision, outdated, message: '' };
  if (outdated.length === 0) {
    plan.message = `all instances run revision ${revision}`;
    return plan;
  }
  if (busy) {
    plan.message = `waiting: ${busy}`;
    return plan;
  }
  const notReady = pods.filter((p) => !fenced.includes(name(p)) && (p.metadata?.deletionTimestamp || !isPodReady(p)));
  if (notReady.length > 0 || pods.length < cluster.spec.instances) {
    plan.message = `waiting for all instances to be ready before restarting the next one (${outdated.join(', ')} outdated)`;
    return plan;
  }

  // Replication just enabled: the primary still runs without it and has no seed for the replicas
  // (they could never become ready), so it goes first: its init container enables publication
  // and writes the offline bootstrap seed from which the replicas are then seeded.
  const primary = pods.find((p) => name(p) === primaryPod);
  if (
    primary &&
    outdated.includes(primaryPod) &&
    !fenced.includes(primaryPod) &&
    (primary.spec?.containers ?? []).length > 0 &&
    !primary.spec!.containers.some((c) => c.name === 'segment-server')
  ) {
    plan.restart = primaryPod;
    plan.message = `replication enabled: restarting the primary ${primaryPod} first to publish and seed the replicas`;
    return plan;
  }

  // replicas first, highest ordinal first like the StatefulSet controller
  const replica = [...outdated].reverse().find((pod) => pod !== primaryPod && !fenced.includes(pod));
  if (replica) {
    plan.restart = replica;
    plan.message = `restarting replica ${replica} on revision ${revision}`;
    return plan;
  }
  if (!outdated.includes(primaryPod)) {
    plan.message = `fenced instances are updated once unfenced (${outdated.join(', ')} outdated)`;
    return plan;
  }
  if (fenced.includes(primaryPod)) {
    plan.message = `the primary ${primaryPod} is fenced: it is updated once unfenced`;
    return plan;
  }
  if (cluster.spec.primaryUpdateStrategy === 'supervised') {
    plan.message =
      `replicas updated; waiting for a switchover (targetPrimary annotation) or a restart of the primary ` +
      `${primaryPod} (primaryUpdateStrategy: supervised)`;
    return plan;
  }
  if (cluster.spec.primaryUpdateMethod === 'switchover') {
    const target = pods
      .map(name)
      .filter((pod) => pod !== primaryPod && !fenced.includes(pod) && !outdated.includes(pod))
      .sort((a, b) => ordinal(a) - ordinal(b))
      .find(
        (pod) =>
          !(lastSwitchover?.phase === 'Failed' && lastSwitchover.target === pod && lastSwitchover.from === primaryPod),
      );
    if (target) {
      plan.switchoverTo = target;
      plan.message = `switching over from ${primaryPod} to ${target} to update the primary`;
      return plan;
    }
  }
  plan.restart = primaryPod;
  plan.message = `restarting the primary ${primaryPod} on revision ${revision}`;
  return plan;
}
