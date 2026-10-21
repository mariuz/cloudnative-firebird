import { V1Pod } from '@kubernetes/client-node';
import { FirebirdCluster, SwitchoverStatus } from '../types';
import { isPodReady } from './routing';

/**
 * Scaling a replication cluster down (spec.instances, also through the scale subresource: kubectl
 * scale, an HPA) removes the instances with the highest ordinals, as the StatefulSet does. The
 * primary can be any ordinal after a switchover or failover, so a scale-down that would remove it
 * waits: the StatefulSet keeps the primary's ordinal (and a switchover's instances) running, and
 * the operator switches over to the lowest-ordinal ready replica that stays. Once the primary is
 * below spec.instances and no switchover is in flight, the StatefulSet shrinks. The volumes of the
 * removed instances stay (a later scale-up reuses them).
 */
export interface ScaleDownHold {
  /** The replicas the StatefulSet keeps meanwhile */
  replicas: number;
  /** The replica to switch over to, when none is requested yet and no switchover is in flight */
  switchoverTo?: string;
  message: string;
}

export function scaleDownHold(cluster: FirebirdCluster, primaryPod: string, pods: V1Pod[], switchover?: SwitchoverStatus): ScaleDownHold | undefined {
  if (!cluster.spec.replication?.enabled || cluster.spec.hibernated) return undefined;
  const name = cluster.metadata.name!;
  const instances = cluster.spec.instances;
  const ordinal = (pod: string | undefined): number => (pod && pod.startsWith(`${name}-`) ? Number(pod.slice(name.length + 1)) : NaN);
  const inFlight = switchover && (switchover.phase === 'Electing' || switchover.phase === 'Stopping' || switchover.phase === 'Promoting');
  let keep = instances;
  const beyond = (pod: string | undefined) => {
    const o = ordinal(pod);
    if (Number.isInteger(o) && o >= instances) keep = Math.max(keep, o + 1);
  };
  beyond(primaryPod);
  if (inFlight) {
    beyond(switchover!.from);
    beyond(switchover!.target);
  }
  if (keep === instances) return undefined;
  if (inFlight) {
    return { replicas: keep, message: `scaling down to ${instances} waits for the ${switchover!.kind === 'failover' ? 'failover' : 'switchover'} in progress` };
  }
  const target = pods
    .filter((p) => p.metadata?.name && p.metadata.name !== primaryPod && isPodReady(p) && ordinal(p.metadata.name) < instances)
    .map((p) => p.metadata!.name!)
    .sort((a, b) => ordinal(a) - ordinal(b))[0];
  return {
    replicas: keep,
    switchoverTo: target,
    message: target
      ? `scaling down to ${instances} removes the primary ${primaryPod}: switching over to ${target} first`
      : `scaling down to ${instances} removes the primary ${primaryPod}: waiting for a ready replica below ordinal ${instances} to switch over to`,
  };
}
