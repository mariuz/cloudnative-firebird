import { V1Pod } from '@kubernetes/client-node';
import { API_GROUP, ReadOnlyRoutingConfiguration } from '../types';

/** Pod label holding the instance role (primary / replica) */
export const ROLE_LABEL = `${API_GROUP}/role`;

/** Pod label marking a pod as eligible for read-only traffic */
export const READ_ROUTABLE_LABEL = `${API_GROUP}/read-routable`;

/** Pod annotation where the replication agent / exporter reports replication lag in seconds */
export const REPLICATION_LAG_ANNOTATION = `${API_GROUP}/replication-lag-seconds`;

/** Default maximum replication lag for a replica to receive read traffic */
export const DEFAULT_MAX_LAG_SECONDS = 30;

/** Desired routing labels for a single pod */
export interface PodRoutingDecision {
  name: string;
  role: 'primary' | 'replica';
  readRoutable: boolean;
}

/** Result of a read-only routing computation across all cluster pods */
export interface ReadRoutingPlan {
  decisions: PodRoutingDecision[];
  readRoutablePods: string[];
  laggingReplicas: string[];
}

/** Returns true when the pod is running and has a True Ready condition */
export function isPodReady(pod: V1Pod): boolean {
  if (pod.metadata?.deletionTimestamp) return false;
  return (
    pod.status?.phase === 'Running' &&
    (pod.status.conditions ?? []).some((c) => c.type === 'Ready' && c.status === 'True')
  );
}

/**
 * Returns the replication lag reported on the pod, or null when it is missing or invalid.
 */
export function replicationLagSeconds(pod: V1Pod): number | null {
  const raw = pod.metadata?.annotations?.[REPLICATION_LAG_ANNOTATION];
  if (raw === undefined || raw.trim() === '') return null;
  const lag = Number(raw);
  return Number.isFinite(lag) && lag >= 0 ? lag : null;
}

/**
 * Computes which pods should receive read-only traffic.
 * A replica is routable when it is ready and its reported replication lag is within
 * maxLagSeconds. Replicas that have not reported lag yet are routed on readiness alone.
 * When no replica is eligible and fallbackToPrimary is enabled, the ready primary
 * serves read-only traffic too.
 */
export function computeReadRouting(
  pods: V1Pod[],
  primaryPod: string,
  config: ReadOnlyRoutingConfiguration,
): ReadRoutingPlan {
  const maxLag = config.maxLagSeconds ?? DEFAULT_MAX_LAG_SECONDS;
  const fallbackToPrimary = config.fallbackToPrimary ?? true;
  const laggingReplicas: string[] = [];

  const decisions: PodRoutingDecision[] = pods
    .filter((pod) => pod.metadata?.name)
    .map((pod) => {
      const name = pod.metadata!.name!;
      if (name === primaryPod) {
        return { name, role: 'primary' as const, readRoutable: false };
      }
      const lag = replicationLagSeconds(pod);
      const lagging = lag !== null && lag > maxLag;
      if (lagging) laggingReplicas.push(name);
      return { name, role: 'replica' as const, readRoutable: isPodReady(pod) && !lagging };
    });

  if (fallbackToPrimary && !decisions.some((d) => d.readRoutable)) {
    const primary = decisions.find((d) => d.role === 'primary');
    const primaryPodObj = pods.find((p) => p.metadata?.name === primaryPod);
    if (primary && primaryPodObj && isPodReady(primaryPodObj)) {
      primary.readRoutable = true;
    }
  }

  return {
    decisions,
    readRoutablePods: decisions.filter((d) => d.readRoutable).map((d) => d.name),
    laggingReplicas,
  };
}

/** Escapes a label key for use in a JSON Patch path (RFC 6901) */
function escapeJsonPointer(key: string): string {
  return key.replace(/~/g, '~0').replace(/\//g, '~1');
}

/**
 * Builds the JSON Patch operations needed to apply routing labels to a pod.
 * Returns an empty array when the pod already carries the desired labels.
 */
export function podRoutingLabelPatch(
  pod: V1Pod,
  decision: PodRoutingDecision,
): Array<{ op: 'add'; path: string; value: unknown }> {
  const current = pod.metadata?.labels;
  const desired: Record<string, string> = {
    [ROLE_LABEL]: decision.role,
    [READ_ROUTABLE_LABEL]: String(decision.readRoutable),
  };

  if (!current) {
    return [{ op: 'add', path: '/metadata/labels', value: desired }];
  }

  return Object.entries(desired)
    .filter(([key, value]) => current[key] !== value)
    .map(([key, value]) => ({
      op: 'add' as const,
      path: `/metadata/labels/${escapeJsonPointer(key)}`,
      value,
    }));
}
