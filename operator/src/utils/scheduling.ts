import { V1Affinity, V1LabelSelector, V1PodAffinityTerm, V1TopologySpreadConstraint } from '@kubernetes/client-node';
import { API_GROUP, FirebirdCluster } from '../types';

/**
 * Spreading the instances (CloudNativePG's affinity.enablePodAntiAffinity, podAntiAffinityType,
 * topologyKey and topologySpreadConstraints).
 *
 * New clusters get a pod anti-affinity among their own instance pods, so that a node (the
 * topology key, kubernetes.io/hostname by default) runs at most one instance where it can:
 * "preferred" (default) still schedules instances together when no other node fits, "required"
 * leaves them pending instead. spec.podAntiAffinity.enabled: false turns it off. The term is added
 * to spec.affinity, whose own rules are kept. Clusters that existed when the operator first saw
 * them are pinned without it (the annotation): adding it would restart their instances. Setting
 * spec.podAntiAffinity.enabled applies it to them.
 *
 * spec.topologySpreadConstraints are passed to the instance pods; a constraint without a
 * labelSelector gets the instance pods' one.
 */

/** The decision recorded on the cluster's first reconcile: "enabled" (new cluster) or "pinned" */
export const POD_ANTI_AFFINITY_ANNOTATION = `${API_GROUP}/pod-anti-affinity`;

export const DEFAULT_TOPOLOGY_KEY = 'kubernetes.io/hostname';

/** Selects the cluster's instance pods (not its Job pods, whose component differs) */
export function instanceLabelSelector(cluster: FirebirdCluster): V1LabelSelector {
  return {
    matchLabels: {
      [`${API_GROUP}/cluster`]: cluster.metadata.name,
      'app.kubernetes.io/component': 'database',
    },
  };
}

/** Whether the instances repel each other: the owner's choice, else what was recorded */
export function podAntiAffinityEnabled(cluster: FirebirdCluster): boolean {
  return cluster.spec.podAntiAffinity?.enabled ?? cluster.metadata.annotations?.[POD_ANTI_AFFINITY_ANNOTATION] === 'enabled';
}

/** The instance pods' affinity: spec.affinity, plus the anti-affinity among the instances */
export function instanceAffinity(cluster: FirebirdCluster): V1Affinity | undefined {
  const own = cluster.spec.affinity as V1Affinity | undefined;
  if (!podAntiAffinityEnabled(cluster)) return own;
  const term: V1PodAffinityTerm = {
    labelSelector: instanceLabelSelector(cluster),
    topologyKey: cluster.spec.podAntiAffinity?.topologyKey ?? DEFAULT_TOPOLOGY_KEY,
  };
  const anti = own?.podAntiAffinity ?? {};
  return {
    ...own,
    podAntiAffinity:
      cluster.spec.podAntiAffinity?.type === 'required'
        ? { ...anti, requiredDuringSchedulingIgnoredDuringExecution: [...(anti.requiredDuringSchedulingIgnoredDuringExecution ?? []), term] }
        : {
            ...anti,
            preferredDuringSchedulingIgnoredDuringExecution: [
              ...(anti.preferredDuringSchedulingIgnoredDuringExecution ?? []),
              { weight: 100, podAffinityTerm: term },
            ],
          },
  };
}

/** spec.topologySpreadConstraints, each with the instance pods' selector unless it has one */
export function instanceTopologySpreadConstraints(cluster: FirebirdCluster): V1TopologySpreadConstraint[] | undefined {
  const constraints = cluster.spec.topologySpreadConstraints as V1TopologySpreadConstraint[] | undefined;
  if (!constraints || constraints.length === 0) return undefined;
  return constraints.map((c) => (c.labelSelector ? c : { ...c, labelSelector: instanceLabelSelector(cluster) }));
}
