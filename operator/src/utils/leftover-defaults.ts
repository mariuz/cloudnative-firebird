import { V1PodSpec, V1StatefulSet } from '@kubernetes/client-node';
import { FirebirdCluster } from '../types';
import { SEGMENT_TLS_CONTAINER } from './segment-tls-pods';
import { PRIMARY_LEASE_ANNOTATION, PRIMARY_LEASE_CONTAINER } from './primary-lease';
import { POD_ANTI_AFFINITY_ANNOTATION } from './scheduling';
import { buildStatefulSet } from './resources';

/** The defaults a cluster's first reconcile records (segment TLS, the primary Lease, pod anti-affinity) */
export interface FirstDefaults {
  segmentTls: boolean;
  primaryLease: boolean;
  podAntiAffinity: boolean;
}

/**
 * Whether the StatefulSet a cluster finds on its first reconcile was left by a deleted cluster of
 * its name (orphan propagation, or a restore by a backup tool) rather than made for this one: it
 * is not owned by this cluster. A StatefulSet this cluster owns is one an earlier version created.
 */
export function leftByDeletedCluster(statefulSet: V1StatefulSet, cluster: FirebirdCluster): boolean {
  return !(statefulSet.metadata?.ownerReferences ?? []).some((r) => r.uid === cluster.metadata.uid);
}

/**
 * The first defaults of a cluster recreated over what a deleted one of its name left: each follows
 * the leftover StatefulSet's pod template where that template shows it (the setting changes the
 * template this cluster would run: its sidecar, its anti-affinity term), so the instances keep
 * running as they did instead of rolling to plain segment shipping, no Lease holder and no
 * anti-affinity. A setting the template cannot show (the Lease holder of a cluster without
 * automatic failover) takes the default of a new cluster.
 */
export function leftoverDefaults(statefulSet: V1StatefulSet, cluster: FirebirdCluster, fresh: FirstDefaults): FirstDefaults {
  const left = statefulSet.spec?.template?.spec;
  const built = (d: FirstDefaults) =>
    buildStatefulSet({
      ...cluster,
      spec: { ...cluster.spec, segmentTLS: { ...cluster.spec.segmentTLS, enabled: d.segmentTls } },
      metadata: {
        ...cluster.metadata,
        annotations: {
          ...cluster.metadata.annotations,
          [PRIMARY_LEASE_ANNOTATION]: d.primaryLease ? 'enabled' : 'pinned',
          [POD_ANTI_AFFINITY_ANNOTATION]: d.podAntiAffinity ? 'enabled' : 'pinned',
        },
      },
    }).spec?.template?.spec;
  const markers: Record<keyof FirstDefaults, (pod: V1PodSpec | undefined) => string> = {
    segmentTls: (pod) => String(names(pod).includes(SEGMENT_TLS_CONTAINER)),
    primaryLease: (pod) => String(names(pod).includes(PRIMARY_LEASE_CONTAINER)),
    podAntiAffinity: (pod) => canonical(pod?.affinity?.podAntiAffinity ?? null),
  };
  const out = { ...fresh };
  for (const key of Object.keys(markers) as Array<keyof FirstDefaults>) {
    const on = markers[key](built({ ...fresh, [key]: true }));
    const off = markers[key](built({ ...fresh, [key]: false }));
    if (on === off) continue; // not visible in this cluster's template: the new-cluster default
    out[key] = markers[key](left) === on;
  }
  return out;
}

function names(pod: V1PodSpec | undefined): string[] {
  return [...(pod?.initContainers ?? []), ...(pod?.containers ?? [])].map((c) => c.name);
}

/** JSON with sorted keys and without undefined fields: the API server orders fields its own way */
function canonical(value: unknown): string {
  return JSON.stringify(value, (_key, v) =>
    v && typeof v === 'object' && !Array.isArray(v) ? Object.fromEntries(Object.entries(v).filter(([, x]) => x !== undefined).sort(([a], [b]) => a.localeCompare(b))) : v,
  );
}
