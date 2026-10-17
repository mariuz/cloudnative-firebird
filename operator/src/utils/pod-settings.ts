import { V1Container, V1CronJob, V1Job, V1ObjectMeta, V1PodSpec } from '@kubernetes/client-node';
import { FirebirdCluster } from '../types';

/**
 * Settings every pod of a cluster gets, the instances' and the operator Jobs' alike
 * (CloudNativePG serviceAccountName, priorityClassName, schedulerName, imagePullSecrets,
 * imagePullPolicy), and the metadata copied onto the cluster's resources (inheritedMetadata).
 */

/** Pod-level fields: the service account, priority class, scheduler and image pull secrets */
export function clusterPodSettings(cluster: FirebirdCluster): Pick<V1PodSpec, 'serviceAccountName' | 'priorityClassName' | 'schedulerName' | 'imagePullSecrets'> {
  const { serviceAccountName, priorityClassName, schedulerName, imagePullSecrets } = cluster.spec;
  return {
    ...(serviceAccountName ? { serviceAccountName } : {}),
    ...(priorityClassName ? { priorityClassName } : {}),
    ...(schedulerName ? { schedulerName } : {}),
    ...(imagePullSecrets && imagePullSecrets.length > 0 ? { imagePullSecrets: imagePullSecrets.map((s) => ({ name: s.name })) } : {}),
  };
}

/** A container with the cluster's image pull policy (kept as it is when none is set) */
export function withPullPolicy(cluster: FirebirdCluster, container: V1Container): V1Container {
  return cluster.spec.imagePullPolicy ? { ...container, imagePullPolicy: cluster.spec.imagePullPolicy } : container;
}

/** A pod spec whose every container (init containers and sidecars too) has the cluster's pull policy */
export function withPodPullPolicy<T extends V1PodSpec>(cluster: FirebirdCluster, spec: T): T {
  if (!cluster.spec.imagePullPolicy) return spec;
  return {
    ...spec,
    containers: spec.containers.map((c) => withPullPolicy(cluster, c)),
    ...(spec.initContainers ? { initContainers: spec.initContainers.map((c) => withPullPolicy(cluster, c)) } : {}),
  };
}

/** Object metadata with the cluster's inherited labels and annotations under its own (which win) */
export function inheritMetadata(cluster: FirebirdCluster, metadata: V1ObjectMeta | undefined): V1ObjectMeta {
  const inherited = cluster.spec.inheritedMetadata;
  const result: V1ObjectMeta = { ...(metadata ?? {}) };
  if (inherited?.labels && Object.keys(inherited.labels).length > 0) result.labels = { ...inherited.labels, ...(metadata?.labels ?? {}) };
  if (inherited?.annotations && Object.keys(inherited.annotations).length > 0) {
    result.annotations = { ...inherited.annotations, ...(metadata?.annotations ?? {}) };
  }
  return result;
}

/** Whether the inherited metadata is missing from (or differs on) an object's metadata */
export function inheritedMetadataMissing(cluster: FirebirdCluster, metadata: V1ObjectMeta | undefined): boolean {
  return Object.keys(missingInheritedMetadata(cluster, metadata)).length > 0;
}

/**
 * The inherited labels and annotations an existing object lacks or has with another value, as
 * merge patch metadata ({} when nothing is missing). Keys the operator sets itself are left out.
 */
export function missingInheritedMetadata(
  cluster: FirebirdCluster,
  metadata: V1ObjectMeta | undefined,
  own: { labels?: Record<string, string>; annotations?: Record<string, string> } = {},
): { labels?: Record<string, string>; annotations?: Record<string, string> } {
  const inherited = cluster.spec.inheritedMetadata ?? {};
  const missing = (wanted: Record<string, string> | undefined, current: Record<string, string> | undefined, mine: Record<string, string> | undefined) =>
    Object.fromEntries(Object.entries(wanted ?? {}).filter(([k, v]) => !(mine && k in mine) && current?.[k] !== v));
  const labels = missing(inherited.labels, metadata?.labels, own.labels);
  const annotations = missing(inherited.annotations, metadata?.annotations, own.annotations);
  return {
    ...(Object.keys(labels).length > 0 ? { labels } : {}),
    ...(Object.keys(annotations).length > 0 ? { annotations } : {}),
  };
}

/**
 * A Job or CronJob of the cluster with the inherited metadata on it and on its pods (and, for a
 * CronJob, on the Jobs it creates)
 */
export function withInheritedMetadata<T extends V1Job | V1CronJob>(cluster: FirebirdCluster, obj: T): T {
  const inherited = cluster.spec.inheritedMetadata;
  if (!inherited || (Object.keys(inherited.labels ?? {}).length === 0 && Object.keys(inherited.annotations ?? {}).length === 0)) return obj;
  if (obj.kind === 'CronJob' || 'jobTemplate' in (obj.spec ?? {})) {
    const cron = obj as V1CronJob;
    const jobTemplate = cron.spec?.jobTemplate;
    return {
      ...cron,
      metadata: inheritMetadata(cluster, cron.metadata),
      spec: cron.spec && {
        ...cron.spec,
        jobTemplate: jobTemplate && {
          ...jobTemplate,
          metadata: inheritMetadata(cluster, jobTemplate.metadata),
          spec: jobTemplate.spec && {
            ...jobTemplate.spec,
            template: { ...jobTemplate.spec.template, metadata: inheritMetadata(cluster, jobTemplate.spec.template.metadata) },
          },
        },
      },
    } as T;
  }
  const job = obj as V1Job;
  return {
    ...job,
    metadata: inheritMetadata(cluster, job.metadata),
    spec: job.spec && { ...job.spec, template: { ...job.spec.template, metadata: inheritMetadata(cluster, job.spec.template.metadata) } },
  } as T;
}
