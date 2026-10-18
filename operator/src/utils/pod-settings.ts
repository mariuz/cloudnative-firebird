import { V1Container, V1CronJob, V1EnvFromSource, V1Job, V1ObjectMeta, V1PodSpec, V1Volume, V1VolumeMount } from '@kubernetes/client-node';
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

/** The projected volume of spec.projectedVolumeTemplate and where the Firebird container mounts it */
export const PROJECTED_VOLUME = 'projected';
export const PROJECTED_DIR = '/projected';

/** spec.projectedVolumeTemplate as a volume, or none */
export function projectedVolume(cluster: FirebirdCluster): V1Volume | undefined {
  const template = cluster.spec.projectedVolumeTemplate;
  return template && (template.sources ?? []).length > 0 ? { name: PROJECTED_VOLUME, projected: template } : undefined;
}

/** Its read-only mount in the Firebird container, or none */
export function projectedVolumeMount(cluster: FirebirdCluster): V1VolumeMount | undefined {
  return projectedVolume(cluster) ? { name: PROJECTED_VOLUME, mountPath: PROJECTED_DIR, readOnly: true } : undefined;
}

/** spec.envFrom for the Firebird container, or none */
export function instanceEnvFrom(cluster: FirebirdCluster): V1EnvFromSource[] | undefined {
  const envFrom = cluster.spec.envFrom;
  return envFrom && envFrom.length > 0 ? envFrom : undefined;
}

const PROJECTION_KINDS = ['secret', 'configMap', 'downwardAPI', 'serviceAccountToken', 'clusterTrustBundle'] as const;

/** Errors of spec.envFrom and spec.projectedVolumeTemplate */
export function extraConfigErrors(spec: FirebirdCluster['spec']): string[] {
  const errors: string[] = [];
  const dnsName = (name: unknown) => typeof name === 'string' && name.length <= 253 && /^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$/.test(name);
  (spec.envFrom ?? []).forEach((source, i) => {
    const refs = [source?.configMapRef, source?.secretRef].filter((r) => r !== undefined);
    if (refs.length !== 1) errors.push(`envFrom[${i}] needs exactly one of configMapRef and secretRef`);
    else if (!dnsName(refs[0]!.name)) errors.push(`envFrom[${i}]: the ConfigMap or Secret name must be a DNS subdomain name`);
    if (source?.prefix !== undefined && !/^[A-Za-z_][A-Za-z0-9_]*$/.test(source.prefix)) {
      errors.push(`envFrom[${i}].prefix must be a valid environment variable name prefix`);
    }
  });
  const template = spec.projectedVolumeTemplate;
  if (template !== undefined) {
    if (!Array.isArray(template.sources)) errors.push('projectedVolumeTemplate.sources must be a list');
    else {
      template.sources.forEach((source, i) => {
        const kinds = PROJECTION_KINDS.filter((k) => (source as Record<string, unknown>)?.[k] !== undefined);
        if (kinds.length !== 1) errors.push(`projectedVolumeTemplate.sources[${i}] needs exactly one of ${PROJECTION_KINDS.join(', ')}`);
      });
    }
    if (template.defaultMode !== undefined && (!Number.isInteger(template.defaultMode) || template.defaultMode < 0 || template.defaultMode > 0o777)) {
      errors.push('projectedVolumeTemplate.defaultMode must be a file mode between 0 and 0777');
    }
  }
  return errors;
}
