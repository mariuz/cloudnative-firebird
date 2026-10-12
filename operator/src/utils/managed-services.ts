import { V1Service, V1ServicePort } from '@kubernetes/client-node';
import { API_GROUP, FirebirdCluster, ManagedService, RESOURCE_KIND } from '../types';
import { READ_ROUTABLE_LABEL, ROLE_LABEL } from './routing';

/**
 * Managed services (CloudNativePG's spec.managed.services): Services built from a template in
 * addition to the default ones (<name> read-write, <name>-replica read-only, <name>-headless), e.g.
 * a LoadBalancer for the primary or a Service over every instance.
 *
 * - selectorType "rw" selects the primary, "ro" the replicas ("ro" needs replication; with
 *   replication.readOnlyRouting the read-routable ones, as <name>-replica does), "r" every
 *   instance. With replication the operator keeps the role label on the instance pods for them.
 * - The template's spec is taken as it is, except the selector and the Firebird port, which are
 *   the operator's (a port number and nodePort in ports[0] are kept).
 * - updateStrategy "patch" (default) merges a changed template into the Service; "replace"
 *   deletes and recreates it (a field the API server does not let change, or a clean slate).
 * - A Service removed from the list is deleted (the operator labels the ones it manages);
 *   disabledDefaultServices: [ro] removes <name>-replica.
 * - A Service of that name that the cluster does not own is left alone and reported.
 */

/** Label on the Services built from spec.managed.services: their selectorType */
export const MANAGED_SERVICE_LABEL = `${API_GROUP}/managed-service`;
const CLUSTER_LABEL = `${API_GROUP}/cluster`;

/** The instance pods' labels common to every selector */
function instanceLabels(cluster: FirebirdCluster): Record<string, string> {
  return {
    'app.kubernetes.io/name': 'firebird',
    'app.kubernetes.io/component': 'database',
    'app.kubernetes.io/managed-by': 'cloudnative-firebird-operator',
    [CLUSTER_LABEL]: cluster.metadata.name,
  };
}

/** The pods a managed Service selects */
export function managedServiceSelector(cluster: FirebirdCluster, selectorType: ManagedService['selectorType']): Record<string, string> {
  const labels = instanceLabels(cluster);
  const replication = cluster.spec.replication?.enabled === true;
  switch (selectorType) {
    case 'rw':
      return replication ? { ...labels, [ROLE_LABEL]: 'primary' } : labels;
    case 'ro':
      return cluster.spec.replication?.readOnlyRouting?.enabled ? { ...labels, [READ_ROUTABLE_LABEL]: 'true' } : { ...labels, [ROLE_LABEL]: 'replica' };
    default:
      return labels;
  }
}

/** Names the default Services take (and managed ones may not) */
export function defaultServiceNames(cluster: FirebirdCluster): string[] {
  const name = cluster.metadata.name;
  return [name, `${name}-headless`, `${name}-replica`];
}

/** Whether the default <name>-replica Service is turned off */
export function replicaServiceDisabled(cluster: FirebirdCluster): boolean {
  return cluster.spec.managed?.services?.disabledDefaultServices?.includes('ro') ?? false;
}

const SERVICE_NAME = /^[a-z]([-a-z0-9]*[a-z0-9])?$/;

/** Errors of spec.managed.services */
export function managedServicesErrors(cluster: FirebirdCluster): string[] {
  const services = cluster.spec.managed?.services;
  if (!services) return [];
  const errors: string[] = [];
  const path = 'spec.managed.services';
  for (const d of services.disabledDefaultServices ?? []) {
    if (d !== 'ro') errors.push(`${path}.disabledDefaultServices: only "ro" (the <name>-replica Service) can be turned off, not "${d}"`);
  }
  const names = new Set<string>();
  const reserved = defaultServiceNames(cluster);
  for (const [i, s] of (services.additional ?? []).entries()) {
    const at = `${path}.additional[${i}]`;
    const name = s.serviceTemplate?.metadata?.name;
    if (!['rw', 'ro', 'r'].includes(s.selectorType)) errors.push(`${at}.selectorType must be rw, ro or r`);
    if (s.selectorType === 'ro' && !cluster.spec.replication?.enabled) errors.push(`${at}: selectorType ro needs replication (there are no replicas)`);
    if (s.updateStrategy !== undefined && !['patch', 'replace'].includes(s.updateStrategy)) errors.push(`${at}.updateStrategy must be patch or replace`);
    if (!name || name.length > 63 || !SERVICE_NAME.test(name)) {
      errors.push(`${at}.serviceTemplate.metadata.name must be a Service name (lowercase letters, digits and "-", at most 63)`);
    } else if (reserved.includes(name)) {
      errors.push(`${at}: ${name} is a default Service of the cluster`);
    } else if (names.has(name)) {
      errors.push(`${at}: ${name} is listed twice`);
    }
    if (name) names.add(name);
    const spec = s.serviceTemplate?.spec ?? {};
    if (spec.type !== undefined && !['ClusterIP', 'NodePort', 'LoadBalancer'].includes(spec.type as string)) {
      errors.push(`${at}.serviceTemplate.spec.type must be ClusterIP, NodePort or LoadBalancer`);
    }
    if (spec.selector !== undefined) errors.push(`${at}.serviceTemplate.spec.selector is set by the operator (selectorType)`);
  }
  return errors;
}

/** A managed Service as desired */
export function buildManagedService(cluster: FirebirdCluster, managed: ManagedService): V1Service {
  const { name, namespace = 'default', uid } = cluster.metadata;
  const template = managed.serviceTemplate;
  const spec = { ...(template.spec ?? {}) } as Record<string, unknown>;
  const templatePort = (Array.isArray(spec.ports) ? (spec.ports[0] as V1ServicePort | undefined) : undefined) ?? undefined;
  delete spec.ports;
  delete spec.selector;
  const port: V1ServicePort = {
    name: 'firebird',
    port: templatePort?.port ?? 3050,
    targetPort: 3050 as unknown as V1ServicePort['targetPort'],
    protocol: 'TCP',
    ...(templatePort?.nodePort ? { nodePort: templatePort.nodePort } : {}),
  };
  return {
    apiVersion: 'v1',
    kind: 'Service',
    metadata: {
      name: template.metadata.name,
      namespace,
      labels: {
        ...(template.metadata.labels ?? {}),
        'app.kubernetes.io/name': 'firebird',
        'app.kubernetes.io/managed-by': 'cloudnative-firebird-operator',
        [CLUSTER_LABEL]: name,
        [MANAGED_SERVICE_LABEL]: managed.selectorType,
      },
      ...(template.metadata.annotations ? { annotations: template.metadata.annotations } : {}),
      ownerReferences: [
        { apiVersion: `${API_GROUP}/v1`, kind: RESOURCE_KIND, name, uid: uid ?? '', controller: true, blockOwnerDeletion: true },
      ],
    },
    spec: { type: 'ClusterIP', ...spec, selector: managedServiceSelector(cluster, managed.selectorType), ports: [port] },
  };
}

/** Whether every field the operator sets on a Service (labels, annotations, spec) has the desired value */
function covers(existing: unknown, desired: unknown): boolean {
  if (desired === null || desired === undefined) return true;
  if (Array.isArray(desired)) {
    return Array.isArray(existing) && existing.length === desired.length && desired.every((d, i) => covers(existing[i], d));
  }
  if (typeof desired === 'object') {
    if (!existing || typeof existing !== 'object') return false;
    return Object.entries(desired as Record<string, unknown>).every(([k, v]) => covers((existing as Record<string, unknown>)[k], v));
  }
  return String(existing) === String(desired);
}

/**
 * Whether an existing Service differs from the desired one in what the operator sets (labels,
 * annotations and spec fields of the template; fields the API server fills in, such as
 * clusterIP or a nodePort it allocated, are not compared)
 */
export function managedServiceDiffers(existing: V1Service, desired: V1Service): boolean {
  if (!covers(existing.metadata?.labels, desired.metadata?.labels)) return true;
  if (!covers(existing.metadata?.annotations, desired.metadata?.annotations)) return true;
  const { ports, selector, ...spec } = desired.spec ?? {};
  if (JSON.stringify(sorted(existing.spec?.selector ?? {})) !== JSON.stringify(sorted(selector ?? {}))) return true;
  if (!covers(existing.spec, spec)) return true;
  const port = ports?.[0];
  const current = existing.spec?.ports?.[0];
  if (!port || !current || current.port !== port.port || String(current.targetPort) !== String(port.targetPort)) return true;
  return port.nodePort !== undefined && current.nodePort !== port.nodePort;
}

function sorted(o: Record<string, string>): Record<string, string> {
  return Object.fromEntries(Object.entries(o).sort(([a], [b]) => a.localeCompare(b)));
}

/** Whether a Service is owned by this cluster */
export function ownedByCluster(service: V1Service, cluster: FirebirdCluster): boolean {
  return (service.metadata?.ownerReferences ?? []).some((o) => o.kind === RESOURCE_KIND && (o.uid === cluster.metadata.uid || (!cluster.metadata.uid && o.name === cluster.metadata.name)));
}
