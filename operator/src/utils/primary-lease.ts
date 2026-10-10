import { V1Container, V1EnvVar, V1Lease, V1PodSpec, V1Role, V1RoleBinding, V1Volume } from '@kubernetes/client-node';
import { API_GROUP, FirebirdCluster, RESOURCE_KIND } from '../types';
import { clusterLabels, FIREBIRD_DATA_DIR } from './resources';
import { OPERATOR_CONFIG_DIR, PRIMARY_KEY, SEGMENT_PORT, replicationDirectories } from './replication';
import { operatorImage } from './operator-image';
import { SEGMENT_SERVER_LOCAL_PORT, segmentTlsEnabled } from './segment-tls-pods';
import { nativeSidecarsSupported } from './segment-tls-default';

/**
 * The primary Lease as a promotion mutex (spec.replication.failover.primaryLease, CloudNativePG
 * 1.30's primary Lease).
 *
 * Without it the cluster Lease (<cluster>-lease) only records which instance is the primary: the
 * operator moves it when it promotes a replica, and nothing renews it. With it the primary holds
 * the Lease itself: a lease-holder sidecar (lease-holder.ts, from the operator image) in every
 * instance pod renews the Lease while its pod is the primary and its database is online, and
 * fences the database (the isolation check's fence: full shutdown) when the Lease names another
 * instance or cannot be renewed for the Lease's duration; it brings a fenced database back
 * (REJOIN) once it re-acquires the Lease. The operator promotes a replica only once the Lease
 * has expired, taking it over with the version it read, so a primary that still renews it
 * (alive, online and reaching the API server) is never replaced, and one the operator replaces
 * has stopped taking writes before: either its database is down (it stopped renewing) or it
 * fenced itself. The operator still lifts the isolation check's fences; the holder's own are
 * lifted by the holder once it holds the Lease again.
 *
 * The sidecar renews the Lease with the pod's ServiceAccount, through a Role on that one Lease
 * and a RoleBinding the operator manages, and a projected token mounted into it alone.
 *
 * The default (v0.87.0): a new cluster gets the primary Lease with automatic failover unless its
 * owner sets failover.primaryLease.enabled, as PRIMARY_LEASE_DEFAULT says (auto, the default: on
 * when the API server runs native sidecars, Kubernetes 1.29 or later; true; false). The decision
 * is recorded in the primary-lease annotation on the cluster's first reconcile ("enabled", or
 * "pinned": off), so it never changes for an existing cluster, and clusters that already have a
 * StatefulSet when the operator first sees them (created by an earlier version) are pinned: the
 * sidecar would restart their instances. Enabling failover later on a cluster follows what was
 * recorded; the owner's spec value always wins.
 */

export const PRIMARY_LEASE_CONTAINER = 'lease-holder';
export const PRIMARY_LEASE_TOKEN_VOLUME = 'primary-lease-token';
export const PRIMARY_LEASE_TOKEN_DIR = '/var/run/primary-lease';
export const DEFAULT_PRIMARY_LEASE_DURATION_SECONDS = 15;
export const MIN_PRIMARY_LEASE_DURATION_SECONDS = 5;
export const MAX_PRIMARY_LEASE_DURATION_SECONDS = 120;

export function leaseName(cluster: FirebirdCluster): string {
  return `${cluster.metadata.name}-lease`;
}

/**
 * The decision recorded on the cluster's first reconcile: "enabled" (the default applied) or
 * "pinned" (off); a pinned cluster moved by PRIMARY_LEASE_MIGRATE (primary-lease-migration.ts)
 * goes through "in-progress" to "done", or "skip" when its owner decided meanwhile
 */
export const PRIMARY_LEASE_ANNOTATION = `${API_GROUP}/primary-lease`;
/** Annotation values under which the primary holds its Lease (the owner's spec value aside) */
export const PRIMARY_LEASE_ON = ['enabled', 'in-progress', 'done'];

/** The default for new clusters: PRIMARY_LEASE_DEFAULT auto (native sidecars) / true / false */
export function primaryLeaseDefault(env = process.env): boolean {
  const setting = (env.PRIMARY_LEASE_DEFAULT ?? 'auto').trim().toLowerCase();
  if (setting === 'true') return true;
  if (setting === 'false') return false;
  return nativeSidecarsSupported() === true;
}

/** Whether the primary holds its Lease: automatic failover, and the owner's choice or the recorded default */
export function primaryLeaseEnabled(cluster: FirebirdCluster): boolean {
  const failover = cluster.spec.replication?.failover;
  if (failover?.enabled !== true) return false;
  return failover.primaryLease?.enabled ?? PRIMARY_LEASE_ON.includes(cluster.metadata.annotations?.[PRIMARY_LEASE_ANNOTATION] ?? '');
}

/** How long the Lease stays valid after a renewal */
export function primaryLeaseDurationSeconds(cluster: FirebirdCluster): number {
  return cluster.spec.replication?.failover?.primaryLease?.durationSeconds ?? DEFAULT_PRIMARY_LEASE_DURATION_SECONDS;
}

/** Seconds since the Lease was last renewed; undefined without a renewal time */
export function leaseAgeSeconds(lease: V1Lease | undefined, now = Date.now()): number | undefined {
  const renewed = lease?.spec?.renewTime;
  if (!renewed) return undefined;
  const at = Date.parse(renewed instanceof Date ? renewed.toISOString() : String(renewed));
  return Number.isFinite(at) ? Math.max(0, (now - at) / 1000) : undefined;
}

/**
 * Whether the Lease no longer protects its holder: no Lease, no holder, no renewal time, or a
 * renewal older than the Lease's duration (what the holder wrote, else the cluster's)
 */
export function leaseExpired(lease: V1Lease | undefined, durationSeconds = DEFAULT_PRIMARY_LEASE_DURATION_SECONDS, now = Date.now()): boolean {
  if (!lease?.spec?.holderIdentity) return true;
  const age = leaseAgeSeconds(lease, now);
  if (age === undefined) return true;
  return age >= (lease.spec.leaseDurationSeconds ?? durationSeconds);
}

export function primaryLeaseRoleName(cluster: FirebirdCluster): string {
  return `${cluster.metadata.name}-primary-lease`;
}

function ownerReference(cluster: FirebirdCluster) {
  return {
    apiVersion: `${API_GROUP}/v1`,
    kind: RESOURCE_KIND,
    name: cluster.metadata.name,
    uid: cluster.metadata.uid ?? '',
    controller: true,
    blockOwnerDeletion: true,
  };
}

/** The Role that lets the instances' ServiceAccount renew this cluster's Lease, and nothing else */
export function buildPrimaryLeaseRole(cluster: FirebirdCluster): V1Role {
  const { name, namespace = 'default' } = cluster.metadata;
  return {
    apiVersion: 'rbac.authorization.k8s.io/v1',
    kind: 'Role',
    metadata: { name: primaryLeaseRoleName(cluster), namespace, labels: clusterLabels(name), ownerReferences: [ownerReference(cluster)] },
    rules: [
      {
        apiGroups: ['coordination.k8s.io'],
        resources: ['leases'],
        resourceNames: [leaseName(cluster)],
        verbs: ['get', 'update', 'patch'],
      },
    ],
  };
}

/** Binds the Role to the ServiceAccount the instance pods run with (spec.serviceAccountName or default) */
export function buildPrimaryLeaseRoleBinding(cluster: FirebirdCluster): V1RoleBinding {
  const { name, namespace = 'default' } = cluster.metadata;
  return {
    apiVersion: 'rbac.authorization.k8s.io/v1',
    kind: 'RoleBinding',
    metadata: { name: primaryLeaseRoleName(cluster), namespace, labels: clusterLabels(name), ownerReferences: [ownerReference(cluster)] },
    roleRef: { apiGroup: 'rbac.authorization.k8s.io', kind: 'Role', name: primaryLeaseRoleName(cluster) },
    subjects: [{ kind: 'ServiceAccount', name: cluster.spec.serviceAccountName ?? 'default', namespace }],
  };
}

/** The lease-holder sidecar (lease-holder.ts), with the SYSDBA credentials it signs FENCE and STATE with */
export function buildLeaseHolderContainer(cluster: FirebirdCluster, credentials: V1EnvVar[], dataDir = FIREBIRD_DATA_DIR): V1Container {
  const dirs = replicationDirectories(cluster, dataDir);
  return {
    name: PRIMARY_LEASE_CONTAINER,
    image: operatorImage(),
    command: ['node', 'dist/lease-holder.js'],
    // native sidecar: keeps running beside the instance, stopped after the main containers
    restartPolicy: 'Always',
    env: [
      ...credentials,
      { name: 'POD_NAME', valueFrom: { fieldRef: { fieldPath: 'metadata.name' } } },
      { name: 'POD_NAMESPACE', valueFrom: { fieldRef: { fieldPath: 'metadata.namespace' } } },
      { name: 'LEASE_NAME', value: leaseName(cluster) },
      { name: 'LEASE_DURATION_SECONDS', value: String(primaryLeaseDurationSeconds(cluster)) },
      { name: 'PRIMARY_FILE', value: `${OPERATOR_CONFIG_DIR}/${PRIMARY_KEY}` },
      { name: 'REPLICATION_DIR', value: dirs.base },
      { name: 'TOKEN_DIR', value: PRIMARY_LEASE_TOKEN_DIR },
      // the segment server of this pod: on localhost behind the TLS proxy, else on the segment port
      { name: 'SEGMENT_SERVER', value: `127.0.0.1:${segmentTlsEnabled(cluster) ? SEGMENT_SERVER_LOCAL_PORT : SEGMENT_PORT}` },
    ],
    resources: { requests: { cpu: '10m', memory: '32Mi' } },
    securityContext: {
      runAsNonRoot: true,
      runAsUser: 65532,
      runAsGroup: 65532,
      allowPrivilegeEscalation: false,
      readOnlyRootFilesystem: true,
      capabilities: { drop: ['ALL'] },
    },
    volumeMounts: [
      { name: PRIMARY_LEASE_TOKEN_VOLUME, mountPath: PRIMARY_LEASE_TOKEN_DIR, readOnly: true },
      { name: 'cluster-config', mountPath: OPERATOR_CONFIG_DIR, readOnly: true },
      // the "promoted" marker of an instance promoted in place, before the ConfigMap names it
      { name: 'firebird-data', mountPath: dataDir, readOnly: true },
    ],
  };
}

/** A ServiceAccount token for the sidecar only (whatever automountServiceAccountToken says), with the API server's CA */
function tokenVolume(): V1Volume {
  return {
    name: PRIMARY_LEASE_TOKEN_VOLUME,
    projected: {
      sources: [
        { serviceAccountToken: { path: 'token', expirationSeconds: 3600 } },
        { configMap: { name: 'kube-root-ca.crt', items: [{ key: 'ca.crt', path: 'ca.crt' }] } },
      ],
    },
  };
}

/** The instance pod spec with the lease-holder sidecar and its token, when the primary holds its Lease */
export function withPrimaryLease<T extends V1PodSpec>(cluster: FirebirdCluster, spec: T, credentials: V1EnvVar[]): T {
  if (!primaryLeaseEnabled(cluster)) return spec;
  return {
    ...spec,
    initContainers: [buildLeaseHolderContainer(cluster, credentials), ...(spec.initContainers ?? [])],
    volumes: [...(spec.volumes ?? []), tokenVolume()],
  };
}
