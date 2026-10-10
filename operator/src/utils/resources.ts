import { withSegmentTls } from './segment-tls-pods';
import { withPrimaryLease } from './primary-lease';
import { createHash } from 'crypto';
import {
  V1StatefulSet,
  V1Service,
  V1CronJob,
  V1Lease,
  V1PodDisruptionBudget,
  V1ConfigMap,
  V1NetworkPolicy,
  V1NetworkPolicyIngressRule,
  V1MicroTime,
  V1Container,
  V1PodSecurityContext,
  V1PodSpec,
  V1SecurityContext,
} from '@kubernetes/client-node';
import {
  FirebirdCluster,
  DEFAULT_FIREBIRD_IMAGE,
  API_GROUP,
  RESOURCE_KIND,
} from '../types';
import { READ_ROUTABLE_LABEL, ROLE_LABEL } from './routing';
import { bootstrapVolumes, buildBootstrapInitContainers } from './backup';
import { databaseOnlineCheck } from './fencing';
import { PENDING_DROPS_DIR, pendingDropsConfigMapName, pendingDropsInitScript } from './pending-drops';
import {
  PRIMARY_KEY,
  REPLICATION_SCRIPTS,
  JOB_SCRIPTS,
  RESEED_KEY,
  PROMOTE_KEY,
  DEMOTE_KEY,
  SEED_SOURCES_KEY,
  PEER_ADDRESSES_KEY,
  SEGMENT_PORT,
  OPERATOR_CONFIG_DIR,
  buildReplicationConf,
  buildReplicationContainers,
  instanceHost,
  replicationEnabled,
} from './replication';

/** The label key used to identify cluster resources */
export const CLUSTER_LABEL = `${API_GROUP}/cluster`;

/** Returns the set of labels to apply to all cluster resources */
export function clusterLabels(name: string): Record<string, string> {
  return {
    'app.kubernetes.io/name': 'firebird',
    'app.kubernetes.io/component': 'database',
    'app.kubernetes.io/managed-by': 'cloudnative-firebird-operator',
    [CLUSTER_LABEL]: name,
  };
}

/**
 * Label selector for the cluster's instance pods. Job pods (backups, fencing, switchover, users)
 * carry the cluster label too, with another component.
 */
export function instancePodSelector(name: string): string {
  return `${CLUSTER_LABEL}=${name},app.kubernetes.io/component=database`;
}

/** Keeps the StatefulSet instance pods (`<cluster>-<ordinal>`) of a pod list */
export function instancePods<T extends { metadata?: { name?: string } }>(items: T[], name: string): T[] {
  const pattern = new RegExp(`^${name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}-\\d+$`);
  return items.filter((p) => pattern.test(p.metadata?.name ?? ''));
}

/**
 * Applies the cluster hibernation state to an operator-managed CronJob:
 * scheduled work is suspended while the cluster is hibernated.
 */
export function withHibernation<T extends V1CronJob>(cronJob: T, cluster: FirebirdCluster): T {
  if (cronJob.spec) cronJob.spec.suspend = Boolean(cluster.spec.hibernated);
  return cronJob;
}

/** Data directory of the official firebirdsql/firebird image (FIREBIRD_DATA); the PVC is mounted here */
export const FIREBIRD_DATA_DIR = '/var/lib/firebird/data';

/**
 * The security database (users and their passwords) lives on the instance volume. The official
 * image keeps it in /opt/firebird on the container filesystem, so every container restart would
 * drop all users but SYSDBA (which the entrypoint recreates).
 */
export const SECURITY_DIR = `${FIREBIRD_DATA_DIR}/system`;
export const SECURITY_DB_PATH = `${SECURITY_DIR}/security.fdb`;

/**
 * Init container that seeds the persistent security database from the image on first start and
 * writes a databases.conf whose `security.db` alias (used by the image entrypoint to set the SYSDBA
 * password) points at it. The server itself is pointed there with SecurityDatabase.
 */
export function buildSecurityDbInitContainer(image: string): V1Container {
  return {
    name: 'security-db-init',
    image,
    command: ['/bin/sh', '-c'],
    args: [
      [
        'set -eu',
        `d=${SECURITY_DIR}`,
        'mkdir -p "$d"',
        'if [ ! -f "$d/security.fdb" ]; then',
        '  cp /opt/firebird/security[0-9]*.fdb "$d/security.fdb.tmp"',
        '  mv "$d/security.fdb.tmp" "$d/security.fdb"',
        '  echo "security database created from the image"',
        'fi',
        // users whose FirebirdUser was deleted while this instance was down (utils/pending-drops.ts)
        pendingDropsInitScript(SECURITY_DB_PATH),
        `printf '%s\n' 'security.db = ${SECURITY_DB_PATH}' '{' '    RemoteAccess = false' '    DefaultDbCachePages = 256' '}' > "$d/databases.conf"`,
        // as root; running as the firebird user (runAsFirebirdUser) it owns what it writes
        '[ "$(id -u)" -ne 0 ] || chown -R firebird:firebird "$d"',
      ].join('\n'),
    ],
    env: [{ name: 'POD_NAME', valueFrom: { fieldRef: { fieldPath: 'metadata.name' } } }],
    volumeMounts: [
      { name: 'firebird-data', mountPath: FIREBIRD_DATA_DIR },
      { name: PENDING_DROPS_VOLUME, mountPath: PENDING_DROPS_DIR, readOnly: true },
    ],
  };
}

/** Volume of the pending user drops ConfigMap (optional: it only exists once a drop is pending) */
const PENDING_DROPS_VOLUME = 'pending-user-drops';

/** Default database file created in each instance */
export const DEFAULT_DATABASE_NAME = 'mydb.fdb';

/** Returns the database file name managed by the cluster */
export function databaseName(cluster: FirebirdCluster): string {
  return cluster.spec.databaseName ?? DEFAULT_DATABASE_NAME;
}

/** Wire encryption plugins allowed with tls.enabled: ChaCha only, not the RC4-based Arc4 */
export const STRICT_WIRE_CRYPT_PLUGINS = 'ChaCha64, ChaCha';

/**
 * Returns the effective firebird.conf settings. Firebird 4 and later already require wire
 * encryption on the server (WireCrypt = Required, with Srp256 authentication, whose session key
 * the encryption uses); tls.enabled makes it explicit and leaves out the Arc4 plugin, so a client
 * that offers only Arc4 (e.g. Firebird 3) is refused.
 */
export function firebirdConfSettings(cluster: FirebirdCluster): Record<string, string> {
  const settings = { ...(cluster.spec.config?.settings ?? {}) };
  if (cluster.spec.tls?.enabled) {
    settings['WireCrypt'] ??= 'Required';
    settings['WireCryptPlugin'] ??= STRICT_WIRE_CRYPT_PLUGINS;
  }
  return settings;
}

/**
 * SYSDBA credentials for Firebird client tools (gfix, gbak, fbsvcmgr, isql), which read
 * ISC_USER / ISC_PASSWORD from the environment so the password never appears in process args.
 */
export function superuserClientEnv(cluster: FirebirdCluster): Array<{ name: string; value?: string; valueFrom?: object }> {
  const secret = cluster.spec.superuserSecret;
  return [
    { name: 'ISC_USER', value: 'SYSDBA' },
    secret
      ? { name: 'ISC_PASSWORD', valueFrom: { secretKeyRef: { name: secret.name, key: 'password' } } }
      : { name: 'ISC_PASSWORD', value: 'masterkey' },
  ];
}

/**
 * Sidecar of instances without replication: the segment server in its files-only mode, through
 * which backup and restore Jobs copy, list and delete backup files in the data directory (with
 * replication the segment server sidecar does this)
 */
export function buildBackupFilesContainer(cluster: FirebirdCluster, image: string): V1Container {
  return {
    name: 'backup-files',
    image,
    command: ['perl', `${OPERATOR_CONFIG_DIR}/segment-server.pl`],
    ports: [{ name: 'segments', containerPort: SEGMENT_PORT, protocol: 'TCP' }],
    env: [
      ...superuserClientEnv(cluster),
      { name: 'FILES_ONLY', value: 'true' },
      { name: 'DATABASE_PATH', value: `${FIREBIRD_DATA_DIR}/${databaseName(cluster)}` },
      { name: 'SEGMENT_PORT', value: String(SEGMENT_PORT) },
      { name: 'POD_NAME', valueFrom: { fieldRef: { fieldPath: 'metadata.name' } } },
    ],
    volumeMounts: [
      { name: 'firebird-data', mountPath: FIREBIRD_DATA_DIR },
      { name: 'cluster-config', mountPath: OPERATOR_CONFIG_DIR, readOnly: true },
    ],
  };
}

/** Scripts the backup file server of instances without replication runs */
const BACKUP_FILE_SCRIPTS = ['segment-server.pl', 'backup-file.pl'];

/** The cluster ConfigMap entries of the backup file server (instances without replication) */
function backupFilesConfigData(): Record<string, string> {
  return Object.fromEntries(BACKUP_FILE_SCRIPTS.map((name) => [name, REPLICATION_SCRIPTS[name]]));
}

/** Pod template annotation carrying the hash of the backup file server's scripts (no replication) */
export const BACKUP_FILES_HASH_ANNOTATION = `${API_GROUP}/backup-files-hash`;

function backupFilesHash(): string {
  const data = backupFilesConfigData();
  const canonical = Object.keys(data).sort().map((key) => `${key}\0${data[key]}`).join('\0');
  return createHash('sha256').update(canonical).digest('hex').slice(0, 16);
}

/** Pod template annotation carrying the hash of the replication configuration */
export const REPLICATION_CONFIG_HASH_ANNOTATION = `${API_GROUP}/replication-config-hash`;

/** Replication entries of the cluster ConfigMap that are fixed for the pod's lifetime */
function replicationConfigData(cluster: FirebirdCluster): Record<string, string> {
  return {
    'replication.conf': buildReplicationConf(
      cluster,
      `${FIREBIRD_DATA_DIR}/${databaseName(cluster)}`,
      FIREBIRD_DATA_DIR,
    ),
    ...REPLICATION_SCRIPTS,
  };
}

/** Hash of replication.conf and the replication scripts (not the current primary) */
export function replicationConfigHash(cluster: FirebirdCluster): string {
  const data = replicationConfigData(cluster);
  const canonical = Object.keys(data).sort().map((key) => `${key}\0${data[key]}`).join('\0');
  return createHash('sha256').update(canonical).digest('hex').slice(0, 16);
}

/** Returns true when lag-aware read-only routing is active for the cluster */
export function readOnlyRoutingEnabled(cluster: FirebirdCluster): boolean {
  return Boolean(cluster.spec.replication?.enabled && cluster.spec.replication.readOnlyRouting?.enabled);
}

/**
 * Returns the pod selector for the primary (read-write) Service.
 * With read-only routing enabled, only the pod labelled as primary receives write traffic.
 */
export function primaryServiceSelector(cluster: FirebirdCluster): Record<string, string> {
  const labels = clusterLabels(cluster.metadata.name);
  return readOnlyRoutingEnabled(cluster) ? { ...labels, [ROLE_LABEL]: 'primary' } : labels;
}

/**
 * Returns the pod selector for the read-only `-replica` Service.
 * With read-only routing enabled, only pods marked read-routable receive read traffic.
 */
export function replicaServiceSelector(cluster: FirebirdCluster): Record<string, string> {
  const labels = clusterLabels(cluster.metadata.name);
  return readOnlyRoutingEnabled(cluster) ? { ...labels, [READ_ROUTABLE_LABEL]: 'true' } : labels;
}

/**
 * Builds the StatefulSet for a FirebirdCluster.
 */
export function buildStatefulSet(
  cluster: FirebirdCluster,
  options?: { superuserSecretHash?: string }
): V1StatefulSet {
  const { name, namespace = 'default' } = cluster.metadata;
  const spec = cluster.spec;
  const image = spec.imageName ?? DEFAULT_FIREBIRD_IMAGE;
  const labels = clusterLabels(name);
  const storageClassName = spec.storage.storageClass;
  const secretHash = options?.superuserSecretHash ?? cluster.status?.superuserSecretHash;

  const env = [
    // SYSDBA password consumed by the official image entrypoint
    spec.superuserSecret
      ? {
          name: 'FIREBIRD_ROOT_PASSWORD',
          valueFrom: { secretKeyRef: { name: spec.superuserSecret.name, key: 'password' } },
        }
      : { name: 'FIREBIRD_ROOT_PASSWORD', value: 'masterkey' },
    // Credentials for client tools run inside the container (probes, kubectl exec)
    ...superuserClientEnv(cluster),
    // The entrypoint creates this database in FIREBIRD_DATA on first start
    { name: 'FIREBIRD_DATABASE', value: databaseName(cluster) },
    // firebird.conf settings are applied by the entrypoint from FIREBIRD_CONF_<key>;
    // changing them updates the pod template and rolls the pods
    ...Object.entries(firebirdConfSettings(cluster)).map(([key, value]) => ({
      name: `FIREBIRD_CONF_${key}`,
      value,
    })),
    // users survive restarts: the security database is on the instance volume
    { name: 'FIREBIRD_CONF_SecurityDatabase', value: SECURITY_DB_PATH },
    // Additional env vars from spec
    ...(spec.env ?? []),
  ];

  // Persistent security database, then bootstrap from a backup or another cluster, before the
  // replication init
  const initContainers: V1Container[] = [
    ...(spec.runAsFirebirdUser ? [buildFirebirdHomeInitContainer(image)] : []),
    buildSecurityDbInitContainer(image),
    ...buildBootstrapInitContainers(cluster),
  ];

  const replication = replicationEnabled(cluster)
    ? buildReplicationContainers(cluster, {
        image,
        databasePath: `${FIREBIRD_DATA_DIR}/${databaseName(cluster)}`,
        dataDir: FIREBIRD_DATA_DIR,
        credentials: superuserClientEnv(cluster),
      })
    : undefined;
  if (replication) initContainers.push(replication.initContainer);
  // the backup file server (or the replication sidecars) run the scripts in the cluster ConfigMap
  const usesConfigVolume = true;

  const containers = [
    {
      name: 'firebird',
      image,
      ports: [
        {
          name: 'firebird',
          containerPort: 3050,
          protocol: 'TCP',
        },
      ],
      env,
      resources: spec.resources,
      volumeMounts: [
        {
          name: 'firebird-data',
          mountPath: '/var/lib/firebird/data',
        },
        ...(spec.runAsFirebirdUser ? [{ name: FIREBIRD_HOME_VOLUME, mountPath: '/opt/firebird' }] : []),
        // written by the security-db-init container
        {
          name: 'firebird-data',
          mountPath: '/opt/firebird/databases.conf',
          subPath: 'system/databases.conf',
        },
        ...(spec.bootstrap?.initSql
          ? [
              {
                name: 'cluster-config',
                mountPath: '/docker-entrypoint-initdb.d/init.sql',
                subPath: 'init.sql',
              },
            ]
          : []),
        ...(replication?.mainMounts ?? []),
      ],
      livenessProbe: {
        tcpSocket: { port: 3050 },
        initialDelaySeconds: 30,
        periodSeconds: 10,
        failureThreshold: 5,
      },
      // ready only while the local server answers the service manager and the database is not
      // shut down, so a fenced instance leaves the Services (see utils/fencing.ts)
      readinessProbe: {
        exec: {
          command: ['/bin/sh', '-c', databaseOnlineCheck(`${FIREBIRD_DATA_DIR}/${databaseName(cluster)}`)],
        },
        // a short delay: the first checks may fail while the server starts, which only delays
        // readiness, never a restart (the liveness probe starts later)
        initialDelaySeconds: 5,
        periodSeconds: 5,
        timeoutSeconds: 5,
        failureThreshold: 3,
      },
    },
    ...(spec.monitoring?.exporter?.enabled
      ? [
          {
            name: 'firebird-exporter',
            image: spec.monitoring.exporter.image ?? 'prom/firebird-exporter:latest',
            ports: [
              {
                name: 'metrics',
                containerPort: spec.monitoring.exporter.port ?? 9108,
                protocol: 'TCP',
              },
            ],
            ...(spec.monitoring.exporter.resources ? { resources: spec.monitoring.exporter.resources } : {}),
            ...(spec.monitoring.exporter.env ? { env: spec.monitoring.exporter.env } : {}),
          },
        ]
      : []),
    ...(replication?.sidecars ?? [buildBackupFilesContainer(cluster, image)]),
  ];

  const volumes = [
    ...(spec.runAsFirebirdUser ? [{ name: FIREBIRD_HOME_VOLUME, emptyDir: {} }] : []),
    { name: PENDING_DROPS_VOLUME, configMap: { name: pendingDropsConfigMapName(name), optional: true } },
    ...bootstrapVolumes(cluster),
    ...(usesConfigVolume
      ? [
          {
            name: 'cluster-config',
            configMap: {
              name: `${name}-config`,
            },
          },
        ]
      : []),
  ];

  const secured = (c: V1Container): V1Container => ({ ...c, securityContext: instanceSecurityContext(cluster) });
  const statefulSet: V1StatefulSet = {
    apiVersion: 'apps/v1',
    kind: 'StatefulSet',
    metadata: {
      name,
      namespace,
      labels,
      ownerReferences: [
        {
          apiVersion: `${API_GROUP}/v1`,
          kind: RESOURCE_KIND,
          name: cluster.metadata.name,
          uid: cluster.metadata.uid ?? '',
          controller: true,
          blockOwnerDeletion: true,
        },
      ],
    },
    spec: {
      // Must reference the headless service for stable pod DNS
      serviceName: `${name}-headless`,
      // Hibernation scales to zero pods while keeping the PVCs
      replicas: spec.hibernated ? 0 : spec.instances,
      // with replication the operator restarts outdated pods itself, replicas first and the
      // primary last (utils/rolling-update.ts)
      // the operator rolls the pods (utils/rolling-update.ts)
      updateStrategy: { type: 'OnDelete' },
      // Pods are (re)created independently: with OrderedReady, a deleted pod is not recreated
      // while a lower ordinal is not ready, e.g. a promoted replica behind the failed primary it
      // replaces, which waits for a seed from it
      podManagementPolicy: 'Parallel',
      selector: {
        matchLabels: labels,
      },
      template: {
        metadata: {
          labels,
          annotations: {
            ...(secretHash ? { 'firebird.cloudnative-firebird.io/superuser-secret-hash': secretHash } : {}),
            // replication.conf is mounted via subPath, which does not follow ConfigMap
            // updates; hashing it into the template rolls the pods when it changes (the scripts
            // are read once at start, too)
            ...(replication
              ? { [REPLICATION_CONFIG_HASH_ANNOTATION]: replicationConfigHash(cluster) }
              : { [BACKUP_FILES_HASH_ANNOTATION]: backupFilesHash() }),
          },
        },
        // segment TLS: the proxy sidecar, the certificates, the segment server on localhost;
        // the primary Lease: its holder sidecar and token
        spec: withSegmentTls(cluster, withPrimaryLease(cluster, {
          ...serviceAccount(cluster),
          securityContext: instancePodSecurityContext(cluster),
          ...(initContainers.length > 0 ? { initContainers: initContainers.map(secured) } : {}),
          ...(spec.nodeSelector ? { nodeSelector: spec.nodeSelector } : {}),
          ...(spec.affinity ? { affinity: spec.affinity } : {}),
          ...(spec.tolerations ? { tolerations: spec.tolerations } : {}),
          containers: containers.map(secured),
          ...(volumes.length > 0 ? { volumes } : {}),
        }, superuserClientEnv(cluster)), true),
      },
      volumeClaimTemplates: [
        {
          metadata: {
            name: 'firebird-data',
            labels,
          },
          spec: {
            accessModes: ['ReadWriteOnce'],
            ...(storageClassName ? { storageClassName } : {}),
            resources: {
              requests: {
                storage: spec.storage.size,
              },
            },
          },
        },
      ],
    },
  };

  return statefulSet;
}

/**
 * Builds the primary Service for a FirebirdCluster.
 * This is the read-write service that clients connect to.
 */
export function buildService(cluster: FirebirdCluster): V1Service {
  const { name, namespace = 'default' } = cluster.metadata;
  const labels = clusterLabels(name);

  const service: V1Service = {
    apiVersion: 'v1',
    kind: 'Service',
    metadata: {
      name,
      namespace,
      labels,
      ...(cluster.spec.serviceAnnotations ? { annotations: cluster.spec.serviceAnnotations } : {}),
      ownerReferences: [
        {
          apiVersion: `${API_GROUP}/v1`,
          kind: RESOURCE_KIND,
          name: cluster.metadata.name,
          uid: cluster.metadata.uid ?? '',
          controller: true,
          blockOwnerDeletion: true,
        },
      ],
    },
    spec: {
      type: cluster.spec.serviceType ?? 'ClusterIP',
      selector: primaryServiceSelector(cluster),
      ports: [
        {
          name: 'firebird',
          port: 3050,
          targetPort: 3050,
          protocol: 'TCP',
        },
      ],
    },
  };

  return service;
}

/**
 * Builds the headless Service used by the StatefulSet for pod DNS discovery.
 */
export function buildHeadlessService(cluster: FirebirdCluster): V1Service {
  const { name, namespace = 'default' } = cluster.metadata;
  const labels = clusterLabels(name);

  const service: V1Service = {
    apiVersion: 'v1',
    kind: 'Service',
    metadata: {
      name: `${name}-headless`,
      namespace,
      labels,
      annotations: {
        'service.alpha.kubernetes.io/tolerate-unready-endpoints': 'true',
      },
      ownerReferences: [
        {
          apiVersion: `${API_GROUP}/v1`,
          kind: RESOURCE_KIND,
          name: cluster.metadata.name,
          uid: cluster.metadata.uid ?? '',
          controller: true,
          blockOwnerDeletion: true,
        },
      ],
    },
    spec: {
      clusterIP: 'None',
      publishNotReadyAddresses: true,
      selector: labels,
      ports: [
        {
          name: 'firebird',
          port: 3050,
          targetPort: 3050,
          protocol: 'TCP',
        },
      ],
    },
  };

  return service;
}

/**
 * Builds the read-replica Service for a FirebirdCluster with replication enabled.
 * This service provides a dedicated endpoint for read-replica connections,
 * allowing clients to route read-only traffic separately from write traffic.
 * Without `replication.readOnlyRouting` the service selects every cluster pod.
 * With it enabled, only ready replicas within the replication lag threshold
 * (labelled read-routable by the operator) are selected.
 */
export function buildReplicaService(cluster: FirebirdCluster): V1Service {
  const { name, namespace = 'default' } = cluster.metadata;
  const labels = clusterLabels(name);

  const service: V1Service = {
    apiVersion: 'v1',
    kind: 'Service',
    metadata: {
      name: `${name}-replica`,
      namespace,
      labels: {
        ...labels,
        'app.kubernetes.io/component': 'database-replica',
      },
      ...(cluster.spec.serviceAnnotations ? { annotations: cluster.spec.serviceAnnotations } : {}),
      ownerReferences: [
        {
          apiVersion: `${API_GROUP}/v1`,
          kind: RESOURCE_KIND,
          name: cluster.metadata.name,
          uid: cluster.metadata.uid ?? '',
          controller: true,
          blockOwnerDeletion: true,
        },
      ],
    },
    spec: {
      type: cluster.spec.serviceType ?? 'ClusterIP',
      selector: replicaServiceSelector(cluster),
      ports: [
        {
          name: 'firebird',
          port: 3050,
          targetPort: 3050,
          protocol: 'TCP',
        },
      ],
    },
  };

  return service;
}

/**
 * Checks if two StatefulSet specs are semantically equal
 * (ignoring server-set fields like resourceVersion).
 */
export function statefulSetNeedsUpdate(
  existing: V1StatefulSet,
  desired: V1StatefulSet,
): boolean {
  const existingSpec = existing.spec;
  const desiredSpec = desired.spec;

  if (!existingSpec || !desiredSpec) return true;

  if (existingSpec.replicas !== desiredSpec.replicas) return true;
  if ((existingSpec.updateStrategy?.type ?? 'RollingUpdate') !== (desiredSpec.updateStrategy?.type ?? 'RollingUpdate')) {
    return true;
  }
  // Only operator-set annotations are compared; others (e.g. kubectl restartedAt) are kept
  const existingAnnotations = existingSpec.template?.metadata?.annotations ?? {};
  const desiredAnnotations = desiredSpec.template?.metadata?.annotations ?? {};
  if (Object.entries(desiredAnnotations).some(([key, value]) => existingAnnotations[key] !== value)) {
    return true;
  }

  const existingPodSpec = existingSpec.template?.spec;
  const desiredPodSpec = desiredSpec.template?.spec;
  if (!existingPodSpec || !desiredPodSpec) return true;

  if ((existingPodSpec.serviceAccountName ?? 'default') !== (desiredPodSpec.serviceAccountName ?? 'default')) return true;
  if (JSON.stringify(existingPodSpec.securityContext ?? {}) !== JSON.stringify(desiredPodSpec.securityContext ?? {})) return true;
  const securityContexts = (spec: V1PodSpec) =>
    JSON.stringify([...(spec.initContainers ?? []), ...(spec.containers ?? [])].map((c) => c.securityContext ?? {}));
  if (securityContexts(existingPodSpec) !== securityContexts(desiredPodSpec)) return true;
  if (JSON.stringify(existingPodSpec.nodeSelector) !== JSON.stringify(desiredPodSpec.nodeSelector)) return true;
  if (JSON.stringify(existingPodSpec.affinity) !== JSON.stringify(desiredPodSpec.affinity)) return true;
  if (JSON.stringify(existingPodSpec.tolerations) !== JSON.stringify(desiredPodSpec.tolerations)) return true;

  const names = (list?: Array<{ name: string }>) => (list ?? []).map((c) => c.name).join(',');
  if (names(existingPodSpec.containers) !== names(desiredPodSpec.containers)) return true;
  if (names(existingPodSpec.initContainers) !== names(desiredPodSpec.initContainers)) return true;

  const existingContainer = existingPodSpec.containers?.[0];
  const desiredContainer = desiredPodSpec.containers?.[0];

  if (!existingContainer || !desiredContainer) return true;
  if (existingContainer.image !== desiredContainer.image) return true;
  if (JSON.stringify(existingContainer.resources) !== JSON.stringify(desiredContainer.resources)) return true;
  if (JSON.stringify(existingContainer.env) !== JSON.stringify(desiredContainer.env)) return true;
  // the API server defaults other probe fields, so only the probe command is compared
  if (
    JSON.stringify(existingContainer.readinessProbe?.exec?.command) !==
    JSON.stringify(desiredContainer.readinessProbe?.exec?.command)
  ) {
    return true;
  }

  return false;
}

/** Namespace the operator runs in (OPERATOR_NAMESPACE, from the downward API) */
export function operatorNamespace(): string {
  return process.env.OPERATOR_NAMESPACE || 'cloudnative-firebird-system';
}

/** User and group of the firebird account in the official image */
export const FIREBIRD_UID = 84;

/**
 * Capabilities the instance containers keep: the official image runs the server as root, which
 * needs DAC_OVERRIDE for its firebird-owned lock directory; the init scripts chown files (CHOWN,
 * FOWNER). Everything else is dropped.
 */
export const INSTANCE_CAPABILITIES = ['CHOWN', 'DAC_OVERRIDE', 'FOWNER'];

/** Pod security context of the instance pods: defaults, overridden by spec.podSecurityContext */
export function instancePodSecurityContext(cluster: FirebirdCluster): V1PodSecurityContext {
  const defaults: V1PodSecurityContext = cluster.spec.runAsFirebirdUser
    ? { runAsNonRoot: true, runAsUser: FIREBIRD_UID, runAsGroup: FIREBIRD_UID, fsGroup: FIREBIRD_UID }
    : { fsGroup: 999 };
  return { ...defaults, seccompProfile: { type: 'RuntimeDefault' }, ...(cluster.spec.podSecurityContext ?? {}) };
}

/** Security context of every instance container: defaults, overridden by spec.securityContext */
export function instanceSecurityContext(cluster: FirebirdCluster): V1SecurityContext {
  return {
    allowPrivilegeEscalation: false,
    // as the firebird user nothing needs a capability (runAsFirebirdUser)
    capabilities: cluster.spec.runAsFirebirdUser ? { drop: ['ALL'] } : { drop: ['ALL'], add: [...INSTANCE_CAPABILITIES] },
    ...(cluster.spec.securityContext ?? {}),
  };
}

/** Writable copy of the image's /opt/firebird (runAsFirebirdUser): see buildFirebirdHomeInitContainer */
export const FIREBIRD_HOME_VOLUME = 'firebird-home';

/**
 * runAsFirebirdUser: the image entrypoint applies FIREBIRD_CONF_* settings by editing
 * /opt/firebird/firebird.conf, which only root may write. The first init container copies
 * /opt/firebird (about 45 MB; the few root-only files are not needed) into an emptyDir that the
 * Firebird container mounts at /opt/firebird.
 */
export function buildFirebirdHomeInitContainer(image: string): V1Container {
  return {
    name: 'firebird-home',
    image,
    command: ['/bin/sh', '-c'],
    args: [
      'cp -a /opt/firebird/. /firebird-home/ 2>/dev/null; ' +
        '[ -x /firebird-home/bin/firebird ] && [ -f /firebird-home/firebird.conf ] || ' +
        '{ echo "could not copy /opt/firebird" >&2; exit 1; }; echo "writable copy of /opt/firebird ready"',
    ],
    volumeMounts: [{ name: FIREBIRD_HOME_VOLUME, mountPath: '/firebird-home' }],
  };
}

/**
 * Pod spec of an operator Job (backup, restore, archive, maintenance, fencing, switchover, users).
 * The Jobs only use client tools over the network, so they meet the "restricted" Pod Security
 * Standard: non-root (the image's firebird user), no privilege escalation, no capabilities,
 * RuntimeDefault seccomp. They run with the cluster's service account.
 */
export function jobPodSpec<T extends V1PodSpec>(cluster: FirebirdCluster, podSpec: T): T {
  const container = (c: V1Container): V1Container => ({
    ...c,
    securityContext: { allowPrivilegeEscalation: false, capabilities: { drop: ['ALL'] }, ...(c.securityContext ?? {}) },
  });
  // segment TLS: Jobs reach the segment servers through the proxy's client side
  return withSegmentTls(cluster, {
    ...podSpec,
    ...serviceAccount(cluster),
    securityContext: {
      runAsNonRoot: true,
      runAsUser: FIREBIRD_UID,
      runAsGroup: FIREBIRD_UID,
      seccompProfile: { type: 'RuntimeDefault' },
      ...(podSpec.securityContext ?? {}),
    },
    containers: podSpec.containers.map(container),
    ...(podSpec.initContainers ? { initContainers: podSpec.initContainers.map(container) } : {}),
  }, false);
}

/**
 * Service account of the instance pods and of every Job the operator runs for the cluster
 * (spec.serviceAccountName), e.g. for S3 access through workload identity instead of static keys.
 */
export function serviceAccount(cluster: FirebirdCluster): { serviceAccountName?: string } {
  return cluster.spec.serviceAccountName ? { serviceAccountName: cluster.spec.serviceAccountName } : {};
}

/** Annotation carrying the hash of an operator-managed CronJob's job template */
export const JOB_TEMPLATE_HASH_ANNOTATION = `${API_GROUP}/job-template-hash`;

/**
 * Stamps a CronJob with the hash of its job template, so that any template change (init
 * containers, env) is detected without comparing against server-side defaults.
 */
export function withTemplateHash(cronJob: V1CronJob): V1CronJob {
  const hash = createHash('sha256')
    .update(JSON.stringify(cronJob.spec?.jobTemplate ?? {}))
    .digest('hex')
    .slice(0, 16);
  cronJob.metadata = {
    ...cronJob.metadata,
    annotations: { ...(cronJob.metadata?.annotations ?? {}), [JOB_TEMPLATE_HASH_ANNOTATION]: hash },
  };
  return cronJob;
}

/**
 * Checks if a CronJob needs updating (schedule, suspension, job template or image change).
 */
export function cronJobNeedsUpdate(existing: V1CronJob, desired: V1CronJob): boolean {
  const existingSpec = existing.spec;
  const desiredSpec = desired.spec;

  if (!existingSpec || !desiredSpec) return true;
  if (existingSpec.schedule !== desiredSpec.schedule) return true;
  if (Boolean(existingSpec.suspend) !== Boolean(desiredSpec.suspend)) return true;
  const desiredHash = desired.metadata?.annotations?.[JOB_TEMPLATE_HASH_ANNOTATION];
  if (desiredHash && existing.metadata?.annotations?.[JOB_TEMPLATE_HASH_ANNOTATION] !== desiredHash) return true;

  const existingContainer = existingSpec.jobTemplate?.spec?.template?.spec?.containers?.[0];
  const desiredContainer = desiredSpec.jobTemplate?.spec?.template?.spec?.containers?.[0];

  if (!existingContainer || !desiredContainer) return true;
  if (existingContainer.image !== desiredContainer.image) return true;
  if (JSON.stringify(existingContainer.args) !== JSON.stringify(desiredContainer.args)) return true;

  return false;
}

/**
 * Builds the Prometheus PodMonitor custom object for a FirebirdCluster.
 */
export function buildPodMonitor(cluster: FirebirdCluster): Record<string, unknown> {
  const { name, namespace = 'default' } = cluster.metadata;
  const labels = clusterLabels(name);
  const exporterEnabled = cluster.spec.monitoring?.exporter?.enabled;

  const podMetricsEndpoints = exporterEnabled
    ? [
        {
          port: 'metrics',
          path: '/metrics',
          interval: '30s',
        },
      ]
    : [
        {
          port: 'firebird',
          path: '/metrics',
          interval: '30s',
        },
      ];

  return {
    apiVersion: 'monitoring.coreos.com/v1',
    kind: 'PodMonitor',
    metadata: {
      name: `${name}-podmonitor`,
      namespace,
      labels,
      ownerReferences: [
        {
          apiVersion: `${API_GROUP}/v1`,
          kind: RESOURCE_KIND,
          name: cluster.metadata.name,
          uid: cluster.metadata.uid ?? '',
          controller: true,
          blockOwnerDeletion: true,
        },
      ],
    },
    spec: {
      selector: {
        matchLabels: labels,
      },
      podMetricsEndpoints,
    },
  };
}

/**
 * Builds the primary leader Lease resource for HA failover and election.
 */
export function buildLease(cluster: FirebirdCluster): V1Lease {
  const { name, namespace = 'default' } = cluster.metadata;
  const labels = clusterLabels(name);

  return {
    apiVersion: 'coordination.k8s.io/v1',
    kind: 'Lease',
    metadata: {
      name: `${name}-lease`,
      namespace,
      labels,
      ownerReferences: [
        {
          apiVersion: `${API_GROUP}/v1`,
          kind: RESOURCE_KIND,
          name: cluster.metadata.name,
          uid: cluster.metadata.uid ?? '',
          controller: true,
          blockOwnerDeletion: true,
        },
      ],
    },
    spec: {
      holderIdentity: `${name}-0`,
      leaseDurationSeconds: 15,
      // Lease times are MicroTime (6 fractional digits); a plain Date serializes
      // with milliseconds and is rejected by the API server
      renewTime: new V1MicroTime(),
    },
  };
}

/**
 * Builds the PodDisruptionBudget for a FirebirdCluster (when instances > 1).
 */
export function buildPodDisruptionBudget(cluster: FirebirdCluster): V1PodDisruptionBudget {
  const { name, namespace = 'default' } = cluster.metadata;
  const labels = clusterLabels(name);

  const pdb: V1PodDisruptionBudget = {
    apiVersion: 'policy/v1',
    kind: 'PodDisruptionBudget',
    metadata: {
      name: `${name}-pdb`,
      namespace,
      labels,
      ownerReferences: [
        {
          apiVersion: `${API_GROUP}/v1`,
          kind: RESOURCE_KIND,
          name: cluster.metadata.name,
          uid: cluster.metadata.uid ?? '',
          controller: true,
          blockOwnerDeletion: true,
        },
      ],
    },
    spec: {
      minAvailable: 1,
      selector: {
        matchLabels: labels,
      },
    },
  };

  return pdb;
}

/**
 * Checks if a PodDisruptionBudget needs updating.
 */
export function podDisruptionBudgetNeedsUpdate(
  existing: V1PodDisruptionBudget,
  desired: V1PodDisruptionBudget,
): boolean {
  const existingSpec = existing.spec;
  const desiredSpec = desired.spec;

  if (!existingSpec || !desiredSpec) return true;
  if (existingSpec.minAvailable !== desiredSpec.minAvailable) return true;

  return false;
}

/**
 * Builds the ConfigMap for custom firebird.conf settings or bootstrap init.sql.
 */
export function buildConfigMap(
  cluster: FirebirdCluster,
  options?: {
    primaryPod?: string;
    seedSourcePods?: string[];
    /** IP addresses of the instance pods (isolation-check.pl, when cluster DNS fails) */
    peerAddresses?: string[];
    reseed?: Record<string, string>;
    promote?: Record<string, string>;
    demote?: Record<string, string>;
  },
): V1ConfigMap | null {
  const { name, namespace = 'default' } = cluster.metadata;
  const labels = clusterLabels(name);
  const data: Record<string, string> = {};

  const settings = firebirdConfSettings(cluster);

  // Informational copy of the effective settings; they are applied via FIREBIRD_CONF_* env vars
  if (Object.keys(settings).length > 0) {
    const lines = Object.entries(settings).map(([key, val]) => `${key} = ${val}`);
    data['firebird.conf'] = lines.join('\n') + '\n';
  }

  if (cluster.spec.bootstrap?.initSql) {
    data['init.sql'] = cluster.spec.bootstrap.initSql;
  }

  if (replicationEnabled(cluster)) {
    Object.assign(data, replicationConfigData(cluster));
    // Read by the replica seeding step and the segment puller on every poll, so it follows
    // the operator's view of the current primary without restarting pods
    data[PRIMARY_KEY] = instanceHost(cluster, options?.primaryPod ?? `${name}-0`);
    // Ready replicas that can serve seed copies without locking the primary
    data[SEED_SOURCES_KEY] = (options?.seedSourcePods ?? []).map((pod) => `${instanceHost(cluster, pod)}\n`).join('');
    // read by the isolation check without DNS: the current addresses of the instances, so a peer
    // that restarted with a new address while DNS was down still counts
    data[PEER_ADDRESSES_KEY] = (options?.peerAddresses ?? []).map((ip) => `${ip}\n`).join('');
    // Replicas to re-seed; always present so that a merge patch clears finished requests
    const directives = (entries: Record<string, string> = {}) =>
      Object.keys(entries)
        .sort()
        .map((pod) => `${pod} ${entries[pod]}\n`)
        .join('');
    data[RESEED_KEY] = directives(options?.reseed);
    // planned switchover (always present, like reseed)
    data[PROMOTE_KEY] = directives(options?.promote);
    data[DEMOTE_KEY] = directives(options?.demote);
  } else {
    Object.assign(data, backupFilesConfigData());
  }
  Object.assign(data, JOB_SCRIPTS);

  if (Object.keys(data).length === 0) return null;

  return {
    apiVersion: 'v1',
    kind: 'ConfigMap',
    metadata: {
      name: `${name}-config`,
      namespace,
      labels,
      ownerReferences: [
        {
          apiVersion: `${API_GROUP}/v1`,
          kind: RESOURCE_KIND,
          name: cluster.metadata.name,
          uid: cluster.metadata.uid ?? '',
          controller: true,
          blockOwnerDeletion: true,
        },
      ],
    },
    data,
  };
}

/**
 * Checks if a ConfigMap needs updating.
 */
export function configMapNeedsUpdate(existing: V1ConfigMap, desired: V1ConfigMap): boolean {
  return JSON.stringify(existing.data ?? {}) !== JSON.stringify(desired.data ?? {});
}

/**
 * Builds the CronJob for periodic Firebird database sweeping (gfix -sweep) of the primary.
 */
export function buildAutoSweepCronJob(cluster: FirebirdCluster, primaryPod?: string): V1CronJob {
  const { name, namespace = 'default' } = cluster.metadata;
  const spec = cluster.spec;
  const autoSweep = spec.autoSweep;
  const schedule = autoSweep?.schedule ?? '0 3 * * *';
  const dbName = autoSweep?.databaseName ?? databaseName(cluster);
  const image = spec.imageName ?? DEFAULT_FIREBIRD_IMAGE;
  const labels = {
    ...clusterLabels(name),
    'app.kubernetes.io/component': 'sweep',
  };

  const cronJob: V1CronJob = {
    apiVersion: 'batch/v1',
    kind: 'CronJob',
    metadata: {
      name: `${name}-sweep`,
      namespace,
      labels,
      ownerReferences: [
        {
          apiVersion: `${API_GROUP}/v1`,
          kind: RESOURCE_KIND,
          name: cluster.metadata.name,
          uid: cluster.metadata.uid ?? '',
          controller: true,
          blockOwnerDeletion: true,
        },
      ],
    },
    spec: {
      schedule,
      concurrencyPolicy: 'Forbid',
      successfulJobsHistoryLimit: 3,
      failedJobsHistoryLimit: 1,
      jobTemplate: {
        spec: {
          template: {
            metadata: {
              labels,
            },
            spec: jobPodSpec(cluster, {
              restartPolicy: 'OnFailure',
              containers: [
                {
                  name: 'firebird-sweep',
                  image,
                  command: ['/bin/sh', '-c'],
                  // Sweep the primary over the network through its stable headless-Service name
                  // (the <name> Service balances across instances unless read-only routing
                  // labels the primary); the Job needs no access to the instance PVC
                  args: [`gfix -sweep ${instanceHost(cluster, primaryPod ?? `${name}-0`)}:${FIREBIRD_DATA_DIR}/${dbName}`],
                  env: superuserClientEnv(cluster),
                },
              ],
            }),
          },
        },
      },
    },
  };

  return cronJob;
}

/**
 * Checks if an AutoSweep CronJob needs updating.
 */
export function autoSweepCronJobNeedsUpdate(existing: V1CronJob, desired: V1CronJob): boolean {
  const existingSpec = existing.spec;
  const desiredSpec = desired.spec;

  if (!existingSpec || !desiredSpec) return true;
  if (existingSpec.schedule !== desiredSpec.schedule) return true;
  if (Boolean(existingSpec.suspend) !== Boolean(desiredSpec.suspend)) return true;

  const existingContainer = existingSpec.jobTemplate?.spec?.template?.spec?.containers?.[0];
  const desiredContainer = desiredSpec.jobTemplate?.spec?.template?.spec?.containers?.[0];

  if (!existingContainer || !desiredContainer) return true;
  if (existingContainer.image !== desiredContainer.image) return true;
  if (JSON.stringify(existingContainer.args) !== JSON.stringify(desiredContainer.args)) return true;

  return false;
}

/**
 * Builds the NetworkPolicy resource for a FirebirdCluster.
 */
/** A cluster cloning from another one (bootstrap.clone), by its namespace and name */
export interface CloneTarget {
  namespace: string;
  name: string;
}

/** The clusters whose bootstrap.clone reads from `source` */
export function cloneTargets(source: FirebirdCluster, clusters: FirebirdCluster[]): CloneTarget[] {
  const { name, namespace = 'default' } = source.metadata;
  return clusters
    .filter((c) => {
      const clone = c.spec?.bootstrap?.clone;
      const targetNamespace = c.metadata.namespace ?? 'default';
      return clone?.sourceCluster === name && (clone.namespace ?? targetNamespace) === namespace;
    })
    .map((c) => ({ namespace: c.metadata.namespace ?? 'default', name: c.metadata.name }))
    .sort((a, b) => `${a.namespace}/${a.name}`.localeCompare(`${b.namespace}/${b.name}`));
}

export function buildNetworkPolicy(cluster: FirebirdCluster, clones: CloneTarget[] = []): V1NetworkPolicy {
  const { name, namespace = 'default' } = cluster.metadata;
  const labels = clusterLabels(name);
  const npConfig = cluster.spec.networkPolicy;

  const ingressRules: V1NetworkPolicyIngressRule[] = npConfig?.ingressFrom?.map((rule) => ({
    _from: [
      ...(rule.podSelector ? [{ podSelector: { matchLabels: rule.podSelector } }] : []),
      ...(rule.namespaceSelector ? [{ namespaceSelector: { matchLabels: rule.namespaceSelector } }] : []),
    ],
    ports: [
      {
        protocol: 'TCP',
        port: 3050,
      },
    ],
  })) ?? [
    {
      ports: [
        {
          protocol: 'TCP',
          port: 3050,
        },
      ],
    },
  ];

  // The cluster's own pods (instances and maintenance Jobs share the cluster label) reach
  // the database over the network, and replicas fetch journal segments from the primary
  const intraClusterRule = {
    _from: [{ podSelector: { matchLabels: { [CLUSTER_LABEL]: name } } }],
    ports: [
      { protocol: 'TCP', port: 3050 },
      // the segment server, or the backup file server without replication (backup Jobs)
      { protocol: 'TCP', port: SEGMENT_PORT },
    ],
  };
  ingressRules.push(intraClusterRule);
  // clusters cloning from this one: their instance pods copy the database with gbak at bootstrap
  if (clones.length > 0) {
    ingressRules.push({
      _from: clones.map((c) => ({
        namespaceSelector: { matchLabels: { 'kubernetes.io/metadata.name': c.namespace } },
        podSelector: { matchLabels: { [CLUSTER_LABEL]: c.name } },
      })),
      ports: [{ protocol: 'TCP', port: 3050 }],
    });
  }
  // the operator asks the segment servers (replication lag, switchover) and, without replication,
  // the backup file server (restore targets, admission webhook included)
  ingressRules.push({
    _from: [
      {
        namespaceSelector: { matchLabels: { 'kubernetes.io/metadata.name': operatorNamespace() } },
        podSelector: { matchLabels: { 'app.kubernetes.io/name': 'cloudnative-firebird' } },
      },
    ],
    ports: [{ protocol: 'TCP', port: SEGMENT_PORT }],
  });

  const networkPolicy: V1NetworkPolicy = {
    apiVersion: 'networking.k8s.io/v1',
    kind: 'NetworkPolicy',
    metadata: {
      name: `${name}-networkpolicy`,
      namespace,
      labels,
      ownerReferences: [
        {
          apiVersion: `${API_GROUP}/v1`,
          kind: RESOURCE_KIND,
          name: cluster.metadata.name,
          uid: cluster.metadata.uid ?? '',
          controller: true,
          blockOwnerDeletion: true,
        },
      ],
    },
    spec: {
      podSelector: {
        matchLabels: labels,
      },
      policyTypes: ['Ingress'],
      ingress: ingressRules,
    },
  };

  return networkPolicy;
}

/**
 * Checks if a NetworkPolicy needs updating.
 */
/**
 * The client's NetworkPolicy model names the "from" field `_from` and only renames it when it
 * serializes a typed body (create). Merge patches send the object as is, so they need the wire
 * name; without it the "from" restrictions would be dropped and the rules allow every pod.
 */
export function networkPolicyWireFormat(policy: V1NetworkPolicy): object {
  return {
    ...policy,
    spec: {
      ...policy.spec,
      ingress: (policy.spec?.ingress ?? []).map(({ _from, ...rule }) => ({ ...rule, ...(_from ? { from: _from } : {}) })),
    },
  };
}

export function networkPolicyNeedsUpdate(
  existing: V1NetworkPolicy,
  desired: V1NetworkPolicy,
): boolean {
  return JSON.stringify(existing.spec?.ingress ?? []) !== JSON.stringify(desired.spec?.ingress ?? []);
}

/**
 * Builds the CronJob for online database diagnostics (gfix -v -full) of the primary.
 */
export function buildDiagnosticsCronJob(cluster: FirebirdCluster, primaryPod?: string): V1CronJob {
  const { name, namespace = 'default' } = cluster.metadata;
  const spec = cluster.spec;
  const diag = spec.diagnostics;
  const schedule = diag?.schedule ?? '0 4 * * 0';
  const dbName = diag?.databaseName ?? databaseName(cluster);
  const image = spec.imageName ?? DEFAULT_FIREBIRD_IMAGE;
  const labels = {
    ...clusterLabels(name),
    'app.kubernetes.io/component': 'diagnostics',
  };

  return {
    apiVersion: 'batch/v1',
    kind: 'CronJob',
    metadata: {
      name: `${name}-diagnostics`,
      namespace,
      labels,
      ownerReferences: [
        {
          apiVersion: `${API_GROUP}/v1`,
          kind: RESOURCE_KIND,
          name: cluster.metadata.name,
          uid: cluster.metadata.uid ?? '',
          controller: true,
          blockOwnerDeletion: true,
        },
      ],
    },
    spec: {
      schedule,
      concurrencyPolicy: 'Forbid',
      successfulJobsHistoryLimit: 3,
      failedJobsHistoryLimit: 1,
      jobTemplate: {
        spec: {
          template: {
            metadata: { labels },
            spec: jobPodSpec(cluster, {
              restartPolicy: 'OnFailure',
              containers: [
                {
                  name: 'firebird-diagnostics',
                  image,
                  command: ['/bin/sh', '-c'],
                  // Online validation through the service manager works while clients are
                  // connected (gfix -v needs exclusive access); fail the Job on reported errors
                  args: [
                    `out=$(fbsvcmgr ${instanceHost(cluster, primaryPod ?? `${name}-0`)}:service_mgr action_validate dbname ${FIREBIRD_DATA_DIR}/${dbName} 2>&1); ` +
                      `status=$?; echo "$out"; ` +
                      `[ $status -eq 0 ] && ! echo "$out" | grep -qiE 'error|corrupt'`,
                  ],
                  env: superuserClientEnv(cluster),
                },
              ],
            }),
          },
        },
      },
    },
  };
}

/**
 * Checks if a Diagnostics CronJob needs updating.
 */
export function diagnosticsCronJobNeedsUpdate(existing: V1CronJob, desired: V1CronJob): boolean {
  const existingSpec = existing.spec;
  const desiredSpec = desired.spec;
  if (!existingSpec || !desiredSpec) return true;
  if (existingSpec.schedule !== desiredSpec.schedule) return true;
  if (Boolean(existingSpec.suspend) !== Boolean(desiredSpec.suspend)) return true;
  // image, or the primary after a switchover or failover
  const existingContainer = existingSpec.jobTemplate?.spec?.template?.spec?.containers?.[0];
  const desiredContainer = desiredSpec.jobTemplate?.spec?.template?.spec?.containers?.[0];
  if (!existingContainer || !desiredContainer) return true;
  if (existingContainer.image !== desiredContainer.image) return true;
  if (JSON.stringify(existingContainer.args) !== JSON.stringify(desiredContainer.args)) return true;
  return false;
}

/**
 * Builds the Grafana Dashboard ConfigMap for database metrics visualization.
 */
export function buildGrafanaDashboardConfigMap(cluster: FirebirdCluster): V1ConfigMap {
  const { name, namespace = 'default' } = cluster.metadata;
  const labels = {
    ...clusterLabels(name),
    grafana_dashboard: '1',
  };

  const dashboardJson = JSON.stringify({
    title: `Firebird Cluster - ${name}`,
    uid: `firebird-${name}`,
    tags: ['firebird', 'database', 'cloudnative'],
    timezone: 'browser',
    panels: [
      {
        title: 'Active Attachments',
        type: 'stat',
        targets: [{ expr: `firebird_active_attachments{cluster="${name}"}` }],
      },
      {
        title: 'Page Reads & Writes',
        type: 'timeseries',
        targets: [
          { expr: `rate(firebird_page_reads_total{cluster="${name}"}[5m])`, legendFormat: 'Reads' },
          { expr: `rate(firebird_page_writes_total{cluster="${name}"}[5m])`, legendFormat: 'Writes' },
        ],
      },
      {
        title: 'Transaction Gap (OAT / OIT)',
        type: 'gauge',
        targets: [{ expr: `firebird_oldest_active_transaction{cluster="${name}"}` }],
      },
      // exported by the operator itself (utils/metrics.ts)
      {
        title: 'Ready Instances',
        type: 'stat',
        targets: [
          { expr: `firebird_cluster_ready_instances{namespace="${namespace}",cluster="${name}"}`, legendFormat: 'Ready' },
          { expr: `firebird_cluster_instances{namespace="${namespace}",cluster="${name}"}`, legendFormat: 'Requested' },
        ],
      },
      {
        title: 'Replication Lag (seconds)',
        type: 'timeseries',
        targets: [
          {
            expr: `firebird_replication_lag_seconds{namespace="${namespace}",cluster="${name}"}`,
            legendFormat: '{{pod}}',
          },
        ],
      },
      {
        title: 'Replication Lag (segments)',
        type: 'timeseries',
        targets: [
          {
            expr: `firebird_replication_lag_segments{namespace="${namespace}",cluster="${name}"}`,
            legendFormat: '{{pod}}',
          },
        ],
      },
    ],
  });

  return {
    apiVersion: 'v1',
    kind: 'ConfigMap',
    metadata: {
      name: `${name}-grafana-dashboard`,
      namespace,
      labels,
      ownerReferences: [
        {
          apiVersion: `${API_GROUP}/v1`,
          kind: RESOURCE_KIND,
          name: cluster.metadata.name,
          uid: cluster.metadata.uid ?? '',
          controller: true,
          blockOwnerDeletion: true,
        },
      ],
    },
    data: {
      [`firebird-${name}.json`]: dashboardJson,
    },
  };
}
