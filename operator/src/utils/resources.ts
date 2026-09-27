import { createHash } from 'crypto';
import {
  V1StatefulSet,
  V1Service,
  V1CronJob,
  V1Job,
  V1Lease,
  V1PodDisruptionBudget,
  V1ConfigMap,
  V1NetworkPolicy,
  V1MicroTime,
} from '@kubernetes/client-node';
import {
  FirebirdCluster,
  FirebirdBackup,
  FirebirdScheduledBackup,
  FirebirdRestore,
  DEFAULT_FIREBIRD_IMAGE,
  API_GROUP,
  RESOURCE_KIND,
} from '../types';
import { READ_ROUTABLE_LABEL, ROLE_LABEL } from './routing';
import {
  PRIMARY_KEY,
  REPLICATION_SCRIPTS,
  SEGMENT_PORT,
  buildReplicationConf,
  buildReplicationContainers,
  instanceHost,
  replicationDirectories,
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
 * Applies the cluster hibernation state to an operator-managed CronJob:
 * scheduled work is suspended while the cluster is hibernated.
 */
export function withHibernation<T extends V1CronJob>(cronJob: T, cluster: FirebirdCluster): T {
  if (cronJob.spec) cronJob.spec.suspend = Boolean(cluster.spec.hibernated);
  return cronJob;
}

/** Data directory of the official firebirdsql/firebird image (FIREBIRD_DATA); the PVC is mounted here */
export const FIREBIRD_DATA_DIR = '/var/lib/firebird/data';

/** Default database file created in each instance */
export const DEFAULT_DATABASE_NAME = 'mydb.fdb';

/** Returns the database file name managed by the cluster */
export function databaseName(cluster: FirebirdCluster): string {
  return cluster.spec.databaseName ?? DEFAULT_DATABASE_NAME;
}

/**
 * Returns the effective firebird.conf settings, including WireCrypt=Required when TLS is enabled.
 */
export function firebirdConfSettings(cluster: FirebirdCluster): Record<string, string> {
  const settings = { ...(cluster.spec.config?.settings ?? {}) };
  if (cluster.spec.tls?.enabled && !settings['WireCrypt']) {
    settings['WireCrypt'] = 'Required';
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
    // Additional env vars from spec
    ...(spec.env ?? []),
  ];

  // Build init containers for recovery or cloning if specified
  const initContainers = [];
  if (spec.bootstrap?.recovery) {
    const recovery = spec.bootstrap.recovery;
    let restoreCmd = 'echo "Starting database recovery..."; ';
    if (recovery.s3) {
      const endpointOpt = recovery.s3.endpoint ? `--endpoint-url ${recovery.s3.endpoint}` : '';
      const prefix = recovery.s3.prefix ? `${recovery.s3.prefix.replace(/\/$/, '')}/` : '';
      restoreCmd += `if [ ! -f /var/lib/firebird/data/mydb.fdb ]; then aws ${endpointOpt} s3 cp s3://${recovery.s3.bucket}/${prefix}backup.fbk /tmp/backup.fbk && gbak -c -v /tmp/backup.fbk /var/lib/firebird/data/mydb.fdb; fi`;
    } else if (recovery.sourcePath) {
      restoreCmd += `if [ ! -f /var/lib/firebird/data/mydb.fdb ]; then gbak -c -v ${recovery.sourcePath} /var/lib/firebird/data/mydb.fdb; fi`;
    }
    initContainers.push({
      name: 'bootstrap-restore',
      image,
      command: ['/bin/sh', '-c'],
      args: [restoreCmd],
      ...(recovery.s3
        ? {
            env: [
              {
                name: 'AWS_ACCESS_KEY_ID',
                valueFrom: { secretKeyRef: { name: recovery.s3.secretRef.name, key: 'AWS_ACCESS_KEY_ID' } },
              },
              {
                name: 'AWS_SECRET_ACCESS_KEY',
                valueFrom: { secretKeyRef: { name: recovery.s3.secretRef.name, key: 'AWS_SECRET_ACCESS_KEY' } },
              },
            ],
          }
        : {}),
      volumeMounts: [
        {
          name: 'firebird-data',
          mountPath: '/var/lib/firebird/data',
        },
      ],
    });
  } else if (spec.bootstrap?.clone) {
    const clone = spec.bootstrap.clone;
    const sourceNs = clone.namespace ?? namespace;
    const cloneCmd = `if [ ! -f /var/lib/firebird/data/mydb.fdb ]; then echo "Cloning database from ${clone.sourceCluster} in ${sourceNs}..."; nc -l -p 9999 | tar -xzf - -C /var/lib/firebird/data/ || true; fi`;
    initContainers.push({
      name: 'bootstrap-clone',
      image,
      command: ['/bin/sh', '-c'],
      args: [cloneCmd],
      volumeMounts: [
        {
          name: 'firebird-data',
          mountPath: '/var/lib/firebird/data',
        },
      ],
    });
  }

  const replication = replicationEnabled(cluster)
    ? buildReplicationContainers(cluster, {
        image,
        databasePath: `${FIREBIRD_DATA_DIR}/${databaseName(cluster)}`,
        dataDir: FIREBIRD_DATA_DIR,
        credentials: superuserClientEnv(cluster),
      })
    : undefined;
  if (replication) initContainers.push(replication.initContainer);
  const usesConfigVolume = Boolean(spec.bootstrap?.initSql || replication);

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
        ...(spec.tls?.enabled
          ? [
              {
                name: 'tls-cert',
                mountPath: '/firebird/etc/tls',
                readOnly: true,
              },
            ]
          : []),
      ],
      livenessProbe: {
        tcpSocket: { port: 3050 },
        initialDelaySeconds: 30,
        periodSeconds: 10,
        failureThreshold: 5,
      },
      readinessProbe: {
        tcpSocket: { port: 3050 },
        initialDelaySeconds: 15,
        periodSeconds: 5,
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
    ...(replication?.sidecars ?? []),
  ];

  const volumes = [
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
    ...(spec.tls?.enabled
      ? [
          {
            name: 'tls-cert',
            secret: {
              secretName: spec.tls.secretName ?? `${name}-tls`,
            },
          },
        ]
      : []),
  ];

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
      selector: {
        matchLabels: labels,
      },
      template: {
        metadata: {
          labels,
          ...(secretHash || replication
            ? {
                annotations: {
                  ...(secretHash ? { 'firebird.cloudnative-firebird.io/superuser-secret-hash': secretHash } : {}),
                  // replication.conf is mounted via subPath, which does not follow ConfigMap
                  // updates; hashing it into the template rolls the pods when it changes
                  ...(replication ? { [REPLICATION_CONFIG_HASH_ANNOTATION]: replicationConfigHash(cluster) } : {}),
                },
              }
            : {}),
        },
        spec: {
          securityContext: {
            fsGroup: 999,
          },
          ...(initContainers.length > 0 ? { initContainers } : {}),
          ...(spec.nodeSelector ? { nodeSelector: spec.nodeSelector } : {}),
          ...(spec.affinity ? { affinity: spec.affinity } : {}),
          ...(spec.tolerations ? { tolerations: spec.tolerations } : {}),
          containers,
          ...(volumes.length > 0 ? { volumes } : {}),
        },
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
  // Only operator-set annotations are compared; others (e.g. kubectl restartedAt) are kept
  const existingAnnotations = existingSpec.template?.metadata?.annotations ?? {};
  const desiredAnnotations = desiredSpec.template?.metadata?.annotations ?? {};
  if (Object.entries(desiredAnnotations).some(([key, value]) => existingAnnotations[key] !== value)) {
    return true;
  }

  const existingPodSpec = existingSpec.template?.spec;
  const desiredPodSpec = desiredSpec.template?.spec;
  if (!existingPodSpec || !desiredPodSpec) return true;

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

  return false;
}

/**
 * Builds the CronJob resource for Firebird cluster backups.
 */
export function buildBackupCronJob(cluster: FirebirdCluster): V1CronJob {
  const { name, namespace = 'default' } = cluster.metadata;
  const spec = cluster.spec;
  const backup = spec.backup;
  const schedule = backup?.schedule ?? '0 2 * * *';
  const image = spec.imageName ?? DEFAULT_FIREBIRD_IMAGE;
  const labels = {
    ...clusterLabels(name),
    'app.kubernetes.io/component': 'backup',
  };

  const backupType = backup?.type ?? 'logical';
  const nbackupLevel = backup?.level ?? 0;
  const backupFileName =
    backupType === 'physical'
      ? `nbackup-lvl${nbackupLevel}-\$(date +%Y%m%d%H%M%S).nbk`
      : `backup-\$(date +%Y%m%d%H%M%S).fbk`;

  const baseCmd =
    backupType === 'physical'
      ? `nbackup -L ${nbackupLevel} -user SYSDBA -pas "\${ISC_PASSWORD}" localhost:/var/lib/firebird/data/mydb.fdb /var/lib/firebird/data/${backupFileName}`
      : `gbak -b -user SYSDBA -pas "\${ISC_PASSWORD}" localhost:/var/lib/firebird/data/mydb.fdb /var/lib/firebird/data/${backupFileName}`;

  const s3Cmd = backup?.s3
    ? ` && aws s3 cp /var/lib/firebird/data/ s3://${backup.s3.bucket}/${backup.s3.prefix ? backup.s3.prefix + '/' : ''} --recursive --exclude "*"`
    : '';

  const backupCommand = baseCmd + s3Cmd;

  const cronJob: V1CronJob = {
    apiVersion: 'batch/v1',
    kind: 'CronJob',
    metadata: {
      name: `${name}-backup`,
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
            spec: {
              restartPolicy: 'OnFailure',
              containers: [
                {
                  name: 'firebird-backup',
                  image,
                  command: ['/bin/sh', '-c'],
                  args: [backupCommand],
                  env: [
                    ...(spec.superuserSecret
                      ? [
                          {
                            name: 'ISC_PASSWORD',
                            valueFrom: {
                              secretKeyRef: {
                                name: spec.superuserSecret.name,
                                key: 'password',
                              },
                            },
                          },
                        ]
                      : [{ name: 'ISC_PASSWORD', value: 'masterkey' }]),
                    ...(backup?.retentionPolicy
                      ? [{ name: 'FIREBIRD_RETENTION_POLICY', value: backup.retentionPolicy }]
                      : []),
                    ...(backup?.s3
                      ? [
                          {
                            name: 'AWS_ACCESS_KEY_ID',
                            valueFrom: {
                              secretKeyRef: {
                                name: backup.s3.secretRef.name,
                                key: 'AWS_ACCESS_KEY_ID',
                              },
                            },
                          },
                          {
                            name: 'AWS_SECRET_ACCESS_KEY',
                            valueFrom: {
                              secretKeyRef: {
                                name: backup.s3.secretRef.name,
                                key: 'AWS_SECRET_ACCESS_KEY',
                              },
                            },
                          },
                        ]
                      : []),
                  ],
                  volumeMounts: [
                    {
                      name: 'firebird-data',
                      mountPath: '/var/lib/firebird/data',
                    },
                  ],
                },
              ],
              volumes: [
                {
                  name: 'firebird-data',
                  persistentVolumeClaim: {
                    claimName: `firebird-data-${name}-0`,
                  },
                },
              ],
            },
          },
        },
      },
    },
  };

  return cronJob;
}

/**
 * Checks if a Backup CronJob needs updating (e.g. schedule or image change).
 */
export function cronJobNeedsUpdate(existing: V1CronJob, desired: V1CronJob): boolean {
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
 * Builds the cert-manager Certificate resource for TLS encryption.
 */
export function buildCertificate(cluster: FirebirdCluster): Record<string, unknown> {
  const { name, namespace = 'default' } = cluster.metadata;
  const labels = clusterLabels(name);
  const tls = cluster.spec.tls;

  return {
    apiVersion: 'cert-manager.io/v1',
    kind: 'Certificate',
    metadata: {
      name: `${name}-cert`,
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
      secretName: tls?.secretName ?? `${name}-tls`,
      dnsNames: [
        name,
        `${name}.${namespace}`,
        `${name}.${namespace}.svc.cluster.local`,
        `*.${name}-headless.${namespace}.svc.cluster.local`,
      ],
      issuerRef: tls?.issuerRef ?? {
        name: 'selfsigned-issuer',
        kind: 'Issuer',
        group: 'cert-manager.io',
      },
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
 * Builds a Kubernetes Job for an on-demand FirebirdBackup CRD.
 */
export function buildBackupJob(backup: FirebirdBackup, cluster: FirebirdCluster): V1Job {
  const { name: backupName, namespace = 'default' } = backup.metadata;
  const clusterName = backup.spec.clusterName;
  const image = cluster.spec.imageName ?? DEFAULT_FIREBIRD_IMAGE;
  const labels = {
    ...clusterLabels(clusterName),
    'app.kubernetes.io/component': 'on-demand-backup',
  };

  const backupType = backup.spec.type ?? 'logical';
  const nbackupLevel = backup.spec.level ?? 0;
  const fileName =
    backupType === 'physical'
      ? `nbackup-manual-lvl${nbackupLevel}-${backupName}.nbk`
      : `backup-manual-${backupName}.fbk`;

  const cmd =
    backupType === 'physical'
      ? `nbackup -L ${nbackupLevel} -user SYSDBA -pas "\${ISC_PASSWORD}" localhost:/var/lib/firebird/data/mydb.fdb /var/lib/firebird/data/${fileName}`
      : `gbak -b -user SYSDBA -pas "\${ISC_PASSWORD}" localhost:/var/lib/firebird/data/mydb.fdb /var/lib/firebird/data/${fileName}`;

  return {
    apiVersion: 'batch/v1',
    kind: 'Job',
    metadata: {
      name: `backup-${backupName}`,
      namespace,
      labels,
    },
    spec: {
      template: {
        metadata: { labels },
        spec: {
          restartPolicy: 'OnFailure',
          containers: [
            {
              name: 'firebird-backup',
              image,
              command: ['/bin/sh', '-c'],
              args: [cmd],
              env: [
                ...(cluster.spec.superuserSecret
                  ? [
                      {
                        name: 'ISC_PASSWORD',
                        valueFrom: {
                          secretKeyRef: {
                            name: cluster.spec.superuserSecret.name,
                            key: 'password',
                          },
                        },
                      },
                    ]
                  : [{ name: 'ISC_PASSWORD', value: 'masterkey' }]),
              ],
              volumeMounts: [
                {
                  name: 'firebird-data',
                  mountPath: '/var/lib/firebird/data',
                },
              ],
            },
          ],
          volumes: [
            {
              name: 'firebird-data',
              persistentVolumeClaim: {
                claimName: `firebird-data-${clusterName}-0`,
              },
            },
          ],
        },
      },
    },
  };
}

/**
 * Builds a Kubernetes Job for a FirebirdRestore CRD.
 */
export function buildRestoreJob(restore: FirebirdRestore, cluster: FirebirdCluster): V1Job {
  const { name: restoreName, namespace = 'default' } = restore.metadata;
  const clusterName = restore.spec.clusterName;
  const image = cluster.spec.imageName ?? DEFAULT_FIREBIRD_IMAGE;
  const labels = {
    ...clusterLabels(clusterName),
    'app.kubernetes.io/component': 'restore',
  };

  const targetDb = restore.spec.targetDatabase ?? 'mydb.fdb';
  const restoreType = restore.spec.restoreType ?? 'logical';
  const backupPath = restore.spec.backupPath ?? '/var/lib/firebird/data/backup-restore.fbk';

  const cmd =
    restoreType === 'physical'
      ? `nbackup -R /var/lib/firebird/data/${targetDb} ${backupPath}`
      : `gbak -c -user SYSDBA -pas "\${ISC_PASSWORD}" ${backupPath} localhost:/var/lib/firebird/data/${targetDb}`;

  return {
    apiVersion: 'batch/v1',
    kind: 'Job',
    metadata: {
      name: `restore-${restoreName}`,
      namespace,
      labels,
    },
    spec: {
      template: {
        metadata: { labels },
        spec: {
          restartPolicy: 'OnFailure',
          containers: [
            {
              name: 'firebird-restore',
              image,
              command: ['/bin/sh', '-c'],
              args: [cmd],
              env: [
                ...(cluster.spec.superuserSecret
                  ? [
                      {
                        name: 'ISC_PASSWORD',
                        valueFrom: {
                          secretKeyRef: {
                            name: cluster.spec.superuserSecret.name,
                            key: 'password',
                          },
                        },
                      },
                    ]
                  : [{ name: 'ISC_PASSWORD', value: 'masterkey' }]),
              ],
              volumeMounts: [
                {
                  name: 'firebird-data',
                  mountPath: '/var/lib/firebird/data',
                },
              ],
            },
          ],
          volumes: [
            {
              name: 'firebird-data',
              persistentVolumeClaim: {
                claimName: `firebird-data-${clusterName}-0`,
              },
            },
          ],
        },
      },
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
  options?: { primaryPod?: string },
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
  }

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
 * Builds the CronJob for periodic Firebird database sweeping (gfix -sweep).
 */
export function buildAutoSweepCronJob(cluster: FirebirdCluster): V1CronJob {
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
            spec: {
              restartPolicy: 'OnFailure',
              containers: [
                {
                  name: 'firebird-sweep',
                  image,
                  command: ['/bin/sh', '-c'],
                  // Sweep the primary over the network through the read-write Service;
                  // the Job needs no access to the instance PVC
                  args: [`gfix -sweep ${name}:${FIREBIRD_DATA_DIR}/${dbName}`],
                  env: superuserClientEnv(cluster),
                },
              ],
            },
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
export function buildNetworkPolicy(cluster: FirebirdCluster): V1NetworkPolicy {
  const { name, namespace = 'default' } = cluster.metadata;
  const labels = clusterLabels(name);
  const npConfig = cluster.spec.networkPolicy;

  const ingressRules = npConfig?.ingressFrom?.map((rule) => ({
    from: [
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
    from: [{ podSelector: { matchLabels: { [CLUSTER_LABEL]: name } } }],
    ports: [
      { protocol: 'TCP', port: 3050 },
      ...(replicationEnabled(cluster) ? [{ protocol: 'TCP', port: SEGMENT_PORT }] : []),
    ],
  };
  ingressRules.push(intraClusterRule);

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
export function networkPolicyNeedsUpdate(
  existing: V1NetworkPolicy,
  desired: V1NetworkPolicy,
): boolean {
  return JSON.stringify(existing.spec?.ingress ?? []) !== JSON.stringify(desired.spec?.ingress ?? []);
}

/**
 * Builds the CronJob for online database diagnostics (gfix -v -full).
 */
export function buildDiagnosticsCronJob(cluster: FirebirdCluster): V1CronJob {
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
            spec: {
              restartPolicy: 'OnFailure',
              containers: [
                {
                  name: 'firebird-diagnostics',
                  image,
                  command: ['/bin/sh', '-c'],
                  // Online validation through the service manager works while clients are
                  // connected (gfix -v needs exclusive access); fail the Job on reported errors
                  args: [
                    `out=$(fbsvcmgr ${name}:service_mgr action_validate dbname ${FIREBIRD_DATA_DIR}/${dbName} 2>&1); ` +
                      `status=$?; echo "$out"; ` +
                      `[ $status -eq 0 ] && ! echo "$out" | grep -qiE 'error|corrupt'`,
                  ],
                  env: superuserClientEnv(cluster),
                },
              ],
            },
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

/**
 * Builds a CronJob for a FirebirdScheduledBackup Custom Resource.
 */
export function buildScheduledBackupCronJob(
  scheduledBackup: FirebirdScheduledBackup,
  cluster: FirebirdCluster,
): V1CronJob {
  const { name: sbName, namespace = 'default' } = scheduledBackup.metadata;
  const spec = scheduledBackup.spec;
  const clusterName = spec.clusterName;
  const schedule = spec.schedule;
  const image = cluster.spec.imageName ?? DEFAULT_FIREBIRD_IMAGE;
  const labels = {
    ...clusterLabels(clusterName),
    'app.kubernetes.io/component': 'scheduled-backup',
  };

  const backupType = spec.type ?? 'logical';
  const nbackupLevel = spec.level ?? 0;
  const backupFileName =
    backupType === 'physical'
      ? `nbackup-sched-lvl${nbackupLevel}-\$(date +%Y%m%d%H%M%S).nbk`
      : `backup-sched-\$(date +%Y%m%d%H%M%S).fbk`;

  const cmd =
    backupType === 'physical'
      ? `nbackup -L ${nbackupLevel} -user SYSDBA -pas "\${ISC_PASSWORD}" localhost:/var/lib/firebird/data/mydb.fdb /var/lib/firebird/data/${backupFileName}`
      : `gbak -b -user SYSDBA -pas "\${ISC_PASSWORD}" localhost:/var/lib/firebird/data/mydb.fdb /var/lib/firebird/data/${backupFileName}`;

  return {
    apiVersion: 'batch/v1',
    kind: 'CronJob',
    metadata: {
      name: `sched-backup-${sbName}`,
      namespace,
      labels,
    },
    spec: {
      schedule,
      suspend: spec.suspend ?? false,
      concurrencyPolicy: 'Forbid',
      jobTemplate: {
        spec: {
          template: {
            metadata: { labels },
            spec: {
              restartPolicy: 'OnFailure',
              containers: [
                {
                  name: 'firebird-scheduled-backup',
                  image,
                  command: ['/bin/sh', '-c'],
                  args: [cmd],
                  env: [
                    ...(cluster.spec.superuserSecret
                      ? [
                          {
                            name: 'ISC_PASSWORD',
                            valueFrom: {
                              secretKeyRef: {
                                name: cluster.spec.superuserSecret.name,
                                key: 'password',
                              },
                            },
                          },
                        ]
                      : [{ name: 'ISC_PASSWORD', value: 'masterkey' }]),
                  ],
                  volumeMounts: [
                    {
                      name: 'firebird-data',
                      mountPath: '/var/lib/firebird/data',
                    },
                  ],
                },
              ],
              volumes: [
                {
                  name: 'firebird-data',
                  persistentVolumeClaim: {
                    claimName: `firebird-data-${clusterName}-0`,
                  },
                },
              ],
            },
          },
        },
      },
    },
  };
}

/**
 * Builds a CronJob for replication journal continuous archiving (PITR) to S3.
 */
export function buildJournalArchiveCronJob(cluster: FirebirdCluster): V1CronJob | null {
  const { name, namespace = 'default' } = cluster.metadata;
  const s3 = cluster.spec.replication?.journalArchiveS3;

  if (!cluster.spec.replication?.enabled || !s3) {
    return null;
  }

  const schedule = cluster.spec.replication.archiveSchedule ?? '*/15 * * * *';
  const archiveDir = replicationDirectories(cluster, FIREBIRD_DATA_DIR).archive;
  const cronJobName = `${name}-journal-archive`;
  const labels = clusterLabels(name);
  const prefix = s3.prefix ? `${s3.prefix.replace(/\/$/, '')}/` : '';
  const endpointOpt = s3.endpoint ? `--endpoint-url ${s3.endpoint}` : '';

  // No --delete: the primary prunes shipped segments locally after segmentRetentionHours,
  // while the object store keeps the full history for point-in-time recovery
  const archiveCmd = `aws ${endpointOpt} s3 sync ${archiveDir}/ s3://${s3.bucket}/${prefix}journals/`;

  const cronJob: V1CronJob = {
    apiVersion: 'batch/v1',
    kind: 'CronJob',
    metadata: {
      name: cronJobName,
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
      jobTemplate: {
        spec: {
          template: {
            metadata: { labels },
            spec: {
              restartPolicy: 'OnFailure',
              containers: [
                {
                  name: 'journal-archiver',
                  image: cluster.spec.imageName ?? DEFAULT_FIREBIRD_IMAGE,
                  command: ['/bin/sh', '-c'],
                  args: [archiveCmd],
                  env: [
                    {
                      name: 'AWS_ACCESS_KEY_ID',
                      valueFrom: { secretKeyRef: { name: s3.secretRef.name, key: 'AWS_ACCESS_KEY_ID' } },
                    },
                    {
                      name: 'AWS_SECRET_ACCESS_KEY',
                      valueFrom: { secretKeyRef: { name: s3.secretRef.name, key: 'AWS_SECRET_ACCESS_KEY' } },
                    },
                  ],
                  volumeMounts: [
                    {
                      name: 'firebird-data',
                      mountPath: '/var/lib/firebird/data',
                    },
                  ],
                },
              ],
              volumes: [
                {
                  name: 'firebird-data',
                  persistentVolumeClaim: {
                    claimName: `firebird-data-${name}-0`,
                  },
                },
              ],
            },
          },
        },
      },
    },
  };

  return cronJob;
}




