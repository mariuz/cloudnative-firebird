/**
 * Type definitions for the FirebirdCluster Custom Resource Definition.
 * Inspired by cloudnative-pg's Cluster CRD.
 */

/**
 * Specification for the Firebird superuser secret.
 */
export interface SuperuserSecretRef {
  /** Name of the Kubernetes Secret containing the superuser password */
  name: string;
}

/**
 * Storage configuration for a Firebird cluster.
 */
export interface StorageConfiguration {
  /** Size of the PersistentVolumeClaim (e.g. "1Gi", "10Gi") */
  size: string;
  /** Optional StorageClass name for the PVC */
  storageClass?: string;
}

/**
 * Resource requirements for a container.
 */
export interface ResourceRequirements {
  limits?: {
    cpu?: string;
    memory?: string;
  };
  requests?: {
    cpu?: string;
    memory?: string;
  };
}

/**
 * S3 cloud object storage configuration for database backups.
 */
export interface S3BackupConfiguration {
  /** S3 Endpoint URL (e.g., "https://s3.us-east-1.amazonaws.com" or MinIO endpoint) */
  endpoint?: string;
  /** S3 Bucket name for archiving backups */
  bucket: string;
  /** AWS/S3 Region (defaults to "us-east-1") */
  region?: string;
  /** Reference to Secret containing AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY */
  secretRef: {
    name: string;
  };
  /** Object key prefix/folder inside bucket */
  prefix?: string;
}

/**
 * Backup configuration for a Firebird cluster.
 */
export interface BackupConfiguration {
  /** Whether backups are enabled */
  enabled: boolean;
  /**
   * Backup strategy:
   * - 'logical': uses gbak tool (defaults to 'logical')
   * - 'physical': uses nbackup tool
   */
  type?: 'logical' | 'physical';
  /**
   * nbackup level (for physical backups):
   * 0: Full physical base backup
   * 1, 2: Incremental physical backup
   * Defaults to 0.
   */
  level?: 0 | 1 | 2;
  /** Cron schedule for backups (e.g. "0 2 * * *") */
  schedule?: string;
  /** Backup retention policy (e.g. "7d", "30d") */
  retentionPolicy?: string;
  /** Cloud object storage configuration for backup archive export */
  s3?: S3BackupConfiguration;
}

/**
 * NetworkPolicy configuration for database traffic isolation.
 */
export interface NetworkPolicyConfiguration {
  /** Whether to create a NetworkPolicy for the cluster */
  enabled: boolean;
  /** Ingress rules to allow incoming database connections on port 3050 */
  ingressFrom?: Array<{
    podSelector?: Record<string, string>;
    namespaceSelector?: Record<string, string>;
  }>;
}

/**
 * Configuration for the Prometheus exporter sidecar container.
 */
export interface ExporterConfiguration {
  /** Whether to run a Prometheus exporter sidecar container */
  enabled: boolean;
  /** Custom Docker image for the exporter container (default: "prom/firebird-exporter:latest") */
  image?: string;
  /** Container port for metrics scraping (default: 9108) */
  port?: number;
  /** Resource requests and limits for the exporter sidecar */
  resources?: ResourceRequirements;
  /** Environment variables for the exporter container */
  env?: Array<{ name: string; value?: string; valueFrom?: object }>;
}

/**
 * Monitoring configuration for a Firebird cluster.
 */
export interface MonitoringConfiguration {
  /** Whether to enable Prometheus metrics via PodMonitor */
  enablePodMonitor?: boolean;
  /** Whether to reconcile a Grafana dashboard ConfigMap for database metrics */
  enableGrafanaDashboard?: boolean;
  /** Metrics exporter sidecar configuration */
  exporter?: ExporterConfiguration;
}

/**
 * Diagnostics configuration for online database integrity verification (gfix -v -full).
 */
export interface DiagnosticsConfiguration {
  /** Whether database integrity verification checks are enabled */
  enabled: boolean;
  /** Cron schedule for diagnostic execution (defaults to "0 4 * * 0") */
  schedule?: string;
  /** Database file name to check (defaults to "mydb.fdb") */
  databaseName?: string;
}

/**
 * TLS / WireCrypt security configuration.
 */
export interface TLSConfiguration {
  /** Whether TLS encryption is required/enabled for database connections */
  enabled: boolean;
  /** Name of Secret containing server TLS certificate and key (tls.crt, tls.key) */
  secretName?: string;
  /** cert-manager Issuer/ClusterIssuer reference for automated TLS certificate issuance */
  issuerRef?: {
    name: string;
    kind?: string;
    group?: string;
  };
}

/**
 * Replication configuration for a Firebird cluster.
 * Uses Firebird 4.0+ journal-based replication.
 */
export interface ReplicationConfiguration {
  /** Whether replication is enabled */
  enabled: boolean;
  /**
   * Replication mode.
   * - sync: writes are confirmed only after replica acknowledges (safer, slower)
   * - async: writes are confirmed immediately, replica catches up (faster, less durable)
   * Defaults to 'async'.
   */
  mode?: 'sync' | 'async';
  /** Directory path for replication journal files (defaults to "/firebird/data/journals") */
  journalDirectory?: string;
  /** Cloud S3 storage configuration for continuous journal archiving (PITR) */
  journalArchiveS3?: S3BackupConfiguration;
  /** Cron schedule for archiving completed journal files to object storage */
  archiveSchedule?: string;
  /** Replication lag and readiness-aware routing of read-only traffic to replicas */
  readOnlyRouting?: ReadOnlyRoutingConfiguration;
}

/**
 * Smart read-only traffic routing configuration.
 * When enabled, the operator labels each pod with its role and routability and the
 * `-replica` Service only selects ready replicas whose reported replication lag is
 * within `maxLagSeconds`. Replication lag is read from the
 * `firebird.cloudnative-firebird.io/replication-lag-seconds` pod annotation, published
 * by the replication agent / metrics exporter.
 */
export interface ReadOnlyRoutingConfiguration {
  /** Whether lag-aware read-only routing is enabled */
  enabled: boolean;
  /** Maximum replication lag (seconds) for a replica to receive read traffic (defaults to 30) */
  maxLagSeconds?: number;
  /** Route read-only traffic to the primary when no replica is eligible (defaults to true) */
  fallbackToPrimary?: boolean;
}

/**
 * AutoSweep configuration for Firebird database transaction garbage collection.
 */
export interface AutoSweepConfiguration {
  /** Whether database sweeping is enabled */
  enabled: boolean;
  /** Cron schedule for sweep execution (defaults to "0 3 * * *") */
  schedule?: string;
  /** Database file name to sweep (defaults to "mydb.fdb") */
  databaseName?: string;
}

/**
 * Custom firebird.conf settings.
 */
export interface FirebirdConfig {
  /** Key-value settings to project into firebird.conf */
  settings?: Record<string, string>;
}

/**
 * Cloud or file recovery settings for initial database bootstrapping.
 */
export interface BackupRecoveryConfiguration {
  /** Backup file path or object storage key */
  sourcePath?: string;
  /** S3 cloud storage source configuration */
  s3?: S3BackupConfiguration;
}

/**
 * Target source cluster configuration for cluster-to-cluster cloning.
 */
export interface CloneConfiguration {
  /** Name of the source FirebirdCluster to clone from */
  sourceCluster: string;
  /** Namespace of the source FirebirdCluster (defaults to same namespace as target cluster) */
  namespace?: string;
}

/**
 * Bootstrap configuration for initializing a new cluster.
 */
export interface BootstrapConfiguration {
  /** Inline DDL/DML SQL script to run on initial database creation */
  initSql?: string;
  /** Cloud or file backup recovery configuration for database bootstrapping */
  recovery?: BackupRecoveryConfiguration;
  /** Source cluster configuration for cluster-to-cluster database cloning */
  clone?: CloneConfiguration;
}

/**
 * Specification of a FirebirdCluster resource.
 */
export interface FirebirdClusterSpec {
  /** Number of Firebird instances to run */
  instances: number;
  /**
   * Docker image name for Firebird.
   * Defaults to firebirdsql/firebird:latest
   */
  imageName?: string;
  /** Reference to the Secret containing the superuser password (SYSDBA) */
  superuserSecret?: SuperuserSecretRef;
  /** Storage configuration for the Firebird data files */
  storage: StorageConfiguration;
  /** Resource requirements for each Firebird container */
  resources?: ResourceRequirements;
  /** Backup configuration */
  backup?: BackupConfiguration;
  /** NetworkPolicy configuration */
  networkPolicy?: NetworkPolicyConfiguration;
  /** Monitoring configuration */
  monitoring?: MonitoringConfiguration;
  /** Replication configuration */
  replication?: ReplicationConfiguration;
  /** AutoSweep configuration for periodic gfix database sweeping */
  autoSweep?: AutoSweepConfiguration;
  /** Diagnostics configuration for online database integrity checks */
  diagnostics?: DiagnosticsConfiguration;
  /** Custom firebird.conf configuration settings */
  config?: FirebirdConfig;
  /** TLS configuration for client and wire communication encryption */
  tls?: TLSConfiguration;
  /** Bootstrap options for initial database creation */
  bootstrap?: BootstrapConfiguration;
  /** Whether the operator should suspend reconciliation for this cluster */
  suspended?: boolean;
  /**
   * Declarative hibernation: scales the cluster down to zero pods and suspends its
   * CronJobs while retaining PVCs, Services and configuration. Set back to false to resume.
   */
  hibernated?: boolean;
  /** Additional environment variables to pass to the Firebird container */
  env?: Array<{ name: string; value?: string; valueFrom?: object }>;
  /** Node labels required for pod scheduling */
  nodeSelector?: Record<string, string>;
  /** Pod affinity and anti-affinity rules */
  affinity?: object;
  /** Node taint tolerations */
  tolerations?: Array<object>;
  /** Kubernetes Service type (defaults to ClusterIP) */
  serviceType?: 'ClusterIP' | 'NodePort' | 'LoadBalancer';
  /** Custom annotations to apply to primary and replica services */
  serviceAnnotations?: Record<string, string>;
}

/**
 * Condition types for the FirebirdCluster status.
 */
export type ConditionType = 'Ready' | 'Progressing' | 'Degraded' | 'Paused' | 'Hibernated';
export type ConditionStatus = 'True' | 'False' | 'Unknown';

/**
 * A single status condition for the FirebirdCluster.
 */
export interface FirebirdClusterCondition {
  type: ConditionType;
  status: ConditionStatus;
  reason: string;
  message: string;
  lastTransitionTime: string;
}

/**
 * Replication status of the cluster.
 */
export interface ReplicationStatus {
  /** Pod name of current primary database instance */
  primaryPod?: string;
  /** Number of active replicating secondary instances */
  activeReplicas?: number;
  /** List of replica pod names operating in synchronous replication mode */
  syncReplicas?: string[];
  /** Pods currently selected by the read-only `-replica` Service */
  readRoutablePods?: string[];
  /** Replicas excluded from read-only routing because of excessive replication lag */
  laggingReplicas?: string[];
}

/**
 * Per-instance storage status used to track PVC volume expansion.
 */
export interface VolumeStatus {
  /** PersistentVolumeClaim name */
  name: string;
  /** Storage currently requested by the PVC */
  requestedSize?: string;
  /** Actual capacity reported by the bound volume */
  capacity?: string;
  /**
   * Volume state:
   * - Ready: capacity matches the desired size
   * - Resizing: expansion requested and in progress
   * - ResizeFailed: expansion was rejected (e.g. StorageClass without allowVolumeExpansion)
   * - ShrinkRejected: desired size is smaller than the current request (PVCs cannot shrink)
   */
  state: 'Ready' | 'Resizing' | 'ResizeFailed' | 'ShrinkRejected';
  /** Additional detail about the volume state */
  message?: string;
}

/**
 * Status of a FirebirdCluster resource.
 */
export interface FirebirdClusterStatus {
  /** Total number of instances */
  instances?: number;
  /** Number of ready instances */
  readyInstances?: number;
  /** Current phase of the cluster */
  phase?: 'Creating' | 'Running' | 'Updating' | 'Degraded' | 'Deleting' | 'Paused' | 'Hibernated';
  /** Human-readable message about current status */
  phaseReason?: string;
  /** List of status conditions */
  conditions?: FirebirdClusterCondition[];
  /** Detailed replication status details */
  replicationStatus?: ReplicationStatus;
  /** Hash digest of current superuser secret for password rotation tracking */
  superuserSecretHash?: string;
  /** Per-instance PVC storage and volume expansion status */
  volumes?: VolumeStatus[];
}

/**
 * FirebirdCluster is the Schema for the firebirdclusters API.
 */
export interface FirebirdCluster {
  apiVersion: 'firebird.cloudnative-firebird.io/v1';
  kind: 'FirebirdCluster';
  metadata: {
    name: string;
    namespace?: string;
    uid?: string;
    resourceVersion?: string;
    generation?: number;
    labels?: Record<string, string>;
    annotations?: Record<string, string>;
  };
  spec: FirebirdClusterSpec;
  status?: FirebirdClusterStatus;
}

/**
 * FirebirdClusterList is a list of FirebirdCluster resources.
 */
export interface FirebirdClusterList {
  apiVersion: 'firebird.cloudnative-firebird.io/v1';
  kind: 'FirebirdClusterList';
  metadata: {
    resourceVersion?: string;
  };
  items: FirebirdCluster[];
}

/** Default Firebird Docker image */
export const DEFAULT_FIREBIRD_IMAGE = 'firebirdsql/firebird:latest';

/** API group for the FirebirdCluster CRD */
export const API_GROUP = 'firebird.cloudnative-firebird.io';

/** API version for the FirebirdCluster CRD */
export const API_VERSION = 'v1';

/** Plural name of the FirebirdCluster resource */
export const RESOURCE_PLURAL = 'firebirdclusters';

/** Singular name of the FirebirdCluster resource */
export const RESOURCE_SINGULAR = 'firebirdcluster';

/** Kind of the FirebirdCluster resource */
export const RESOURCE_KIND = 'FirebirdCluster';
