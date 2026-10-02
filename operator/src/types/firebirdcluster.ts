import type { PointInTimeRecovery } from './backup';
import type { V1PodSecurityContext, V1SecurityContext } from '@kubernetes/client-node';
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
  /**
   * Secret containing AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY. Without it the S3 client uses
   * the credentials of its environment, e.g. workload identity through spec.serviceAccountName
   * (EKS IRSA or Pod Identity) or the node's instance profile.
   */
  secretRef?: {
    name: string;
  };
  /** Object key prefix/folder inside bucket */
  prefix?: string;
  /** Image providing the `aws` CLI for uploads and downloads (default "amazon/aws-cli:2.37.4") */
  clientImage?: string;
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
  /**
   * How long scheduled backups are kept: "<n>d", "<n>w" or "<n>m" (30 days). Enforced for backups
   * to S3 and server-side backups (see
   * FirebirdScheduledBackupSpec.retentionPolicy).
   */
  retentionPolicy?: string;
  /** Upload backups to S3 instead of the primary's data directory */
  s3?: S3BackupConfiguration;
  /**
   * Where the backup runs: "primary" (default) or "prefer-standby", a ready replica when one
   * qualifies (backups to S3 only; server-side backups always run on the primary)
   */
  target?: 'primary' | 'prefer-standby';
  /**
   * Restore each backup into a scratch database and validate it (gbak -c, then a full
   * validation); the backup fails when it does not restore or validate. Logical backups only.
   */
  verify?: boolean;
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
  /** Database file name to check (defaults to spec.databaseName) */
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
   * Replication mode. Only 'async' (journal shipping) is supported; 'sync' is rejected
   * by validation until synchronous replication is implemented. Defaults to 'async'.
   */
  mode?: 'sync' | 'async';
  /**
   * Base directory for replication files on each instance's volume; journal, archive and
   * source directories are created below it (defaults to "/var/lib/firebird/data/replication")
   */
  journalDirectory?: string;
  /** Seconds after which a partially filled journal segment is archived and shipped (defaults to 10) */
  archiveTimeoutSeconds?: number;
  /** Hours archived segments are kept on the primary for replicas to fetch (defaults to 24) */
  segmentRetentionHours?: number;
  /**
   * Hours archived segments a replica has not applied yet are kept past segmentRetentionHours, so
   * a slow or stopped replica can catch up without being re-seeded (defaults to 168, at least
   * segmentRetentionHours)
   */
  maxSegmentRetentionHours?: number;
  /**
   * Delete archived segments on the primary as soon as every replica has applied them (and, with
   * journalArchiveS3, the archive Job has uploaded them), instead of keeping them for
   * segmentRetentionHours: the archive is bounded by the replicas' progress. New replicas are then
   * seeded from a replica rather than from the primary's offline bootstrap seed. Defaults to false.
   */
  pruneAppliedSegments?: boolean;
  /**
   * Allow seeding a new replica with a locked copy of the live primary when no ready replica
   * and no usable offline bootstrap seed exist (default true). The copy's in-flight
   * transactions, including those of the commit/TIP window (ISSUES.md, issue 2), are replayed
   * on the new replica. false never locks the primary: a new replica then waits for a ready
   * replica or a fresh offline seed (the primary's next clean restart).
   */
  allowLiveSeedFromPrimary?: boolean;
  /**
   * Automatic failover: when the primary pod has not been ready for delaySeconds, the most
   * advanced ready replica is promoted and the old primary is re-seeded when it returns. Replication
   * is asynchronous: transactions the replicas had not received are lost.
   */
  failover?: FailoverConfiguration;
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
  /** Database file name to sweep (defaults to spec.databaseName) */
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
  /**
   * gbak backup to restore: the object key relative to s3.prefix (default "backup.fbk") when s3
   * is set, otherwise a file path readable from the init container (e.g. baked into imageName)
   */
  sourcePath?: string;
  /** S3 cloud storage source configuration */
  s3?: S3BackupConfiguration;
  /**
   * Point-in-time recovery: sourcePath is an nbackup level 0 object key (with
   * incrementalPaths on top), and the journal archive in pointInTime.journalS3 (required) is
   * replayed on it up to the target. A recovery Job prepares the first instance's volume
   * before the instances start.
   */
  pointInTime?: PointInTimeRecovery;
  /** nbackup level 1 and 2 object keys applied on top of sourcePath (pointInTime only) */
  incrementalPaths?: string[];
}

/**
 * Target source cluster configuration for cluster-to-cluster cloning.
 */
export interface CloneConfiguration {
  /** Name of the source FirebirdCluster to clone from */
  sourceCluster: string;
  /** Namespace of the source FirebirdCluster (defaults to same namespace as target cluster) */
  namespace?: string;
  /** Database file name in the source cluster (defaults to this cluster's databaseName) */
  databaseName?: string;
  /**
   * Secret (key "password") in this namespace holding the source cluster's SYSDBA password.
   * Defaults to this cluster's superuserSecret.
   */
  superuserSecret?: {
    name: string;
  };
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
   * Defaults to the operator's FIREBIRD_DEFAULT_IMAGE, or firebirdsql/firebird:latest
   */
  imageName?: string;
  /** Reference to the Secret containing the superuser password (SYSDBA) */
  superuserSecret?: SuperuserSecretRef;
  /** Database file created in each instance's data directory (defaults to "mydb.fdb") */
  databaseName?: string;
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
  /**
   * How the primary is updated during a rolling update with replication (CloudNativePG):
   * "unsupervised" (default) updates it automatically after the replicas; "supervised" waits
   * for a switchover (targetPrimary annotation) or a manual restart of the primary.
   */
  primaryUpdateStrategy?: 'unsupervised' | 'supervised';
  /**
   * How an unsupervised rolling update updates the primary: "restart" (default) restarts it in
   * place, "switchover" promotes an updated replica first.
   */
  primaryUpdateMethod?: 'restart' | 'switchover';
  /** Additional environment variables to pass to the Firebird container */
  env?: Array<{ name: string; value?: string; valueFrom?: object }>;
  /** Node labels required for pod scheduling */
  nodeSelector?: Record<string, string>;
  /** Pod affinity and anti-affinity rules */
  affinity?: object;
  /** Node taint tolerations */
  tolerations?: Array<object>;
  /**
   * Existing ServiceAccount for the instance pods and every Job of the cluster (backups,
   * restores, journal archiving, maintenance), e.g. for S3 access through workload identity
   * instead of static keys (CloudNativePG 1.29 serviceAccountName). Defaults to the namespace's
   * default ServiceAccount.
   */
  serviceAccountName?: string;
  /**
   * Pod security context of the instance pods (CloudNativePG 1.28 podSecurityContext), merged
   * over the defaults: fsGroup 999, seccomp RuntimeDefault
   */
  podSecurityContext?: V1PodSecurityContext;
  /**
   * Security context of every instance container (CloudNativePG 1.28 securityContext), merged
   * over the defaults: no privilege escalation, all capabilities dropped except CHOWN,
   * DAC_OVERRIDE and FOWNER (the official image runs the server as root)
   */
  securityContext?: V1SecurityContext;
  /** Kubernetes Service type (defaults to ClusterIP) */
  serviceType?: 'ClusterIP' | 'NodePort' | 'LoadBalancer';
  /** Custom annotations to apply to primary and replica services */
  serviceAnnotations?: Record<string, string>;
}

/**
 * Condition types for the FirebirdCluster status.
 */
export type ConditionType = 'Ready' | 'Progressing' | 'Degraded' | 'Paused' | 'Hibernated' | 'Fenced';
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
  /** Pods currently selected by the read-only `-replica` Service */
  readRoutablePods?: string[];
  /** Replicas excluded from read-only routing because of excessive replication lag */
  laggingReplicas?: string[];
  /** Last journal segment archived on the primary */
  lastArchivedSequence?: number;
  /** Replication position and lag of each replica, measured from the segment servers */
  replicas?: ReplicaLagStatus[];
  /** Archived segments the primary keeps for the replicas past segmentRetentionHours */
  segmentRetention?: SegmentRetentionStatus;
}

/** Segments kept on the primary for replicas that have not applied them */
export interface SegmentRetentionStatus {
  /** Lowest segment applied by a replica: the primary keeps every archived segment after it */
  floorSequence?: number;
  /** Last applied segment known for each replica, including replicas not ready now */
  replicas?: Array<{ name: string; appliedSequence: number }>;
}

/** A replica whose volume is being re-created */
export interface VolumeRecreationStatus {
  pod: string;
  /** UID of the claim being replaced: the request is done once a new claim serves a ready pod */
  claimUid: string;
}

/** Replication progress of one replica */
export interface ReplicaLagStatus {
  name: string;
  /** Journal segment applied by the replica (its replica control file) */
  appliedSequence?: number;
  /** Segments received but not applied yet */
  pendingSegments?: number;
  /** Segments archived on the primary after the applied one */
  lagSegments?: number;
  /** Age of the oldest archived segment not applied yet (0 when up to date) */
  lagSeconds?: number;
  /** Why the position could not be measured */
  error?: string;
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
  /**
   * Instances whose database is fenced (in full shutdown), as applied from the
   * fencedInstances annotation
   */
  fencedInstances?: string[];
  /** Since when the primary pod has not been ready (automatic failover) */
  primaryNotReadySince?: string;
  /** Planned switchover or failover in progress, or the last one */
  switchover?: SwitchoverStatus;
  /** Replicas being re-seeded (reseed annotation) */
  reseedingInstances?: string[];
  /** Replicas whose volume is being re-created (reseed=volume annotation) */
  recreatingVolumes?: VolumeRecreationStatus[];
  /** Label selector of the instance pods, for the scale subresource (HPA / VPA) */
  selector?: string;
  /** Rolling update in progress (replication clusters, primary last) */
  rollingUpdate?: RollingUpdateStatus;
}

/** Progress of a rolling update of the instance pods */
export interface RollingUpdateStatus {
  /** StatefulSet revision the instances are updated to */
  revision: string;
  /** Instances still running an older revision */
  outdatedInstances: string[];
  message: string;
  /** The primary pod restarted by the update (automatic failover waits for it to return) */
  primaryRestart?: { pod: string; uid: string; time: string };
}

/** Automatic failover settings */
export interface FailoverConfiguration {
  /** Whether the operator promotes a replica when the primary is unavailable (default false) */
  enabled?: boolean;
  /** How long the primary must be unavailable before a failover starts (default 30) */
  delaySeconds?: number;
}

/** State of a planned switchover */
export interface SwitchoverStatus {
  /** "switchover" (targetPrimary annotation) or "failover" (primary unavailable) */
  kind?: 'switchover' | 'failover';
  /** Instance being promoted (empty while a failover elects it) */
  target: string;
  /** Primary being demoted */
  from: string;
  phase: 'Electing' | 'Stopping' | 'Promoting' | 'Completed' | 'Failed';
  message?: string;
  startTime?: string;
  completionTime?: string;
  /** Pod UIDs of the target and the old primary when they were restarted (directive tokens) */
  targetToken?: string;
  fromToken?: string;
  /** Replicas that were not ready when the primary moved, with their pod UIDs: re-seeded */
  reseed?: Record<string, string>;
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

/**
 * Firebird image of clusters without spec.imageName: the operator's FIREBIRD_DEFAULT_IMAGE
 * environment variable (e.g. a mirror in a private registry, or another Firebird version), or
 * firebirdsql/firebird:latest. Changing it updates those clusters like any image change.
 */
export const DEFAULT_FIREBIRD_IMAGE = process.env.FIREBIRD_DEFAULT_IMAGE?.trim() || 'firebirdsql/firebird:latest';

/** API group for the FirebirdCluster CRD */
export const API_GROUP = 'firebird.cloudnative-firebird.io';

/**
 * Pauses the reconciliation of one FirebirdBackup, FirebirdScheduledBackup, FirebirdRestore or
 * FirebirdUser when set to "true" (CloudNativePG 1.29 cnpg.io/reconciliationDisabled): the
 * operator leaves the resource, its status and its Jobs / CronJob alone until it is removed.
 */
export const RECONCILIATION_DISABLED_ANNOTATION = `${API_GROUP}/reconciliationDisabled`;

/** Whether reconciliation is paused for the object */
export function reconciliationDisabled(obj: { metadata?: { annotations?: Record<string, string> } }): boolean {
  return obj.metadata?.annotations?.[RECONCILIATION_DISABLED_ANNOTATION]?.trim().toLowerCase() === 'true';
}

/** API version for the FirebirdCluster CRD */
export const API_VERSION = 'v1';

/** Plural name of the FirebirdCluster resource */
export const RESOURCE_PLURAL = 'firebirdclusters';

/** Singular name of the FirebirdCluster resource */
export const RESOURCE_SINGULAR = 'firebirdcluster';

/** Kind of the FirebirdCluster resource */
export const RESOURCE_KIND = 'FirebirdCluster';
