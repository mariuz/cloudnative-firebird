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
 * Backup configuration for a Firebird cluster.
 */
export interface BackupConfiguration {
  /** Whether backups are enabled */
  enabled: boolean;
  /** Cron schedule for backups (e.g. "0 2 * * *") */
  schedule?: string;
  /** Backup retention policy (e.g. "7d", "30d") */
  retentionPolicy?: string;
}

/**
 * Monitoring configuration for a Firebird cluster.
 */
export interface MonitoringConfiguration {
  /** Whether to enable Prometheus metrics via PodMonitor */
  enablePodMonitor?: boolean;
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
  /** Resource requirements for each Firebird instance */
  resources?: ResourceRequirements;
  /** Backup configuration */
  backup?: BackupConfiguration;
  /** Monitoring configuration */
  monitoring?: MonitoringConfiguration;
  /** Additional environment variables to pass to the Firebird container */
  env?: Array<{ name: string; value?: string; valueFrom?: object }>;
}

/**
 * Condition types for the FirebirdCluster status.
 */
export type ConditionType = 'Ready' | 'Progressing' | 'Degraded';
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
 * Status of a FirebirdCluster resource.
 */
export interface FirebirdClusterStatus {
  /** Total number of instances */
  instances?: number;
  /** Number of ready instances */
  readyInstances?: number;
  /** Current phase of the cluster */
  phase?: 'Creating' | 'Running' | 'Updating' | 'Degraded' | 'Deleting';
  /** Human-readable message about current status */
  phaseReason?: string;
  /** List of status conditions */
  conditions?: FirebirdClusterCondition[];
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
