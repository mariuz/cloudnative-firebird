/**
 * Specification of a FirebirdDatabase: a database file of a cluster managed as its own Kubernetes
 * object, following CloudNativePG's Database resource. Clusters without replication only: the
 * operator replicates exactly one database per cluster (spec.databaseName).
 */
export interface FirebirdDatabaseSpec {
  /** Name of the FirebirdCluster (immutable) */
  clusterName: string;
  /**
   * Database file in each instance's data directory (immutable). Defaults to "<resource name>.fdb".
   * It cannot be the cluster database (spec.databaseName) or a file another FirebirdDatabase of
   * the cluster manages.
   */
  databaseName?: string;
  /** Page size of the new database (default: the engine's, 8192 since Firebird 4) (immutable) */
  pageSize?: 4096 | 8192 | 16384 | 32768;
  /** Default character set of the new database (default UTF8) (immutable) */
  characterSet?: string;
  /** SQL run once, right after the database is created on an instance (not on an adopted one) */
  initSql?: string;
  /**
   * What happens to the database files when this resource is deleted: "retain" (default) keeps
   * them, "delete" drops the database on every instance (DROP DATABASE).
   */
  reclaimPolicy?: 'retain' | 'delete';
}

/** An instance the database exists on */
export interface FirebirdDatabaseInstanceStatus {
  /** Instance (pod) name */
  name: string;
  /** UID of the instance volume the database was created (or found) on: a new volume gets it again */
  volume: string;
}

export interface FirebirdDatabaseStatus {
  phase?: 'Pending' | 'Creating' | 'Ready' | 'Failed' | 'Dropping';
  /** Human-readable detail */
  message?: string;
  /** Path of the database on the instances (clients connect to <service>:<path>) */
  path?: string;
  /** Instances the database exists on */
  instances?: FirebirdDatabaseInstanceStatus[];
  /** Job creating or dropping the database */
  jobName?: string;
  /** When the last Job failed (retried after a delay) */
  lastFailureTime?: string;
  /** Instances (and volumes) the database has been dropped from while the resource is deleted */
  droppedFrom?: FirebirdDatabaseInstanceStatus[];
}

export interface FirebirdDatabase {
  apiVersion: 'firebird.cloudnative-firebird.io/v1';
  kind: 'FirebirdDatabase';
  metadata: {
    name: string;
    namespace?: string;
    uid?: string;
    resourceVersion?: string;
    generation?: number;
    labels?: Record<string, string>;
    annotations?: Record<string, string>;
    finalizers?: string[];
    creationTimestamp?: string | Date;
    deletionTimestamp?: string | Date;
  };
  spec: FirebirdDatabaseSpec;
  status?: FirebirdDatabaseStatus;
}
