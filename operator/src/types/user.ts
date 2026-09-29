/**
 * Specification of a FirebirdUser: a Firebird login managed as its own Kubernetes object,
 * following CloudNativePG's DatabaseRole.
 */
export interface FirebirdUserSpec {
  /** Name of the FirebirdCluster (immutable) */
  clusterName: string;
  /**
   * Firebird user name, a regular identifier (stored in upper case). Defaults to the resource
   * name with "-" replaced by "_".
   */
  username?: string;
  /** Secret holding the password */
  passwordSecret: {
    name: string;
    /** Key in the Secret (default "password") */
    key?: string;
  };
  /** Whether the user can log in (default true) */
  active?: boolean;
  /** Grant the RDB$ADMIN role in the security database (user management rights) */
  admin?: boolean;
  /**
   * Roles granted to the user in the cluster database. Roles must exist; roles granted
   * earlier and no longer listed are revoked.
   */
  roles?: string[];
  /**
   * What happens to the Firebird user when this resource is deleted: "retain" (default) keeps
   * it, "delete" drops it and revokes its privileges.
   */
  reclaimPolicy?: 'retain' | 'delete';
}

/** User state applied to one instance */
export interface FirebirdUserInstanceStatus {
  /** Instance (pod) name */
  name: string;
  /** Hash of the applied spec and Secret version */
  hash: string;
  /** UID of the instance volume the user was applied to (a new volume is applied again) */
  volume: string;
}

export interface FirebirdUserStatus {
  phase?: 'Pending' | 'Applying' | 'Applied' | 'Failed' | 'Dropping';
  /** Human-readable detail */
  message?: string;
  /** Firebird user name */
  username?: string;
  /** Instances the current spec has been applied to */
  instances?: FirebirdUserInstanceStatus[];
  /** Job applying pending changes */
  jobName?: string;
  /** When the last Job failed (a failed spec is retried after a delay) */
  lastFailureTime?: string;
  /** Hash of the spec whose Job failed last */
  failedHash?: string;
  /** Instances (and volumes) the user has been dropped from while the resource is deleted */
  droppedFrom?: Array<{ name: string; volume: string }>;
}

export interface FirebirdUser {
  apiVersion: 'firebird.cloudnative-firebird.io/v1';
  kind: 'FirebirdUser';
  metadata: {
    name: string;
    namespace?: string;
    uid?: string;
    resourceVersion?: string;
    generation?: number;
    labels?: Record<string, string>;
    annotations?: Record<string, string>;
    finalizers?: string[];
    deletionTimestamp?: string | Date;
  };
  spec: FirebirdUserSpec;
  status?: FirebirdUserStatus;
}
