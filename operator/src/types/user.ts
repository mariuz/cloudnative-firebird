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
  /** Secret holding the password; exactly one of passwordSecret and password */
  passwordSecret?: {
    name: string;
    /** Key in the Secret (default "password") */
    key?: string;
  };
  /** A password the operator generates into a Secret it owns (CloudNativePG 1.31's password stanza) */
  password?: FirebirdUserPasswordSpec;
  /** Whether the user can log in (default true) */
  active?: boolean;
  /** Grant the RDB$ADMIN role in the security database (user management rights) */
  admin?: boolean;
  /**
   * Roles granted to the user in the cluster database: regular identifiers, or names in double
   * quotes ('"Sales Team"') for roles with delimited names. Roles must exist; roles granted
   * earlier and no longer listed are revoked.
   */
  roles?: string[];
  /**
   * What happens to the Firebird user when this resource is deleted: "retain" (default) keeps
   * it, "delete" drops it and revokes its privileges.
   */
  reclaimPolicy?: 'retain' | 'delete';
}

/** A generated password: kept in a kubernetes.io/basic-auth Secret owned by the FirebirdUser */
export interface FirebirdUserPasswordSpec {
  /** "generate": the only mode */
  mode: 'generate';
  /** Name of the Secret (default "<resource name>-password"); one the operator does not own is never overwritten */
  secretName?: string;
  /** Number of characters, letters and digits (default 32, 16 to 128) */
  length?: number;
  /**
   * Lifetime of a password ("2160h", "90m", "1h30m"): the operator generates a new one
   * renewBefore ahead of the expiration. Unset: generated once and kept.
   */
  duration?: string;
  /** How long before the expiration the password is renewed (default a third of duration) */
  renewBefore?: string;
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
  /** The generated password (spec.password): its Secret, when it was generated and when it expires */
  password?: { secretName: string; generatedTime?: string; expiration?: string };
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
