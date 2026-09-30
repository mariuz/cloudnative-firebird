/** Database object kinds a FirebirdRole can hold privileges on */
export type FirebirdRoleObjectKind = 'table' | 'view' | 'procedure' | 'function' | 'package' | 'sequence' | 'exception';

/** Privileges on one database object */
export interface FirebirdRolePrivilege {
  /**
   * Privileges: SELECT, INSERT, UPDATE, DELETE, REFERENCES or ALL on tables and views; EXECUTE on
   * procedures, functions and packages; USAGE on sequences and exceptions
   */
  privileges: string[];
  /**
   * The object: its name is a regular identifier (stored in upper case), or with quoted the exact,
   * case-sensitive name of an object created with a delimited identifier (CREATE TABLE "Orders")
   */
  object: {
    kind: FirebirdRoleObjectKind;
    name: string;
    /** Use the name as a delimited identifier (case-sensitive; spaces, punctuation and reserved words allowed) */
    quoted?: boolean;
  };
}

/**
 * Specification of a FirebirdRole: a role of the cluster database and exactly the privileges
 * it holds (after CloudNativePG's declarative database objects).
 */
export interface FirebirdRoleSpec {
  /** Name of the FirebirdCluster (immutable) */
  clusterName: string;
  /**
   * Role name, a regular identifier (stored in upper case). Defaults to the resource name with
   * "-" replaced by "_".
   */
  roleName?: string;
  /**
   * Privileges held by the role. Declarative: privileges the role holds that are not listed
   * (granted earlier, or by hand) are revoked. Memberships of users in the role are kept.
   */
  privileges?: FirebirdRolePrivilege[];
  /**
   * What happens to the role when this resource is deleted: "retain" (default) keeps it,
   * "delete" drops it (and with it the privileges and memberships).
   */
  reclaimPolicy?: 'retain' | 'delete';
}

/** Role state applied to one instance's database (clusters without replication) */
export interface FirebirdRoleInstanceStatus {
  name: string;
  hash: string;
  /** UID of the instance volume (a new volume is applied again) */
  volume: string;
}

export interface FirebirdRoleStatus {
  phase?: 'Pending' | 'Applying' | 'Applied' | 'Failed' | 'Dropping';
  message?: string;
  /** Firebird role name */
  roleName?: string;
  /**
   * With replication: hash of the spec applied on the primary (the database, roles included,
   * replicates). Without replication, see instances.
   */
  appliedHash?: string;
  /** Without replication: the spec applied to each instance's database */
  instances?: FirebirdRoleInstanceStatus[];
  /** Job applying pending changes */
  jobName?: string;
  /** When the last Job failed (a failed spec is retried after a delay) */
  lastFailureTime?: string;
  /** Hash of the spec whose Job failed last */
  failedHash?: string;
}

export interface FirebirdRole {
  apiVersion: 'firebird.cloudnative-firebird.io/v1';
  kind: 'FirebirdRole';
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
  spec: FirebirdRoleSpec;
  status?: FirebirdRoleStatus;
}
