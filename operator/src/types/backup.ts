import { S3BackupConfiguration } from './firebirdcluster';

/**
  * Specification for an on-demand FirebirdBackup resource.
  */
export interface FirebirdBackupSpec {
  /** Name of the target FirebirdCluster */
  clusterName: string;
  /**
   * Backup strategy:
   * - 'logical': uses gbak tool (default)
   * - 'physical': uses nbackup tool
   */
  type?: 'logical' | 'physical';
  /**
   * nbackup level (for physical backups):
   * 0: Full physical base backup
   * 1, 2: Incremental physical backup
   */
  level?: 0 | 1 | 2;
  /** Optional S3 cloud storage export configuration */
  s3?: S3BackupConfiguration;
}

/**
  * Status of a FirebirdBackup resource.
  */
export interface FirebirdBackupStatus {
  /** Phase of the backup execution */
  phase?: 'Pending' | 'Running' | 'Completed' | 'Failed';
  /** Timestamp when backup execution started */
  startTime?: string;
  /** Timestamp when backup execution completed */
  completionTime?: string;
  /** Resulting backup file path/name */
  backupFileName?: string;
  /** Error message if backup failed */
  error?: string;
}

/**
  * FirebirdBackup Custom Resource.
  */
export interface FirebirdBackup {
  apiVersion: 'firebird.cloudnative-firebird.io/v1';
  kind: 'FirebirdBackup';
  metadata: {
    name: string;
    namespace?: string;
    uid?: string;
    resourceVersion?: string;
    generation?: number;
    labels?: Record<string, string>;
    annotations?: Record<string, string>;
  };
  spec: FirebirdBackupSpec;
  status?: FirebirdBackupStatus;
}

/**
  * Specification for a FirebirdRestore resource.
  */
export interface FirebirdRestoreSpec {
  /** Name of the target FirebirdCluster to restore into */
  clusterName: string;
  /** Name of a FirebirdBackup custom resource to restore from */
  backupName?: string;
  /** Direct backup file path or S3 key to restore from */
  backupPath?: string;
  /**
   * Restore strategy:
   * - 'logical': uses gbak tool
   * - 'physical': uses nbackup tool
   */
  restoreType?: 'logical' | 'physical';
  /** Target database file name (defaults to "mydb.fdb") */
  targetDatabase?: string;
  /** Optional S3 source configuration */
  s3?: S3BackupConfiguration;
}

/**
  * Status of a FirebirdRestore resource.
  */
export interface FirebirdRestoreStatus {
  /** Phase of the restore execution */
  phase?: 'Pending' | 'Restoring' | 'Completed' | 'Failed';
  /** Timestamp when restore completed */
  completionTime?: string;
  /** Error message if restore failed */
  error?: string;
}

/**
  * FirebirdRestore Custom Resource.
  */
export interface FirebirdRestore {
  apiVersion: 'firebird.cloudnative-firebird.io/v1';
  kind: 'FirebirdRestore';
  metadata: {
    name: string;
    namespace?: string;
    uid?: string;
    resourceVersion?: string;
    generation?: number;
    labels?: Record<string, string>;
    annotations?: Record<string, string>;
  };
  spec: FirebirdRestoreSpec;
  status?: FirebirdRestoreStatus;
}
