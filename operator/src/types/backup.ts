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
  /**
   * Upload the backup to S3 instead of keeping it in the primary's data directory.
   * Logical backups only.
   */
  s3?: S3BackupConfiguration;
  /**
   * Where the backup runs: "primary" (default) or "prefer-standby", a ready replica when one
   * qualifies (logical backups to S3 only; others always run on the primary)
   */
  target?: 'primary' | 'prefer-standby';
  /**
   * Restore each backup into a scratch database and validate it (gbak -c, then a full
   * validation); the backup fails when it does not restore or validate. Logical backups only.
   */
  verify?: boolean;
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
  /** Backup file name (in the primary's data directory, or the object key relative to s3.prefix) */
  backupFileName?: string;
  /** Full location of the backup: a path on the primary or an s3:// URI */
  location?: string;
  /** Job taking the backup */
  jobName?: string;
  /** Instance the backup was taken from */
  instance?: string;
  /** The backup was restored into a scratch database and validated (spec.verify) */
  verified?: boolean;
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
  /**
   * Backup file to restore from, when backupName is not set: a path in the primary's data
   * directory (relative, or absolute within it), or the object key relative to s3.prefix
   */
  backupPath?: string;
  /** nbackup level 1 and 2 files applied on top of backupPath by a physical restore (paths like backupPath, or object keys with s3) */
  incrementalBackupPaths?: string[];
  /**
   * Restore strategy:
   * - 'logical': uses gbak tool
   * - 'physical': uses nbackup tool
   */
  restoreType?: 'logical' | 'physical';
  /**
   * New database file created in the primary's data directory (defaults to
   * "restore-<name>.fdb"). It must not exist yet and cannot be the cluster database.
   */
  targetDatabase?: string;
  /** S3 source: backupPath (and incrementalBackupPaths) are object keys relative to s3.prefix */
  s3?: S3BackupConfiguration;
}

/**
  * Status of a FirebirdRestore resource.
  */
export interface FirebirdRestoreStatus {
  /** Phase of the restore execution */
  phase?: 'Pending' | 'Restoring' | 'Completed' | 'Failed';
  /** Timestamp when restore started */
  startTime?: string;
  /** Path of the restored database on the primary */
  targetPath?: string;
  /** Job running the restore */
  jobName?: string;
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

/**
 * Specification for a FirebirdScheduledBackup resource.
 */
export interface FirebirdScheduledBackupSpec {
  /** Target FirebirdCluster name */
  clusterName: string;
  /** Cron schedule expression (e.g. "0 2 * * *") */
  schedule: string;
  /** Whether scheduled backup execution is suspended */
  suspend?: boolean;
  /** Backup strategy ('logical' or 'physical') */
  type?: 'logical' | 'physical';
  /** Physical backup level (0, 1, 2) */
  level?: 0 | 1 | 2;
  /**
   * How long backups of this schedule are kept: "<n>d", "<n>w" or "<n>m" (30 days). Enforced for
   * backups to S3 and server-side backups: each run deletes the
   * schedule's backups older than that, always keeping the newest; an nbackup file is kept while
   * a kept backup's chain (from the primary's backup history) needs it.
   */
  retentionPolicy?: string;
  /** Upload backups to S3 instead of the primary's data directory */
  s3?: S3BackupConfiguration;
  /**
   * Where the backup runs: "primary" (default) or "prefer-standby", a ready replica when one
   * qualifies (logical backups to S3 only; others always run on the primary)
   */
  target?: 'primary' | 'prefer-standby';
  /**
   * Restore each backup into a scratch database and validate it (gbak -c, then a full
   * validation); the backup fails when it does not restore or validate. Logical backups only.
   */
  verify?: boolean;
}

/**
 * Status of a FirebirdScheduledBackup resource.
 */
export interface FirebirdScheduledBackupStatus {
  /** Timestamp of the last scheduled backup execution (from the CronJob) */
  lastScheduleTime?: string;
  /** Timestamp when the last backup completed successfully (from the CronJob) */
  lastSuccessfulTime?: string;
  /** Name of the CronJob running the backups */
  cronJobName?: string;
}

/**
 * FirebirdScheduledBackup Custom Resource.
 */
export interface FirebirdScheduledBackup {
  apiVersion: 'firebird.cloudnative-firebird.io/v1';
  kind: 'FirebirdScheduledBackup';
  metadata: {
    name: string;
    namespace?: string;
    uid?: string;
    resourceVersion?: string;
    generation?: number;
    labels?: Record<string, string>;
    annotations?: Record<string, string>;
  };
  spec: FirebirdScheduledBackupSpec;
  status?: FirebirdScheduledBackupStatus;
}
