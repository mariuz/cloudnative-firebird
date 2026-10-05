import { FirebirdCluster, FirebirdBackup, FirebirdRestore, PointInTimeRecovery, S3BackupConfiguration } from '../types';
import { parseQuantity } from './storage';

export class ValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ValidationError';
  }
}

/**
 * Validates the specification of a FirebirdCluster resource.
 * Throws a ValidationError if any quantitative boundaries or required fields are violated.
 */
export function validateClusterSpec(cluster: FirebirdCluster): void {
  const spec = cluster.spec;

  if (!spec) {
    throw new ValidationError('FirebirdCluster spec is required');
  }

  const managedSettings = Object.keys(spec.config?.settings ?? {}).filter((k) => k.toLowerCase() === 'securitydatabase');
  if (managedSettings.length > 0) {
    throw new ValidationError(
      'config.settings.SecurityDatabase is managed by the operator (the security database is kept on the instance volume)',
    );
  }

  if (
    typeof spec.instances !== 'number' ||
    !Number.isInteger(spec.instances) ||
    spec.instances < 1 ||
    spec.instances > 10
  ) {
    throw new ValidationError(
      `Invalid instances value: ${spec.instances}. Must be an integer between 1 and 10.`,
    );
  }

  if (!spec.storage || !spec.storage.size || spec.storage.size.trim() === '') {
    throw new ValidationError('Storage size is required (e.g. "1Gi")');
  }

  const storageBytes = parseQuantity(spec.storage.size);
  if (storageBytes === null || storageBytes <= 0) {
    throw new ValidationError(
      `Invalid storage size: "${spec.storage.size}". Must be a positive Kubernetes quantity (e.g. "10Gi").`,
    );
  }

  // Interpolated into file paths and maintenance shell commands
  if (spec.databaseName !== undefined && !/^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(spec.databaseName)) {
    throw new ValidationError(
      `Invalid databaseName: "${spec.databaseName}". Use a plain file name (letters, digits, ".", "_", "-").`,
    );
  }

  if (
    spec.superuserSecret &&
    (!spec.superuserSecret.name || spec.superuserSecret.name.trim() === '')
  ) {
    throw new ValidationError('superuserSecret name cannot be empty');
  }

  if (spec.serviceAccountName !== undefined && !/^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$/.test(spec.serviceAccountName)) {
    throw new ValidationError(`Invalid serviceAccountName: "${spec.serviceAccountName}". Must be a DNS subdomain name.`);
  }

  if (
    spec.serviceType &&
    !['ClusterIP', 'NodePort', 'LoadBalancer'].includes(spec.serviceType)
  ) {
    throw new ValidationError(
      `Invalid serviceType: ${spec.serviceType}. Must be ClusterIP, NodePort, or LoadBalancer.`,
    );
  }

  if (spec.primaryUpdateStrategy && !['unsupervised', 'supervised'].includes(spec.primaryUpdateStrategy)) {
    throw new ValidationError(
      `Invalid primaryUpdateStrategy: ${spec.primaryUpdateStrategy}. Must be unsupervised or supervised.`,
    );
  }
  if (spec.primaryUpdateMethod && !['restart', 'switchover'].includes(spec.primaryUpdateMethod)) {
    throw new ValidationError(`Invalid primaryUpdateMethod: ${spec.primaryUpdateMethod}. Must be restart or switchover.`);
  }

  if (spec.replication?.enabled) {
    if (spec.replication.mode && !['sync', 'async'].includes(spec.replication.mode)) {
      throw new ValidationError(
        `Invalid replication mode: ${spec.replication.mode}. Must be 'sync' or 'async'.`,
      );
    }
    if (spec.replication.mode === 'sync' && spec.instances < 2) {
      throw new ValidationError("Replication mode 'sync' needs at least 2 instances (a primary and a standby).");
    }
    const sync = spec.replication.synchronous;
    if (sync?.dataDurability !== undefined && !['required', 'preferred'].includes(sync.dataDurability)) {
      throw new ValidationError(
        `Invalid replication synchronous.dataDurability: ${sync.dataDurability}. Must be required or preferred.`,
      );
    }
    if (sync?.standbyUnavailableSeconds !== undefined && (!Number.isInteger(sync.standbyUnavailableSeconds) || sync.standbyUnavailableSeconds < 1)) {
      throw new ValidationError(
        `Invalid replication synchronous.standbyUnavailableSeconds: ${sync.standbyUnavailableSeconds}. Must be a positive integer.`,
      );
    }
    // Must live on the instance volume (shared with the replication sidecars) and is written
    // into replication.conf and shell environment
    if (
      spec.replication.journalDirectory !== undefined &&
      !/^\/var\/lib\/firebird\/data\/[A-Za-z0-9._/-]+$/.test(spec.replication.journalDirectory)
    ) {
      throw new ValidationError(
        `Invalid replication journalDirectory: "${spec.replication.journalDirectory}". ` +
          'It must be a path below /var/lib/firebird/data.',
      );
    }
    for (const [field, value] of [
      ['archiveTimeoutSeconds', spec.replication.archiveTimeoutSeconds],
      ['segmentRetentionHours', spec.replication.segmentRetentionHours],
      ['maxSegmentRetentionHours', spec.replication.maxSegmentRetentionHours],
      ['failover.delaySeconds', spec.replication.failover?.delaySeconds],
    ] as const) {
      if (value !== undefined && (!Number.isInteger(value) || value < 1)) {
        throw new ValidationError(`Invalid replication ${field}: ${value}. Must be a positive integer.`);
      }
    }
    const isolationTimeout = spec.replication.failover?.isolationCheck?.timeoutSeconds;
    if (
      isolationTimeout !== undefined &&
      (!Number.isInteger(isolationTimeout) || isolationTimeout < 5 || isolationTimeout > 3600)
    ) {
      throw new ValidationError(
        `Invalid replication failover.isolationCheck.timeoutSeconds: ${isolationTimeout}. Must be an integer from 5 to 3600.`,
      );
    }
    const routing = spec.replication.readOnlyRouting;
    if (
      routing?.maxLagSeconds !== undefined &&
      (typeof routing.maxLagSeconds !== 'number' ||
        !Number.isFinite(routing.maxLagSeconds) ||
        routing.maxLagSeconds < 0)
    ) {
      throw new ValidationError(
        `Invalid readOnlyRouting maxLagSeconds: ${routing.maxLagSeconds}. Must be a non-negative number.`,
      );
    }
    if (spec.replication.journalArchiveS3) {
      if (!spec.replication.journalArchiveS3.bucket || spec.replication.journalArchiveS3.bucket.trim() === '') {
        throw new ValidationError('Replication journalArchiveS3 bucket name is required');
      }
      if (spec.replication.journalArchiveS3.secretRef && (!spec.replication.journalArchiveS3.secretRef.name || spec.replication.journalArchiveS3.secretRef.name.trim() === '')) {
        throw new ValidationError('Replication journalArchiveS3 secretRef name is required');
      }
    }
  }

  if (spec.bootstrap?.clone && spec.bootstrap.recovery) {
    throw new ValidationError('Bootstrap recovery and clone are mutually exclusive');
  }

  if (spec.bootstrap?.clone) {
    if (!spec.bootstrap.clone.sourceCluster || spec.bootstrap.clone.sourceCluster.trim() === '') {
      throw new ValidationError('Bootstrap clone sourceCluster is required');
    }
    if (cluster.metadata?.name && spec.bootstrap.clone.sourceCluster === cluster.metadata.name) {
      throw new ValidationError('Bootstrap clone sourceCluster cannot be the cluster itself');
    }
  }

  if (spec.bootstrap?.recovery) {
    const { sourcePath, s3 } = spec.bootstrap.recovery;
    if ((!sourcePath || sourcePath.trim() === '') && !s3) {
      throw new ValidationError('Bootstrap recovery requires either sourcePath or s3 configuration');
    }
    if (s3) {
      if (!s3.bucket || s3.bucket.trim() === '') {
        throw new ValidationError('Bootstrap recovery S3 bucket is required');
      }
      if (s3.secretRef && (!s3.secretRef.name || s3.secretRef.name.trim() === '')) {
        throw new ValidationError('Bootstrap recovery S3 secretRef name is required');
      }
    }
    const { pointInTime, incrementalPaths } = spec.bootstrap.recovery;
    if (incrementalPaths?.length && !pointInTime) {
      throw new ValidationError('bootstrap.recovery.incrementalPaths applies to point-in-time recovery only');
    }
    if (pointInTime) {
      if (!s3 || !sourcePath) {
        throw new ValidationError('bootstrap.recovery.pointInTime needs s3 and sourcePath (an nbackup level 0 object key)');
      }
      if (!pointInTime.journalS3) {
        throw new ValidationError('bootstrap.recovery.pointInTime.journalS3 is required (the source cluster\'s journal archive)');
      }
      validatePointInTime('bootstrap.recovery.pointInTime', pointInTime);
      const own = spec.replication?.journalArchiveS3;
      if (own && sameS3Location(own, pointInTime.journalS3)) {
        throw new ValidationError(
          'bootstrap.recovery.pointInTime.journalS3 is this cluster\'s own replication.journalArchiveS3: ' +
            'the recovered cluster would archive its segments over the ones it recovers from',
        );
      }
      if (spec.instances > 1 && !spec.replication?.enabled) {
        throw new ValidationError(
          'bootstrap.recovery.pointInTime with more than one instance needs replication: only the first instance is recovered',
        );
      }
    }
  }

  if (spec.backup?.enabled) {
    if (spec.backup.type && !['logical', 'physical'].includes(spec.backup.type)) {
      throw new ValidationError(
        `Invalid backup type: ${spec.backup.type}. Must be 'logical' or 'physical'.`,
      );
    }
    if (
      spec.backup.level !== undefined &&
      ![0, 1, 2].includes(spec.backup.level)
    ) {
      throw new ValidationError(
        `Invalid physical backup level: ${spec.backup.level}. Must be 0, 1, or 2.`,
      );
    }
    if (spec.backup.schedule) {
      const parts = spec.backup.schedule.trim().split(/\s+/);
      if (parts.length !== 5) {
        throw new ValidationError(
          `Invalid backup schedule cron expression: "${spec.backup.schedule}". Standard 5-field cron expression required.`,
        );
      }
    }
    if (spec.backup.s3) {
      if (!spec.backup.s3.bucket || spec.backup.s3.bucket.trim() === '') {
        throw new ValidationError('S3 backup bucket name is required');
      }
      if (spec.backup.s3.secretRef && (!spec.backup.s3.secretRef.name || spec.backup.s3.secretRef.name.trim() === '')) {
        throw new ValidationError('S3 backup secretRef name is required');
      }
    }
    validateBackupDestination('spec.backup', spec.backup.type, spec.backup.verify);
    validateRetention('spec.backup', spec.backup.retentionPolicy);
    validateTarget('spec.backup', spec.backup.target);
  }

  if (spec.monitoring?.exporter?.enabled) {
    if (spec.monitoring.exporter.port !== undefined) {
      if (
        !Number.isInteger(spec.monitoring.exporter.port) ||
        spec.monitoring.exporter.port < 1024 ||
        spec.monitoring.exporter.port > 65535
      ) {
        throw new ValidationError(
          `Invalid exporter port: ${spec.monitoring.exporter.port}. Must be an integer between 1024 and 65535.`,
        );
      }
    }
  }

  if (spec.tls?.issuerRef && (!spec.tls.issuerRef.name || spec.tls.issuerRef.name.trim() === '')) {
    throw new ValidationError('TLS issuerRef name cannot be empty');
  }
  if (spec.tls?.enabled) {
    // tls.enabled is strict wire encryption: settings that weaken it contradict it
    const settings = spec.config?.settings ?? {};
    const wireCrypt = settings['WireCrypt'];
    if (wireCrypt !== undefined && wireCrypt.trim().toLowerCase() !== 'required') {
      throw new ValidationError(`tls.enabled requires WireCrypt = Required, but config.settings sets WireCrypt = ${wireCrypt}`);
    }
    const plugins = settings['WireCryptPlugin'];
    if (plugins !== undefined && plugins.split(',').some((p) => !['chacha', 'chacha64'].includes(p.trim().toLowerCase()))) {
      throw new ValidationError(`tls.enabled allows only the ChaCha64 and ChaCha wire encryption plugins, but config.settings sets WireCryptPlugin = ${plugins}`);
    }
  }

  if (spec.autoSweep?.enabled && spec.autoSweep.schedule) {
    const parts = spec.autoSweep.schedule.trim().split(/\s+/);
    if (parts.length !== 5) {
      throw new ValidationError(
        `Invalid autoSweep schedule cron expression: "${spec.autoSweep.schedule}". Standard 5-field cron expression required.`,
      );
    }
  }

  if (spec.diagnostics?.enabled && spec.diagnostics.schedule && spec.diagnostics.schedule.trim().split(/\s+/).length !== 5) {
    throw new ValidationError(
      `Invalid diagnostics schedule cron expression: "${spec.diagnostics.schedule}". Standard 5-field cron expression required.`,
    );
  }
}

/**
 * Physical (nbackup) backups are taken by the primary's server into its own data directory, so
 * they cannot be streamed to a Job for upload.
 */
function validateBackupDestination(field: string, type?: string, verify?: boolean): void {
  if (type === 'physical' && verify) {
    throw new ValidationError(`${field}: verify is supported for logical backups only`);
  }
}

/** Backup target: "primary" or "prefer-standby" */
function validateTarget(field: string, target?: string): void {
  if (target !== undefined && !['primary', 'prefer-standby'].includes(target)) {
    throw new ValidationError(`${field}.target "${target}" is invalid: use "primary" or "prefer-standby"`);
  }
}

/** retentionPolicy: "<n>d", "<n>w" or "<n>m" (CloudNativePG's format) */
function validateRetention(field: string, policy?: string): void {
  if (policy !== undefined && !/^[1-9][0-9]*[dwm]$/.test(policy)) {
    throw new ValidationError(`${field}.retentionPolicy "${policy}" is invalid: use <n>d, <n>w or <n>m (e.g. "30d")`);
  }
}

/** A bucket and credentials Secret are required for S3 sources and destinations */
function validateS3(field: string, s3?: S3BackupConfiguration): void {
  if (!s3) return;
  if (!s3.bucket || s3.bucket.trim() === '') throw new ValidationError(`${field}.s3.bucket is required`);
  if (s3.secretRef && (!s3.secretRef.name || s3.secretRef.name.trim() === '')) {
    throw new ValidationError(`${field}.s3.secretRef.name is required`);
  }
}

/** Database file names created on the primary: a plain file name */
const DATABASE_FILE_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]*$/;

/** Server-side backup paths: inside the data directory, no parent references */
function validateServerPath(field: string, path: string): void {
  if (path.split('/').includes('..')) throw new ValidationError(`${field} must not contain ".."`);
  if (path.startsWith('/') && !path.startsWith('/var/lib/firebird/data/')) {
    throw new ValidationError(`${field} must be inside /var/lib/firebird/data`);
  }
}

/**
 * Validates a FirebirdBackup custom resource specification.
 */
export function validateBackupSpec(backup: FirebirdBackup): void {
  if (!backup.spec?.clusterName || backup.spec.clusterName.trim() === '') {
    throw new ValidationError('FirebirdBackup clusterName is required');
  }
  if (backup.spec.type && !['logical', 'physical'].includes(backup.spec.type)) {
    throw new ValidationError(`Invalid backup type: ${backup.spec.type}. Must be 'logical' or 'physical'.`);
  }
  if (backup.spec.level !== undefined && ![0, 1, 2].includes(backup.spec.level)) {
    throw new ValidationError(`Invalid physical backup level: ${backup.spec.level}. Must be 0, 1, or 2.`);
  }
  validateS3('spec', backup.spec.s3);
  validateBackupDestination('spec', backup.spec.type, backup.spec.verify);
  validateTarget('spec', backup.spec.target);
}

/**
 * Validates a FirebirdRestore custom resource specification.
 */
export function validateRestoreSpec(restore: FirebirdRestore): void {
  if (!restore.spec?.clusterName || restore.spec.clusterName.trim() === '') {
    throw new ValidationError('FirebirdRestore clusterName is required');
  }
  const spec = restore.spec;
  if (spec.restoreType && !['logical', 'physical'].includes(spec.restoreType)) {
    throw new ValidationError(`Invalid restoreType: ${spec.restoreType}. Must be 'logical' or 'physical'.`);
  }
  if (!spec.backupName && !spec.backupPath) {
    throw new ValidationError('FirebirdRestore requires backupName or backupPath');
  }
  if (spec.backupName && spec.backupPath) {
    throw new ValidationError('FirebirdRestore backupName and backupPath are mutually exclusive');
  }
  if (spec.targetDatabase !== undefined && !DATABASE_FILE_PATTERN.test(spec.targetDatabase)) {
    throw new ValidationError(
      `Invalid targetDatabase "${spec.targetDatabase}": must be a file name (letters, digits, ".", "_", "-")`,
    );
  }
  validateS3('spec', spec.s3);
  if (spec.backupPath && !spec.s3) validateServerPath('backupPath', spec.backupPath);
  if (spec.incrementalBackupPaths?.length) {
    if (spec.restoreType !== 'physical' || spec.backupName) {
      throw new ValidationError('incrementalBackupPaths applies to physical restores from backupPath only');
    }
    // with s3 they are object keys relative to s3.prefix, like backupPath
    if (!spec.s3) spec.incrementalBackupPaths.forEach((p, i) => validateServerPath(`incrementalBackupPaths[${i}]`, p));
  }
  const pitr = spec.pointInTime;
  if (pitr) {
    if (spec.restoreType && spec.restoreType !== 'physical') {
      throw new ValidationError('pointInTime applies to physical restores only');
    }
    validatePointInTime('pointInTime', pitr);
  }
}

/** Validates a point-in-time recovery target and journal archive (`field` names it in messages) */
function validatePointInTime(field: string, pitr: PointInTimeRecovery): void {
  if (pitr.targetTime !== undefined && pitr.targetSegment !== undefined) {
    throw new ValidationError(`${field}.targetTime and ${field}.targetSegment are mutually exclusive`);
  }
  if (pitr.targetTime !== undefined && (!RFC3339.test(pitr.targetTime) || Number.isNaN(Date.parse(pitr.targetTime)))) {
    throw new ValidationError(`Invalid ${field}.targetTime "${pitr.targetTime}": must be an RFC 3339 time, e.g. 2026-10-01T10:15:00Z`);
  }
  if (pitr.targetSegment !== undefined && (!Number.isSafeInteger(pitr.targetSegment) || pitr.targetSegment < 1)) {
    throw new ValidationError(`${field}.targetSegment must be a positive integer`);
  }
  validateS3(`spec.${field}.journalS3`, pitr.journalS3);
}

/** Whether two S3 locations are the same bucket and prefix (on the same endpoint) */
function sameS3Location(a: S3BackupConfiguration, b: S3BackupConfiguration): boolean {
  const prefix = (s: S3BackupConfiguration) => (s.prefix ?? '').replace(/^\/+|\/+$/g, '');
  return a.bucket === b.bucket && prefix(a) === prefix(b) && (a.endpoint ?? '') === (b.endpoint ?? '');
}

/** RFC 3339 date-time (with a time zone offset), as accepted by pointInTime.targetTime */
const RFC3339 = /^\d{4}-\d{2}-\d{2}[Tt ]\d{2}:\d{2}:\d{2}(\.\d+)?([Zz]|[+-]\d{2}:\d{2})$/;

/**
 * Validates a FirebirdScheduledBackup custom resource specification.
 */
export function validateScheduledBackupSpec(scheduledBackup: {
  spec: {
    clusterName: string;
    schedule: string;
    type?: string;
    level?: number;
    s3?: S3BackupConfiguration;
    retentionPolicy?: string;
    target?: string;
    verify?: boolean;
  };
}): void {
  if (!scheduledBackup.spec?.clusterName || scheduledBackup.spec.clusterName.trim() === '') {
    throw new ValidationError('FirebirdScheduledBackup clusterName is required');
  }
  if (!scheduledBackup.spec?.schedule || scheduledBackup.spec.schedule.trim().split(/\s+/).length !== 5) {
    throw new ValidationError(`Invalid scheduled backup schedule cron expression: "${scheduledBackup.spec?.schedule}". Standard 5-field cron expression required.`);
  }
  if (scheduledBackup.spec.type && !['logical', 'physical'].includes(scheduledBackup.spec.type)) {
    throw new ValidationError(`Invalid backup type: ${scheduledBackup.spec.type}. Must be 'logical' or 'physical'.`);
  }
  if (scheduledBackup.spec.level !== undefined && ![0, 1, 2].includes(scheduledBackup.spec.level)) {
    throw new ValidationError(`Invalid physical backup level: ${scheduledBackup.spec.level}. Must be 0, 1, or 2.`);
  }
  validateS3('spec', scheduledBackup.spec.s3);
  validateBackupDestination('spec', scheduledBackup.spec.type, scheduledBackup.spec.verify);
  validateRetention('spec', scheduledBackup.spec.retentionPolicy);
  validateTarget('spec', scheduledBackup.spec.target);
}

