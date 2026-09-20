import { FirebirdCluster, FirebirdBackup, FirebirdRestore } from '../types';

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

  if (
    spec.superuserSecret &&
    (!spec.superuserSecret.name || spec.superuserSecret.name.trim() === '')
  ) {
    throw new ValidationError('superuserSecret name cannot be empty');
  }

  if (
    spec.serviceType &&
    !['ClusterIP', 'NodePort', 'LoadBalancer'].includes(spec.serviceType)
  ) {
    throw new ValidationError(
      `Invalid serviceType: ${spec.serviceType}. Must be ClusterIP, NodePort, or LoadBalancer.`,
    );
  }

  if (
    spec.replication?.enabled &&
    spec.replication.mode &&
    !['sync', 'async'].includes(spec.replication.mode)
  ) {
    throw new ValidationError(
      `Invalid replication mode: ${spec.replication.mode}. Must be 'sync' or 'async'.`,
    );
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
      if (!spec.backup.s3.secretRef?.name || spec.backup.s3.secretRef.name.trim() === '') {
        throw new ValidationError('S3 backup secretRef name is required');
      }
    }
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

  if (spec.tls?.enabled) {
    if (spec.tls.issuerRef && (!spec.tls.issuerRef.name || spec.tls.issuerRef.name.trim() === '')) {
      throw new ValidationError('TLS issuerRef name cannot be empty');
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
}

/**
 * Validates a FirebirdRestore custom resource specification.
 */
export function validateRestoreSpec(restore: FirebirdRestore): void {
  if (!restore.spec?.clusterName || restore.spec.clusterName.trim() === '') {
    throw new ValidationError('FirebirdRestore clusterName is required');
  }
  if (restore.spec.restoreType && !['logical', 'physical'].includes(restore.spec.restoreType)) {
    throw new ValidationError(`Invalid restoreType: ${restore.spec.restoreType}. Must be 'logical' or 'physical'.`);
  }
}

