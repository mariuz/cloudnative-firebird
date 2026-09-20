import { BatchV1Api, CustomObjectsApi, KubeConfig } from '@kubernetes/client-node';
import { logger } from '../utils/logger';
import { buildBackupJob, buildRestoreJob } from '../utils/resources';
import { validateBackupSpec, validateRestoreSpec } from '../utils/validation';
import { API_GROUP, API_VERSION, FirebirdCluster, FirebirdBackup, FirebirdRestore } from '../types';

export class FirebirdBackupController {
  private readonly batchApi: BatchV1Api;
  private readonly customApi: CustomObjectsApi;

  constructor(kubeConfig: KubeConfig) {
    this.batchApi = kubeConfig.makeApiClient(BatchV1Api);
    this.customApi = kubeConfig.makeApiClient(CustomObjectsApi);
  }

  /**
   * Reconcile a FirebirdBackup resource.
   */
  async reconcileBackup(backup: FirebirdBackup): Promise<void> {
    const { name, namespace = 'default' } = backup.metadata;
    const log = logger.child({ backup: name, namespace });

    log.info('Reconciling FirebirdBackup');

    try {
      validateBackupSpec(backup);

      if (backup.status?.phase === 'Completed' || backup.status?.phase === 'Failed') {
        log.debug({ phase: backup.status.phase }, 'FirebirdBackup is already in terminal state');
        return;
      }

      // Fetch the referenced cluster
      const clusterObj = await this.customApi.getNamespacedCustomObject({
        group: API_GROUP,
        version: API_VERSION,
        namespace,
        plural: 'firebirdclusters',
        name: backup.spec.clusterName,
      });

      const cluster = clusterObj as FirebirdCluster;
      const job = buildBackupJob(backup, cluster);

      try {
        await this.batchApi.readNamespacedJob({ name: job.metadata!.name!, namespace });
        log.debug('Backup job already exists');
      } catch {
        log.info({ jobName: job.metadata!.name }, 'Creating backup job');
        await this.batchApi.createNamespacedJob({ namespace, body: job });
      }

      await this.updateBackupStatus(backup, {
        phase: 'Completed',
        completionTime: new Date().toISOString(),
        backupFileName: `backup-${name}`,
      });

      log.info('FirebirdBackup reconciliation completed');
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      log.error({ err }, 'FirebirdBackup reconciliation failed');

      await this.updateBackupStatus(backup, {
        phase: 'Failed',
        error: message,
      }).catch((statusErr) => {
        log.error({ err: statusErr }, 'Failed to update backup status');
      });

      throw err;
    }
  }

  /**
   * Reconcile a FirebirdRestore resource.
   */
  async reconcileRestore(restore: FirebirdRestore): Promise<void> {
    const { name, namespace = 'default' } = restore.metadata;
    const log = logger.child({ restore: name, namespace });

    log.info('Reconciling FirebirdRestore');

    try {
      validateRestoreSpec(restore);

      if (restore.status?.phase === 'Completed' || restore.status?.phase === 'Failed') {
        log.debug({ phase: restore.status.phase }, 'FirebirdRestore is already in terminal state');
        return;
      }

      // Fetch the referenced cluster
      const clusterObj = await this.customApi.getNamespacedCustomObject({
        group: API_GROUP,
        version: API_VERSION,
        namespace,
        plural: 'firebirdclusters',
        name: restore.spec.clusterName,
      });

      const cluster = clusterObj as FirebirdCluster;
      const job = buildRestoreJob(restore, cluster);

      try {
        await this.batchApi.readNamespacedJob({ name: job.metadata!.name!, namespace });
        log.debug('Restore job already exists');
      } catch {
        log.info({ jobName: job.metadata!.name }, 'Creating restore job');
        await this.batchApi.createNamespacedJob({ namespace, body: job });
      }

      await this.updateRestoreStatus(restore, {
        phase: 'Completed',
        completionTime: new Date().toISOString(),
      });

      log.info('FirebirdRestore reconciliation completed');
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      log.error({ err }, 'FirebirdRestore reconciliation failed');

      await this.updateRestoreStatus(restore, {
        phase: 'Failed',
        error: message,
      }).catch((statusErr) => {
        log.error({ err: statusErr }, 'Failed to update restore status');
      });

      throw err;
    }
  }

  private async updateBackupStatus(
    backup: FirebirdBackup,
    status: NonNullable<FirebirdBackup['status']>,
  ): Promise<void> {
    const { name, namespace = 'default' } = backup.metadata;
    const patch = [{ op: 'replace' as const, path: '/status', value: { ...backup.status, ...status } }];

    await this.customApi.patchNamespacedCustomObjectStatus({
      group: API_GROUP,
      version: API_VERSION,
      namespace,
      plural: 'firebirdbackups',
      name,
      body: patch,
    });
  }

  private async updateRestoreStatus(
    restore: FirebirdRestore,
    status: NonNullable<FirebirdRestore['status']>,
  ): Promise<void> {
    const { name, namespace = 'default' } = restore.metadata;
    const patch = [{ op: 'replace' as const, path: '/status', value: { ...restore.status, ...status } }];

    await this.customApi.patchNamespacedCustomObjectStatus({
      group: API_GROUP,
      version: API_VERSION,
      namespace,
      plural: 'firebirdrestores',
      name,
      body: patch,
    });
  }
}
