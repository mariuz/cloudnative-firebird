import {
  BatchV1Api,
  CoordinationV1Api,
  CoreV1Api,
  CustomObjectsApi,
  KubeConfig,
  PatchStrategy,
  setHeaderOptions,
  V1CronJob,
  V1Job,
} from '@kubernetes/client-node';
import { EventReason, EventRecorder, EventType } from '../utils/events';
import { BackupTarget, chooseBackupInstance } from '../utils/backup-target';
import { logger } from '../utils/logger';
import {
  BackupSource,
  backupLocation,
  buildBackupJob,
  buildRestoreJob,
  buildScheduledBackupCronJob,
  jobOutcome,
  onDemandBackupFileName,
  physicalS3NeedsReplication,
  restoreTargetDatabase,
} from '../utils/backup';
import { cronJobNeedsUpdate, databaseName, FIREBIRD_DATA_DIR, instancePodSelector } from '../utils/resources';
import {
  ValidationError,
  validateBackupSpec,
  validateRestoreSpec,
  validateScheduledBackupSpec,
} from '../utils/validation';
import {
  API_GROUP,
  API_VERSION,
  FirebirdCluster,
  FirebirdBackup,
  FirebirdScheduledBackup,
  FirebirdRestore,
  reconciliationDisabled,
  S3BackupConfiguration,
} from '../types';

const MERGE_PATCH = setHeaderOptions('Content-Type', PatchStrategy.MergePatch);

/** Instance a backup Job connects to (its FIREBIRD_HOST, "<pod>.<cluster>-headless") */
function jobInstance(job: V1Job): string | undefined {
  const spec = job.spec?.template.spec;
  const host = [...(spec?.initContainers ?? []), ...(spec?.containers ?? [])]
    .flatMap((c) => c.env ?? [])
    .find((e) => e.name === 'FIREBIRD_HOST')?.value;
  return host?.split('.')[0];
}

/** Returns true for a Kubernetes API "not found" error */
function isNotFound(err: unknown): boolean {
  const e = err as { code?: number; statusCode?: number; response?: { statusCode?: number } };
  return e?.code === 404 || e?.statusCode === 404 || e?.response?.statusCode === 404;
}

/**
 * Reconciles FirebirdBackup, FirebirdScheduledBackup and FirebirdRestore resources.
 *
 * Backups and restores run as Jobs against the current primary. Their status follows the Job:
 * reconciles are repeated on the operator's resync interval until the Job completes or fails.
 * Only invalid specs mark a resource Failed directly; API errors are retried.
 */
export class FirebirdBackupController {
  private readonly batchApi: BatchV1Api;
  private readonly customApi: CustomObjectsApi;
  private readonly coordinationApi: CoordinationV1Api;
  private readonly events: EventRecorder;
  private readonly coreApi: CoreV1Api;

  constructor(kubeConfig: KubeConfig) {
    this.batchApi = kubeConfig.makeApiClient(BatchV1Api);
    this.customApi = kubeConfig.makeApiClient(CustomObjectsApi);
    this.coordinationApi = kubeConfig.makeApiClient(CoordinationV1Api);
    this.coreApi = kubeConfig.makeApiClient(CoreV1Api);
    this.events = new EventRecorder(this.coreApi);
  }

  /** Instance a backup runs on: the primary, or a replica with target prefer-standby */
  private async backupInstance(
    cluster: FirebirdCluster,
    spec: { target?: BackupTarget; type?: 'logical' | 'physical'; s3?: S3BackupConfiguration },
  ): Promise<string> {
    const primaryPod = await this.primaryPod(cluster);
    if (spec.target !== 'prefer-standby') return primaryPod;
    const { name, namespace = 'default' } = cluster.metadata;
    const pods = (await this.coreApi.listNamespacedPod({ namespace, labelSelector: instancePodSelector(name) })).items;
    return chooseBackupInstance({ cluster, primaryPod, pods, ...spec });
  }

  /** Records a Kubernetes event on a backup or restore */
  private event(
    object: FirebirdBackup | FirebirdRestore,
    kind: 'FirebirdBackup' | 'FirebirdRestore',
    type: EventType,
    reason: string,
    message: string,
  ): Promise<void> {
    return this.events.record({ apiVersion: `${API_GROUP}/${API_VERSION}`, kind, metadata: object.metadata }, type, reason, message);
  }

  private async getCluster(namespace: string, name: string): Promise<FirebirdCluster> {
    return (await this.customApi.getNamespacedCustomObject({
      group: API_GROUP,
      version: API_VERSION,
      namespace,
      plural: 'firebirdclusters',
      name,
    })) as FirebirdCluster;
  }

  /** The primary is the leader Lease holder, or ordinal 0 before a Lease exists */
  private async primaryPod(cluster: FirebirdCluster): Promise<string> {
    const { name, namespace = 'default' } = cluster.metadata;
    try {
      const lease = await this.coordinationApi.readNamespacedLease({ name: `${name}-lease`, namespace });
      if (lease.spec?.holderIdentity) return lease.spec.holderIdentity;
    } catch {
      // no Lease yet
    }
    return `${name}-0`;
  }

  /** Reads a Job, or returns undefined when it does not exist */
  private async readJob(name: string, namespace: string): Promise<V1Job | undefined> {
    try {
      return await this.batchApi.readNamespacedJob({ name, namespace });
    } catch (err) {
      if (isNotFound(err)) return undefined;
      throw err;
    }
  }

  /**
   * Reconcile a FirebirdBackup resource.
   */
  async reconcileBackup(backup: FirebirdBackup): Promise<void> {
    const { name, namespace = 'default' } = backup.metadata;
    const log = logger.child({ backup: name, namespace });
    if (reconciliationDisabled(backup)) {
      log.debug('Reconciliation disabled by annotation');
      return;
    }

    if (backup.status?.phase === 'Completed' || backup.status?.phase === 'Failed') {
      log.debug({ phase: backup.status.phase }, 'FirebirdBackup is already in terminal state');
      return;
    }

    try {
      validateBackupSpec(backup);
      const cluster = await this.getCluster(namespace, backup.spec.clusterName);
      if (backup.spec.type === 'physical' && backup.spec.s3) {
        const problem = physicalS3NeedsReplication(cluster, `FirebirdBackup ${name}`);
        if (problem) throw new ValidationError(problem);
      }
      const jobName = `backup-${name}`;
      const fileName = onDemandBackupFileName(backup);
      const base = { jobName, backupFileName: fileName, location: backupLocation(fileName, backup.spec.s3) };

      let job = await this.readJob(jobName, namespace);
      if (!job) {
        if (cluster.spec.hibernated) {
          await this.updateBackupStatus(backup, { phase: 'Pending', error: 'cluster is hibernated' });
          return;
        }
        const desired = buildBackupJob(backup, cluster, await this.backupInstance(cluster, backup.spec));
        log.info({ jobName }, 'Creating backup Job');
        job = await this.batchApi.createNamespacedJob({ namespace, body: desired });
        await this.event(backup, 'FirebirdBackup', 'Normal', EventReason.BackupStarted, `${backup.spec.type ?? 'logical'} backup of cluster ${backup.spec.clusterName} started (Job ${jobName})`);
      }

      const outcome = jobOutcome(job);
      const startTime = backup.status?.startTime ?? new Date().toISOString();
      const instance = jobInstance(job);
      if (instance) Object.assign(base, { instance });
      if (outcome === 'Completed') {
        const verified = Boolean(backup.spec.verify) && backup.spec.type !== 'physical';
        log.info({ location: base.location, verified }, 'Backup completed');
        await this.event(
          backup,
          'FirebirdBackup',
          'Normal',
          EventReason.BackupCompleted,
          `backup stored at ${base.location}${verified ? ', restored and validated' : ''}`,
        );
        await this.updateBackupStatus(backup, {
          ...base,
          ...(verified ? { verified } : {}),
          phase: 'Completed',
          startTime,
          completionTime: new Date().toISOString(),
          error: undefined,
        });
      } else if (outcome === 'Failed') {
        log.warn({ jobName }, 'Backup Job failed');
        await this.event(backup, 'FirebirdBackup', 'Warning', EventReason.BackupFailed, `backup Job ${jobName} failed; see its pod logs`);
        await this.updateBackupStatus(backup, {
          ...base,
          phase: 'Failed',
          startTime,
          error: `backup Job ${jobName} failed${backup.spec.verify ? ' (or the backup did not restore and validate)' : ''}; see its pod logs`,
        });
      } else {
        await this.updateBackupStatus(backup, { ...base, phase: 'Running', startTime, error: undefined });
      }
    } catch (err) {
      if (!(err instanceof ValidationError)) throw err;
      log.error({ err }, 'Invalid FirebirdBackup');
      await this.event(backup, 'FirebirdBackup', 'Warning', EventReason.BackupFailed, err.message);
      await this.updateBackupStatus(backup, { phase: 'Failed', error: err.message }).catch((statusErr) => {
        log.error({ err: statusErr }, 'Failed to update backup status');
      });
      throw err;
    }
  }

  /**
   * Reconcile a FirebirdScheduledBackup resource.
   */
  async reconcileScheduledBackup(scheduledBackup: FirebirdScheduledBackup): Promise<void> {
    const { name, namespace = 'default' } = scheduledBackup.metadata;
    const log = logger.child({ scheduledBackup: name, namespace });
    if (reconciliationDisabled(scheduledBackup)) {
      log.debug('Reconciliation disabled by annotation');
      return;
    }

    validateScheduledBackupSpec(scheduledBackup);
    const cluster = await this.getCluster(namespace, scheduledBackup.spec.clusterName);
    if (scheduledBackup.spec.type === 'physical' && scheduledBackup.spec.s3) {
      const problem = physicalS3NeedsReplication(cluster, `FirebirdScheduledBackup ${name}`);
      if (problem) throw new ValidationError(problem);
    }
    const desired = buildScheduledBackupCronJob(scheduledBackup, cluster, await this.backupInstance(cluster, scheduledBackup.spec));
    const cronName = desired.metadata!.name!;

    let current: V1CronJob;
    try {
      current = await this.batchApi.readNamespacedCronJob({ name: cronName, namespace });
      if (cronJobNeedsUpdate(current, desired)) {
        log.info({ cronName }, 'Updating scheduled backup CronJob');
        current = await this.batchApi.patchNamespacedCronJob({ name: cronName, namespace, body: desired }, MERGE_PATCH);
      }
    } catch (err) {
      if (!isNotFound(err)) throw err;
      log.info({ cronName }, 'Creating scheduled backup CronJob');
      current = await this.batchApi.createNamespacedCronJob({ namespace, body: desired });
    }

    const iso = (d?: Date | string) => (d ? new Date(d).toISOString() : undefined);
    await this.updateScheduledBackupStatus(scheduledBackup, {
      cronJobName: cronName,
      lastScheduleTime: iso(current.status?.lastScheduleTime),
      lastSuccessfulTime: iso(current.status?.lastSuccessfulTime),
    });
  }

  /** Resolves the backup a restore reads from; undefined while the referenced backup is not done */
  private async restoreSource(restore: FirebirdRestore, namespace: string): Promise<BackupSource | undefined> {
    const spec = restore.spec;
    if (!spec.backupName) {
      return {
        type: spec.restoreType ?? 'logical',
        path: spec.backupPath!,
        incrementalPaths: spec.incrementalBackupPaths,
        s3: spec.s3,
      };
    }
    let backup: FirebirdBackup;
    try {
      backup = (await this.customApi.getNamespacedCustomObject({
        group: API_GROUP,
        version: API_VERSION,
        namespace,
        plural: 'firebirdbackups',
        name: spec.backupName,
      })) as FirebirdBackup;
    } catch (err) {
      if (isNotFound(err)) throw new ValidationError(`FirebirdBackup ${spec.backupName} not found`);
      throw err;
    }
    if (backup.status?.phase === 'Failed') {
      throw new ValidationError(`FirebirdBackup ${spec.backupName} failed`);
    }
    if (backup.status?.phase !== 'Completed' || !backup.status.backupFileName) return undefined;
    const type = backup.spec.type ?? 'logical';
    if (spec.restoreType && spec.restoreType !== type) {
      throw new ValidationError(`restoreType ${spec.restoreType} does not match the ${type} backup ${spec.backupName}`);
    }
    return { type, path: backup.status.backupFileName, s3: backup.spec.s3 };
  }

  /**
   * Reconcile a FirebirdRestore resource.
   */
  async reconcileRestore(restore: FirebirdRestore): Promise<void> {
    const { name, namespace = 'default' } = restore.metadata;
    const log = logger.child({ restore: name, namespace });
    if (reconciliationDisabled(restore)) {
      log.debug('Reconciliation disabled by annotation');
      return;
    }

    if (restore.status?.phase === 'Completed' || restore.status?.phase === 'Failed') {
      log.debug({ phase: restore.status.phase }, 'FirebirdRestore is already in terminal state');
      return;
    }

    try {
      validateRestoreSpec(restore);
      const cluster = await this.getCluster(namespace, restore.spec.clusterName);
      const target = restoreTargetDatabase(restore);
      if (target === databaseName(cluster)) {
        throw new ValidationError(
          `targetDatabase ${target} is the cluster database; restores create a new database file ` +
            '(bootstrap a new cluster from the backup to replace a database)',
        );
      }
      const jobName = `restore-${name}`;
      const base = { jobName, targetPath: `${FIREBIRD_DATA_DIR}/${target}` };

      let job = await this.readJob(jobName, namespace);
      if (!job) {
        const source = await this.restoreSource(restore, namespace);
        if (!source) {
          await this.updateRestoreStatus(restore, {
            ...base,
            phase: 'Pending',
            error: `waiting for FirebirdBackup ${restore.spec.backupName} to complete`,
          });
          return;
        }
        if (source.type === 'physical' && source.s3) {
          const problem = physicalS3NeedsReplication(cluster, `FirebirdRestore ${name}`);
          if (problem) throw new ValidationError(problem);
        }
        if (cluster.spec.hibernated) {
          await this.updateRestoreStatus(restore, { ...base, phase: 'Pending', error: 'cluster is hibernated' });
          return;
        }
        const desired = buildRestoreJob(restore, cluster, source, await this.primaryPod(cluster));
        log.info({ jobName }, 'Creating restore Job');
        job = await this.batchApi.createNamespacedJob({ namespace, body: desired });
        await this.event(restore, 'FirebirdRestore', 'Normal', EventReason.RestoreStarted, `restoring into ${base.targetPath} (Job ${jobName})`);
      }

      const outcome = jobOutcome(job);
      const startTime = restore.status?.startTime ?? new Date().toISOString();
      if (outcome === 'Completed') {
        log.info({ target: base.targetPath }, 'Restore completed');
        await this.event(restore, 'FirebirdRestore', 'Normal', EventReason.RestoreCompleted, `restored into ${base.targetPath}`);
        await this.updateRestoreStatus(restore, {
          ...base,
          phase: 'Completed',
          startTime,
          completionTime: new Date().toISOString(),
          error: undefined,
        });
      } else if (outcome === 'Failed') {
        log.warn({ jobName }, 'Restore Job failed');
        await this.event(restore, 'FirebirdRestore', 'Warning', EventReason.RestoreFailed, `restore Job ${jobName} failed; see its pod logs`);
        await this.updateRestoreStatus(restore, {
          ...base,
          phase: 'Failed',
          startTime,
          error: `restore Job ${jobName} failed; see its pod logs`,
        });
      } else {
        await this.updateRestoreStatus(restore, { ...base, phase: 'Restoring', startTime, error: undefined });
      }
    } catch (err) {
      if (!(err instanceof ValidationError)) throw err;
      log.error({ err }, 'Invalid FirebirdRestore');
      await this.event(restore, 'FirebirdRestore', 'Warning', EventReason.RestoreFailed, err.message);
      await this.updateRestoreStatus(restore, { phase: 'Failed', error: err.message }).catch((statusErr) => {
        log.error({ err: statusErr }, 'Failed to update restore status');
      });
      throw err;
    }
  }

  /** Replaces the status when it changed; undefined fields are dropped */
  private async patchStatus(
    plural: string,
    metadata: { name: string; namespace?: string },
    current: object | undefined,
    update: object,
  ): Promise<void> {
    const merged = Object.fromEntries(
      Object.entries({ ...(current ?? {}), ...update }).filter(([, v]) => v !== undefined),
    );
    const canonical = (o: object) => JSON.stringify(Object.entries(o).sort(([a], [b]) => a.localeCompare(b)));
    if (current && canonical(current) === canonical(merged)) return;
    await this.customApi.patchNamespacedCustomObjectStatus({
      group: API_GROUP,
      version: API_VERSION,
      namespace: metadata.namespace ?? 'default',
      plural,
      name: metadata.name,
      body: [{ op: 'replace' as const, path: '/status', value: merged }],
    });
  }

  private updateBackupStatus(backup: FirebirdBackup, status: NonNullable<FirebirdBackup['status']>): Promise<void> {
    return this.patchStatus('firebirdbackups', backup.metadata, backup.status, status);
  }

  private updateRestoreStatus(restore: FirebirdRestore, status: NonNullable<FirebirdRestore['status']>): Promise<void> {
    return this.patchStatus('firebirdrestores', restore.metadata, restore.status, status);
  }

  private updateScheduledBackupStatus(
    scheduledBackup: FirebirdScheduledBackup,
    status: NonNullable<FirebirdScheduledBackup['status']>,
  ): Promise<void> {
    return this.patchStatus('firebirdscheduledbackups', scheduledBackup.metadata, scheduledBackup.status, status);
  }
}
