import { describe, it, expect, vi, beforeEach } from 'vitest';
import { FirebirdBackupController } from '../src/controllers/backup.controller';
import { KubeConfig, BatchV1Api, CoordinationV1Api, CustomObjectsApi, V1Job } from '@kubernetes/client-node';
import { FirebirdBackup, FirebirdScheduledBackup, FirebirdRestore, FirebirdCluster } from '../src/types';
import { ValidationError } from '../src/utils/validation';

const notFound = Object.assign(new Error('not found'), { code: 404 });

const complete: V1Job = { status: { conditions: [{ type: 'Complete', status: 'True' }] } };
const failed: V1Job = { status: { conditions: [{ type: 'Failed', status: 'True' }] } };

describe('FirebirdBackupController', () => {
  let batchApi: Record<string, ReturnType<typeof vi.fn>>;
  let customApi: Record<string, ReturnType<typeof vi.fn>>;
  let coordinationApi: Record<string, ReturnType<typeof vi.fn>>;
  let controller: FirebirdBackupController;
  let segment: ReturnType<typeof vi.fn>;
  let cluster: FirebirdCluster;
  let objects: Record<string, unknown>;

  beforeEach(() => {
    cluster = {
      apiVersion: 'firebird.cloudnative-firebird.io/v1',
      kind: 'FirebirdCluster',
      metadata: { name: 'test-cluster', namespace: 'default' },
      spec: { instances: 2, storage: { size: '1Gi' } },
    };
    objects = { 'firebirdclusters/test-cluster': cluster };
    batchApi = {
      readNamespacedJob: vi.fn().mockRejectedValue(notFound),
      createNamespacedJob: vi.fn().mockImplementation(({ body }) => Promise.resolve(body)),
      readNamespacedCronJob: vi.fn().mockRejectedValue(notFound),
      createNamespacedCronJob: vi.fn().mockImplementation(({ body }) => Promise.resolve(body)),
      patchNamespacedCronJob: vi.fn().mockImplementation(({ body }) => Promise.resolve(body)),
    };
    customApi = {
      getNamespacedCustomObject: vi.fn().mockImplementation(({ plural, name }) => {
        const obj = objects[`${plural}/${name}`];
        return obj ? Promise.resolve(obj) : Promise.reject(notFound);
      }),
      patchNamespacedCustomObjectStatus: vi.fn().mockResolvedValue({}),
    };
    coordinationApi = {
      readNamespacedLease: vi.fn().mockResolvedValue({ spec: { holderIdentity: 'test-cluster-1' } }),
    };

    const kubeConfig = new KubeConfig();
    vi.spyOn(kubeConfig, 'makeApiClient').mockImplementation((apiClass: unknown) => {
      if (apiClass === BatchV1Api) return batchApi as unknown as BatchV1Api;
      if (apiClass === CustomObjectsApi) return customApi as unknown as CustomObjectsApi;
      if (apiClass === CoordinationV1Api) return coordinationApi as unknown as CoordinationV1Api;
      return {} as never;
    });
    // the primary's segment server: the restore target does not exist
    segment = vi.fn().mockResolvedValue(['OK no']);
    controller = new FirebirdBackupController(kubeConfig, segment);
  });

  const lastStatus = () => {
    const calls = customApi.patchNamespacedCustomObjectStatus.mock.calls;
    return calls[calls.length - 1]?.[0].body[0].value;
  };

  const makeBackup = (overrides: Partial<FirebirdBackup> = {}): FirebirdBackup => ({
    apiVersion: 'firebird.cloudnative-firebird.io/v1',
    kind: 'FirebirdBackup',
    metadata: { name: 'test-backup', namespace: 'default' },
    spec: { clusterName: 'test-cluster', type: 'logical' },
    ...overrides,
  });

  const makeRestore = (spec: Partial<FirebirdRestore['spec']> = {}, status?: FirebirdRestore['status']): FirebirdRestore => ({
    apiVersion: 'firebird.cloudnative-firebird.io/v1',
    kind: 'FirebirdRestore',
    metadata: { name: 'test-restore', namespace: 'default' },
    spec: { clusterName: 'test-cluster', backupPath: 'backup-x.fbk', ...spec },
    ...(status ? { status } : {}),
  });

  describe('FirebirdBackup', () => {
    it('creates the Job against the Lease holder and reports Running until the Job finishes', async () => {
      await controller.reconcileBackup(makeBackup());

      const job = batchApi.createNamespacedJob.mock.calls[0][0].body as V1Job;
      expect(job.metadata?.name).toBe('backup-test-backup');
      const env = job.spec?.template.spec?.containers[0].env ?? [];
      expect(env.find((e) => e.name === 'FIREBIRD_HOST')?.value).toBe('test-cluster-1.test-cluster-headless');
      expect(lastStatus()).toMatchObject({
        phase: 'Running',
        jobName: 'backup-test-backup',
        backupFileName: 'backup-test-backup.fbk',
        location: '/var/lib/firebird/data/backup-test-backup.fbk',
      });
    });

    it('reports a verified backup', async () => {
      batchApi.readNamespacedJob.mockResolvedValue(complete);
      await controller.reconcileBackup(
        makeBackup({ spec: { clusterName: 'test-cluster', verify: true }, status: { phase: 'Running' } }),
      );
      expect(lastStatus()).toMatchObject({ phase: 'Completed', verified: true });
    });

    it('marks the backup Completed when the Job completes', async () => {
      batchApi.readNamespacedJob.mockResolvedValue(complete);
      await controller.reconcileBackup(makeBackup({ status: { phase: 'Running', startTime: '2026-01-01T00:00:00.000Z' } }));

      expect(batchApi.createNamespacedJob).not.toHaveBeenCalled();
      expect(lastStatus()).toMatchObject({ phase: 'Completed', startTime: '2026-01-01T00:00:00.000Z' });
      expect(lastStatus().completionTime).toBeDefined();
    });

    it('marks the backup Failed when the Job fails', async () => {
      batchApi.readNamespacedJob.mockResolvedValue(failed);
      await controller.reconcileBackup(makeBackup({ status: { phase: 'Running' } }));
      expect(lastStatus()).toMatchObject({ phase: 'Failed', error: expect.stringContaining('backup-test-backup') });
    });

    it('reports an S3 location for uploaded backups', async () => {
      const s3 = { bucket: 'b', prefix: 'p', secretRef: { name: 's' } };
      await controller.reconcileBackup(makeBackup({ spec: { clusterName: 'test-cluster', s3 } }));
      expect(lastStatus().location).toBe('s3://b/p/backup-test-backup.fbk');
    });

    it('waits while the cluster is hibernated', async () => {
      cluster.spec.hibernated = true;
      await controller.reconcileBackup(makeBackup());
      expect(batchApi.createNamespacedJob).not.toHaveBeenCalled();
      expect(lastStatus()).toMatchObject({ phase: 'Pending', error: 'cluster is hibernated' });
    });

    it('skips reconciliation in a terminal state', async () => {
      await controller.reconcileBackup(makeBackup({ status: { phase: 'Completed' } }));
      expect(customApi.getNamespacedCustomObject).not.toHaveBeenCalled();
      expect(batchApi.createNamespacedJob).not.toHaveBeenCalled();
    });

    it('does not patch an unchanged status', async () => {
      batchApi.readNamespacedJob.mockResolvedValue({});
      await controller.reconcileBackup(
        makeBackup({
          status: {
            phase: 'Running',
            startTime: '2026-01-01T00:00:00.000Z',
            jobName: 'backup-test-backup',
            backupFileName: 'backup-test-backup.fbk',
            location: '/var/lib/firebird/data/backup-test-backup.fbk',
          },
        }),
      );
      expect(customApi.patchNamespacedCustomObjectStatus).not.toHaveBeenCalled();
    });

    it('marks an invalid spec Failed', async () => {
      await expect(
        controller.reconcileBackup(makeBackup({ spec: { clusterName: 'test-cluster', type: 'physical', verify: true } })),
      ).rejects.toThrow(ValidationError);
      expect(lastStatus()).toMatchObject({ phase: 'Failed', error: expect.stringContaining('verify') });
    });

    it('takes physical backups to S3 on clusters with replication', async () => {
      cluster.spec.replication = { enabled: true };
      const s3 = { bucket: 'b', secretRef: { name: 's' } };
      await controller.reconcileBackup(makeBackup({ spec: { clusterName: 'test-cluster', type: 'physical', s3 } }));
      const job = batchApi.createNamespacedJob.mock.calls[0][0].body as V1Job;
      expect(job.spec?.template.spec?.initContainers?.[0].args?.[0]).toContain('backup-file.pl get');
      expect(lastStatus()).toMatchObject({ phase: 'Running', location: 's3://b/nbackup-l0-test-backup.nbk' });
    });

    it('retries API errors without failing the backup', async () => {
      customApi.getNamespacedCustomObject.mockRejectedValue(new Error('connection refused'));
      await expect(controller.reconcileBackup(makeBackup())).rejects.toThrow('connection refused');
      expect(customApi.patchNamespacedCustomObjectStatus).not.toHaveBeenCalled();
    });
  });

  describe('FirebirdRestore', () => {
    it('restores a server-side backupPath into restore-<name>.fdb', async () => {
      await controller.reconcileRestore(makeRestore());
      const job = batchApi.createNamespacedJob.mock.calls[0][0].body as V1Job;
      expect(job.metadata?.name).toBe('restore-test-restore');
      expect(job.spec?.template.spec?.containers[0].args?.[0]).toContain('action_restore');
      expect(lastStatus()).toMatchObject({
        phase: 'Restoring',
        targetPath: '/var/lib/firebird/data/restore-test-restore.fdb',
      });
    });

    it('waits for the referenced FirebirdBackup to complete, then restores it', async () => {
      const backup = makeBackup({ metadata: { name: 'nightly', namespace: 'default' }, status: { phase: 'Running' } });
      objects['firebirdbackups/nightly'] = backup;
      await controller.reconcileRestore(makeRestore({ backupPath: undefined, backupName: 'nightly' }));
      expect(batchApi.createNamespacedJob).not.toHaveBeenCalled();
      expect(lastStatus()).toMatchObject({ phase: 'Pending', error: expect.stringContaining('nightly') });

      backup.status = { phase: 'Completed', backupFileName: 'backup-nightly.fbk' };
      await controller.reconcileRestore(makeRestore({ backupPath: undefined, backupName: 'nightly' }));
      const job = batchApi.createNamespacedJob.mock.calls[0][0].body as V1Job;
      expect(job.spec?.template.spec?.containers[0].args?.[0]).toContain('/var/lib/firebird/data/backup-nightly.fbk');
    });

    it('fails when the referenced backup failed or does not exist', async () => {
      objects['firebirdbackups/broken'] = makeBackup({ status: { phase: 'Failed' } });
      await expect(controller.reconcileRestore(makeRestore({ backupPath: undefined, backupName: 'broken' }))).rejects.toThrow(
        ValidationError,
      );
      await expect(controller.reconcileRestore(makeRestore({ backupPath: undefined, backupName: 'missing' }))).rejects.toThrow(
        'not found',
      );
      expect(lastStatus()).toMatchObject({ phase: 'Failed' });
    });

    it('refuses a target that already exists on the primary, without creating the Job', async () => {
      segment.mockResolvedValue(['OK yes']);
      await expect(controller.reconcileRestore(makeRestore({ targetDatabase: 'old.fdb' }))).rejects.toThrow(
        'targetDatabase old.fdb already exists on test-cluster-1',
      );
      // asked from the primary (Lease holder), signed with the SYSDBA password
      expect(segment).toHaveBeenCalledWith('test-cluster-1.test-cluster-headless', 3051, 'masterkey EXISTS old.fdb', undefined);
      expect(batchApi.createNamespacedJob).not.toHaveBeenCalled();
      expect(lastStatus()).toMatchObject({ phase: 'Failed', error: expect.stringContaining('never overwrite') });
    });

    it('creates the Job when the segment server cannot tell (no answer, earlier version, no password)', async () => {
      segment.mockRejectedValueOnce(new Error('timed out'));
      await controller.reconcileRestore(makeRestore());
      segment.mockResolvedValueOnce(['ERR bad request']);
      await controller.reconcileRestore(makeRestore({ targetDatabase: 'b.fdb' }));
      // a superuser Secret that cannot be read: nothing to sign with, so nothing is asked
      cluster.spec.superuserSecret = { name: 'missing' };
      segment.mockClear();
      await controller.reconcileRestore(makeRestore({ targetDatabase: 'c.fdb' }));
      expect(segment).not.toHaveBeenCalled();
      expect(batchApi.createNamespacedJob).toHaveBeenCalledTimes(3);
    });

    it('refuses to restore over the cluster database', async () => {
      await expect(controller.reconcileRestore(makeRestore({ targetDatabase: 'mydb.fdb' }))).rejects.toThrow('cluster database');
      expect(batchApi.createNamespacedJob).not.toHaveBeenCalled();
      expect(lastStatus()).toMatchObject({ phase: 'Failed' });
    });

    it('restores a physical backup from S3, with or without replication', async () => {
      const s3 = { bucket: 'b', secretRef: { name: 's' } };
      objects['firebirdbackups/base'] = makeBackup({
        metadata: { name: 'base', namespace: 'default' },
        spec: { clusterName: 'test-cluster', type: 'physical', s3 },
        status: { phase: 'Completed', backupFileName: 'nbackup-l0-base.nbk' },
      });
      const restore = makeRestore({ backupPath: undefined, backupName: 'base' });
      // without replication the files go through the instance's backup file server
      await controller.reconcileRestore(restore);
      const job = batchApi.createNamespacedJob.mock.calls[0][0].body as V1Job;
      expect(job.spec?.template.spec?.initContainers?.[0].args?.[0]).toContain(`'s3://b/nbackup-l0-base.nbk'`);
      expect(job.spec?.template.spec?.containers[0].args?.[0]).toContain('action_nrest');
    });

    it('replays the journal archive for pointInTime, and fails without one or with a logical backup', async () => {
      objects['firebirdbackups/logical'] = makeBackup({
        metadata: { name: 'logical', namespace: 'default' },
        status: { phase: 'Completed', backupFileName: 'backup-logical.fbk' },
      });
      await expect(
        controller.reconcileRestore(makeRestore({ backupPath: undefined, backupName: 'logical', pointInTime: {} })),
      ).rejects.toThrow('physical');
      const physical = makeRestore({ backupPath: 'l0.nbk', restoreType: 'physical', pointInTime: {} });
      await expect(controller.reconcileRestore(physical)).rejects.toThrow('journal archive');
      expect(batchApi.createNamespacedJob).not.toHaveBeenCalled();
      expect(lastStatus()).toMatchObject({ phase: 'Failed', error: expect.stringContaining('journalArchiveS3') });

      cluster.spec.replication = { enabled: true, journalArchiveS3: { bucket: 'j' } };
      await controller.reconcileRestore(physical);
      const job = batchApi.createNamespacedJob.mock.calls[0][0].body as V1Job;
      expect(job.spec?.template.spec?.containers.map((c) => c.name)).toEqual(['firebird-restore', 'journal-fetch']);
      expect(lastStatus()).toMatchObject({ phase: 'Restoring' });
    });

    it('follows the Job to Completed', async () => {
      batchApi.readNamespacedJob.mockResolvedValue(complete);
      await controller.reconcileRestore(makeRestore({}, { phase: 'Restoring' }));
      expect(lastStatus()).toMatchObject({ phase: 'Completed' });
    });
  });

  describe('FirebirdScheduledBackup', () => {
    const scheduled: FirebirdScheduledBackup = {
      apiVersion: 'firebird.cloudnative-firebird.io/v1',
      kind: 'FirebirdScheduledBackup',
      metadata: { name: 'nightly-backup', namespace: 'default' },
      spec: { clusterName: 'test-cluster', schedule: '0 2 * * *' },
    };

    it('creates the CronJob and records its name', async () => {
      await controller.reconcileScheduledBackup(scheduled);
      expect(batchApi.createNamespacedCronJob.mock.calls[0][0].body.metadata.name).toBe('sched-backup-nightly-backup');
      expect(lastStatus()).toEqual({ cronJobName: 'sched-backup-nightly-backup' });
    });

    it('updates a changed CronJob and reports its schedule times', async () => {
      batchApi.readNamespacedCronJob.mockResolvedValue({ spec: { schedule: '0 1 * * *' }, metadata: {} });
      batchApi.patchNamespacedCronJob.mockResolvedValue({
        status: { lastScheduleTime: new Date('2026-01-02T02:00:00Z'), lastSuccessfulTime: new Date('2026-01-02T02:01:00Z') },
      });
      await controller.reconcileScheduledBackup(scheduled);
      expect(batchApi.patchNamespacedCronJob).toHaveBeenCalled();
      expect(lastStatus()).toEqual({
        cronJobName: 'sched-backup-nightly-backup',
        lastScheduleTime: '2026-01-02T02:00:00.000Z',
        lastSuccessfulTime: '2026-01-02T02:01:00.000Z',
      });
    });
  });
});
