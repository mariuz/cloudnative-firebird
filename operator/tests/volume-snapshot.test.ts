import { describe, it, expect, vi, beforeEach } from 'vitest';
import { BatchV1Api, CoordinationV1Api, CustomObjectsApi, KubeConfig, V1Job } from '@kubernetes/client-node';
import { FirebirdBackupController } from '../src/controllers/backup.controller';
import { FirebirdBackup, FirebirdCluster, FirebirdScheduledBackup } from '../src/types';
import { cronMatches, latestCronTime, parseCron } from '../src/utils/cron';
import {
  boundedJobName,
  buildScheduledSnapshotBackup,
  buildSnapshotLockJob,
  buildSnapshotRecoveryClaim,
  buildSnapshotRecoveryJob,
  buildVolumeSnapshot,
  expiredSnapshotBackups,
  labelValue,
  lockScript,
  SCHEDULED_BACKUP_LABEL,
  SNAPSHOT_FINALIZER,
  snapshotRecoveryJobName,
  VolumeSnapshot,
} from '../src/utils/volume-snapshot';
import { validateBackupSpec, validateClusterSpec, validateScheduledBackupSpec, ValidationError } from '../src/utils/validation';
import { buildBootstrapInitContainers } from '../src/utils/backup';
import { validateRestoreSpec } from '../src/utils/validation';

const notFound = Object.assign(new Error('not found'), { code: 404 });
const conflict = Object.assign(new Error('exists'), { code: 409 });
const complete = (job: V1Job): V1Job => ({ ...job, status: { conditions: [{ type: 'Complete', status: 'True' }] } });
const failed = (job: V1Job): V1Job => ({ ...job, status: { conditions: [{ type: 'Failed', status: 'True' }] } });

const makeCluster = (spec: Partial<FirebirdCluster['spec']> = {}): FirebirdCluster => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdCluster',
  metadata: { name: 'db', namespace: 'default', uid: 'u1' },
  spec: { instances: 1, storage: { size: '1Gi' }, ...spec },
});

const makeBackup = (spec: Partial<FirebirdBackup['spec']> = {}): FirebirdBackup => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdBackup',
  metadata: { name: 'snap1', namespace: 'default', uid: 'b1', resourceVersion: '1' },
  spec: { clusterName: 'db', type: 'volumeSnapshot', ...spec },
});

describe('cron', () => {
  it('matches standard expressions in UTC', () => {
    const at = (iso: string) => new Date(iso);
    expect(cronMatches(parseCron('*/15 2 * * *'), at('2026-10-11T02:30:00Z'))).toBe(true);
    expect(cronMatches(parseCron('*/15 2 * * *'), at('2026-10-11T02:31:00Z'))).toBe(false);
    expect(cronMatches(parseCron('0 0 * * SUN'), at('2026-10-11T00:00:00Z'))).toBe(true); // a Sunday
    expect(cronMatches(parseCron('0 0 * * 7'), at('2026-10-11T00:00:00Z'))).toBe(true);
    expect(cronMatches(parseCron('0 0 * * 1-5'), at('2026-10-11T00:00:00Z'))).toBe(false);
    expect(cronMatches(parseCron('0 6 1,15 JAN-MAR *'), at('2026-02-15T06:00:00Z'))).toBe(true);
    // both day fields restricted: either matches
    expect(cronMatches(parseCron('0 0 13 * MON'), at('2026-10-12T00:00:00Z'))).toBe(true);
    expect(cronMatches(parseCron('0 0 13 * MON'), at('2026-10-13T00:00:00Z'))).toBe(true);
    expect(cronMatches(parseCron('0 0 13 * MON'), at('2026-10-14T00:00:00Z'))).toBe(false);
    expect(cronMatches(parseCron('5/20 * * * *'), at('2026-10-14T00:45:00Z'))).toBe(true);
  });

  it('rejects invalid expressions', () => {
    expect(() => parseCron('* * * *')).toThrow(/5-field/);
    expect(() => parseCron('60 * * * *')).toThrow(/out of range/);
    expect(() => parseCron('*/0 * * * *')).toThrow(/step/);
    expect(() => parseCron('x * * * *')).toThrow(/invalid value/);
  });

  it('finds the latest time it fired after a time', () => {
    const after = new Date('2026-10-11T01:59:30Z');
    expect(latestCronTime('0 2 * * *', after, new Date('2026-10-11T01:59:59Z'))).toBeUndefined();
    expect(latestCronTime('0 2 * * *', after, new Date('2026-10-11T02:00:10Z'))?.toISOString()).toBe('2026-10-11T02:00:00.000Z');
    // missed runs collapse into the latest
    expect(latestCronTime('0 * * * *', after, new Date('2026-10-11T05:10:00Z'))?.toISOString()).toBe('2026-10-11T05:00:00.000Z');
    // the time it fired last is not repeated
    expect(latestCronTime('0 2 * * *', new Date('2026-10-11T02:00:00Z'), new Date('2026-10-11T02:00:40Z'))).toBeUndefined();
  });
});

describe('volume snapshot resources', () => {
  it('locks and unlocks the database of the instance through isql', () => {
    const cluster = makeCluster({ superuserSecret: { name: 'su' } });
    const job = buildSnapshotLockJob(makeBackup(), cluster, 'db-1', 'lock');
    expect(job.metadata?.name).toBe('backup-snap1-lock');
    expect(job.spec?.backoffLimit).toBe(0);
    const container = job.spec!.template.spec!.containers[0];
    expect(container.args![0]).toContain('ALTER DATABASE BEGIN BACKUP');
    expect(container.env).toEqual(
      expect.arrayContaining([
        { name: 'FIREBIRD_HOST', value: 'db-1.db-headless' },
        { name: 'DATABASE_PATH', value: '/var/lib/firebird/data/mydb.fdb' },
        expect.objectContaining({ name: 'ISC_PASSWORD', valueFrom: { secretKeyRef: { name: 'su', key: 'password' } } }),
      ]),
    );
    expect(job.metadata?.ownerReferences?.[0]).toMatchObject({ kind: 'FirebirdBackup', name: 'snap1' });
    const unlock = buildSnapshotLockJob(makeBackup(), cluster, 'db-1', 'unlock');
    expect(unlock.spec?.backoffLimit).toBe(3);
    expect(lockScript('unlock')).toContain('ALTER DATABASE END BACKUP');
    // a database already in backup mode is not locked again (nor unlocked unless locked)
    expect(lockScript('lock')).toMatch(/if \[ "\$state" != 0 \]; then .*exit 1/);
    expect(lockScript('unlock')).toContain('if [ "$state" = 1 ]');
  });

  it('snapshots the instance volume with the configured class', () => {
    const backup = makeBackup({ volumeSnapshot: { className: 'csi-snap', labels: { team: 'a' } } });
    const vs = buildVolumeSnapshot(backup, makeCluster(), 'db-2');
    expect(vs.metadata.name).toBe('snap1');
    expect(vs.spec).toEqual({ volumeSnapshotClassName: 'csi-snap', source: { persistentVolumeClaimName: 'firebird-data-db-2' } });
    expect(vs.metadata.labels).toMatchObject({ team: 'a', 'firebird.cloudnative-firebird.io/cluster': 'db' });
    expect(() => buildVolumeSnapshot(backup, makeCluster(), 'other-0')).toThrow(/not an instance/);
  });

  it('keeps generated names and label values within the limits', () => {
    const long = 'x'.repeat(80);
    expect(boundedJobName('backup', long, 'unlock').length).toBeLessThanOrEqual(63);
    expect(boundedJobName('backup', long, 'unlock')).toMatch(/^backup-x+-[0-9a-f]{8}-unlock$/);
    expect(boundedJobName('backup', long, 'lock')).not.toBe(boundedJobName('backup', `${long}y`, 'lock'));
    expect(labelValue(long).length).toBeLessThanOrEqual(63);
    expect(labelValue('short')).toBe('short');
  });

  it('restores the first volume from the snapshot and prepares its database with a Job', () => {
    const cluster = makeCluster({ bootstrap: { recovery: { volumeSnapshot: { name: 'snap1', databaseName: 'old.fdb' } } } });
    const claim = buildSnapshotRecoveryClaim(cluster, { metadata: { name: 'firebird-data' }, spec: { accessModes: ['ReadWriteOnce'] } });
    expect(claim.metadata?.name).toBe('firebird-data-db-0');
    expect(claim.spec?.dataSource).toEqual({ apiGroup: 'snapshot.storage.k8s.io', kind: 'VolumeSnapshot', name: 'snap1' });

    const job = buildSnapshotRecoveryJob(cluster);
    expect(job.metadata?.name).toBe(snapshotRecoveryJobName(cluster));
    const spec = job.spec!.template.spec!;
    expect(spec.volumes).toEqual([{ name: 'firebird-data', persistentVolumeClaim: { claimName: 'firebird-data-db-0' } }]);
    // the instances' security context: the snapshot's files belong to their users
    expect(spec.securityContext).toMatchObject({ fsGroup: 999 });
    const env = Object.fromEntries((spec.containers[0].env ?? []).map((e) => [e.name, e.value]));
    expect(env).toMatchObject({ SOURCE_DATABASE: '/var/lib/firebird/data/old.fdb', TARGET_PATH: '/var/lib/firebird/data/mydb.fdb' });
    expect(spec.containers[0].args![0]).toContain('nbackup" -F');
    // no bootstrap init container: the database is on the volume before the instance starts
    expect(buildBootstrapInitContainers(cluster)).toEqual([]);

    // with replication the restored database becomes the new primary's bootstrap database
    const replicated = makeCluster({
      instances: 3,
      replication: { enabled: true },
      bootstrap: { recovery: { volumeSnapshot: { name: 'snap1' } } },
    });
    const replicatedEnv = buildSnapshotRecoveryJob(replicated).spec!.template.spec!.containers[0].env ?? [];
    expect(replicatedEnv.find((e) => e.name === 'TARGET_PATH')?.value).toBe('/var/lib/firebird/data/.bootstrap.fdb');
    expect(replicatedEnv.find((e) => e.name === 'REPLICATION_DIRS')?.value).toBe('/var/lib/firebird/data/replication');
  });

  it('expires the backups of a schedule, keeping the newest completed one', () => {
    const now = new Date('2026-10-11T00:00:00Z');
    const backup = (name: string, phase: 'Completed' | 'Failed' | 'Running', time: string): FirebirdBackup => ({
      ...makeBackup(),
      metadata: { name },
      status: { phase, startTime: time, ...(phase === 'Completed' ? { completionTime: time } : {}) },
    });
    const backups = [
      backup('old', 'Completed', '2026-09-01T00:00:00Z'),
      backup('old-failed', 'Failed', '2026-09-02T00:00:00Z'),
      backup('old-running', 'Running', '2026-09-03T00:00:00Z'),
      backup('recent', 'Completed', '2026-10-10T00:00:00Z'),
    ];
    expect(expiredSnapshotBackups(backups, '7d', now).map((b) => b.metadata.name)).toEqual(['old', 'old-failed']);
    expect(expiredSnapshotBackups(backups, undefined, now)).toEqual([]);
    // the newest completed backup stays, however old
    expect(expiredSnapshotBackups([backups[0]], '7d', now)).toEqual([]);
  });
});

describe('volume snapshot validation', () => {
  it('validates snapshot backups', () => {
    expect(() => validateBackupSpec(makeBackup())).not.toThrow();
    expect(() => validateBackupSpec(makeBackup({ level: 0 }))).toThrow(/level/);
    expect(() => validateBackupSpec(makeBackup({ s3: { bucket: 'b' } }))).toThrow(/s3/);
    expect(() => validateBackupSpec(makeBackup({ target: 'prefer-standby' }))).toThrow(/primary/);
    expect(() => validateBackupSpec(makeBackup({ volumeSnapshot: { lockTimeoutSeconds: 5 } }))).toThrow(/lockTimeoutSeconds/);
    expect(() => validateBackupSpec(makeBackup({ type: 'logical', volumeSnapshot: {} }))).toThrow(/only used by type volumeSnapshot/);
    expect(() =>
      validateScheduledBackupSpec({ spec: { clusterName: 'db', schedule: '61 * * * *', type: 'volumeSnapshot' } }),
    ).toThrow(/out of range/);
    expect(() => validateScheduledBackupSpec({ spec: { clusterName: 'db', schedule: '0 2 * * *', type: 'volumeSnapshot' } })).not.toThrow();
  });

  it('validates a bootstrap from a volume snapshot', () => {
    expect(() => validateClusterSpec(makeCluster({ bootstrap: { recovery: { volumeSnapshot: { name: 'snap1' } } } }))).not.toThrow();
    expect(() =>
      validateClusterSpec(makeCluster({ bootstrap: { recovery: { volumeSnapshot: { name: 'snap1' }, sourcePath: 'x.fbk' } } })),
    ).toThrow(ValidationError);
    expect(() =>
      validateClusterSpec(makeCluster({ instances: 2, bootstrap: { recovery: { volumeSnapshot: { name: 'snap1' } } } })),
    ).toThrow(/needs replication/);
    expect(() =>
      validateClusterSpec(makeCluster({ bootstrap: { recovery: { volumeSnapshot: { name: 'snap1', databaseName: '../x' } } } })),
    ).toThrow(/databaseName/);
    expect(() => validateRestoreSpec({ ...makeRestore() })).not.toThrow();
  });
});

const makeRestore = () => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1' as const,
  kind: 'FirebirdRestore' as const,
  metadata: { name: 'r', namespace: 'default' },
  spec: { clusterName: 'db', backupName: 'snap1' },
});

describe('FirebirdBackupController volume snapshots', () => {
  let jobs: Map<string, V1Job>;
  let snapshots: Map<string, VolumeSnapshot>;
  let backups: Map<string, FirebirdBackup>;
  let cluster: FirebirdCluster;
  let batchApi: Record<string, ReturnType<typeof vi.fn>>;
  let customApi: Record<string, ReturnType<typeof vi.fn>>;
  let controller: FirebirdBackupController;

  beforeEach(() => {
    jobs = new Map();
    snapshots = new Map();
    backups = new Map();
    cluster = makeCluster({ instances: 2, replication: { enabled: true } });
    batchApi = {
      readNamespacedJob: vi.fn().mockImplementation(({ name }) => (jobs.has(name) ? Promise.resolve(jobs.get(name)) : Promise.reject(notFound))),
      createNamespacedJob: vi.fn().mockImplementation(({ body }) => {
        jobs.set(body.metadata.name, body);
        return Promise.resolve(body);
      }),
      deleteNamespacedCronJob: vi.fn().mockRejectedValue(notFound),
    };
    const store = (plural: string): Map<string, unknown> | undefined =>
      plural === 'volumesnapshots' ? snapshots : plural === 'firebirdbackups' ? backups : undefined;
    customApi = {
      getNamespacedCustomObject: vi.fn().mockImplementation(({ plural, name }) => {
        if (plural === 'firebirdclusters' && name === 'db') return Promise.resolve(cluster);
        const found = store(plural)?.get(name);
        return found ? Promise.resolve(structuredClone(found)) : Promise.reject(notFound);
      }),
      createNamespacedCustomObject: vi.fn().mockImplementation(({ plural, body }) => {
        const map = store(plural)!;
        if (map.has(body.metadata.name)) return Promise.reject(conflict);
        map.set(body.metadata.name, structuredClone(body));
        return Promise.resolve(body);
      }),
      deleteNamespacedCustomObject: vi.fn().mockImplementation(({ plural, name }) => {
        const map = store(plural)!;
        if (!map.delete(name)) return Promise.reject(notFound);
        return Promise.resolve({});
      }),
      listNamespacedCustomObject: vi.fn().mockImplementation(({ labelSelector }) => {
        const [key, value] = (labelSelector as string).split('=');
        return Promise.resolve({ items: [...backups.values()].filter((b) => b.metadata.labels?.[key] === value).map((b) => structuredClone(b)) });
      }),
      patchNamespacedCustomObject: vi.fn().mockImplementation(({ name, body }) => {
        const b = backups.get(name)!;
        for (const op of body) {
          if (op.op === 'test' && b.metadata.resourceVersion !== op.value) return Promise.reject(Object.assign(new Error('conflict'), { code: 422 }));
          if (op.path === '/metadata/finalizers') b.metadata.finalizers = op.value;
        }
        b.metadata.resourceVersion = String(Number(b.metadata.resourceVersion ?? '1') + 1);
        return Promise.resolve(b);
      }),
      patchNamespacedCustomObjectStatus: vi.fn().mockImplementation(({ plural, name, body }) => {
        const b = store(plural)?.get(name) as { status?: unknown; metadata: { resourceVersion?: string } } | undefined;
        if (b) {
          b.status = body[0].value;
          b.metadata.resourceVersion = String(Number(b.metadata.resourceVersion ?? '1') + 1);
        }
        return Promise.resolve({});
      }),
    };
    const coordinationApi = { readNamespacedLease: vi.fn().mockResolvedValue({ spec: { holderIdentity: 'db-1' } }) };
    const kubeConfig = new KubeConfig();
    vi.spyOn(kubeConfig, 'makeApiClient').mockImplementation((apiClass: unknown) => {
      if (apiClass === BatchV1Api) return batchApi as unknown as BatchV1Api;
      if (apiClass === CustomObjectsApi) return customApi as unknown as CustomObjectsApi;
      if (apiClass === CoordinationV1Api) return coordinationApi as unknown as CoordinationV1Api;
      return { createNamespacedEvent: vi.fn().mockResolvedValue({}) } as never;
    });
    controller = new FirebirdBackupController(kubeConfig, vi.fn());
  });

  /** Reconciles the stored backup once */
  const step = async (name = 'snap1') => controller.reconcileBackup(structuredClone(backups.get(name)!));
  const stored = (name = 'snap1') => backups.get(name)!;

  it('locks the primary, snapshots its volume, unlocks once cut and completes once ready', async () => {
    backups.set('snap1', makeBackup());
    await step();
    expect(stored().metadata.finalizers).toEqual([SNAPSHOT_FINALIZER]);
    expect(stored().status).toMatchObject({ phase: 'Running', jobName: 'backup-snap1-lock', instance: 'db-1' });
    expect(snapshots.size).toBe(0);

    await step(); // the lock Job still runs
    expect(snapshots.size).toBe(0);

    jobs.set('backup-snap1-lock', complete(jobs.get('backup-snap1-lock')!));
    await step();
    expect(snapshots.get('snap1')?.spec.source.persistentVolumeClaimName).toBe('firebird-data-db-1');
    expect(stored().status?.volumeSnapshot).toMatchObject({ name: 'snap1', claimName: 'firebird-data-db-1' });
    expect(stored().status?.volumeSnapshot?.lockedAt).toBeDefined();

    await step(); // not cut yet: still locked
    expect(jobs.has('backup-snap1-unlock')).toBe(false);

    snapshots.get('snap1')!.status = { creationTime: '2026-10-11T00:00:00Z', readyToUse: false };
    await step();
    expect(jobs.get('backup-snap1-unlock')?.spec?.template.spec?.containers[0].args?.[0]).toContain('END BACKUP');

    jobs.set('backup-snap1-unlock', complete(jobs.get('backup-snap1-unlock')!));
    await step();
    expect(stored().status?.volumeSnapshot?.unlockedAt).toBeDefined();
    expect(stored().status?.phase).toBe('Running');
    expect(stored().metadata.finalizers).toEqual([]);

    snapshots.get('snap1')!.status = { creationTime: '2026-10-11T00:00:00Z', readyToUse: true, restoreSize: '1Gi' };
    await step();
    expect(stored().status).toMatchObject({
      phase: 'Completed',
      location: 'VolumeSnapshot default/snap1',
      volumeSnapshot: { readyToUse: true, restoreSize: '1Gi', creationTime: '2026-10-11T00:00:00.000Z' },
    });
  });

  it('unlocks and fails when the snapshot is not cut in time, deleting it first', async () => {
    backups.set('snap1', makeBackup({ volumeSnapshot: { lockTimeoutSeconds: 60 } }));
    await step();
    jobs.set('backup-snap1-lock', complete(jobs.get('backup-snap1-lock')!));
    await step();
    stored().status!.volumeSnapshot!.lockedAt = new Date(Date.now() - 120_000).toISOString();
    await step();
    expect(snapshots.has('snap1')).toBe(false);
    expect(jobs.has('backup-snap1-unlock')).toBe(true);
    expect(stored().status?.error).toMatch(/not cut within 60s/);

    jobs.set('backup-snap1-unlock', complete(jobs.get('backup-snap1-unlock')!));
    await step();
    await step();
    expect(stored().status).toMatchObject({ phase: 'Failed', error: expect.stringMatching(/not cut within 60s/) });
    expect(stored().metadata.finalizers).toEqual([]);
  });

  it('unlocks and fails when the snapshot fails', async () => {
    backups.set('snap1', makeBackup());
    await step();
    jobs.set('backup-snap1-lock', complete(jobs.get('backup-snap1-lock')!));
    await step();
    snapshots.get('snap1')!.status = { error: { message: 'driver refused' } };
    await step();
    expect(jobs.has('backup-snap1-unlock')).toBe(true);
    jobs.set('backup-snap1-unlock', complete(jobs.get('backup-snap1-unlock')!));
    await step();
    expect(stored().status).toMatchObject({ phase: 'Failed', error: 'VolumeSnapshot snap1 failed: driver refused' });
  });

  it('reports a database left locked when the unlock fails', async () => {
    backups.set('snap1', makeBackup());
    await step();
    jobs.set('backup-snap1-lock', complete(jobs.get('backup-snap1-lock')!));
    await step();
    snapshots.get('snap1')!.status = { creationTime: '2026-10-11T00:00:00Z' };
    await step();
    jobs.set('backup-snap1-unlock', failed(jobs.get('backup-snap1-unlock')!));
    await step();
    expect(stored().status).toMatchObject({ phase: 'Failed', error: expect.stringContaining('ALTER DATABASE END BACKUP') });
    expect(stored().metadata.finalizers).toEqual([]);
  });

  it('does not unlock after a failed lock (another backup may hold it)', async () => {
    backups.set('snap1', makeBackup());
    await step();
    jobs.set('backup-snap1-lock', failed(jobs.get('backup-snap1-lock')!));
    await step();
    expect(stored().status).toMatchObject({ phase: 'Failed', error: expect.stringContaining('locking the database on db-1 failed') });
    expect(jobs.has('backup-snap1-unlock')).toBe(false);
    expect(stored().metadata.finalizers).toEqual([]);
  });

  it('unlocks a backup deleted while the database is locked, then releases it', async () => {
    backups.set('snap1', makeBackup());
    await step();
    jobs.set('backup-snap1-lock', complete(jobs.get('backup-snap1-lock')!));
    await step();
    stored().metadata.deletionTimestamp = new Date().toISOString();
    await step();
    expect(snapshots.has('snap1')).toBe(false); // not cut: deleted
    expect(jobs.has('backup-snap1-unlock')).toBe(true);
    expect(stored().metadata.finalizers).toEqual([SNAPSHOT_FINALIZER]);
    jobs.set('backup-snap1-unlock', complete(jobs.get('backup-snap1-unlock')!));
    await step();
    await step();
    expect(stored().metadata.finalizers).toEqual([]);
  });

  it('waits while the cluster is hibernated and refuses restores from a snapshot backup', async () => {
    cluster.spec.hibernated = true;
    backups.set('snap1', makeBackup());
    await step();
    expect(stored().status?.phase).toBe('Pending');
    expect(jobs.size).toBe(0);

    await expect(controller.reconcileRestore(makeRestore())).rejects.toThrow(/bootstrap a new cluster from it/);
  });

  it('creates the backups of a schedule and applies its retention', async () => {
    const scheduled: FirebirdScheduledBackup = {
      apiVersion: 'firebird.cloudnative-firebird.io/v1',
      kind: 'FirebirdScheduledBackup',
      metadata: { name: 'nightly', namespace: 'default', creationTimestamp: new Date(Date.now() - 2 * 60_000).toISOString() },
      spec: { clusterName: 'db', schedule: '* * * * *', type: 'volumeSnapshot', retentionPolicy: '7d' },
    };
    const old: FirebirdBackup = {
      ...buildScheduledSnapshotBackup(scheduled, new Date('2026-01-01T00:00:00Z')),
      status: { phase: 'Completed', completionTime: '2026-01-01T00:01:00Z', volumeSnapshot: { name: 'nightly-202601010000', claimName: 'c' } },
    };
    backups.set(old.metadata.name, old);
    snapshots.set('nightly-202601010000', { metadata: { name: 'nightly-202601010000' }, spec: { source: {} } });
    const recent: FirebirdBackup = {
      ...buildScheduledSnapshotBackup(scheduled, new Date(Date.now() - 86_400_000)),
      status: { phase: 'Completed', completionTime: new Date(Date.now() - 86_400_000).toISOString() },
    };
    backups.set(recent.metadata.name, recent);

    await controller.reconcileScheduledBackup(scheduled);
    const created = [...backups.values()].filter((b) => !b.status);
    expect(created).toHaveLength(1);
    expect(created[0].metadata.labels?.[SCHEDULED_BACKUP_LABEL]).toBe('nightly');
    expect(created[0].spec).toEqual({ clusterName: 'db', type: 'volumeSnapshot' });
    // the expired backup and its snapshot are gone; the recent one stays
    expect(backups.has(old.metadata.name)).toBe(false);
    expect(snapshots.has('nightly-202601010000')).toBe(false);
    expect(backups.has(recent.metadata.name)).toBe(true);
    const status = customApi.patchNamespacedCustomObjectStatus.mock.calls.at(-1)?.[0].body[0].value;
    expect(status).toMatchObject({ lastBackupName: created[0].metadata.name, lastSuccessfulTime: recent.status!.completionTime });
    expect(new Date(status.lastScheduleTime).getUTCSeconds()).toBe(0);

    // the next run waits while that backup runs
    await controller.reconcileScheduledBackup({ ...scheduled, status: { lastScheduleTime: new Date(Date.now() - 120_000).toISOString() } });
    expect([...backups.values()].filter((b) => !b.status)).toHaveLength(1);
  });
});
