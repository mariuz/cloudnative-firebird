import { describe, it, expect } from 'vitest';
import { V1Container, V1Job } from '@kubernetes/client-node';
import {
  DEFAULT_S3_CLIENT_IMAGE,
  buildBackupCronJob,
  buildBackupJob,
  buildBootstrapInitContainers,
  buildJournalArchiveCronJob,
  journalArchiveListedSequence,
  journalArchivePodSelector,
  buildRestoreJob,
  buildScheduledBackupCronJob,
  compactUtc,
  jobOutcome,
  pointInTimeSourceError,
  s3Uri,
  shellQuote,
} from '../src/utils/backup';
import {
  JOB_TEMPLATE_HASH_ANNOTATION,
  buildStatefulSet,
  cronJobNeedsUpdate,
  withHibernation,
} from '../src/utils/resources';
import { FirebirdBackup, FirebirdCluster, FirebirdRestore, FirebirdScheduledBackup } from '../src/types';

const makeCluster = (overrides: Partial<FirebirdCluster['spec']> = {}): FirebirdCluster => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdCluster',
  metadata: { name: 'db', namespace: 'default', uid: 'cluster-uid' },
  spec: { instances: 1, storage: { size: '1Gi' }, ...overrides },
});

const s3 = { bucket: 'bkt', prefix: 'fb/', secretRef: { name: 's3-creds' }, endpoint: 'http://minio:9000' };

const makeBackup = (spec: Partial<FirebirdBackup['spec']> = {}): FirebirdBackup => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdBackup',
  metadata: { name: 'b1', namespace: 'default', uid: 'backup-uid' },
  spec: { clusterName: 'db', ...spec },
});

const makeRestore = (spec: Partial<FirebirdRestore['spec']> = {}): FirebirdRestore => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdRestore',
  metadata: { name: 'r1', namespace: 'default', uid: 'restore-uid' },
  spec: { clusterName: 'db', ...spec },
});

const podOf = (job: V1Job) => job.spec!.template.spec!;
const env = (c: V1Container, name: string) => c.env?.find((e) => e.name === name);
const allMounts = (containers: V1Container[] = []) =>
  containers.flatMap((c) => c.volumeMounts ?? []).map((m) => m.name);

describe('backup Jobs (server-side, no S3)', () => {
  it('takes a logical backup through the primary service manager into its data directory', () => {
    const job = buildBackupJob(makeBackup(), makeCluster(), 'db-1');
    const pod = podOf(job);
    expect(pod.initContainers).toBeUndefined();
    expect(pod.containers).toHaveLength(1);
    const [c] = pod.containers;
    expect(c.args?.[0]).toContain('fbsvcmgr "$FIREBIRD_HOST:service_mgr" action_backup');
    expect(c.args?.[0]).toContain('bkp_file "/var/lib/firebird/data/$f"');
    expect(c.args?.[0]).toContain('f="backup-b1.fbk"');
    expect(env(c, 'FIREBIRD_HOST')?.value).toBe('db-1.db-headless');
    expect(env(c, 'DATABASE_PATH')?.value).toBe('/var/lib/firebird/data/mydb.fdb');
  });

  it('takes a physical backup with the nbackup service action and level', () => {
    const job = buildBackupJob(makeBackup({ type: 'physical', level: 1 }), makeCluster());
    const args = podOf(job).containers[0].args?.[0];
    expect(args).toContain('action_nbak dbname "$DATABASE_PATH"');
    expect(args).toContain('nbk_level 1');
    expect(args).toContain('f="nbackup-l1-b1.nbk"');
  });

  it('never mounts an instance volume and never passes the password on the command line', () => {
    const pod = podOf(buildBackupJob(makeBackup(), makeCluster({ superuserSecret: { name: 'su' } })));
    expect(pod.volumes).toBeUndefined();
    const c = pod.containers[0];
    expect(c.args?.[0]).not.toMatch(/-pas|ISC_PASSWORD/);
    expect(env(c, 'ISC_PASSWORD')?.valueFrom).toEqual({ secretKeyRef: { name: 'su', key: 'password' } });
  });

  it('defaults to the ordinal 0 primary and uses the cluster database name', () => {
    const c = podOf(buildBackupJob(makeBackup(), makeCluster({ databaseName: 'erp.fdb' }))).containers[0];
    expect(env(c, 'FIREBIRD_HOST')?.value).toBe('db-0.db-headless');
    expect(env(c, 'DATABASE_PATH')?.value).toBe('/var/lib/firebird/data/erp.fdb');
  });

  it('is owned by the FirebirdBackup, fails visibly and does not match the database Services', () => {
    const job = buildBackupJob(makeBackup(), makeCluster());
    expect(job.metadata?.name).toBe('backup-b1');
    expect(job.metadata?.ownerReferences?.[0]).toMatchObject({ kind: 'FirebirdBackup', name: 'b1', uid: 'backup-uid' });
    expect(job.spec?.backoffLimit).toBe(2);
    expect(podOf(job).restartPolicy).toBe('Never');
    expect(job.spec?.template.metadata?.labels?.['app.kubernetes.io/component']).toBe('on-demand-backup');
  });
});

describe('backup Jobs (S3)', () => {
  it('streams a logical backup to the pod and uploads it with an S3 client container', () => {
    const pod = podOf(buildBackupJob(makeBackup({ s3 }), makeCluster()));
    expect(pod.initContainers?.map((c) => c.name)).toEqual(['firebird-backup']);
    expect(pod.initContainers?.[0].args?.[0]).toContain('gbak -b "$FIREBIRD_HOST:$DATABASE_PATH" "/work/$f"');
    const upload = pod.containers[0];
    expect(upload.name).toBe('upload');
    expect(upload.image).toBe(DEFAULT_S3_CLIENT_IMAGE);
    expect(upload.args?.[0]).toContain(`aws --endpoint-url 'http://minio:9000' s3 cp "/work/$f" "s3://bkt/fb/$f"`);
    expect(env(upload, 'AWS_ACCESS_KEY_ID')?.valueFrom).toEqual({
      secretKeyRef: { name: 's3-creds', key: 'AWS_ACCESS_KEY_ID' },
    });
    expect(env(upload, 'AWS_DEFAULT_REGION')?.value).toBe('us-east-1');
    expect(pod.volumes).toEqual([{ name: 'work', emptyDir: {} }]);
  });

  it('honours a custom S3 client image and region', () => {
    const pod = podOf(buildBackupJob(makeBackup({ s3: { ...s3, clientImage: 'my/aws:1', region: 'eu-west-1' } }), makeCluster()));
    expect(pod.containers[0].image).toBe('my/aws:1');
    expect(env(pod.containers[0], 'AWS_DEFAULT_REGION')?.value).toBe('eu-west-1');
  });
});

describe('backup verification', () => {
  it('restores and validates an S3 backup in the pod before it is uploaded', () => {
    const pod = podOf(buildBackupJob(makeBackup({ s3, verify: true }), makeCluster()));
    expect(pod.initContainers?.map((c) => c.name)).toEqual(['firebird-backup', 'verify']);
    const script = pod.initContainers![1].args![0];
    expect(script).toContain('gbak -c "/work/$f" "$v"');
    expect(script).toContain('gfix -v -full "$v"');
    expect(pod.initContainers![1].volumeMounts).toEqual(pod.initContainers![0].volumeMounts);
    // without verify, no restore
    expect(podOf(buildBackupJob(makeBackup({ s3 }), makeCluster())).initContainers?.map((c) => c.name)).toEqual([
      'firebird-backup',
    ]);
  });

  it('restores a server-side backup next to it through the server, validates it and drops the copy', () => {
    const script = podOf(buildBackupJob(makeBackup({ verify: true }), makeCluster())).containers[0].args![0];
    expect(script).toContain('action_backup');
    expect(script).toContain('v="/var/lib/firebird/data/.verify-${f%.*}.fdb"');
    expect(script).toContain('action_restore bkp_file "/var/lib/firebird/data/$f" dbname "$v" res_replace');
    expect(script).toContain('action_validate dbname "$v"');
    expect(script).toContain('echo "drop database;" | isql -q "$FIREBIRD_HOST:$v"');
    expect(podOf(buildBackupJob(makeBackup(), makeCluster())).containers[0].args![0]).not.toContain('action_restore');
  });

  it('verifies scheduled backups too, and never physical ones', () => {
    const cj = buildBackupCronJob(makeCluster({ backup: { enabled: true, s3, verify: true } }));
    expect(cj.spec!.jobTemplate.spec!.template.spec!.initContainers?.map((c) => c.name)).toContain('verify');
    const physical = podOf(buildBackupJob(makeBackup({ type: 'physical', verify: true }), makeCluster()));
    expect(physical.containers[0].args![0]).not.toContain('action_restore');
  });
});

describe('backup CronJobs', () => {
  it('builds the cluster backup CronJob with a timestamped file name against the primary', () => {
    const cj = buildBackupCronJob(makeCluster({ backup: { enabled: true, schedule: '0 3 * * *' } }), 'db-2');
    expect(cj.metadata?.name).toBe('db-backup');
    expect(cj.spec?.schedule).toBe('0 3 * * *');
    const c = cj.spec!.jobTemplate.spec!.template.spec!.containers[0];
    expect(c.args?.[0]).toContain('f="backup-db-$(date -u +%Y%m%dT%H%M%SZ).fbk"');
    expect(env(c, 'FIREBIRD_HOST')?.value).toBe('db-2.db-headless');
    expect(cj.metadata?.ownerReferences?.[0]).toMatchObject({ kind: 'FirebirdCluster', name: 'db' });
    expect(cj.metadata?.annotations?.[JOB_TEMPLATE_HASH_ANNOTATION]).toMatch(/^[0-9a-f]{16}$/);
  });

  it('detects job template changes such as a new primary', () => {
    const cluster = makeCluster({ backup: { enabled: true } });
    const a = buildBackupCronJob(cluster, 'db-0');
    expect(cronJobNeedsUpdate(a, buildBackupCronJob(cluster, 'db-0'))).toBe(false);
    expect(cronJobNeedsUpdate(a, buildBackupCronJob(cluster, 'db-1'))).toBe(true);
  });

  it('keeps the suspend flag separate from the template hash', () => {
    const on = makeCluster({ backup: { enabled: true }, hibernated: true });
    const off = makeCluster({ backup: { enabled: true } });
    const suspended = withHibernation(buildBackupCronJob(on), on);
    expect(suspended.spec?.suspend).toBe(true);
    expect(cronJobNeedsUpdate(suspended, withHibernation(buildBackupCronJob(off), off))).toBe(true);
  });

  it('builds a FirebirdScheduledBackup CronJob owned by the resource and suspended with the cluster', () => {
    const sb: FirebirdScheduledBackup = {
      apiVersion: 'firebird.cloudnative-firebird.io/v1',
      kind: 'FirebirdScheduledBackup',
      metadata: { name: 'nightly', namespace: 'default', uid: 'sb-uid' },
      spec: { clusterName: 'db', schedule: '0 1 * * *', type: 'physical', level: 2 },
    };
    const cj = buildScheduledBackupCronJob(sb, makeCluster());
    expect(cj.metadata?.name).toBe('sched-backup-nightly');
    expect(cj.metadata?.ownerReferences?.[0]).toMatchObject({ kind: 'FirebirdScheduledBackup', uid: 'sb-uid' });
    expect(cj.spec?.jobTemplate.spec?.template.spec?.containers[0].args?.[0]).toContain('nbk_level 2');
    expect(cj.spec?.suspend).toBe(false);
    expect(buildScheduledBackupCronJob(sb, makeCluster({ hibernated: true })).spec?.suspend).toBe(true);
    expect(buildScheduledBackupCronJob({ ...sb, spec: { ...sb.spec, suspend: true } }, makeCluster()).spec?.suspend).toBe(true);
  });
});

describe('restore Jobs', () => {
  it('restores a server-side logical backup into a new database through the service manager', () => {
    const job = buildRestoreJob(makeRestore(), makeCluster(), { type: 'logical', path: 'backup-b1.fbk' }, 'db-1');
    const c = podOf(job).containers[0];
    expect(c.args?.[0]).toContain(
      `fbsvcmgr "$FIREBIRD_HOST:service_mgr" action_restore bkp_file '/var/lib/firebird/data/backup-b1.fbk' dbname "$TARGET_PATH"`,
    );
    expect(env(c, 'TARGET_PATH')?.value).toBe('/var/lib/firebird/data/restore-r1.fdb');
    expect(env(c, 'FIREBIRD_HOST')?.value).toBe('db-1.db-headless');
    expect(job.metadata?.ownerReferences?.[0]).toMatchObject({ kind: 'FirebirdRestore', uid: 'restore-uid' });
  });

  it('restores a physical chain with one nbk_file per level', () => {
    const job = buildRestoreJob(makeRestore({ targetDatabase: 'copy.fdb' }), makeCluster(), {
      type: 'physical',
      path: 'nbackup-l0.nbk',
      incrementalPaths: ['/var/lib/firebird/data/nbackup-l1.nbk'],
    });
    const c = podOf(job).containers[0];
    expect(c.args?.[0]).toContain(
      `action_nrest dbname "$TARGET_PATH" nbk_file '/var/lib/firebird/data/nbackup-l0.nbk' nbk_file '/var/lib/firebird/data/nbackup-l1.nbk'`,
    );
    expect(env(c, 'TARGET_PATH')?.value).toBe('/var/lib/firebird/data/copy.fdb');
  });

  it('removes a partial physical restore, never a target that existed before', () => {
    const script = podOf(
      buildRestoreJob(makeRestore(), makeCluster(), { type: 'physical', path: 'l0.nbk', incrementalPaths: ['l1.nbk'] }),
    ).containers[0].args![0];
    const nrest = script.indexOf('action_nrest');
    const guard = script.indexOf(`grep -q 'File exists'`);
    const fixup = script.indexOf('action_nfix dbname "$TARGET_PATH"');
    const drop = script.indexOf(`echo 'drop database;' | isql -q "$FIREBIRD_HOST:$TARGET_PATH"`);
    expect(nrest).toBeGreaterThan(-1);
    expect(guard).toBeGreaterThan(nrest);
    expect(fixup).toBeGreaterThan(guard);
    expect(drop).toBeGreaterThan(fixup);
    expect(script).toContain('exit $rc');
    // a backup taken on a replica restores as a read-only replica: made a normal database
    expect(script.indexOf('prp_replica_mode prp_rm_none')).toBeGreaterThan(script.indexOf('exit $rc'));
    expect(script.indexOf('prp_replica_mode prp_rm_none')).toBeLessThan(script.indexOf('echo "restored into'));
    // logical restores (gbak removes its own partial file) are unchanged
    const logical = podOf(buildRestoreJob(makeRestore(), makeCluster(), { type: 'logical', path: 'b.fbk' })).containers[0].args![0];
    expect(logical).not.toContain('action_nfix');
  });

  it('downloads an S3 backup and restores it over the network', () => {
    const pod = podOf(buildRestoreJob(makeRestore(), makeCluster(), { type: 'logical', path: 'backup-b1.fbk', s3 }));
    expect(pod.initContainers?.[0].name).toBe('download');
    expect(pod.initContainers?.[0].args?.[0]).toContain(`s3 cp 's3://bkt/fb/backup-b1.fbk' /work/backup.fbk`);
    expect(pod.containers[0].args?.[0]).toContain('gbak -c /work/backup.fbk "$FIREBIRD_HOST:$TARGET_PATH"');
  });
});

describe('physical backups and restores with S3', () => {
  const cluster = makeCluster({ replication: { enabled: true } });

  it('copies the nbackup file through the segment server, removes it from the volume and uploads it', () => {
    const pod = podOf(buildBackupJob(makeBackup({ type: 'physical', level: 1, s3 }), cluster, 'db-1'));
    const [backup] = pod.initContainers!;
    expect(pod.initContainers).toHaveLength(1);
    const script = backup.args![0];
    expect(script).toContain('f="nbackup-l1-b1.nbk"');
    // removal is armed before the backup is taken, so a failed copy leaves nothing behind
    expect(script.indexOf('trap')).toBeLessThan(script.indexOf('action_nbak'));
    expect(script).toContain(`trap 'perl /etc/firebird-operator/backup-file.pl remove "$f" || true' EXIT`);
    expect(script).toContain('action_nbak dbname "$DATABASE_PATH" nbk_file "/var/lib/firebird/data/$f" nbk_level 1');
    expect(script).toContain('perl /etc/firebird-operator/backup-file.pl get "$f" "/work/$f"; echo "$f" > /work/.name');
    expect(env(backup, 'FIREBIRD_HOST')?.value).toBe('db-1.db-headless');
    expect(env(backup, 'SEGMENT_PORT')?.value).toBe('3051');
    expect(backup.volumeMounts?.map((m) => m.name)).toEqual(['work', 'cluster-config']);
    expect(pod.volumes).toContainEqual({ name: 'cluster-config', configMap: { name: 'db-config' } });
    expect(pod.containers[0].name).toBe('upload');
    expect(pod.containers[0].args![0]).toContain('s3 cp "/work/$f" "s3://bkt/fb/$f"');
  });

  it('prunes nbackup series in S3 along their chains, read from the primary\'s history', () => {
    const sb: FirebirdScheduledBackup = {
      apiVersion: 'firebird.cloudnative-firebird.io/v1',
      kind: 'FirebirdScheduledBackup',
      metadata: { name: 'nightly', namespace: 'default', uid: 'sb' },
      spec: { clusterName: 'db', schedule: '0 1 * * *', type: 'physical', s3, retentionPolicy: '7d' },
    };
    const pod = buildScheduledBackupCronJob(sb, cluster).spec!.jobTemplate.spec!.template.spec!;
    const backup = pod.initContainers![0].args![0];
    expect(backup).toContain('from rdb$backup_history order by rdb$backup_id');
    expect(backup.indexOf('backup-file.pl get')).toBeLessThan(backup.indexOf('rdb$backup_history'));
    expect(backup).toContain('> /work/history');
    const upload = pod.containers[0].args![0];
    expect(upload).toContain(`grep -E '^nbackup-l[0-2]-nightly-[0-9]{8}T[0-9]{6}Z[.]nbk$'`);
    expect(upload).toContain(' /work/history /work/all /work/series > /work/expired');
    // without a retentionPolicy there is no history query
    const plain = buildScheduledBackupCronJob({ ...sb, spec: { ...sb.spec, retentionPolicy: undefined } }, cluster);
    expect(plain.spec!.jobTemplate.spec!.template.spec!.initContainers![0].args![0]).not.toContain('rdb$backup_history');
  });

  it('downloads the chain, stores it next to the database, restores it and removes the copies', () => {
    const pod = podOf(
      buildRestoreJob(
        makeRestore({ targetDatabase: 'copy.fdb' }),
        cluster,
        { type: 'physical', path: 'nbackup-l0-b0.nbk', incrementalPaths: ['nbackup-l1-b1.nbk'], s3 },
        'db-1',
      ),
    );
    const download = pod.initContainers![0].args![0];
    expect(download).toContain(`s3 cp 's3://bkt/fb/nbackup-l0-b0.nbk' /work/restore-r1-0.nbk`);
    expect(download).toContain(`s3 cp 's3://bkt/fb/nbackup-l1-b1.nbk' /work/restore-r1-1.nbk`);
    const [restore] = pod.containers;
    const script = restore.args![0];
    expect(script).toContain(`trap 'perl /etc/firebird-operator/backup-file.pl remove restore-r1-0.nbk restore-r1-1.nbk || true' EXIT`);
    expect(script).toContain('backup-file.pl put /work/restore-r1-0.nbk restore-r1-0.nbk');
    expect(script).toContain('backup-file.pl put /work/restore-r1-1.nbk restore-r1-1.nbk');
    expect(script).toContain(
      'action_nrest dbname "$TARGET_PATH" nbk_file "/var/lib/firebird/data/restore-r1-0.nbk" nbk_file "/var/lib/firebird/data/restore-r1-1.nbk"',
    );
    expect(env(restore, 'FIREBIRD_HOST')?.value).toBe('db-1.db-headless');
    expect(env(restore, 'TARGET_PATH')?.value).toBe('/var/lib/firebird/data/copy.fdb');
    expect(pod.volumes?.map((v) => v.name)).toEqual(['work', 'cluster-config']);
  });
});

describe('point-in-time recovery', () => {
  const journals = { bucket: 'arch', prefix: 'prod', secretRef: { name: 'arch-creds' } };
  const replicated = makeCluster({ replication: { enabled: true, journalArchiveS3: journals } });
  const chain = { type: 'physical' as const, path: 'nbackup-l0-a.nbk', incrementalPaths: ['nbackup-l1-b.nbk'], s3 };

  it('restores the chain, replays the journal archive and restores the result on the primary', () => {
    const restore = makeRestore({ restoreType: 'physical', pointInTime: { targetTime: '2026-10-01T12:15:30+02:00' } });
    const pod = podOf(buildRestoreJob(restore, replicated, chain, 'db-1'));
    const download = pod.initContainers![0];
    expect(download.env).toContainEqual(expect.objectContaining({ name: 'AWS_ACCESS_KEY_ID', valueFrom: { secretKeyRef: { name: 's3-creds', key: 'AWS_ACCESS_KEY_ID' } } }));
    expect(download.args![0]).toContain(`s3 cp 's3://bkt/fb/nbackup-l0-a.nbk' /work/chain/0.nbk`);
    expect(download.args![0]).toContain(`s3 cp 's3://bkt/fb/nbackup-l1-b.nbk' /work/chain/1.nbk`);
    const [firebird, fetch] = pod.containers;
    expect(firebird.args![0]).toMatch(/^set -eu; \. \/etc\/firebird-operator\/pitr-restore\.sh; out=\$\(fbsvcmgr/);
    expect(firebird.args![0]).toContain('action_nrest dbname "$TARGET_PATH" nbk_file "/var/lib/firebird/data/restore-$RESTORE_NAME-pitr.nbk"');
    expect(env(firebird, 'TARGET_TIME')?.value).toBe('20261001T101530Z');
    expect(env(firebird, 'TARGET_SEGMENT')).toBeUndefined();
    expect(env(firebird, 'CHAIN_COUNT')?.value).toBe('2');
    expect(env(firebird, 'SERVER_CHAIN')).toBeUndefined();
    expect(env(firebird, 'RESTORE_NAME')?.value).toBe('r1');
    expect(env(firebird, 'FIREBIRD_HOST')?.value).toBe('db-1.db-headless');
    // the journal archive is read with its own credentials
    expect(fetch.name).toBe('journal-fetch');
    expect(fetch.env).toContainEqual(expect.objectContaining({ valueFrom: { secretKeyRef: { name: 'arch-creds', key: 'AWS_ACCESS_KEY_ID' } } }));
    expect(fetch.args![0]).toContain(`aws s3 ls 's3://arch/prod/journals/' > $W/journals.tmp`);
    expect(fetch.args![0]).toContain(`s3 cp --only-show-errors 's3://arch/prod/journals/'"$s" "$W/segments/$s"`);
    expect(fetch.args![0]).toContain('case "$s" in *[!A-Za-z0-9._-]*)');
    expect(pod.securityContext?.runAsNonRoot).toBe(true);
    expect(pod.volumes?.map((v) => v.name)).toEqual(['work', 'cluster-config']);
  });

  it('takes a server-side chain through the file server, and a journal archive of its own', () => {
    const restore = makeRestore({
      restoreType: 'physical',
      pointInTime: { targetSegment: 42, journalS3: { bucket: 'other' } },
    });
    const pod = podOf(
      buildRestoreJob(restore, makeCluster(), { type: 'physical', path: '/var/lib/firebird/data/l0.nbk', incrementalPaths: ['l1.nbk'] }),
    );
    expect(pod.initContainers).toEqual([]);
    expect(env(pod.containers[0], 'SERVER_CHAIN')?.value).toBe('l0.nbk l1.nbk');
    expect(env(pod.containers[0], 'TARGET_SEGMENT')?.value).toBe('42');
    expect(pod.containers[1].args![0]).toContain(`s3 ls 's3://other/journals/'`);
  });

  it('needs a physical backup, a journal archive and server-side files the file server serves', () => {
    const restore = makeRestore({ restoreType: 'physical', pointInTime: {} });
    expect(pointInTimeSourceError(restore, replicated, chain)).toBeUndefined();
    expect(pointInTimeSourceError(makeRestore(), makeCluster(), chain)).toBeUndefined();
    expect(pointInTimeSourceError(restore, replicated, { type: 'logical', path: 'b.fbk' })).toContain('physical');
    expect(pointInTimeSourceError(restore, makeCluster(), chain)).toContain('journal archive');
    expect(pointInTimeSourceError(restore, replicated, { type: 'physical', path: 'backups/l0.nbk' })).toContain('backups/l0.nbk');
    expect(pointInTimeSourceError(restore, replicated, { type: 'physical', path: '/var/lib/firebird/data/l0.nbk' })).toBeUndefined();
  });

  it('converts target times to the archive markers\' UTC form', () => {
    expect(compactUtc('2026-10-01T10:15:30Z')).toBe('20261001T101530Z');
    expect(compactUtc('2026-10-01T10:15:30.999-01:00')).toBe('20261001T111530Z');
  });
});

describe('journal archive CronJob', () => {
  it('is only built with replication and journalArchiveS3', () => {
    expect(buildJournalArchiveCronJob(makeCluster())).toBeNull();
    expect(buildJournalArchiveCronJob(makeCluster({ replication: { enabled: true } }))).toBeNull();
  });

  it('fetches segments from the primary segment server and syncs only new ones to S3', () => {
    const cj = buildJournalArchiveCronJob(
      makeCluster({ replication: { enabled: true, journalArchiveS3: s3, archiveSchedule: '*/5 * * * *' } }),
      'db-1',
    )!;
    expect(cj.spec?.schedule).toBe('*/5 * * * *');
    const pod = cj.spec!.jobTemplate.spec!.template.spec!;
    expect(pod.initContainers?.map((c) => c.name)).toEqual(['list-uploaded', 'fetch-segments']);
    // without pruneAppliedSegments the upload ends the Job, as before
    expect(pod.containers.map((c) => c.name)).toEqual(['upload']);
    expect(pod.containers[0].args?.[0]).toContain(`s3 sync /work/segments/ 's3://bkt/fb/journals/'`);
    expect(pod.volumes?.map((v) => v.name)).toEqual(['work', 'cluster-config']);
    expect(allMounts([...pod.initContainers!, ...pod.containers])).not.toContain('firebird-data');
  });

  it('reports the uploaded segments to the primary with pruneAppliedSegments', () => {
    const cj = buildJournalArchiveCronJob(
      makeCluster({ replication: { enabled: true, journalArchiveS3: s3, pruneAppliedSegments: true } }),
      'db-1',
    )!;
    const pod = cj.spec!.jobTemplate.spec!.template.spec!;
    expect(pod.initContainers?.map((c) => c.name)).toEqual(['list-uploaded', 'fetch-segments', 'upload']);
    const fetch = pod.initContainers![1];
    expect(fetch.command).toEqual(['perl', '/etc/firebird-operator/fetch-segments.pl']);
    expect(env(fetch, 'FIREBIRD_HOST')?.value).toBe('db-1.db-headless');
    expect(env(fetch, 'SKIP_FILE')?.value).toBe('/work/uploaded');
    expect(env(fetch, 'LISTED_FILE')?.value).toBe('/work/listed-max');
    expect(env(fetch, 'RESULT_FILE')?.value).toBe('/dev/termination-log');
    const upload = pod.initContainers![2];
    expect(upload.args?.[0]).toContain(`s3 sync /work/segments/ 's3://bkt/fb/journals/'`);
    expect(upload.args?.[0]).not.toContain('--delete');
    // only after a successful upload: the primary learns which segments are in the bucket
    const report = pod.containers[0];
    expect(report.name).toBe('report-uploaded');
    expect(report.command).toEqual(['perl', '/etc/firebird-operator/fetch-segments.pl']);
    expect(env(report, 'REPORT')?.value).toBe('true');
    expect(env(report, 'LISTED_FILE')?.value).toBe('/work/listed-max');
    expect(env(report, 'FIREBIRD_HOST')?.value).toBe('db-1.db-headless');
    expect(pod.volumes?.map((v) => v.name)).toEqual(['work', 'cluster-config']);
    expect(allMounts([...pod.initContainers!, ...pod.containers])).not.toContain('firebird-data');
  });
});

describe('journalArchiveListedSequence', () => {
  const archivePod = (message?: string, exitCode = 0, name = 'fetch-segments') => ({
    status: { initContainerStatuses: [{ name, image: '', imageID: '', ready: false, restartCount: 0, state: { terminated: { exitCode, message } } }] },
  });
  it('is the highest sequence a successful fetch listed, at least the stored one', () => {
    expect(journalArchiveListedSequence([])).toBeUndefined();
    expect(journalArchiveListedSequence([], 4)).toBe(4);
    expect(journalArchiveListedSequence([archivePod('listed=7'), archivePod('listed=12'), archivePod()], 4)).toBe(12);
    expect(journalArchiveListedSequence([archivePod('listed=7')], 9)).toBe(9);
    // failed, another container, or no segment listed
    expect(journalArchiveListedSequence([archivePod('listed=30', 1), archivePod('listed=30', 0, 'upload'), archivePod('')])).toBeUndefined();
    expect(journalArchivePodSelector(makeCluster())).toContain('app.kubernetes.io/component=journal-archive');
  });
});

describe('bootstrap init containers', () => {
  it('downloads and restores an S3 backup (default key backup.fbk)', () => {
    const cluster = makeCluster({ bootstrap: { recovery: { s3 } } });
    const init = buildBootstrapInitContainers(cluster);
    expect(init.map((c) => c.name)).toEqual(['bootstrap-download', 'bootstrap-restore']);
    expect(init[0].args?.[0]).toContain(`s3 cp 's3://bkt/fb/backup.fbk' /work/backup.fbk`);
    expect(init[1].args?.[0]).toContain(`gbak -c '/work/backup.fbk' "$TARGET_PATH.tmp"`);
    expect(init[1].args?.[0]).toContain('mv "$TARGET_PATH.tmp" "$TARGET_PATH"');
    expect(env(init[1], 'TARGET_PATH')?.value).toBe('/var/lib/firebird/data/mydb.fdb');
    const sts = buildStatefulSet(cluster);
    expect(sts.spec?.template.spec?.volumes?.map((v) => v.name)).toContain('work');
  });

  it('uses sourcePath as the object key', () => {
    const init = buildBootstrapInitContainers(makeCluster({ bootstrap: { recovery: { s3, sourcePath: 'nightly/x.fbk' } } }));
    expect(init[0].args?.[0]).toContain(`'s3://bkt/fb/nightly/x.fbk'`);
  });

  it('restores a local sourcePath without a download step', () => {
    const init = buildBootstrapInitContainers(makeCluster({ bootstrap: { recovery: { sourcePath: '/seed/db.fbk' } } }));
    expect(init.map((c) => c.name)).toEqual(['bootstrap-restore']);
    expect(init[0].args?.[0]).toContain(`gbak -c '/seed/db.fbk'`);
  });

  it('clones by streaming gbak from the source cluster Service into a local restore', () => {
    const init = buildBootstrapInitContainers(
      makeCluster({
        superuserSecret: { name: 'su' },
        bootstrap: { clone: { sourceCluster: 'prod-db', namespace: 'prod', superuserSecret: { name: 'prod-su' } } },
      }),
    );
    expect(init.map((c) => c.name)).toEqual(['bootstrap-clone']);
    const c = init[0];
    expect(c.args?.[0]).toContain('gbak -b "$SOURCE_HOST:$SOURCE_DATABASE" stdout | gbak -c stdin "$TARGET_PATH.tmp"');
    expect(env(c, 'SOURCE_HOST')?.value).toBe('prod-db.prod');
    expect(env(c, 'SOURCE_DATABASE')?.value).toBe('/var/lib/firebird/data/mydb.fdb');
    expect(env(c, 'ISC_PASSWORD')?.valueFrom).toEqual({ secretKeyRef: { name: 'prod-su', key: 'password' } });
  });

  it('clones the named source database with the cluster superuser Secret by default', () => {
    const c = buildBootstrapInitContainers(
      makeCluster({ superuserSecret: { name: 'su' }, bootstrap: { clone: { sourceCluster: 'src', databaseName: 'erp.fdb' } } }),
    )[0];
    expect(env(c, 'SOURCE_HOST')?.value).toBe('src');
    expect(env(c, 'SOURCE_DATABASE')?.value).toBe('/var/lib/firebird/data/erp.fdb');
    expect(env(c, 'ISC_PASSWORD')?.valueFrom).toEqual({ secretKeyRef: { name: 'su', key: 'password' } });
  });

  it('skips when the database exists, and on replicas when replication is enabled', () => {
    const plain = buildBootstrapInitContainers(makeCluster({ bootstrap: { clone: { sourceCluster: 'src' } } }))[0];
    expect(plain.args?.[0]).toMatch(/^set -eu; if \[ -f "\$DATABASE_PATH" \]/);
    expect(env(plain, 'PRIMARY_FILE')).toBeUndefined();

    const replicated = buildBootstrapInitContainers(
      makeCluster({ replication: { enabled: true }, bootstrap: { clone: { sourceCluster: 'src' } } }),
    )[0];
    expect(env(replicated, 'PRIMARY_FILE')?.value).toBe('/etc/firebird-operator/primary');
    expect(env(replicated, 'TARGET_PATH')?.value).toBe('/var/lib/firebird/data/.bootstrap.fdb');
    expect(replicated.volumeMounts?.map((m) => m.name)).toContain('cluster-config');
  });

  it('runs before the replication init container, which takes over the bootstrapped file', () => {
    const sts = buildStatefulSet(makeCluster({ replication: { enabled: true }, bootstrap: { recovery: { s3 } } }));
    expect(sts.spec?.template.spec?.initContainers?.map((c) => c.name)).toEqual([
      'security-db-init',
      'bootstrap-download',
      'bootstrap-restore',
      'replication-init',
    ]);
  });

  it('adds nothing without recovery or clone', () => {
    expect(buildBootstrapInitContainers(makeCluster({ bootstrap: { initSql: 'select 1 from rdb$database;' } }))).toEqual([]);
  });
});

describe('helpers', () => {
  it('reads the Job outcome from its conditions', () => {
    expect(jobOutcome({})).toBe('Running');
    expect(jobOutcome({ status: { conditions: [{ type: 'Complete', status: 'True' }] } })).toBe('Completed');
    expect(jobOutcome({ status: { conditions: [{ type: 'Failed', status: 'True' }] } })).toBe('Failed');
  });

  it('builds S3 URIs with a normalised prefix and quotes shell values', () => {
    expect(s3Uri({ bucket: 'b', secretRef: { name: 's' } }, 'k')).toBe('s3://b/k');
    expect(s3Uri({ bucket: 'b', prefix: '/a/b/', secretRef: { name: 's' } }, 'k')).toBe('s3://b/a/b/k');
    expect(shellQuote("it's")).toBe(`'it'\\''s'`);
  });
});
