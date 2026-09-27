import { describe, it, expect } from 'vitest';
import { V1Container, V1Job } from '@kubernetes/client-node';
import {
  DEFAULT_S3_CLIENT_IMAGE,
  buildBackupCronJob,
  buildBackupJob,
  buildBootstrapInitContainers,
  buildJournalArchiveCronJob,
  buildRestoreJob,
  buildScheduledBackupCronJob,
  jobOutcome,
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

  it('downloads an S3 backup and restores it over the network', () => {
    const pod = podOf(buildRestoreJob(makeRestore(), makeCluster(), { type: 'logical', path: 'backup-b1.fbk', s3 }));
    expect(pod.initContainers?.[0].name).toBe('download');
    expect(pod.initContainers?.[0].args?.[0]).toContain(`s3 cp 's3://bkt/fb/backup-b1.fbk' /work/backup.fbk`);
    expect(pod.containers[0].args?.[0]).toContain('gbak -c /work/backup.fbk "$FIREBIRD_HOST:$TARGET_PATH"');
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
    const fetch = pod.initContainers![1];
    expect(fetch.command).toEqual(['perl', '/etc/firebird-operator/fetch-segments.pl']);
    expect(env(fetch, 'FIREBIRD_HOST')?.value).toBe('db-1.db-headless');
    expect(env(fetch, 'SKIP_FILE')?.value).toBe('/work/uploaded');
    expect(pod.containers[0].args?.[0]).toContain(`s3 sync /work/segments/ 's3://bkt/fb/journals/'`);
    expect(pod.containers[0].args?.[0]).not.toContain('--delete');
    expect(pod.volumes?.map((v) => v.name)).toEqual(['work', 'cluster-config']);
    expect(allMounts([...pod.initContainers!, ...pod.containers])).not.toContain('firebird-data');
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
