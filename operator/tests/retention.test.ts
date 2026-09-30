import { describe, it, expect } from 'vitest';
import {
  buildBackupCronJob,
  buildScheduledBackupCronJob,
  retentionSeconds,
  s3RetentionScript,
  serverSideRetentionScript,
} from '../src/utils/backup';
import { validateClusterSpec, validateScheduledBackupSpec } from '../src/utils/validation';
import { FirebirdCluster, FirebirdScheduledBackup } from '../src/types';

const s3 = { bucket: 'b', prefix: 'prod', endpoint: 'http://s3:8333' };
const cluster = (backup: FirebirdCluster['spec']['backup']): FirebirdCluster => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdCluster',
  metadata: { name: 'db', namespace: 'default', uid: 'c' },
  spec: { instances: 1, storage: { size: '1Gi' }, backup },
});
const scheduled = (spec: Partial<FirebirdScheduledBackup['spec']>): FirebirdScheduledBackup => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdScheduledBackup',
  metadata: { name: 'db.nightly', namespace: 'default', uid: 's' },
  spec: { clusterName: 'db', schedule: '0 2 * * *', ...spec },
});
const uploadArgs = (podSpec?: { containers: Array<{ args?: string[] }> }) => podSpec!.containers[0].args![0];

describe('backup retention', () => {
  it('parses days, weeks and months', () => {
    expect(retentionSeconds('7d')).toBe(7 * 86400);
    expect(retentionSeconds('2w')).toBe(14 * 86400);
    expect(retentionSeconds('3m')).toBe(90 * 86400);
    expect(() => retentionSeconds('0d')).toThrow();
    expect(() => retentionSeconds('7 days')).toThrow();
  });

  it('only matches the schedule\'s own timestamped series, and keeps the newest and the new upload', () => {
    const script = s3RetentionScript(s3, 'db.nightly', 86400);
    expect(script).toContain(`grep -E '^backup-db[.]nightly-[0-9]{8}T[0-9]{6}Z[.]fbk$'`);
    expect(script).toContain(`s3 ls 's3://b/prod/'`);
    expect(script).toContain('- 86400 ))');
    expect(script).toContain('$0 != n && $0 != f');
  });

  it('prunes after the upload of scheduled logical S3 backups with a retentionPolicy', () => {
    const cron = buildBackupCronJob(cluster({ enabled: true, s3, retentionPolicy: '30d' }), 'db-0');
    const args = uploadArgs(cron.spec?.jobTemplate.spec?.template.spec);
    expect(args.indexOf('s3 cp')).toBeLessThan(args.indexOf('retention: deleted'));
    expect(args).toContain('^backup-db-[0-9]{8}');
    expect(args).toContain(`- ${30 * 86400} ))`);

    const sched = buildScheduledBackupCronJob(scheduled({ s3, retentionPolicy: '1w' }), cluster(undefined), 'db-0');
    expect(uploadArgs(sched.spec?.jobTemplate.spec?.template.spec)).toContain('^backup-db[.]nightly-');
  });

  it('does not prune without a retentionPolicy, or server-side backups without replication', () => {
    const cron = buildBackupCronJob(cluster({ enabled: true, s3 }), 'db-0');
    expect(uploadArgs(cron.spec?.jobTemplate.spec?.template.spec)).not.toContain('retention');
    const serverSide = buildBackupCronJob(cluster({ enabled: true, retentionPolicy: '7d' }), 'db-0');
    expect(JSON.stringify(serverSide.spec?.jobTemplate.spec?.template.spec)).not.toContain('retention');
  });

  it('prunes server-side logical backups through the segment server with replication', () => {
    const repl = (backup: FirebirdCluster['spec']['backup']): FirebirdCluster => ({
      ...cluster(backup),
      spec: { ...cluster(backup).spec, replication: { enabled: true } },
    });
    const pod = buildBackupCronJob(repl({ enabled: true, retentionPolicy: '7d', verify: true }), 'db-0').spec!.jobTemplate.spec!.template.spec!;
    const [c] = pod.containers;
    const args = c.args![0];
    // after the backup and its verification, never before
    expect(args.indexOf('action_backup')).toBeLessThan(args.indexOf('backup-file.pl list'));
    expect(args.indexOf('restored and validated')).toBeLessThan(args.indexOf('backup-file.pl list'));
    expect(args).toContain(`grep -E '^backup-db-[0-9]{8}T[0-9]{6}Z[.]fbk$'`);
    expect(args).toContain(`perl /etc/firebird-operator/backup-file.pl remove "$k"`);
    expect(args).toContain(`- ${7 * 86400} ))`);
    expect(c.env?.find((e) => e.name === 'SEGMENT_PORT')?.value).toBe('3051');
    expect(c.volumeMounts?.map((m) => m.name)).toEqual(['cluster-config']);
    expect(pod.volumes).toEqual([{ name: 'cluster-config', configMap: { name: 'db-config' } }]);

    const sched = buildScheduledBackupCronJob(scheduled({ retentionPolicy: '1w' }), repl(undefined), 'db-0');
    expect(sched.spec!.jobTemplate.spec!.template.spec!.containers[0].args![0]).toContain('^backup-db[.]nightly-');
    // nbackup chains span schedules: never pruned
    const physical = buildBackupCronJob(repl({ enabled: true, type: 'physical', retentionPolicy: '7d' }), 'db-0');
    expect(JSON.stringify(physical.spec?.jobTemplate.spec?.template.spec)).not.toContain('retention');
    expect(serverSideRetentionScript('x', 60)).toMatch(/^rt=\$\(mktemp -d\); .*; rm -rf "\$rt"$/);
  });

  it('validates the retentionPolicy format', () => {
    expect(() => validateClusterSpec(cluster({ enabled: true, retentionPolicy: '7 days' }))).toThrow(/retentionPolicy/);
    expect(() => validateClusterSpec(cluster({ enabled: true, retentionPolicy: '7d' }))).not.toThrow();
    expect(() => validateScheduledBackupSpec(scheduled({ retentionPolicy: 'forever' }))).toThrow(/retentionPolicy/);
  });
});
