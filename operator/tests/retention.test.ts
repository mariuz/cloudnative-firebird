import { describe, it, expect } from 'vitest';
import {
  buildBackupCronJob,
  buildScheduledBackupCronJob,
  retentionSeconds,
  s3RetentionScript,
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

  it('does not prune without a retentionPolicy, or for server-side backups', () => {
    const cron = buildBackupCronJob(cluster({ enabled: true, s3 }), 'db-0');
    expect(uploadArgs(cron.spec?.jobTemplate.spec?.template.spec)).not.toContain('retention');
    const serverSide = buildBackupCronJob(cluster({ enabled: true, retentionPolicy: '7d' }), 'db-0');
    expect(JSON.stringify(serverSide.spec?.jobTemplate.spec?.template.spec)).not.toContain('retention');
  });

  it('validates the retentionPolicy format', () => {
    expect(() => validateClusterSpec(cluster({ enabled: true, retentionPolicy: '7 days' }))).toThrow(/retentionPolicy/);
    expect(() => validateClusterSpec(cluster({ enabled: true, retentionPolicy: '7d' }))).not.toThrow();
    expect(() => validateScheduledBackupSpec(scheduled({ retentionPolicy: 'forever' }))).toThrow(/retentionPolicy/);
  });
});
