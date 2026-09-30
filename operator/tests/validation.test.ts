import { describe, it, expect } from 'vitest';
import {
  validateClusterSpec,
  validateBackupSpec,
  validateRestoreSpec,
  validateScheduledBackupSpec,
  ValidationError,
} from '../src/utils/validation';
import { FirebirdCluster } from '../src/types';

const makeCluster = (overrides: Partial<FirebirdCluster['spec']> = {}): FirebirdCluster => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdCluster',
  metadata: {
    name: 'test-cluster',
    namespace: 'default',
  },
  spec: {
    instances: 1,
    storage: { size: '1Gi' },
    ...overrides,
  },
});

describe('validateClusterSpec', () => {
  it('does not throw for a valid cluster spec', () => {
    const cluster = makeCluster();
    expect(() => validateClusterSpec(cluster)).not.toThrow();
  });

  it.each(['10GB', 'lots', '0', '-5Gi'])('throws for invalid storage size %j', (size) => {
    const cluster = makeCluster({ storage: { size } });
    expect(() => validateClusterSpec(cluster)).toThrow(/Invalid storage size/);
  });

  it.each(['app.fdb', 'my_db-2.fdb'])('accepts databaseName %j', (databaseName) => {
    expect(() => validateClusterSpec(makeCluster({ databaseName }))).not.toThrow();
  });

  it.each(['../etc/passwd', 'db.fdb; rm -rf /', '/abs/path.fdb', ''])('rejects databaseName %j', (databaseName) => {
    expect(() => validateClusterSpec(makeCluster({ databaseName }))).toThrow(/Invalid databaseName/);
  });

  it('accepts readOnlyRouting with a valid maxLagSeconds', () => {
    const cluster = makeCluster({
      replication: { enabled: true, readOnlyRouting: { enabled: true, maxLagSeconds: 0 } },
    });
    expect(() => validateClusterSpec(cluster)).not.toThrow();
  });

  it('throws for a negative readOnlyRouting maxLagSeconds', () => {
    const cluster = makeCluster({
      replication: { enabled: true, readOnlyRouting: { enabled: true, maxLagSeconds: -1 } },
    });
    expect(() => validateClusterSpec(cluster)).toThrow(/maxLagSeconds/);
  });

  it('throws ValidationError if instances is less than 1', () => {
    const cluster = makeCluster({ instances: 0 });
    expect(() => validateClusterSpec(cluster)).toThrow(ValidationError);
    expect(() => validateClusterSpec(cluster)).toThrow(/Must be an integer between 1 and 10/);
  });

  it('throws ValidationError if instances is greater than 10', () => {
    const cluster = makeCluster({ instances: 11 });
    expect(() => validateClusterSpec(cluster)).toThrow(ValidationError);
    expect(() => validateClusterSpec(cluster)).toThrow(/Must be an integer between 1 and 10/);
  });

  it('throws ValidationError if instances is a non-integer number', () => {
    const cluster = makeCluster({ instances: 2.5 as unknown as number });
    expect(() => validateClusterSpec(cluster)).toThrow(ValidationError);
  });

  it('throws ValidationError if storage size is empty', () => {
    const cluster = makeCluster({ storage: { size: '' } });
    expect(() => validateClusterSpec(cluster)).toThrow(ValidationError);
    expect(() => validateClusterSpec(cluster)).toThrow(/Storage size is required/);
  });

  it('throws ValidationError if superuserSecret name is empty', () => {
    const cluster = makeCluster({ superuserSecret: { name: '   ' } });
    expect(() => validateClusterSpec(cluster)).toThrow(ValidationError);
    expect(() => validateClusterSpec(cluster)).toThrow(/superuserSecret name cannot be empty/);
  });

  it('throws ValidationError for invalid serviceType', () => {
    const cluster = makeCluster({ serviceType: 'InvalidType' as unknown as 'ClusterIP' });
    expect(() => validateClusterSpec(cluster)).toThrow(ValidationError);
    expect(() => validateClusterSpec(cluster)).toThrow(/Invalid serviceType/);
  });

  it('throws ValidationError for invalid replication mode', () => {
    const cluster = makeCluster({ replication: { enabled: true, mode: 'invalid' as unknown as 'sync' } });
    expect(() => validateClusterSpec(cluster)).toThrow(ValidationError);
    expect(() => validateClusterSpec(cluster)).toThrow(/Invalid replication mode/);
  });

  it('throws ValidationError for invalid backup schedule expression', () => {
    const cluster = makeCluster({ backup: { enabled: true, schedule: 'invalid cron' } });
    expect(() => validateClusterSpec(cluster)).toThrow(ValidationError);
    expect(() => validateClusterSpec(cluster)).toThrow(/Invalid backup schedule cron expression/);
  });

  it('throws ValidationError for invalid backup type', () => {
    const cluster = makeCluster({ backup: { enabled: true, type: 'invalid' as unknown as 'logical' } });
    expect(() => validateClusterSpec(cluster)).toThrow(ValidationError);
    expect(() => validateClusterSpec(cluster)).toThrow(/Invalid backup type/);
  });

  it('throws ValidationError for invalid backup level', () => {
    const cluster = makeCluster({ backup: { enabled: true, type: 'physical', level: 5 as unknown as 0 } });
    expect(() => validateClusterSpec(cluster)).toThrow(ValidationError);
    expect(() => validateClusterSpec(cluster)).toThrow(/Invalid physical backup level/);
  });

  it('passes validation for physical backup with valid level', () => {
    const cluster = makeCluster({ backup: { enabled: true, type: 'physical', level: 1 } });
    expect(() => validateClusterSpec(cluster)).not.toThrow();
  });

  it('passes validation for valid backup schedule expression', () => {
    const cluster = makeCluster({ backup: { enabled: true, schedule: '0 2 * * *' } });
    expect(() => validateClusterSpec(cluster)).not.toThrow();
  });

  it('throws ValidationError for invalid autoSweep schedule expression', () => {
    const cluster = makeCluster({ autoSweep: { enabled: true, schedule: 'bad schedule' } });
    expect(() => validateClusterSpec(cluster)).toThrow(ValidationError);
    expect(() => validateClusterSpec(cluster)).toThrow(/Invalid autoSweep schedule cron expression/);
  });

  it('passes validation for valid autoSweep schedule expression', () => {
    const cluster = makeCluster({ autoSweep: { enabled: true, schedule: '0 3 * * *' } });
    expect(() => validateClusterSpec(cluster)).not.toThrow();
  });

  it('passes validation for valid serviceType values', () => {
    expect(() => validateClusterSpec(makeCluster({ serviceType: 'ClusterIP' }))).not.toThrow();
    expect(() => validateClusterSpec(makeCluster({ serviceType: 'NodePort' }))).not.toThrow();
    expect(() => validateClusterSpec(makeCluster({ serviceType: 'LoadBalancer' }))).not.toThrow();
  });

  it('accepts async replication and rejects sync until it is implemented', () => {
    expect(() => validateClusterSpec(makeCluster({ replication: { enabled: true, mode: 'async' } }))).not.toThrow();
    expect(() => validateClusterSpec(makeCluster({ replication: { enabled: true, mode: 'sync' } }))).toThrow(
      /'sync' is not supported yet/,
    );
  });

  it.each(['/var/lib/firebird/data/repl', '/var/lib/firebird/data/a/b_c-1'])(
    'accepts journalDirectory %j on the data volume',
    (journalDirectory) => {
      expect(() => validateClusterSpec(makeCluster({ replication: { enabled: true, journalDirectory } }))).not.toThrow();
    },
  );

  it.each(['/tmp/journals', '/var/lib/firebird/data', '/var/lib/firebird/data/x; rm -rf /'])(
    'rejects journalDirectory %j',
    (journalDirectory) => {
      expect(() => validateClusterSpec(makeCluster({ replication: { enabled: true, journalDirectory } }))).toThrow(
        /journalDirectory/,
      );
    },
  );

  it.each([
    ['archiveTimeoutSeconds', 0],
    ['segmentRetentionHours', -1],
    ['maxSegmentRetentionHours', 0],
    ['archiveTimeoutSeconds', 1.5],
  ])('rejects replication %s = %s', (field, value) => {
    expect(() =>
      validateClusterSpec(makeCluster({ replication: { enabled: true, [field]: value } })),
    ).toThrow(new RegExp(field));
  });

  it('throws ValidationError when replication journalArchiveS3 bucket is empty', () => {
    const cluster = makeCluster({
      replication: {
        enabled: true,
        journalArchiveS3: { bucket: '', secretRef: { name: 'secret' } },
      },
    });
    expect(() => validateClusterSpec(cluster)).toThrow(ValidationError);
    expect(() => validateClusterSpec(cluster)).toThrow(/Replication journalArchiveS3 bucket name is required/);
  });

  it('throws ValidationError when bootstrap clone sourceCluster is empty', () => {
    const cluster = makeCluster({
      bootstrap: { clone: { sourceCluster: '' } },
    });
    expect(() => validateClusterSpec(cluster)).toThrow(ValidationError);
    expect(() => validateClusterSpec(cluster)).toThrow(/Bootstrap clone sourceCluster is required/);
  });

  it('throws ValidationError when bootstrap clone sourceCluster is self', () => {
    const cluster = makeCluster({
      bootstrap: { clone: { sourceCluster: 'test-cluster' } },
    });
    expect(() => validateClusterSpec(cluster)).toThrow(ValidationError);
    expect(() => validateClusterSpec(cluster)).toThrow(/Bootstrap clone sourceCluster cannot be the cluster itself/);
  });

  it('throws ValidationError when bootstrap recovery is missing sourcePath and s3', () => {
    const cluster = makeCluster({
      bootstrap: { recovery: {} },
    });
    expect(() => validateClusterSpec(cluster)).toThrow(ValidationError);
    expect(() => validateClusterSpec(cluster)).toThrow(/Bootstrap recovery requires either sourcePath or s3 configuration/);
  });

  it('throws ValidationError for missing S3 bucket name', () => {
    const cluster = makeCluster({
      backup: {
        enabled: true,
        s3: { bucket: '', secretRef: { name: 's3-secret' } },
      },
    });
    expect(() => validateClusterSpec(cluster)).toThrow(/S3 backup bucket name is required/);
  });

  it('throws ValidationError for invalid exporter sidecar port', () => {
    const cluster = makeCluster({
      monitoring: {
        exporter: { enabled: true, port: 80 },
      },
    });
    expect(() => validateClusterSpec(cluster)).toThrow(/Invalid exporter port/);
  });

  it('throws ValidationError for empty TLS issuerRef name', () => {
    const cluster = makeCluster({
      tls: { enabled: true, issuerRef: { name: ' ' } },
    });
    expect(() => validateClusterSpec(cluster)).toThrow(/TLS issuerRef name cannot be empty/);
  });
});

describe('validateBackupSpec & validateRestoreSpec', () => {
  it('throws ValidationError if FirebirdBackup clusterName is missing', () => {
    const backup = {
      apiVersion: 'firebird.cloudnative-firebird.io/v1',
      kind: 'FirebirdBackup',
      metadata: { name: 'test-backup' },
      spec: { clusterName: '' },
    };
    expect(() => validateBackupSpec(backup)).toThrow(/clusterName is required/);
  });

  it('throws ValidationError if FirebirdRestore clusterName is missing', () => {
    const restore = {
      apiVersion: 'firebird.cloudnative-firebird.io/v1',
      kind: 'FirebirdRestore',
      metadata: { name: 'test-restore' },
      spec: { clusterName: '' },
    };
    expect(() => validateRestoreSpec(restore)).toThrow(/clusterName is required/);
  });

  it('throws ValidationError if FirebirdScheduledBackup schedule is invalid', () => {
    const scheduledBackup = {
      apiVersion: 'firebird.cloudnative-firebird.io/v1',
      kind: 'FirebirdScheduledBackup',
      metadata: { name: 'bad-schedule' },
      spec: { clusterName: 'test-cluster', schedule: 'invalid cron' },
    };
    expect(() => validateScheduledBackupSpec(scheduledBackup)).toThrow(/Invalid scheduled backup schedule/);
  });
});

describe('backup destinations and restore sources', () => {
  const s3 = { bucket: 'b', secretRef: { name: 's' } };
  const backup = (spec: object) => ({
    apiVersion: 'firebird.cloudnative-firebird.io/v1' as const,
    kind: 'FirebirdBackup' as const,
    metadata: { name: 'b' },
    spec: { clusterName: 'c', ...spec },
  });
  const restore = (spec: object) => ({
    apiVersion: 'firebird.cloudnative-firebird.io/v1' as const,
    kind: 'FirebirdRestore' as const,
    metadata: { name: 'r' },
    spec: { clusterName: 'c', ...spec },
  });

  it('accepts physical backups to S3, with or without replication', () => {
    expect(() => validateBackupSpec(backup({ type: 'physical', s3 }))).not.toThrow();
    expect(() =>
      validateScheduledBackupSpec({ spec: { clusterName: 'c', schedule: '0 1 * * *', type: 'physical', s3 } }),
    ).not.toThrow();
    expect(() => validateClusterSpec(makeCluster({ backup: { enabled: true, type: 'physical', s3 } }))).not.toThrow();
    expect(() =>
      validateClusterSpec(makeCluster({ backup: { enabled: true, type: 'physical', s3 }, replication: { enabled: true } })),
    ).not.toThrow();
    expect(() => validateBackupSpec(backup({ type: 'logical', s3 }))).not.toThrow();
  });

  it('verifies logical backups only', () => {
    expect(() => validateBackupSpec(backup({ type: 'physical', verify: true }))).toThrow(/verify is supported for logical/);
    expect(() =>
      validateScheduledBackupSpec({ spec: { clusterName: 'c', schedule: '0 1 * * *', type: 'physical', verify: true } }),
    ).toThrow(/verify/);
    expect(() => validateClusterSpec(makeCluster({ backup: { enabled: true, type: 'physical', verify: true } }))).toThrow(
      /verify/,
    );
    expect(() => validateBackupSpec(backup({ verify: true }))).not.toThrow();
    expect(() => validateBackupSpec(backup({ type: 'physical', verify: false }))).not.toThrow();
  });

  it('requires a bucket and credentials for S3', () => {
    expect(() => validateBackupSpec(backup({ s3: { bucket: '', secretRef: { name: 's' } } }))).toThrow(/bucket/);
  });

  it('requires exactly one of backupName and backupPath', () => {
    expect(() => validateRestoreSpec(restore({}))).toThrow(/backupName or backupPath/);
    expect(() => validateRestoreSpec(restore({ backupName: 'x', backupPath: 'y' }))).toThrow(/mutually exclusive/);
    expect(() => validateRestoreSpec(restore({ backupName: 'x' }))).not.toThrow();
  });

  it('only accepts a plain file name as targetDatabase', () => {
    expect(() => validateRestoreSpec(restore({ backupName: 'x', targetDatabase: '../etc/x.fdb' }))).toThrow(/targetDatabase/);
    expect(() => validateRestoreSpec(restore({ backupName: 'x', targetDatabase: 'copy.fdb' }))).not.toThrow();
  });

  it('keeps server-side backup paths inside the data directory', () => {
    expect(() => validateRestoreSpec(restore({ backupPath: '../x.fbk' }))).toThrow(/\.\./);
    expect(() => validateRestoreSpec(restore({ backupPath: '/etc/passwd' }))).toThrow(/inside/);
    expect(() => validateRestoreSpec(restore({ backupPath: '/var/lib/firebird/data/x.fbk' }))).not.toThrow();
    expect(() => validateRestoreSpec(restore({ backupPath: 'nightly/x.fbk', s3 }))).not.toThrow();
  });

  it('accepts physical restores from S3 and rejects misplaced incremental paths', () => {
    expect(() =>
      validateRestoreSpec(restore({ restoreType: 'physical', backupPath: 'x.nbk', incrementalBackupPaths: ['p/y.nbk'], s3 })),
    ).not.toThrow();
    expect(() => validateRestoreSpec(restore({ backupPath: 'x.fbk', incrementalBackupPaths: ['y'] }))).toThrow(/physical/);
    expect(() =>
      validateRestoreSpec(restore({ restoreType: 'physical', backupPath: 'l0.nbk', incrementalBackupPaths: ['l1.nbk'] })),
    ).not.toThrow();
    expect(() =>
      validateRestoreSpec(restore({ restoreType: 'physical', backupPath: 'l0.nbk', incrementalBackupPaths: ['../l1.nbk'] })),
    ).toThrow(/\.\./);
  });

  it('rejects bootstrap recovery combined with clone', () => {
    expect(() =>
      validateClusterSpec(makeCluster({ bootstrap: { recovery: { sourcePath: '/x.fbk' }, clone: { sourceCluster: 'src' } } })),
    ).toThrow(/mutually exclusive/);
  });
});

