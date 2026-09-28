import { describe, it, expect, vi, Mock } from 'vitest';
import { KubeConfig, V1PodSpec } from '@kubernetes/client-node';
import {
  buildBackupJob,
  buildJournalArchiveCronJob,
  buildRestoreJob,
  buildScheduledBackupCronJob,
  s3ClientEnv,
} from '../src/utils/backup';
import { buildStatefulSet, statefulSetNeedsUpdate } from '../src/utils/resources';
import { buildFencingJob } from '../src/utils/fencing';
import { buildFailoverJob, buildSwitchoverJob } from '../src/utils/switchover';
import { buildUserJob } from '../src/utils/users';
import { validateBackupSpec, validateClusterSpec } from '../src/utils/validation';
import { FirebirdBackupController } from '../src/controllers/backup.controller';
import { FirebirdUserController } from '../src/controllers/user.controller';
import {
  FirebirdBackup,
  FirebirdCluster,
  FirebirdRestore,
  FirebirdScheduledBackup,
  FirebirdUser,
  RECONCILIATION_DISABLED_ANNOTATION,
} from '../src/types';

const cluster = (spec: Partial<FirebirdCluster['spec']> = {}): FirebirdCluster => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdCluster',
  metadata: { name: 'db', namespace: 'default', uid: 'c' },
  spec: {
    instances: 3,
    storage: { size: '1Gi' },
    serviceAccountName: 'firebird-sa',
    replication: { enabled: true, journalArchiveS3: { bucket: 'b' } },
    ...spec,
  },
});
const s3 = { bucket: 'b', region: 'eu-west-1' };
const backup: FirebirdBackup = {
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdBackup',
  metadata: { name: 'nightly', namespace: 'default', uid: 'b' },
  spec: { clusterName: 'db', s3 },
};

describe('serviceAccountName', () => {
  it('runs the instance pods and every Job of the cluster with the service account', () => {
    const c = cluster();
    const restore: FirebirdRestore = {
      apiVersion: 'firebird.cloudnative-firebird.io/v1',
      kind: 'FirebirdRestore',
      metadata: { name: 'r', namespace: 'default', uid: 'r' },
      spec: { clusterName: 'db', backupPath: 'x.fbk', s3 },
    };
    const scheduled: FirebirdScheduledBackup = {
      apiVersion: 'firebird.cloudnative-firebird.io/v1',
      kind: 'FirebirdScheduledBackup',
      metadata: { name: 's', namespace: 'default', uid: 's' },
      spec: { clusterName: 'db', schedule: '0 2 * * *' },
    };
    const user = {
      apiVersion: 'firebird.cloudnative-firebird.io/v1',
      kind: 'FirebirdUser',
      metadata: { name: 'app', namespace: 'default', uid: 'u' },
      spec: { clusterName: 'db', passwordSecret: { name: 'pw' } },
    } as FirebirdUser;
    const podSpecs: Array<[string, V1PodSpec | undefined]> = [
      ['statefulset', buildStatefulSet(c).spec?.template.spec],
      ['backup', buildBackupJob(backup, c, 'db-0').spec?.template.spec],
      ['restore', buildRestoreJob(restore, c, { type: 'logical', path: 'x.fbk', s3 }, 'db-0').spec?.template.spec],
      ['scheduled backup', buildScheduledBackupCronJob(scheduled, c, 'db-0').spec?.jobTemplate.spec?.template.spec],
      ['journal archive', buildJournalArchiveCronJob(c, 'db-0')?.spec?.jobTemplate.spec?.template.spec],
      ['fencing', buildFencingJob(c, 'db-1', 'fence').spec?.template.spec],
      ['switchover', buildSwitchoverJob(c, { from: 'db-0', target: 'db-1', replicas: [] }).spec?.template.spec],
      ['failover', buildFailoverJob(c, ['db-1']).spec?.template.spec],
      [
        'user',
        buildUserJob(c, user, { action: 'apply', instances: ['db-0'], grantInstances: ['db-0'], hash: 'h', targets: '[]' })
          .spec?.template.spec,
      ],
    ];
    for (const [what, spec] of podSpecs) {
      expect(spec?.serviceAccountName, what).toBe('firebird-sa');
    }
    expect(buildStatefulSet(cluster({ serviceAccountName: undefined })).spec?.template.spec?.serviceAccountName).toBeUndefined();
  });

  it('rolls the instances when the service account changes', () => {
    const existing = buildStatefulSet(cluster());
    expect(statefulSetNeedsUpdate(existing, buildStatefulSet(cluster()))).toBe(false);
    expect(statefulSetNeedsUpdate(existing, buildStatefulSet(cluster({ serviceAccountName: 'other' })))).toBe(true);
    expect(statefulSetNeedsUpdate(existing, buildStatefulSet(cluster({ serviceAccountName: undefined })))).toBe(true);
  });

  it('is validated as a Kubernetes name', () => {
    expect(() => validateClusterSpec(cluster({ serviceAccountName: 'Bad_Name' }))).toThrow(/serviceAccountName/);
  });
});

describe('S3 without static keys', () => {
  it('passes no keys when secretRef is not set, so the aws CLI uses workload identity', () => {
    expect(s3ClientEnv(s3)).toEqual([{ name: 'AWS_DEFAULT_REGION', value: 'eu-west-1' }]);
    expect(s3ClientEnv({ ...s3, secretRef: { name: 'keys' } }).map((e) => e.name)).toEqual([
      'AWS_ACCESS_KEY_ID',
      'AWS_SECRET_ACCESS_KEY',
      'AWS_DEFAULT_REGION',
    ]);
    expect(() => validateBackupSpec(backup)).not.toThrow();
    expect(() => validateClusterSpec(cluster({ backup: { enabled: true, s3 } }))).not.toThrow();
    expect(() => validateBackupSpec({ ...backup, spec: { ...backup.spec, s3: { ...s3, secretRef: { name: '' } } } })).toThrow();
  });
});

describe('reconciliationDisabled', () => {
  const paused = <T extends { metadata: object }>(obj: T): T => ({
    ...obj,
    metadata: { ...obj.metadata, annotations: { [RECONCILIATION_DISABLED_ANNOTATION]: 'true' } },
  });
  const apis = () => {
    const api: Record<string, Mock> = {};
    const kubeConfig = new KubeConfig();
    vi.spyOn(kubeConfig, 'makeApiClient').mockReturnValue(
      new Proxy({}, { get: (_t, p: string) => (api[p] ??= vi.fn().mockResolvedValue({})) }) as never,
    );
    return { kubeConfig, api };
  };

  it('leaves a paused backup, scheduled backup and restore alone', async () => {
    const { kubeConfig, api } = apis();
    const controller = new FirebirdBackupController(kubeConfig);
    await controller.reconcileBackup(paused(backup));
    await controller.reconcileScheduledBackup(
      paused({ ...backup, kind: 'FirebirdScheduledBackup', spec: { clusterName: 'db', schedule: '0 2 * * *' } }),
    );
    await controller.reconcileRestore(
      paused({ ...backup, kind: 'FirebirdRestore', spec: { clusterName: 'db', backupPath: 'x.fbk' } }),
    );
    expect(Object.keys(api)).toEqual([]);
  });

  it('releases the finalizer of a paused user being deleted, without dropping the Firebird user', async () => {
    const { kubeConfig, api } = apis();
    const controller = new FirebirdUserController(kubeConfig);
    const user = paused({
      apiVersion: 'firebird.cloudnative-firebird.io/v1',
      kind: 'FirebirdUser',
      metadata: {
        name: 'app',
        namespace: 'default',
        resourceVersion: '7',
        deletionTimestamp: new Date(),
        finalizers: ['firebird.cloudnative-firebird.io/drop-user'],
      },
      spec: { clusterName: 'db', passwordSecret: { name: 'pw' }, reclaimPolicy: 'delete' },
    } as FirebirdUser);
    await controller.reconcileUser(user);
    expect(api.createNamespacedJob).toBeUndefined();
    expect(api.patchNamespacedCustomObject.mock.calls[0][0].body[1]).toEqual({
      op: 'add',
      path: '/metadata/finalizers',
      value: [],
    });
  });
});
