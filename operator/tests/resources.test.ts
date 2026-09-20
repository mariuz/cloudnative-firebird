import { describe, it, expect } from 'vitest';
import {
  buildService,
  buildStatefulSet,
  buildHeadlessService,
  buildReplicaService,
  buildBackupCronJob,
  cronJobNeedsUpdate,
  buildPodMonitor,
  buildPodDisruptionBudget,
  podDisruptionBudgetNeedsUpdate,
  buildConfigMap,
  configMapNeedsUpdate,
  buildAutoSweepCronJob,
  autoSweepCronJobNeedsUpdate,
  buildNetworkPolicy,
  networkPolicyNeedsUpdate,
  buildCertificate,
  buildLease,
  buildBackupJob,
  buildRestoreJob,
  buildDiagnosticsCronJob,
  buildGrafanaDashboardConfigMap,
  buildScheduledBackupCronJob,
  buildJournalArchiveCronJob,
  clusterLabels,
  statefulSetNeedsUpdate,
} from '../src/utils/resources';
import { FirebirdCluster, FirebirdScheduledBackup, DEFAULT_FIREBIRD_IMAGE } from '../src/types';

const makeCluster = (overrides: Partial<FirebirdCluster['spec']> = {}): FirebirdCluster => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdCluster',
  metadata: {
    name: 'test-cluster',
    namespace: 'default',
    uid: 'test-uid-1234',
  },
  spec: {
    instances: 1,
    storage: { size: '1Gi' },
    ...overrides,
  },
});

describe('clusterLabels', () => {
  it('returns required labels for a cluster', () => {
    const labels = clusterLabels('my-cluster');
    expect(labels['app.kubernetes.io/name']).toBe('firebird');
    expect(labels['app.kubernetes.io/managed-by']).toBe('cloudnative-firebird-operator');
    expect(labels['firebird.cloudnative-firebird.io/cluster']).toBe('my-cluster');
  });
});

describe('buildStatefulSet', () => {
  it('sets serviceName to the headless service name', () => {
    const cluster = makeCluster();
    const sts = buildStatefulSet(cluster);
    expect(sts.spec?.serviceName).toBe('test-cluster-headless');
  });

  it('creates a StatefulSet with the correct name and namespace', () => {
    const cluster = makeCluster();
    const sts = buildStatefulSet(cluster);
    expect(sts.metadata?.name).toBe('test-cluster');
    expect(sts.metadata?.namespace).toBe('default');
  });

  it('uses the default Firebird image when imageName is not specified', () => {
    const cluster = makeCluster();
    const sts = buildStatefulSet(cluster);
    const container = sts.spec?.template?.spec?.containers?.[0];
    expect(container?.image).toBe(DEFAULT_FIREBIRD_IMAGE);
  });

  it('uses the specified imageName when provided', () => {
    const cluster = makeCluster({ imageName: 'firebirdsql/firebird:4.0' });
    const sts = buildStatefulSet(cluster);
    const container = sts.spec?.template?.spec?.containers?.[0];
    expect(container?.image).toBe('firebirdsql/firebird:4.0');
  });

  it('sets the correct number of replicas', () => {
    const cluster = makeCluster({ instances: 3 });
    const sts = buildStatefulSet(cluster);
    expect(sts.spec?.replicas).toBe(3);
  });

  it('uses ISC_PASSWORD from secret when superuserSecret is set', () => {
    const cluster = makeCluster({
      superuserSecret: { name: 'my-secret' },
    });
    const sts = buildStatefulSet(cluster);
    const container = sts.spec?.template?.spec?.containers?.[0];
    const passwordEnv = container?.env?.find((e: { name: string }) => e.name === 'ISC_PASSWORD');
    expect(passwordEnv?.valueFrom).toEqual({
      secretKeyRef: { name: 'my-secret', key: 'password' },
    });
  });

  it('uses default ISC_PASSWORD when no superuserSecret is set', () => {
    const cluster = makeCluster();
    const sts = buildStatefulSet(cluster);
    const container = sts.spec?.template?.spec?.containers?.[0];
    const passwordEnv = container?.env?.find((e: { name: string }) => e.name === 'ISC_PASSWORD');
    expect(passwordEnv?.value).toBe('masterkey');
  });

  it('includes a volumeClaimTemplate for firebird-data', () => {
    const cluster = makeCluster({ storage: { size: '5Gi' } });
    const sts = buildStatefulSet(cluster);
    const vct = sts.spec?.volumeClaimTemplates?.[0];
    expect(vct?.metadata?.name).toBe('firebird-data');
    expect(vct?.spec?.resources?.requests?.['storage']).toBe('5Gi');
  });

  it('sets storageClass when specified', () => {
    const cluster = makeCluster({ storage: { size: '1Gi', storageClass: 'fast-ssd' } });
    const sts = buildStatefulSet(cluster);
    const vct = sts.spec?.volumeClaimTemplates?.[0];
    expect(vct?.spec?.storageClassName).toBe('fast-ssd');
  });

  it('sets ownerReference pointing to the FirebirdCluster', () => {
    const cluster = makeCluster();
    const sts = buildStatefulSet(cluster);
    const ownerRef = sts.metadata?.ownerReferences?.[0];
    expect(ownerRef?.kind).toBe('FirebirdCluster');
    expect(ownerRef?.name).toBe('test-cluster');
    expect(ownerRef?.uid).toBe('test-uid-1234');
    expect(ownerRef?.controller).toBe(true);
  });

  it('exposes port 3050', () => {
    const cluster = makeCluster();
    const sts = buildStatefulSet(cluster);
    const container = sts.spec?.template?.spec?.containers?.[0];
    const port = container?.ports?.[0];
    expect(port?.containerPort).toBe(3050);
  });

  it('includes additional env vars from spec.env', () => {
    const cluster = makeCluster({
      env: [{ name: 'FIREBIRD_DATABASE', value: 'mydb.fdb' }],
    });
    const sts = buildStatefulSet(cluster);
    const container = sts.spec?.template?.spec?.containers?.[0];
    const dbEnv = container?.env?.find((e: { name: string }) => e.name === 'FIREBIRD_DATABASE');
    expect(dbEnv?.value).toBe('mydb.fdb');
  });

  it('sets resources when provided', () => {
    const cluster = makeCluster({
      resources: {
        requests: { cpu: '100m', memory: '256Mi' },
        limits: { cpu: '500m', memory: '512Mi' },
      },
    });
    const sts = buildStatefulSet(cluster);
    const container = sts.spec?.template?.spec?.containers?.[0];
    expect(container?.resources?.requests?.['cpu']).toBe('100m');
    expect(container?.resources?.limits?.['memory']).toBe('512Mi');
  });
});

describe('buildService', () => {
  it('creates a ClusterIP service with the cluster name', () => {
    const cluster = makeCluster();
    const svc = buildService(cluster);
    expect(svc.metadata?.name).toBe('test-cluster');
    expect(svc.spec?.type).toBe('ClusterIP');
  });

  it('exposes port 3050', () => {
    const cluster = makeCluster();
    const svc = buildService(cluster);
    expect(svc.spec?.ports?.[0]?.port).toBe(3050);
  });

  it('sets ownerReference pointing to the FirebirdCluster', () => {
    const cluster = makeCluster();
    const svc = buildService(cluster);
    const ownerRef = svc.metadata?.ownerReferences?.[0];
    expect(ownerRef?.kind).toBe('FirebirdCluster');
    expect(ownerRef?.name).toBe('test-cluster');
  });
});

describe('buildHeadlessService', () => {
  it('creates a headless service named <cluster>-headless', () => {
    const cluster = makeCluster();
    const svc = buildHeadlessService(cluster);
    expect(svc.metadata?.name).toBe('test-cluster-headless');
    expect(svc.spec?.clusterIP).toBe('None');
  });

  it('sets publishNotReadyAddresses to true', () => {
    const cluster = makeCluster();
    const svc = buildHeadlessService(cluster);
    expect(svc.spec?.publishNotReadyAddresses).toBe(true);
  });
});

describe('statefulSetNeedsUpdate', () => {
  it('returns false when replicas and image are the same', () => {
    const cluster = makeCluster({ instances: 1 });
    const sts = buildStatefulSet(cluster);
    expect(statefulSetNeedsUpdate(sts, sts)).toBe(false);
  });

  it('returns true when replicas differ', () => {
    const cluster1 = makeCluster({ instances: 1 });
    const cluster2 = makeCluster({ instances: 3 });
    const sts1 = buildStatefulSet(cluster1);
    const sts2 = buildStatefulSet(cluster2);
    expect(statefulSetNeedsUpdate(sts1, sts2)).toBe(true);
  });

  it('returns true when image differs', () => {
    const cluster1 = makeCluster({ imageName: 'firebirdsql/firebird:3.0' });
    const cluster2 = makeCluster({ imageName: 'firebirdsql/firebird:4.0' });
    const sts1 = buildStatefulSet(cluster1);
    const sts2 = buildStatefulSet(cluster2);
    expect(statefulSetNeedsUpdate(sts1, sts2)).toBe(true);
  });
});

describe('buildStatefulSet (replication)', () => {
  it('does not set replication env vars when replication is disabled', () => {
    const cluster = makeCluster({ replication: { enabled: false } });
    const sts = buildStatefulSet(cluster);
    const container = sts.spec?.template?.spec?.containers?.[0];
    const replicationEnv = container?.env?.find(
      (e: { name: string }) => e.name === 'FIREBIRD_REPLICATION_ENABLED',
    );
    expect(replicationEnv).toBeUndefined();
  });

  it('does not set replication env vars when replication spec is absent', () => {
    const cluster = makeCluster();
    const sts = buildStatefulSet(cluster);
    const container = sts.spec?.template?.spec?.containers?.[0];
    const replicationEnv = container?.env?.find(
      (e: { name: string }) => e.name === 'FIREBIRD_REPLICATION_ENABLED',
    );
    expect(replicationEnv).toBeUndefined();
  });

  it('sets FIREBIRD_REPLICATION_ENABLED=true when replication is enabled', () => {
    const cluster = makeCluster({ replication: { enabled: true } });
    const sts = buildStatefulSet(cluster);
    const container = sts.spec?.template?.spec?.containers?.[0];
    const replicationEnv = container?.env?.find(
      (e: { name: string }) => e.name === 'FIREBIRD_REPLICATION_ENABLED',
    );
    expect(replicationEnv?.value).toBe('true');
  });

  it('defaults FIREBIRD_REPLICATION_MODE to async when mode is not specified', () => {
    const cluster = makeCluster({ replication: { enabled: true } });
    const sts = buildStatefulSet(cluster);
    const container = sts.spec?.template?.spec?.containers?.[0];
    const modeEnv = container?.env?.find(
      (e: { name: string }) => e.name === 'FIREBIRD_REPLICATION_MODE',
    );
    expect(modeEnv?.value).toBe('async');
  });

  it('sets FIREBIRD_REPLICATION_MODE to sync when mode is sync', () => {
    const cluster = makeCluster({ replication: { enabled: true, mode: 'sync' } });
    const sts = buildStatefulSet(cluster);
    const container = sts.spec?.template?.spec?.containers?.[0];
    const modeEnv = container?.env?.find(
      (e: { name: string }) => e.name === 'FIREBIRD_REPLICATION_MODE',
    );
    expect(modeEnv?.value).toBe('sync');
  });

  it('sets FIREBIRD_REPLICATION_MODE to async when mode is explicitly async', () => {
    const cluster = makeCluster({ replication: { enabled: true, mode: 'async' } });
    const sts = buildStatefulSet(cluster);
    const container = sts.spec?.template?.spec?.containers?.[0];
    const modeEnv = container?.env?.find(
      (e: { name: string }) => e.name === 'FIREBIRD_REPLICATION_MODE',
    );
    expect(modeEnv?.value).toBe('async');
  });

  it('sets FIREBIRD_REPLICATION_JOURNAL_DIR when replication is enabled', () => {
    const cluster = makeCluster({ replication: { enabled: true, journalDirectory: '/custom/journals' } });
    const sts = buildStatefulSet(cluster);
    const container = sts.spec?.template?.spec?.containers?.[0];
    const journalEnv = container?.env?.find(
      (e: { name: string }) => e.name === 'FIREBIRD_REPLICATION_JOURNAL_DIR',
    );
    expect(journalEnv?.value).toBe('/custom/journals');
  });

  it('adds secret hash annotation to pod template when superuserSecretHash is provided', () => {
    const cluster = makeCluster({ superuserSecret: { name: 'my-secret' } });
    const sts = buildStatefulSet(cluster, { superuserSecretHash: 'abc123hash' });
    const annotations = sts.spec?.template?.metadata?.annotations;
    expect(annotations?.['firebird.cloudnative-firebird.io/superuser-secret-hash']).toBe('abc123hash');
  });

  it('creates bootstrap-restore init container when spec.bootstrap.recovery is specified with S3', () => {
    const cluster = makeCluster({
      bootstrap: {
        recovery: {
          s3: {
            bucket: 'my-restore-bucket',
            secretRef: { name: 's3-secret' },
          },
        },
      },
    });
    const sts = buildStatefulSet(cluster);
    const initContainers = sts.spec?.template?.spec?.initContainers;
    expect(initContainers).toHaveLength(1);
    expect(initContainers?.[0].name).toBe('bootstrap-restore');
    expect(initContainers?.[0].args?.[0]).toContain('aws  s3 cp s3://my-restore-bucket/backup.fbk');
  });

  it('creates bootstrap-clone init container when spec.bootstrap.clone is specified', () => {
    const cluster = makeCluster({
      bootstrap: {
        clone: {
          sourceCluster: 'source-db',
          namespace: 'prod',
        },
      },
    });
    const sts = buildStatefulSet(cluster);
    const initContainers = sts.spec?.template?.spec?.initContainers;
    expect(initContainers).toHaveLength(1);
    expect(initContainers?.[0].name).toBe('bootstrap-clone');
    expect(initContainers?.[0].args?.[0]).toContain('Cloning database from source-db in prod');
  });
});

describe('buildJournalArchiveCronJob', () => {
  it('returns null when replication is disabled', () => {
    const cluster = makeCluster({ replication: { enabled: false } });
    expect(buildJournalArchiveCronJob(cluster)).toBeNull();
  });

  it('returns null when journalArchiveS3 is not configured', () => {
    const cluster = makeCluster({ replication: { enabled: true } });
    expect(buildJournalArchiveCronJob(cluster)).toBeNull();
  });

  it('builds a valid CronJob when journalArchiveS3 is configured', () => {
    const cluster = makeCluster({
      replication: {
        enabled: true,
        journalArchiveS3: {
          bucket: 'journal-bucket',
          secretRef: { name: 's3-secret' },
          endpoint: 'https://minio.local:9000',
        },
        archiveSchedule: '*/10 * * * *',
      },
    });
    const cronJob = buildJournalArchiveCronJob(cluster);
    expect(cronJob).not.toBeNull();
    expect(cronJob?.metadata.name).toBe('test-cluster-journal-archive');
    expect(cronJob?.spec?.schedule).toBe('*/10 * * * *');
    const container = cronJob?.spec?.jobTemplate.spec?.template.spec?.containers[0];
    expect(container?.name).toBe('journal-archiver');
    expect(container?.args?.[0]).toContain('aws --endpoint-url https://minio.local:9000 s3 sync /firebird/data/journals/ s3://journal-bucket/journals/ --delete');
  });
});

describe('buildReplicaService', () => {
  it('creates a replica service named <cluster>-replica', () => {
    const cluster = makeCluster();
    const svc = buildReplicaService(cluster);
    expect(svc.metadata?.name).toBe('test-cluster-replica');
    expect(svc.metadata?.namespace).toBe('default');
  });

  it('creates a ClusterIP service', () => {
    const cluster = makeCluster();
    const svc = buildReplicaService(cluster);
    expect(svc.spec?.type).toBe('ClusterIP');
  });

  it('exposes port 3050', () => {
    const cluster = makeCluster();
    const svc = buildReplicaService(cluster);
    expect(svc.spec?.ports?.[0]?.port).toBe(3050);
  });

  it('sets the database-replica component label', () => {
    const cluster = makeCluster();
    const svc = buildReplicaService(cluster);
    expect(svc.metadata?.labels?.['app.kubernetes.io/component']).toBe('database-replica');
  });

  it('sets ownerReference pointing to the FirebirdCluster', () => {
    const cluster = makeCluster();
    const svc = buildReplicaService(cluster);
    const ownerRef = svc.metadata?.ownerReferences?.[0];
    expect(ownerRef?.kind).toBe('FirebirdCluster');
    expect(ownerRef?.name).toBe('test-cluster');
    expect(ownerRef?.uid).toBe('test-uid-1234');
    expect(ownerRef?.controller).toBe(true);
  });

  it('selects the same pods as the primary service via cluster labels', () => {
    const cluster = makeCluster();
    const primarySvc = buildService(cluster);
    const replicaSvc = buildReplicaService(cluster);
    expect(replicaSvc.spec?.selector).toEqual(primarySvc.spec?.selector);
  });
});

describe('buildBackupCronJob', () => {
  it('creates a CronJob named <cluster>-backup', () => {
    const cluster = makeCluster({ backup: { enabled: true } });
    const cronJob = buildBackupCronJob(cluster);
    expect(cronJob.metadata?.name).toBe('test-cluster-backup');
    expect(cronJob.metadata?.namespace).toBe('default');
  });

  it('uses default schedule 0 2 * * * when not specified', () => {
    const cluster = makeCluster({ backup: { enabled: true } });
    const cronJob = buildBackupCronJob(cluster);
    expect(cronJob.spec?.schedule).toBe('0 2 * * *');
  });

  it('uses custom schedule when specified', () => {
    const cluster = makeCluster({ backup: { enabled: true, schedule: '0 4 * * *' } });
    const cronJob = buildBackupCronJob(cluster);
    expect(cronJob.spec?.schedule).toBe('0 4 * * *');
  });

  it('uses default ISC_PASSWORD when superuserSecret is not provided', () => {
    const cluster = makeCluster({ backup: { enabled: true } });
    const cronJob = buildBackupCronJob(cluster);
    const container = cronJob.spec?.jobTemplate?.spec?.template?.spec?.containers?.[0];
    const passwordEnv = container?.env?.find((e: { name: string }) => e.name === 'ISC_PASSWORD');
    expect(passwordEnv?.value).toBe('masterkey');
  });

  it('uses secret reference for ISC_PASSWORD when superuserSecret is provided', () => {
    const cluster = makeCluster({
      backup: { enabled: true },
      superuserSecret: { name: 'backup-secret' },
    });
    const cronJob = buildBackupCronJob(cluster);
    const container = cronJob.spec?.jobTemplate?.spec?.template?.spec?.containers?.[0];
    const passwordEnv = container?.env?.find((e: { name: string }) => e.name === 'ISC_PASSWORD');
    expect(passwordEnv?.valueFrom?.secretKeyRef?.name).toBe('backup-secret');
  });

  it('includes FIREBIRD_RETENTION_POLICY when retentionPolicy is specified', () => {
    const cluster = makeCluster({ backup: { enabled: true, retentionPolicy: '7d' } });
    const cronJob = buildBackupCronJob(cluster);
    const container = cronJob.spec?.jobTemplate?.spec?.template?.spec?.containers?.[0];
    const retentionEnv = container?.env?.find((e: { name: string }) => e.name === 'FIREBIRD_RETENTION_POLICY');
    expect(retentionEnv?.value).toBe('7d');
  });

  it('sets ownerReference pointing to FirebirdCluster', () => {
    const cluster = makeCluster({ backup: { enabled: true } });
    const cronJob = buildBackupCronJob(cluster);
    const ownerRef = cronJob.metadata?.ownerReferences?.[0];
    expect(ownerRef?.kind).toBe('FirebirdCluster');
    expect(ownerRef?.name).toBe('test-cluster');
  });

  it('sets backup component label', () => {
    const cluster = makeCluster({ backup: { enabled: true } });
    const cronJob = buildBackupCronJob(cluster);
    expect(cronJob.metadata?.labels?.['app.kubernetes.io/component']).toBe('backup');
  });
});

describe('cronJobNeedsUpdate', () => {
  it('returns false when schedule and image match', () => {
    const cluster = makeCluster({ backup: { enabled: true } });
    const cj = buildBackupCronJob(cluster);
    expect(cronJobNeedsUpdate(cj, cj)).toBe(false);
  });

  it('returns true when schedule differs', () => {
    const cj1 = buildBackupCronJob(makeCluster({ backup: { enabled: true, schedule: '0 1 * * *' } }));
    const cj2 = buildBackupCronJob(makeCluster({ backup: { enabled: true, schedule: '0 2 * * *' } }));
    expect(cronJobNeedsUpdate(cj1, cj2)).toBe(true);
  });

  it('returns true when image differs', () => {
    const cj1 = buildBackupCronJob(makeCluster({ backup: { enabled: true }, imageName: 'firebirdsql/firebird:3.0' }));
    const cj2 = buildBackupCronJob(makeCluster({ backup: { enabled: true }, imageName: 'firebirdsql/firebird:4.0' }));
    expect(cronJobNeedsUpdate(cj1, cj2)).toBe(true);
  });
});

describe('buildPodMonitor', () => {
  it('creates a PodMonitor custom object with name <cluster>-podmonitor', () => {
    const cluster = makeCluster({ monitoring: { enablePodMonitor: true } });
    const pm = buildPodMonitor(cluster) as { metadata: { name: string; namespace: string }; apiVersion: string; kind: string };
    expect(pm.metadata.name).toBe('test-cluster-podmonitor');
    expect(pm.metadata.namespace).toBe('default');
    expect(pm.apiVersion).toBe('monitoring.coreos.com/v1');
    expect(pm.kind).toBe('PodMonitor');
  });

  it('sets ownerReference pointing to FirebirdCluster', () => {
    const cluster = makeCluster({ monitoring: { enablePodMonitor: true } });
    const pm = buildPodMonitor(cluster) as { metadata: { ownerReferences: Array<{ kind: string; name: string }> } };
    expect(pm.metadata.ownerReferences[0].kind).toBe('FirebirdCluster');
    expect(pm.metadata.ownerReferences[0].name).toBe('test-cluster');
  });

  it('configures metric endpoint for port firebird on /metrics', () => {
    const cluster = makeCluster({ monitoring: { enablePodMonitor: true } });
    const pm = buildPodMonitor(cluster) as { spec: { podMetricsEndpoints: Array<{ port: string; path: string }> } };
    expect(pm.spec.podMetricsEndpoints[0].port).toBe('firebird');
    expect(pm.spec.podMetricsEndpoints[0].path).toBe('/metrics');
  });
});

describe('buildStatefulSet (scheduling & advanced options)', () => {
  it('sets nodeSelector when provided', () => {
    const cluster = makeCluster({ nodeSelector: { 'disktype': 'ssd' } });
    const sts = buildStatefulSet(cluster);
    expect(sts.spec?.template?.spec?.nodeSelector).toEqual({ 'disktype': 'ssd' });
  });

  it('sets affinity when provided', () => {
    const affinity = { nodeAffinity: { requiredDuringSchedulingIgnoredDuringExecution: { nodeSelectorTerms: [] } } };
    const cluster = makeCluster({ affinity });
    const sts = buildStatefulSet(cluster);
    expect(sts.spec?.template?.spec?.affinity).toEqual(affinity);
  });

  it('sets tolerations when provided', () => {
    const tolerations = [{ key: 'dedicated', operator: 'Equal', value: 'database', effect: 'NoSchedule' }];
    const cluster = makeCluster({ tolerations });
    const sts = buildStatefulSet(cluster);
    expect(sts.spec?.template?.spec?.tolerations).toEqual(tolerations);
  });
});

describe('buildService (custom serviceType & annotations)', () => {
  it('sets custom serviceType when specified', () => {
    const cluster = makeCluster({ serviceType: 'LoadBalancer' });
    const svc = buildService(cluster);
    expect(svc.spec?.type).toBe('LoadBalancer');
  });

  it('applies serviceAnnotations when specified', () => {
    const cluster = makeCluster({ serviceAnnotations: { 'service.beta.kubernetes.io/aws-load-balancer-type': 'nlb' } });
    const svc = buildService(cluster);
    expect(svc.metadata?.annotations?.['service.beta.kubernetes.io/aws-load-balancer-type']).toBe('nlb');
  });
});

describe('statefulSetNeedsUpdate (extended properties)', () => {
  it('returns true when nodeSelector differs', () => {
    const sts1 = buildStatefulSet(makeCluster({ nodeSelector: { zone: 'a' } }));
    const sts2 = buildStatefulSet(makeCluster({ nodeSelector: { zone: 'b' } }));
    expect(statefulSetNeedsUpdate(sts1, sts2)).toBe(true);
  });

  it('returns true when env differs', () => {
    const sts1 = buildStatefulSet(makeCluster({ env: [{ name: 'A', value: '1' }] }));
    const sts2 = buildStatefulSet(makeCluster({ env: [{ name: 'A', value: '2' }] }));
    expect(statefulSetNeedsUpdate(sts1, sts2)).toBe(true);
  });

  it('returns true when resources differ', () => {
    const sts1 = buildStatefulSet(makeCluster({ resources: { requests: { memory: '128Mi' } } }));
    const sts2 = buildStatefulSet(makeCluster({ resources: { requests: { memory: '256Mi' } } }));
    expect(statefulSetNeedsUpdate(sts1, sts2)).toBe(true);
  });
});

describe('buildPodDisruptionBudget', () => {
  it('creates a PDB named <cluster>-pdb', () => {
    const cluster = makeCluster({ instances: 2 });
    const pdb = buildPodDisruptionBudget(cluster);
    expect(pdb.metadata?.name).toBe('test-cluster-pdb');
    expect(pdb.metadata?.namespace).toBe('default');
    expect(pdb.spec?.minAvailable).toBe(1);
  });

  it('sets ownerReference pointing to FirebirdCluster', () => {
    const cluster = makeCluster({ instances: 2 });
    const pdb = buildPodDisruptionBudget(cluster);
    const ownerRef = pdb.metadata?.ownerReferences?.[0];
    expect(ownerRef?.kind).toBe('FirebirdCluster');
    expect(ownerRef?.name).toBe('test-cluster');
  });
});

describe('podDisruptionBudgetNeedsUpdate', () => {
  it('returns false when minAvailable matches', () => {
    const pdb = buildPodDisruptionBudget(makeCluster({ instances: 2 }));
    expect(podDisruptionBudgetNeedsUpdate(pdb, pdb)).toBe(false);
  });

  it('returns true when minAvailable differs', () => {
    const pdb1 = buildPodDisruptionBudget(makeCluster({ instances: 2 }));
    const pdb2 = { ...pdb1, spec: { ...pdb1.spec, minAvailable: 2 } };
    expect(podDisruptionBudgetNeedsUpdate(pdb1, pdb2)).toBe(true);
  });
});

describe('buildConfigMap & configMapNeedsUpdate', () => {
  it('returns null when neither config nor bootstrap initSql is provided', () => {
    const cluster = makeCluster();
    expect(buildConfigMap(cluster)).toBeNull();
  });

  it('creates ConfigMap with custom firebird.conf settings', () => {
    const cluster = makeCluster({
      config: { settings: { DefaultCacheMem: '256M', FileSystemCacheThreshold: '64K' } },
    });
    const cm = buildConfigMap(cluster);
    expect(cm).not.toBeNull();
    expect(cm?.metadata?.name).toBe('test-cluster-config');
    expect(cm?.data?.['firebird.conf']).toContain('DefaultCacheMem = 256M');
    expect(cm?.data?.['firebird.conf']).toContain('FileSystemCacheThreshold = 64K');
  });

  it('creates ConfigMap with bootstrap init.sql script', () => {
    const cluster = makeCluster({
      bootstrap: { initSql: 'CREATE TABLE users (id INT);' },
    });
    const cm = buildConfigMap(cluster);
    expect(cm).not.toBeNull();
    expect(cm?.data?.['init.sql']).toBe('CREATE TABLE users (id INT);');
  });

  it('detects ConfigMap updates correctly', () => {
    const cm1 = buildConfigMap(makeCluster({ config: { settings: { A: '1' } } }));
    const cm2 = buildConfigMap(makeCluster({ config: { settings: { A: '2' } } }));
    expect(configMapNeedsUpdate(cm1!, cm1!)).toBe(false);
    expect(configMapNeedsUpdate(cm1!, cm2!)).toBe(true);
  });
});

describe('buildAutoSweepCronJob & autoSweepCronJobNeedsUpdate', () => {
  it('creates an AutoSweep CronJob with default schedule and db name', () => {
    const cluster = makeCluster({ autoSweep: { enabled: true } });
    const cronJob = buildAutoSweepCronJob(cluster);
    expect(cronJob.metadata?.name).toBe('test-cluster-sweep');
    expect(cronJob.spec?.schedule).toBe('0 3 * * *');
    const container = cronJob.spec?.jobTemplate?.spec?.template?.spec?.containers?.[0];
    expect(container?.args?.[0]).toContain('gfix -sweep');
    expect(container?.args?.[0]).toContain('localhost:/firebird/data/mydb.fdb');
  });

  it('uses custom schedule and database name when provided', () => {
    const cluster = makeCluster({
      autoSweep: { enabled: true, schedule: '0 4 * * *', databaseName: 'custom.fdb' },
    });
    const cronJob = buildAutoSweepCronJob(cluster);
    expect(cronJob.spec?.schedule).toBe('0 4 * * *');
    const container = cronJob.spec?.jobTemplate?.spec?.template?.spec?.containers?.[0];
    expect(container?.args?.[0]).toContain('localhost:/firebird/data/custom.fdb');
  });

  it('detects AutoSweep CronJob updates correctly', () => {
    const cj1 = buildAutoSweepCronJob(makeCluster({ autoSweep: { enabled: true, schedule: '0 3 * * *' } }));
    const cj2 = buildAutoSweepCronJob(makeCluster({ autoSweep: { enabled: true, schedule: '0 5 * * *' } }));
    expect(autoSweepCronJobNeedsUpdate(cj1, cj1)).toBe(false);
    expect(autoSweepCronJobNeedsUpdate(cj1, cj2)).toBe(true);
  });
});

describe('buildStatefulSet (config & bootstrap volume mounting)', () => {
  it('mounts firebird.conf volume when config settings are provided', () => {
    const cluster = makeCluster({
      config: { settings: { DefaultCacheMem: '128M' } },
    });
    const sts = buildStatefulSet(cluster);
    const container = sts.spec?.template?.spec?.containers?.[0];
    const mount = container?.volumeMounts?.find((vm) => vm.mountPath === '/firebird/etc/firebird.conf');
    expect(mount).toBeDefined();
    expect(mount?.subPath).toBe('firebird.conf');
  });

  it('mounts init.sql volume when bootstrap initSql is provided', () => {
    const cluster = makeCluster({
      bootstrap: { initSql: 'CREATE TABLE t (id INT);' },
    });
    const sts = buildStatefulSet(cluster);
    const container = sts.spec?.template?.spec?.containers?.[0];
    const mount = container?.volumeMounts?.find((vm) => vm.mountPath === '/docker-entrypoint-initdb.d/init.sql');
    expect(mount).toBeDefined();
    expect(mount?.subPath).toBe('init.sql');
  });
});

describe('buildBackupCronJob (physical nbackup & S3 cloud support)', () => {
  it('generates nbackup command when backup.type is physical', () => {
    const cluster = makeCluster({ backup: { enabled: true, type: 'physical', level: 0 } });
    const cronJob = buildBackupCronJob(cluster);
    const container = cronJob.spec?.jobTemplate?.spec?.template?.spec?.containers?.[0];
    expect(container?.args?.[0]).toContain('nbackup -L 0');
    expect(container?.args?.[0]).toContain('/firebird/data/mydb.fdb');
  });

  it('uses nbackup level 1 when specified', () => {
    const cluster = makeCluster({ backup: { enabled: true, type: 'physical', level: 1 } });
    const cronJob = buildBackupCronJob(cluster);
    const container = cronJob.spec?.jobTemplate?.spec?.template?.spec?.containers?.[0];
    expect(container?.args?.[0]).toContain('nbackup -L 1');
  });

  it('injects S3 environment variables and upload command when S3 is configured', () => {
    const cluster = makeCluster({
      backup: {
        enabled: true,
        s3: {
          bucket: 'my-firebird-backups',
          secretRef: { name: 's3-credentials' },
          prefix: 'production',
        },
      },
    });
    const cronJob = buildBackupCronJob(cluster);
    const container = cronJob.spec?.jobTemplate?.spec?.template?.spec?.containers?.[0];
    expect(container?.args?.[0]).toContain('aws s3 cp');
    expect(container?.env).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: 'AWS_ACCESS_KEY_ID',
          valueFrom: { secretKeyRef: { name: 's3-credentials', key: 'AWS_ACCESS_KEY_ID' } },
        }),
      ]),
    );
  });
});

describe('buildNetworkPolicy & networkPolicyNeedsUpdate', () => {
  it('creates NetworkPolicy with default ingress rule for port 3050', () => {
    const cluster = makeCluster({ networkPolicy: { enabled: true } });
    const np = buildNetworkPolicy(cluster);
    expect(np.metadata?.name).toBe('test-cluster-networkpolicy');
    expect(np.spec?.podSelector?.matchLabels?.['firebird.cloudnative-firebird.io/cluster']).toBe('test-cluster');
    expect(np.spec?.ingress?.[0]?.ports?.[0]?.port).toBe(3050);
  });

  it('creates NetworkPolicy with custom ingressFrom selectors', () => {
    const cluster = makeCluster({
      networkPolicy: {
        enabled: true,
        ingressFrom: [{ podSelector: { app: 'api' } }],
      },
    });
    const np = buildNetworkPolicy(cluster);
    expect(np.spec?.ingress?.[0]?.from?.[0]?.podSelector?.matchLabels).toEqual({ app: 'api' });
  });

  it('detects NetworkPolicy updates correctly', () => {
    const np1 = buildNetworkPolicy(makeCluster({ networkPolicy: { enabled: true } }));
    const np2 = buildNetworkPolicy(
      makeCluster({ networkPolicy: { enabled: true, ingressFrom: [{ podSelector: { role: 'backend' } }] } }),
    );
    expect(networkPolicyNeedsUpdate(np1, np1)).toBe(false);
    expect(networkPolicyNeedsUpdate(np1, np2)).toBe(true);
  });
});

describe('Monitoring Exporter Sidecar & PodMonitor', () => {
  it('injects firebird-exporter container into StatefulSet when exporter is enabled', () => {
    const cluster = makeCluster({
      monitoring: {
        exporter: {
          enabled: true,
          image: 'prom/firebird-exporter:v1.2.0',
          port: 9108,
        },
      },
    });
    const sts = buildStatefulSet(cluster);
    const containers = sts.spec?.template?.spec?.containers;
    expect(containers?.length).toBe(2);
    expect(containers?.[1].name).toBe('firebird-exporter');
    expect(containers?.[1].image).toBe('prom/firebird-exporter:v1.2.0');
    expect(containers?.[1].ports?.[0].containerPort).toBe(9108);
  });

  it('configures PodMonitor with metrics endpoint when exporter is enabled', () => {
    const cluster = makeCluster({
      monitoring: {
        enablePodMonitor: true,
        exporter: { enabled: true },
      },
    });
    const podMonitor = buildPodMonitor(cluster) as { spec: { podMetricsEndpoints: Array<{ port: string }> } };
    expect(podMonitor.spec.podMetricsEndpoints[0].port).toBe('metrics');
  });
});

describe('TLS & cert-manager integration', () => {
  it('mounts tls-cert volume into container when TLS is enabled', () => {
    const cluster = makeCluster({
      tls: { enabled: true, secretName: 'my-custom-tls' },
    });
    const sts = buildStatefulSet(cluster);
    const container = sts.spec?.template?.spec?.containers?.[0];
    const mount = container?.volumeMounts?.find((vm) => vm.mountPath === '/firebird/etc/tls');
    expect(mount).toBeDefined();

    const volumes = sts.spec?.template?.spec?.volumes;
    const tlsVol = volumes?.find((v) => v.name === 'tls-cert');
    expect(tlsVol?.secret?.secretName).toBe('my-custom-tls');
  });

  it('injects WireCrypt = Required into ConfigMap when TLS is enabled', () => {
    const cluster = makeCluster({ tls: { enabled: true } });
    const cm = buildConfigMap(cluster);
    expect(cm?.data?.['firebird.conf']).toContain('WireCrypt = Required');
  });

  it('builds cert-manager Certificate resource', () => {
    const cluster = makeCluster({
      tls: {
        enabled: true,
        issuerRef: { name: 'letsencrypt-prod', kind: 'ClusterIssuer' },
      },
    });
    const cert = buildCertificate(cluster) as { apiVersion: string; metadata: { name: string }; spec: { issuerRef: { name: string } } };
    expect(cert.apiVersion).toBe('cert-manager.io/v1');
    expect(cert.metadata.name).toBe('test-cluster-cert');
    expect(cert.spec.issuerRef.name).toBe('letsencrypt-prod');
  });
});

describe('Leader Election Lease', () => {
  it('builds primary leader Lease resource', () => {
    const cluster = makeCluster();
    const lease = buildLease(cluster);
    expect(lease.apiVersion).toBe('coordination.k8s.io/v1');
    expect(lease.metadata?.name).toBe('test-cluster-lease');
    expect(lease.spec?.holderIdentity).toBe('test-cluster-0');
  });
});

describe('FirebirdBackup & FirebirdRestore Job builders', () => {
  it('builds a manual backup Job for FirebirdBackup', () => {
    const cluster = makeCluster();
    const backup = {
      apiVersion: 'firebird.cloudnative-firebird.io/v1' as const,
      kind: 'FirebirdBackup' as const,
      metadata: { name: 'my-backup', namespace: 'default' },
      spec: { clusterName: 'test-cluster', type: 'logical' as const },
    };
    const job = buildBackupJob(backup, cluster);
    expect(job.metadata?.name).toBe('backup-my-backup');
    expect(job.spec?.template?.spec?.containers?.[0].args?.[0]).toContain('gbak -b');
  });

  it('builds a restore Job for FirebirdRestore', () => {
    const cluster = makeCluster();
    const restore = {
      apiVersion: 'firebird.cloudnative-firebird.io/v1' as const,
      kind: 'FirebirdRestore' as const,
      metadata: { name: 'my-restore', namespace: 'default' },
      spec: { clusterName: 'test-cluster', restoreType: 'logical' as const, backupPath: '/firebird/data/dump.fbk' },
    };
    const job = buildRestoreJob(restore, cluster);
    expect(job.metadata?.name).toBe('restore-my-restore');
    expect(job.spec?.template?.spec?.containers?.[0].args?.[0]).toContain('gbak -c');
  });
});

describe('Diagnostics & Grafana Dashboard Builders', () => {
  it('builds a Diagnostics CronJob (gfix -v -full)', () => {
    const cluster = makeCluster({ diagnostics: { enabled: true, schedule: '0 4 * * 0' } });
    const cronJob = buildDiagnosticsCronJob(cluster);
    expect(cronJob.metadata?.name).toBe('test-cluster-diagnostics');
    expect(cronJob.spec?.schedule).toBe('0 4 * * 0');
    expect(cronJob.spec?.jobTemplate?.spec?.template?.spec?.containers?.[0].args?.[0]).toContain('gfix -v -full');
  });

  it('builds a Grafana Dashboard ConfigMap', () => {
    const cluster = makeCluster({ monitoring: { enableGrafanaDashboard: true } });
    const cm = buildGrafanaDashboardConfigMap(cluster);
    expect(cm.metadata?.name).toBe('test-cluster-grafana-dashboard');
    expect(cm.metadata?.labels?.['grafana_dashboard']).toBe('1');
    expect(cm.data?.['firebird-test-cluster.json']).toContain('Active Attachments');
  });

  it('builds a CronJob for FirebirdScheduledBackup CRD', () => {
    const cluster = makeCluster();
    const scheduledBackup: FirebirdScheduledBackup = {
      apiVersion: 'firebird.cloudnative-firebird.io/v1',
      kind: 'FirebirdScheduledBackup',
      metadata: { name: 'nightly-backup', namespace: 'default' },
      spec: { clusterName: 'test-cluster', schedule: '0 1 * * *', type: 'physical', level: 0 },
    };
    const cronJob = buildScheduledBackupCronJob(scheduledBackup, cluster);
    expect(cronJob.metadata?.name).toBe('sched-backup-nightly-backup');
    expect(cronJob.spec?.schedule).toBe('0 1 * * *');
    expect(cronJob.spec?.jobTemplate?.spec?.template?.spec?.containers?.[0].args?.[0]).toContain('nbackup -L 0');
  });
});






