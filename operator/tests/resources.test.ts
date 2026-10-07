import { describe, it, expect } from 'vitest';
import { V1CronJob } from '@kubernetes/client-node';
import {
  buildService,
  buildStatefulSet,
  buildHeadlessService,
  buildReplicaService,
  cronJobNeedsUpdate,
  buildPodMonitor,
  buildPodDisruptionBudget,
  podDisruptionBudgetNeedsUpdate,
  buildConfigMap,
  configMapNeedsUpdate,
  buildAutoSweepCronJob,
  autoSweepCronJobNeedsUpdate,
  buildNetworkPolicy,
  cloneTargets,
  networkPolicyNeedsUpdate,
  networkPolicyWireFormat,
  buildLease,
  buildDiagnosticsCronJob,
  buildGrafanaDashboardConfigMap,
  clusterLabels,
  statefulSetNeedsUpdate,
  primaryServiceSelector,
  replicaServiceSelector,
  readOnlyRoutingEnabled,
  withHibernation,
  replicationConfigHash,
  diagnosticsCronJobNeedsUpdate,
} from '../src/utils/resources';
import { buildBackupCronJob } from '../src/utils/backup';
import { FirebirdCluster, DEFAULT_FIREBIRD_IMAGE } from '../src/types';

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
  const podSpecOf = (cluster: FirebirdCluster) => buildStatefulSet(cluster).spec?.template?.spec;

  it('adds no replication containers when replication is disabled or absent', () => {
    for (const cluster of [makeCluster({ replication: { enabled: false } }), makeCluster()]) {
      const podSpec = podSpecOf(cluster);
      expect(podSpec?.initContainers?.map((c) => c.name)).toEqual(['security-db-init']);
      expect(podSpec?.containers?.map((c) => c.name)).toEqual(['firebird', 'backup-files']);
    }
  });

  it('runs the segment server in its files-only mode as the backup file server without replication', () => {
    const podSpec = podSpecOf(makeCluster());
    const files = podSpec!.containers!.find((c) => c.name === 'backup-files')!;
    expect(files.command).toEqual(['perl', '/etc/firebird-operator/segment-server.pl']);
    const env = Object.fromEntries(files.env!.map((e) => [e.name, e.value]));
    expect(env).toMatchObject({ FILES_ONLY: 'true', DATABASE_PATH: '/var/lib/firebird/data/mydb.fdb', SEGMENT_PORT: '3051' });
    expect(files.env).toContainEqual({ name: 'ISC_USER', value: 'SYSDBA' });
    expect(files.ports).toEqual([{ name: 'segments', containerPort: 3051, protocol: 'TCP' }]);
    expect(files.volumeMounts?.map((m) => m.name)).toEqual(['firebird-data', 'cluster-config']);
    // the scripts are read at start: a new version rolls the pods
    const sts = buildStatefulSet(makeCluster());
    expect(sts.spec!.template.metadata!.annotations!['firebird.cloudnative-firebird.io/backup-files-hash']).toMatch(/^[0-9a-f]{16}$/);
    expect(Object.keys(buildConfigMap(makeCluster())!.data!).sort()).toEqual(['backup-file.pl', 'fetch-segments.pl', 'pitr-plan.pl', 'pitr-restore.sh', 'segment-server.pl', 'sync-standby.pl']);
  });

  it('seeds replicas in an init container and ships segments with two sidecars', () => {
    const podSpec = podSpecOf(makeCluster({ replication: { enabled: true } }));
    expect(podSpec?.initContainers?.map((c) => c.name)).toEqual(['security-db-init', 'replication-init']);
    expect(podSpec?.containers?.map((c) => c.name)).toEqual(['firebird', 'segment-server', 'segment-puller']);
    expect(podSpec?.containers?.[1].ports).toEqual([{ name: 'segments', containerPort: 3051, protocol: 'TCP' }]);
  });

  it('runs the replication init after bootstrap recovery', () => {
    const podSpec = podSpecOf(
      makeCluster({ replication: { enabled: true }, bootstrap: { recovery: { sourcePath: '/backup/db.fbk' } } }),
    );
    expect(podSpec?.initContainers?.map((c) => c.name)).toEqual(['security-db-init', 'bootstrap-restore', 'replication-init']);
  });

  it('mounts replication.conf into the Firebird container; the init container creates the database', () => {
    const podSpec = podSpecOf(makeCluster({ replication: { enabled: true } }));
    const mounts = podSpec?.containers?.[0].volumeMounts;
    expect(mounts).toContainEqual({
      name: 'cluster-config',
      mountPath: '/opt/firebird/replication.conf',
      subPath: 'replication.conf',
    });
    expect(mounts?.some((m) => m.mountPath.includes('enable-publication'))).toBe(false);
    expect(podSpec?.initContainers?.find((c) => c.name === 'replication-init')?.command).toEqual(['sh', '/etc/firebird-operator/init-instance.sh']);
  });

  it('passes seed sources and live seeding (on unless opted out) to the replication containers', () => {
    const envOf = (allowLiveSeedFromPrimary?: boolean) =>
      podSpecOf(makeCluster({ replication: { enabled: true, allowLiveSeedFromPrimary } }))?.initContainers?.find(
        (c) => c.name === 'replication-init',
      )?.env;
    const env = envOf(undefined);
    expect(env).toContainEqual({ name: 'SEED_SOURCES_FILE', value: '/etc/firebird-operator/seed-sources' });
    expect(env).toContainEqual({ name: 'ALLOW_LIVE_SEED', value: 'true' });
    expect(env).toContainEqual({ name: 'REPLICATION_DIR', value: '/var/lib/firebird/data/replication' });
    expect(envOf(true)).toContainEqual({ name: 'ALLOW_LIVE_SEED', value: 'true' });
    expect(envOf(false)).toContainEqual({ name: 'ALLOW_LIVE_SEED', value: 'false' });
  });

  it('gives the sidecars the data volume, the scripts and SYSDBA credentials', () => {
    const cluster = makeCluster({ superuserSecret: { name: 'fb-secret' }, replication: { enabled: true } });
    const puller = podSpecOf(cluster)?.containers?.find((c) => c.name === 'segment-puller');
    expect(puller?.command).toEqual(['perl', '/etc/firebird-operator/segment-puller.pl']);
    expect(puller?.volumeMounts).toContainEqual({ name: 'firebird-data', mountPath: '/var/lib/firebird/data' });
    expect(puller?.volumeMounts).toContainEqual({
      name: 'cluster-config',
      mountPath: '/etc/firebird-operator',
      readOnly: true,
    });
    expect(puller?.env).toContainEqual({
      name: 'ISC_PASSWORD',
      valueFrom: { secretKeyRef: { name: 'fb-secret', key: 'password' } },
    });
    expect(puller?.env).toContainEqual({ name: 'SOURCE_DIR', value: '/var/lib/firebird/data/replication/source' });
    expect(puller?.env).toContainEqual({ name: 'PRIMARY_FILE', value: '/etc/firebird-operator/primary' });
  });

  it('derives replication directories from journalDirectory', () => {
    const cluster = makeCluster({ replication: { enabled: true, journalDirectory: '/var/lib/firebird/data/repl' } });
    const env = podSpecOf(cluster)?.initContainers?.find((c) => c.name === 'replication-init')?.env;
    expect(env).toContainEqual({ name: 'JOURNAL_DIR', value: '/var/lib/firebird/data/repl/journal' });
    expect(env).toContainEqual({ name: 'ARCHIVE_DIR', value: '/var/lib/firebird/data/repl/archive' });
  });

  it('rolls pods when the replication configuration changes, but not the ConfigMap primary', () => {
    const a = buildStatefulSet(makeCluster({ replication: { enabled: true } }));
    const b = buildStatefulSet(makeCluster({ replication: { enabled: true, archiveTimeoutSeconds: 30 } }));
    expect(statefulSetNeedsUpdate(a, b)).toBe(true);
    expect(statefulSetNeedsUpdate(a, buildStatefulSet(makeCluster({ replication: { enabled: true } })))).toBe(false);
  });

  it('detects enabling replication on an existing cluster', () => {
    const before = buildStatefulSet(makeCluster());
    const after = buildStatefulSet(makeCluster({ replication: { enabled: true } }));
    expect(statefulSetNeedsUpdate(before, after)).toBe(true);
  });

  it('ignores annotations added by other tools when comparing pod templates', () => {
    const desired = buildStatefulSet(makeCluster({ replication: { enabled: true } }));
    const existing = JSON.parse(JSON.stringify(desired));
    existing.spec.template.metadata.annotations['kubectl.kubernetes.io/restartedAt'] = '2026-01-01T00:00:00Z';
    expect(statefulSetNeedsUpdate(existing, desired)).toBe(false);
  });

  it('adds secret hash annotation to pod template when superuserSecretHash is provided', () => {
    const cluster = makeCluster({ superuserSecret: { name: 'my-secret' } });
    const sts = buildStatefulSet(cluster, { superuserSecretHash: 'abc123hash' });
    const annotations = sts.spec?.template?.metadata?.annotations;
    expect(annotations?.['firebird.cloudnative-firebird.io/superuser-secret-hash']).toBe('abc123hash');
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

describe('read-only routing service selectors', () => {
  const routed = makeCluster({
    replication: { enabled: true, readOnlyRouting: { enabled: true } },
  });

  it('is disabled unless both replication and readOnlyRouting are enabled', () => {
    expect(readOnlyRoutingEnabled(makeCluster())).toBe(false);
    expect(readOnlyRoutingEnabled(makeCluster({ replication: { enabled: true } }))).toBe(false);
    expect(
      readOnlyRoutingEnabled(makeCluster({ replication: { enabled: false, readOnlyRouting: { enabled: true } } })),
    ).toBe(false);
    expect(readOnlyRoutingEnabled(routed)).toBe(true);
  });

  it('keeps plain cluster label selectors when routing is disabled', () => {
    const cluster = makeCluster();
    expect(primaryServiceSelector(cluster)).toEqual(clusterLabels('test-cluster'));
    expect(replicaServiceSelector(cluster)).toEqual(clusterLabels('test-cluster'));
  });

  it('selects only the primary pod for the read-write service', () => {
    expect(buildService(routed).spec?.selector).toEqual({
      ...clusterLabels('test-cluster'),
      'firebird.cloudnative-firebird.io/role': 'primary',
    });
  });

  it('selects only read-routable pods for the replica service', () => {
    expect(buildReplicaService(routed).spec?.selector).toEqual({
      ...clusterLabels('test-cluster'),
      'firebird.cloudnative-firebird.io/read-routable': 'true',
    });
  });

  it('does not change the headless service selector', () => {
    expect(buildHeadlessService(routed).spec?.selector).toEqual(clusterLabels('test-cluster'));
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
  it('only ships the backup file server scripts when neither config nor bootstrap initSql is provided', () => {
    const cluster = makeCluster();
    expect(Object.keys(buildConfigMap(cluster)!.data!).sort()).toEqual(['backup-file.pl', 'fetch-segments.pl', 'pitr-plan.pl', 'pitr-restore.sh', 'segment-server.pl', 'sync-standby.pl']);
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
    expect(container?.args?.[0]).toBe('gfix -sweep test-cluster-0.test-cluster-headless:/var/lib/firebird/data/mydb.fdb');
  });

  it('sweeps the primary over the network without mounting the instance PVC', () => {
    const cronJob = buildAutoSweepCronJob(makeCluster({ autoSweep: { enabled: true } }));
    const podSpec = cronJob.spec?.jobTemplate?.spec?.template?.spec;
    expect(podSpec?.volumes).toBeUndefined();
    expect(podSpec?.containers?.[0].volumeMounts).toBeUndefined();
  });

  it('passes SYSDBA credentials via ISC_USER/ISC_PASSWORD, not process args', () => {
    const cronJob = buildAutoSweepCronJob(
      makeCluster({ autoSweep: { enabled: true }, superuserSecret: { name: 'fb-secret' } }),
    );
    const container = cronJob.spec?.jobTemplate?.spec?.template?.spec?.containers?.[0];
    expect(container?.args?.[0]).not.toContain('ISC_PASSWORD');
    expect(container?.env).toEqual([
      { name: 'ISC_USER', value: 'SYSDBA' },
      { name: 'ISC_PASSWORD', valueFrom: { secretKeyRef: { name: 'fb-secret', key: 'password' } } },
    ]);
  });

  it('defaults the swept database to spec.databaseName', () => {
    const cronJob = buildAutoSweepCronJob(makeCluster({ databaseName: 'app.fdb', autoSweep: { enabled: true } }));
    expect(cronJob.spec?.jobTemplate?.spec?.template?.spec?.containers?.[0].args?.[0]).toContain(
      'test-cluster-0.test-cluster-headless:/var/lib/firebird/data/app.fdb',
    );
  });

  it('uses custom schedule and database name when provided', () => {
    const cluster = makeCluster({
      autoSweep: { enabled: true, schedule: '0 4 * * *', databaseName: 'custom.fdb' },
    });
    const cronJob = buildAutoSweepCronJob(cluster);
    expect(cronJob.spec?.schedule).toBe('0 4 * * *');
    const container = cronJob.spec?.jobTemplate?.spec?.template?.spec?.containers?.[0];
    expect(container?.args?.[0]).toContain('test-cluster-0.test-cluster-headless:/var/lib/firebird/data/custom.fdb');
  });

  it('follows the primary: the CronJob is updated after a switchover or failover', () => {
    const cluster = makeCluster({ autoSweep: { enabled: true }, diagnostics: { enabled: true } });
    const sweep = buildAutoSweepCronJob(cluster, 'test-cluster-2');
    const diag = buildDiagnosticsCronJob(cluster, 'test-cluster-2');
    expect(sweep.spec?.jobTemplate?.spec?.template?.spec?.containers?.[0].args?.[0]).toBe(
      'gfix -sweep test-cluster-2.test-cluster-headless:/var/lib/firebird/data/mydb.fdb',
    );
    expect(diag.spec?.jobTemplate?.spec?.template?.spec?.containers?.[0].args?.[0]).toContain(
      'fbsvcmgr test-cluster-2.test-cluster-headless:service_mgr',
    );
    expect(autoSweepCronJobNeedsUpdate(buildAutoSweepCronJob(cluster, 'test-cluster-0'), sweep)).toBe(true);
    expect(diagnosticsCronJobNeedsUpdate(buildDiagnosticsCronJob(cluster, 'test-cluster-0'), diag)).toBe(true);
    expect(diagnosticsCronJobNeedsUpdate(diag, diag)).toBe(false);
  });

  it('detects AutoSweep CronJob updates correctly', () => {
    const cj1 = buildAutoSweepCronJob(makeCluster({ autoSweep: { enabled: true, schedule: '0 3 * * *' } }));
    const cj2 = buildAutoSweepCronJob(makeCluster({ autoSweep: { enabled: true, schedule: '0 5 * * *' } }));
    expect(autoSweepCronJobNeedsUpdate(cj1, cj1)).toBe(false);
    expect(autoSweepCronJobNeedsUpdate(cj1, cj2)).toBe(true);
  });
});

describe('buildStatefulSet (config & bootstrap volume mounting)', () => {
  it('applies firebird.conf settings as FIREBIRD_CONF_* env vars instead of a file mount', () => {
    const cluster = makeCluster({
      config: { settings: { DefaultCacheMem: '128M' } },
    });
    const sts = buildStatefulSet(cluster);
    const container = sts.spec?.template?.spec?.containers?.[0];
    expect(container?.env).toContainEqual({ name: 'FIREBIRD_CONF_DefaultCacheMem', value: '128M' });
    expect(container?.volumeMounts?.some((vm) => vm.mountPath.endsWith('firebird.conf'))).toBe(false);
    expect(sts.spec?.template?.spec?.volumes?.map((v) => v.name)).toEqual(['pending-user-drops', 'cluster-config']);
  });

  it('adds WireCrypt=Required to the conf env when TLS is enabled', () => {
    const sts = buildStatefulSet(makeCluster({ tls: { enabled: true } }));
    expect(sts.spec?.template?.spec?.containers?.[0].env).toContainEqual({
      name: 'FIREBIRD_CONF_WireCrypt',
      value: 'Required',
    });
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
    expect(np.spec?.ingress?.[0]?._from?.[0]?.podSelector?.matchLabels).toEqual({ app: 'api' });
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
    expect(containers?.map((c) => c.name)).toEqual(['firebird', 'firebird-exporter', 'backup-files']);
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

describe('wire encryption (tls)', () => {
  it('requires wire encryption with the ChaCha plugins only when tls is enabled', () => {
    const env = buildStatefulSet(makeCluster({ tls: { enabled: true } })).spec?.template?.spec?.containers?.[0].env;
    expect(env).toContainEqual({ name: 'FIREBIRD_CONF_WireCrypt', value: 'Required' });
    expect(env).toContainEqual({ name: 'FIREBIRD_CONF_WireCryptPlugin', value: 'ChaCha64, ChaCha' });
    const cm = buildConfigMap(makeCluster({ tls: { enabled: true } }));
    expect(cm?.data?.['firebird.conf']).toContain('WireCrypt = Required');
    expect(cm?.data?.['firebird.conf']).toContain('WireCryptPlugin = ChaCha64, ChaCha');
    // Firebird's own defaults otherwise (already WireCrypt = Required on the server)
    const plain = buildStatefulSet(makeCluster()).spec?.template?.spec?.containers?.[0].env ?? [];
    expect(plain.map((e) => e.name)).not.toContain('FIREBIRD_CONF_WireCryptPlugin');
  });

  it('mounts no certificate: Firebird has no TLS listener', () => {
    const sts = buildStatefulSet(makeCluster({ tls: { enabled: true, secretName: 'my-custom-tls' } }));
    expect(sts.spec?.template?.spec?.volumes?.map((v) => v.name)).not.toContain('tls-cert');
    expect(sts.spec?.template?.spec?.containers?.[0].volumeMounts?.map((m) => m.mountPath)).not.toContain('/firebird/etc/tls');
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

  it('serializes renewTime with microsecond precision as required by the Lease API', () => {
    const lease = buildLease(makeCluster());
    const serialized = JSON.parse(JSON.stringify(lease)).spec.renewTime as string;
    expect(serialized).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$/);
  });
});

describe('Diagnostics & Grafana Dashboard Builders', () => {
  it('builds a Diagnostics CronJob using online validation on the primary', () => {
    const cluster = makeCluster({ diagnostics: { enabled: true, schedule: '0 4 * * 0' } });
    const cronJob = buildDiagnosticsCronJob(cluster);
    expect(cronJob.metadata?.name).toBe('test-cluster-diagnostics');
    expect(cronJob.spec?.schedule).toBe('0 4 * * 0');
    const podSpec = cronJob.spec?.jobTemplate?.spec?.template?.spec;
    // gfix -v needs exclusive access; online validation works with clients connected
    expect(podSpec?.containers?.[0].args?.[0]).toContain(
      'fbsvcmgr test-cluster-0.test-cluster-headless:service_mgr action_validate dbname /var/lib/firebird/data/mydb.fdb',
    );
    expect(podSpec?.containers?.[0].args?.[0]).not.toContain('gfix -v');
    expect(podSpec?.volumes).toBeUndefined();
  });

  it('builds a Grafana Dashboard ConfigMap', () => {
    const cluster = makeCluster({ monitoring: { enableGrafanaDashboard: true } });
    const cm = buildGrafanaDashboardConfigMap(cluster);
    expect(cm.metadata?.name).toBe('test-cluster-grafana-dashboard');
    expect(cm.metadata?.labels?.['grafana_dashboard']).toBe('1');
    expect(cm.data?.['firebird-test-cluster.json']).toContain('Active Attachments');
  });

});

describe('hibernation', () => {
  it('scales the StatefulSet to zero replicas when hibernated', () => {
    expect(buildStatefulSet(makeCluster({ instances: 3, hibernated: true })).spec?.replicas).toBe(0);
    expect(buildStatefulSet(makeCluster({ instances: 3, hibernated: false })).spec?.replicas).toBe(3);
  });

  it('keeps the volumeClaimTemplates when hibernated', () => {
    const sts = buildStatefulSet(makeCluster({ hibernated: true }));
    expect(sts.spec?.volumeClaimTemplates?.[0]?.metadata?.name).toBe('firebird-data');
  });

  it('withHibernation sets spec.suspend from the cluster state', () => {
    const on = makeCluster({ hibernated: true, backup: { enabled: true } });
    const off = makeCluster({ backup: { enabled: true } });
    expect(withHibernation(buildBackupCronJob(on), on).spec?.suspend).toBe(true);
    expect(withHibernation(buildBackupCronJob(off), off).spec?.suspend).toBe(false);
  });

  it('CronJob needsUpdate helpers detect suspend changes', () => {
    const cluster = makeCluster({
      backup: { enabled: true },
      autoSweep: { enabled: true },
      diagnostics: { enabled: true },
    });
    const pairs: Array<[V1CronJob, (a: V1CronJob, b: V1CronJob) => boolean]> = [
      [buildBackupCronJob(cluster), cronJobNeedsUpdate],
      [buildAutoSweepCronJob(cluster), autoSweepCronJobNeedsUpdate],
      [buildDiagnosticsCronJob(cluster), diagnosticsCronJobNeedsUpdate],
    ];
    for (const [cronJob, needsUpdate] of pairs) {
      const suspended = { ...cronJob, spec: { ...cronJob.spec!, suspend: true } };
      expect(needsUpdate(cronJob, cronJob)).toBe(false);
      expect(needsUpdate(cronJob, suspended)).toBe(true);
    }
  });
});

describe('buildStatefulSet (official image runtime contract)', () => {
  const envOf = (cluster: FirebirdCluster) => buildStatefulSet(cluster).spec?.template?.spec?.containers?.[0].env ?? [];

  it('mounts the data PVC at the image data directory', () => {
    const container = buildStatefulSet(makeCluster()).spec?.template?.spec?.containers?.[0];
    expect(container?.volumeMounts).toContainEqual({ name: 'firebird-data', mountPath: '/var/lib/firebird/data' });
  });

  it('sets FIREBIRD_ROOT_PASSWORD and client credentials from the superuser Secret', () => {
    const ref = { secretKeyRef: { name: 'fb-secret', key: 'password' } };
    const env = envOf(makeCluster({ superuserSecret: { name: 'fb-secret' } }));
    expect(env).toContainEqual({ name: 'FIREBIRD_ROOT_PASSWORD', valueFrom: ref });
    expect(env).toContainEqual({ name: 'ISC_USER', value: 'SYSDBA' });
    expect(env).toContainEqual({ name: 'ISC_PASSWORD', valueFrom: ref });
  });

  it('falls back to the masterkey password without a Secret', () => {
    expect(envOf(makeCluster())).toContainEqual({ name: 'FIREBIRD_ROOT_PASSWORD', value: 'masterkey' });
  });

  it('asks the entrypoint to create the cluster database', () => {
    expect(envOf(makeCluster())).toContainEqual({ name: 'FIREBIRD_DATABASE', value: 'mydb.fdb' });
    expect(envOf(makeCluster({ databaseName: 'app.fdb' }))).toContainEqual({
      name: 'FIREBIRD_DATABASE',
      value: 'app.fdb',
    });
  });

  it('lets spec.env override operator defaults (later entries win)', () => {
    const env = envOf(makeCluster({ env: [{ name: 'FIREBIRD_DATABASE', value: 'other.fdb' }] }));
    const last = [...env].reverse().find((e) => e.name === 'FIREBIRD_DATABASE');
    expect(last?.value).toBe('other.fdb');
  });

  it('rolls pods when firebird.conf settings change', () => {
    const a = buildStatefulSet(makeCluster({ config: { settings: { DefaultDbCachePages: '2048' } } }));
    const b = buildStatefulSet(makeCluster({ config: { settings: { DefaultDbCachePages: '4096' } } }));
    expect(statefulSetNeedsUpdate(a, b)).toBe(true);
  });
});

describe('buildConfigMap (replication)', () => {
  it('publishes replication.conf, the scripts and the current primary', () => {
    const cm = buildConfigMap(makeCluster({ replication: { enabled: true } }), { primaryPod: 'test-cluster-1' });
    expect(cm?.data?.primary).toBe('test-cluster-1.test-cluster-headless');
    expect(Object.keys(cm?.data ?? {}).sort()).toEqual([
      'backup-file.pl',
      'demote',
      'enable-publication.sql',
      'failover.pl',
      'fetch-seed.pl',
      'fetch-segments.pl',
      'init-instance.sh',
      'isolation-check.pl',
      'pitr-plan.pl',
      'pitr-restore.sh',
      'primary',
      'promote',
      'replica-control.pl',
      'replication.conf',
      'reseed',
      'seed-sources',
      'segment-puller.pl',
      'segment-request.pl',
      'segment-server.pl',
      'set-repl-seq.pl',
      'switchover.pl',
      'sync-standby.pl',
    ]);
  });

  it('lists ready replicas as seed sources, one host per line', () => {
    const cm = buildConfigMap(makeCluster({ replication: { enabled: true } }), {
      primaryPod: 'test-cluster-0',
      seedSourcePods: ['test-cluster-1', 'test-cluster-2'],
    });
    expect(cm?.data?.['seed-sources']).toBe(
      'test-cluster-1.test-cluster-headless\ntest-cluster-2.test-cluster-headless\n',
    );
  });

  it('does not roll pods when seed sources change', () => {
    const cluster = makeCluster({ replication: { enabled: true } });
    expect(replicationConfigHash(cluster)).toBe(replicationConfigHash(cluster));
    expect(Object.keys(buildConfigMap(cluster, { seedSourcePods: ['x'] })?.data ?? {})).toContain('seed-sources');
  });

  it('defaults the primary to ordinal 0', () => {
    expect(buildConfigMap(makeCluster({ replication: { enabled: true } }))?.data?.primary).toBe(
      'test-cluster-0.test-cluster-headless',
    );
  });

  it('writes primary and replica settings for the cluster database into replication.conf', () => {
    const conf = buildConfigMap(makeCluster({ databaseName: 'app.fdb', replication: { enabled: true } }))?.data?.[
      'replication.conf'
    ];
    expect(conf).toContain('database = /var/lib/firebird/data/app.fdb');
    expect(conf).toContain('journal_directory = /var/lib/firebird/data/replication/journal');
    expect(conf).toContain('journal_archive_directory = /var/lib/firebird/data/replication/archive');
    expect(conf).toContain('journal_source_directory = /var/lib/firebird/data/replication/source');
    expect(conf).toContain('journal_archive_timeout = 10');
  });
});

describe('buildNetworkPolicy (intra-cluster traffic)', () => {
  it('lets the cluster pods reach the database and, with replication, the segment port', () => {
    const np = buildNetworkPolicy(
      makeCluster({
        networkPolicy: { enabled: true, ingressFrom: [{ podSelector: { app: 'client' } }] },
        replication: { enabled: true },
      }),
    );
    expect(np.spec?.ingress).toContainEqual({
      _from: [{ podSelector: { matchLabels: { 'firebird.cloudnative-firebird.io/cluster': 'test-cluster' } } }],
      ports: [
        { protocol: 'TCP', port: 3050 },
        { protocol: 'TCP', port: 3051 },
      ],
    });
  });

  it('keeps the "from" restrictions on the wire, when created and when patched', async () => {
    // the client's typed serializer (used for create) maps _from to "from"; a plain object with a
    // "from" key would lose its restrictions and allow every pod
    const { ObjectSerializer } = await import('../node_modules/@kubernetes/client-node/dist/gen/models/ObjectSerializer.js');
    const np = buildNetworkPolicy(
      makeCluster({ networkPolicy: { enabled: true, ingressFrom: [{ podSelector: { app: 'client' } }] }, replication: { enabled: true } }),
    );
    const created = ObjectSerializer.serialize(np, 'V1NetworkPolicy', '') as { spec: { ingress: Array<{ from?: unknown[] }> } };
    const patched = networkPolicyWireFormat(np) as { spec: { ingress: Array<{ from?: unknown[]; _from?: unknown }> } };
    for (const ingress of [created.spec.ingress, patched.spec.ingress]) {
      expect(ingress).toHaveLength(3);
      for (const rule of ingress) {
        expect(rule.from?.length).toBeGreaterThan(0);
        expect(rule).not.toHaveProperty('_from');
      }
    }
    expect(created.spec.ingress[0].from).toEqual([{ podSelector: { matchLabels: { app: 'client' } } }]);
    // the operator can reach the segment port to measure replication lag
    expect(patched.spec.ingress[2]).toEqual({
      from: [
        {
          namespaceSelector: { matchLabels: { 'kubernetes.io/metadata.name': 'cloudnative-firebird-system' } },
          podSelector: { matchLabels: { 'app.kubernetes.io/name': 'cloudnative-firebird' } },
        },
      ],
      ports: [{ protocol: 'TCP', port: 3051 }],
    });
  });

  it('admits the instances of the clusters cloning from it, and only those', () => {
    const source = makeCluster({ networkPolicy: { enabled: true, ingressFrom: [{ podSelector: { app: 'client' } }] } });
    const clusterIn = (namespace: string, name: string, clone?: { sourceCluster: string; namespace?: string }): FirebirdCluster => ({
      ...makeCluster(clone ? { bootstrap: { clone } } : {}),
      metadata: { name, namespace, uid: name },
    });
    const clones = cloneTargets(source, [
      clusterIn('default', 'copy', { sourceCluster: 'test-cluster' }),
      clusterIn('staging', 'staging-copy', { sourceCluster: 'test-cluster', namespace: 'default' }),
      clusterIn('staging', 'same-name-elsewhere', { sourceCluster: 'test-cluster' }), // its own namespace
      clusterIn('default', 'other', { sourceCluster: 'another-cluster' }),
      clusterIn('default', 'plain'),
    ]);
    expect(clones).toEqual([
      { namespace: 'default', name: 'copy' },
      { namespace: 'staging', name: 'staging-copy' },
    ]);
    const wire = networkPolicyWireFormat(buildNetworkPolicy(source, clones)) as {
      spec: { ingress: Array<{ from?: unknown[]; ports?: unknown[] }> };
    };
    expect(wire.spec.ingress).toContainEqual({
      from: [
        {
          namespaceSelector: { matchLabels: { 'kubernetes.io/metadata.name': 'default' } },
          podSelector: { matchLabels: { 'firebird.cloudnative-firebird.io/cluster': 'copy' } },
        },
        {
          namespaceSelector: { matchLabels: { 'kubernetes.io/metadata.name': 'staging' } },
          podSelector: { matchLabels: { 'firebird.cloudnative-firebird.io/cluster': 'staging-copy' } },
        },
      ],
      ports: [{ protocol: 'TCP', port: 3050 }],
    });
    // no clone, no rule (clients, the cluster's own pods, the operator)
    expect(buildNetworkPolicy(source).spec?.ingress).toHaveLength(3);
  });

  it('opens the backup file server port to the cluster\'s own pods and the operator only, without replication', () => {
    const np = buildNetworkPolicy(makeCluster({ networkPolicy: { enabled: true } }));
    const withPort = (np.spec?.ingress ?? []).filter((rule) => (rule.ports ?? []).some((p) => p.port === 3051));
    expect(withPort.map((rule) => rule._from)).toEqual([
      [{ podSelector: { matchLabels: { 'firebird.cloudnative-firebird.io/cluster': 'test-cluster' } } }],
      // the operator asks it whether restore targets exist
      [
        {
          namespaceSelector: { matchLabels: { 'kubernetes.io/metadata.name': 'cloudnative-firebird-system' } },
          podSelector: { matchLabels: { 'app.kubernetes.io/name': 'cloudnative-firebird' } },
        },
      ],
    ]);
  });
});
