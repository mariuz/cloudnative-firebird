import { buildService, buildStatefulSet, buildHeadlessService, buildReplicaService, clusterLabels, statefulSetNeedsUpdate } from '../src/utils/resources';
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
