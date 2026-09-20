import { describe, it, expect, vi, type Mock } from 'vitest';
import { FirebirdClusterController } from '../src/controllers/firebirdcluster.controller';
import { FirebirdCluster } from '../src/types';
import { AppsV1Api, BatchV1Api, CoordinationV1Api, CoreV1Api, CustomObjectsApi, KubeConfig, PolicyV1Api } from '@kubernetes/client-node';

/** Shape of arguments passed to createNamespacedService mock calls */
type CreateServiceCall = [{ namespace: string; body: { metadata?: { name?: string; labels?: Record<string, string>; ownerReferences?: Array<{ kind?: string; uid?: string }> } } }];
/** Shape of arguments passed to readNamespacedService mock calls */
type ReadServiceCall = [{ name: string; namespace: string }];

// Build a minimal FirebirdCluster fixture
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

// Kubernetes HTTP 404 error shape emitted by @kubernetes/client-node
const notFoundError = Object.assign(new Error('Not Found'), { statusCode: 404 });

// Build a mock KubeConfig whose makeApiClient returns controllable fakes
function makeMockKubeConfig({
  readNamespacedServiceImpl = vi.fn().mockRejectedValue(notFoundError),
  createNamespacedServiceImpl = vi.fn().mockResolvedValue({}),
  readNamespacedStatefulSetImpl = vi.fn().mockRejectedValue(notFoundError),
  createNamespacedStatefulSetImpl = vi.fn().mockImplementation(async ({ body }: { body?: { spec?: { replicas?: number } } }) => ({
    ...body,
    status: { readyReplicas: body?.spec?.replicas ?? 1 },
  })),
  patchNamespacedStatefulSetImpl = vi.fn().mockImplementation(async ({ body }: { body?: { spec?: { replicas?: number } } }) => ({
    ...body,
    status: { readyReplicas: body?.spec?.replicas ?? 1 },
  })),
  readNamespacedCronJobImpl = vi.fn().mockRejectedValue(notFoundError),
  createNamespacedCronJobImpl = vi.fn().mockResolvedValue({}),
  patchNamespacedCronJobImpl = vi.fn().mockResolvedValue({}),
  deleteNamespacedCronJobImpl = vi.fn().mockResolvedValue({}),
  getNamespacedCustomObjectImpl = vi.fn().mockRejectedValue(notFoundError),
  createNamespacedCustomObjectImpl = vi.fn().mockResolvedValue({}),
  deleteNamespacedCustomObjectImpl = vi.fn().mockResolvedValue({}),
  patchNamespacedCustomObjectStatusImpl = vi.fn().mockResolvedValue({}),
  readNamespacedPodDisruptionBudgetImpl = vi.fn().mockRejectedValue(notFoundError),
  createNamespacedPodDisruptionBudgetImpl = vi.fn().mockResolvedValue({}),
  patchNamespacedPodDisruptionBudgetImpl = vi.fn().mockResolvedValue({}),
  deleteNamespacedPodDisruptionBudgetImpl = vi.fn().mockResolvedValue({}),
}: {
  readNamespacedServiceImpl?: Mock;
  createNamespacedServiceImpl?: Mock;
  readNamespacedStatefulSetImpl?: Mock;
  createNamespacedStatefulSetImpl?: Mock;
  patchNamespacedStatefulSetImpl?: Mock;
  readNamespacedCronJobImpl?: Mock;
  createNamespacedCronJobImpl?: Mock;
  patchNamespacedCronJobImpl?: Mock;
  deleteNamespacedCronJobImpl?: Mock;
  getNamespacedCustomObjectImpl?: Mock;
  createNamespacedCustomObjectImpl?: Mock;
  deleteNamespacedCustomObjectImpl?: Mock;
  patchNamespacedCustomObjectStatusImpl?: Mock;
  readNamespacedPodDisruptionBudgetImpl?: Mock;
  createNamespacedPodDisruptionBudgetImpl?: Mock;
  patchNamespacedPodDisruptionBudgetImpl?: Mock;
  deleteNamespacedPodDisruptionBudgetImpl?: Mock;
} = {}) {
  const mockCoreApi = {
    readNamespacedService: readNamespacedServiceImpl,
    createNamespacedService: createNamespacedServiceImpl,
  };

  const mockAppsApi = {
    readNamespacedStatefulSet: readNamespacedStatefulSetImpl,
    createNamespacedStatefulSet: createNamespacedStatefulSetImpl,
    patchNamespacedStatefulSet: patchNamespacedStatefulSetImpl,
  };

  const mockBatchApi = {
    readNamespacedCronJob: readNamespacedCronJobImpl,
    createNamespacedCronJob: createNamespacedCronJobImpl,
    patchNamespacedCronJob: patchNamespacedCronJobImpl,
    deleteNamespacedCronJob: deleteNamespacedCronJobImpl,
  };

  const mockCustomApi = {
    getNamespacedCustomObject: getNamespacedCustomObjectImpl,
    createNamespacedCustomObject: createNamespacedCustomObjectImpl,
    deleteNamespacedCustomObject: deleteNamespacedCustomObjectImpl,
    patchNamespacedCustomObjectStatus: patchNamespacedCustomObjectStatusImpl,
  };

  const mockPolicyApi = {
    readNamespacedPodDisruptionBudget: readNamespacedPodDisruptionBudgetImpl,
    createNamespacedPodDisruptionBudget: createNamespacedPodDisruptionBudgetImpl,
    patchNamespacedPodDisruptionBudget: patchNamespacedPodDisruptionBudgetImpl,
    deleteNamespacedPodDisruptionBudget: deleteNamespacedPodDisruptionBudgetImpl,
  };

  const mockCoordinationApi = {
    readNamespacedLease: vi.fn().mockRejectedValue(notFoundError),
    createNamespacedLease: vi.fn().mockResolvedValue({}),
  };

  const mockKubeConfig = new KubeConfig();
  vi.spyOn(mockKubeConfig, 'makeApiClient').mockImplementation((ApiType: unknown) => {
    if (ApiType === CoreV1Api) return mockCoreApi as unknown as CoreV1Api;
    if (ApiType === AppsV1Api) return mockAppsApi as unknown as AppsV1Api;
    if (ApiType === BatchV1Api) return mockBatchApi as unknown as BatchV1Api;
    if (ApiType === CustomObjectsApi) return mockCustomApi as unknown as CustomObjectsApi;
    if (ApiType === PolicyV1Api) return mockPolicyApi as unknown as PolicyV1Api;
    if (ApiType === CoordinationV1Api) return mockCoordinationApi as unknown as CoordinationV1Api;
    return {} as unknown as CoreV1Api;
  });

  return {
    mockKubeConfig,
    mockCoreApi,
    mockAppsApi,
    mockBatchApi,
    mockCustomApi,
    mockPolicyApi,
  };
}

describe('FirebirdClusterController – replication integration', () => {
  describe('reconcile() with replication enabled', () => {
    it('creates the replica service when replication is enabled', async () => {
      const { mockKubeConfig, mockCoreApi } = makeMockKubeConfig();
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ replication: { enabled: true } });

      await controller.reconcile(cluster);

      const createdNames = (mockCoreApi.createNamespacedService as Mock).mock.calls.map(
        (call: CreateServiceCall) => call[0].body?.metadata?.name,
      );
      expect(createdNames).toContain('test-cluster-replica');
    });

    it('creates the headless and primary services alongside the replica service', async () => {
      const { mockKubeConfig, mockCoreApi } = makeMockKubeConfig();
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ replication: { enabled: true } });

      await controller.reconcile(cluster);

      const createdNames = (mockCoreApi.createNamespacedService as Mock).mock.calls.map(
        (call: CreateServiceCall) => call[0].body?.metadata?.name,
      );
      expect(createdNames).toContain('test-cluster-headless');
      expect(createdNames).toContain('test-cluster');
      expect(createdNames).toContain('test-cluster-replica');
    });

    it('reads the replica service to check existence before creating it', async () => {
      const { mockKubeConfig, mockCoreApi } = makeMockKubeConfig();
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ replication: { enabled: true } });

      await controller.reconcile(cluster);

      const readCalls = (mockCoreApi.readNamespacedService as Mock).mock.calls.map(
        (call: ReadServiceCall) => call[0].name,
      );
      expect(readCalls).toContain('test-cluster-replica');
    });

    it('does not create the replica service when it already exists', async () => {
      const readNamespacedServiceImpl = vi.fn().mockResolvedValue({}); // all services exist
      const createNamespacedServiceImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCoreApi } = makeMockKubeConfig({
        readNamespacedServiceImpl,
        createNamespacedServiceImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ replication: { enabled: true } });

      await controller.reconcile(cluster);

      const createdNames = (mockCoreApi.createNamespacedService as Mock).mock.calls.map(
        (call: CreateServiceCall) => call[0].body?.metadata?.name,
      );
      expect(createdNames).not.toContain('test-cluster-replica');
    });

    it('creates a StatefulSet with FIREBIRD_REPLICATION_ENABLED=true', async () => {
      const createNamespacedStatefulSetImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockAppsApi } = makeMockKubeConfig({
        createNamespacedStatefulSetImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ replication: { enabled: true } });

      await controller.reconcile(cluster);

      const createdSts = (mockAppsApi.createNamespacedStatefulSet as Mock).mock.calls[0][0]
        .body;
      const container = createdSts.spec?.template?.spec?.containers?.[0];
      const replicationEnv = container?.env?.find(
        (e: { name: string }) => e.name === 'FIREBIRD_REPLICATION_ENABLED',
      );
      expect(replicationEnv?.value).toBe('true');
    });

    it('creates a StatefulSet with FIREBIRD_REPLICATION_MODE=async by default', async () => {
      const createNamespacedStatefulSetImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockAppsApi } = makeMockKubeConfig({
        createNamespacedStatefulSetImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ replication: { enabled: true } });

      await controller.reconcile(cluster);

      const createdSts = (mockAppsApi.createNamespacedStatefulSet as Mock).mock.calls[0][0]
        .body;
      const container = createdSts.spec?.template?.spec?.containers?.[0];
      const modeEnv = container?.env?.find(
        (e: { name: string }) => e.name === 'FIREBIRD_REPLICATION_MODE',
      );
      expect(modeEnv?.value).toBe('async');
    });

    it('creates a StatefulSet with FIREBIRD_REPLICATION_MODE=sync when mode is sync', async () => {
      const createNamespacedStatefulSetImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockAppsApi } = makeMockKubeConfig({
        createNamespacedStatefulSetImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ replication: { enabled: true, mode: 'sync' } });

      await controller.reconcile(cluster);

      const createdSts = (mockAppsApi.createNamespacedStatefulSet as Mock).mock.calls[0][0]
        .body;
      const container = createdSts.spec?.template?.spec?.containers?.[0];
      const modeEnv = container?.env?.find(
        (e: { name: string }) => e.name === 'FIREBIRD_REPLICATION_MODE',
      );
      expect(modeEnv?.value).toBe('sync');
    });

    it('creates a StatefulSet with FIREBIRD_REPLICATION_MODE=async when mode is explicitly async', async () => {
      const createNamespacedStatefulSetImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockAppsApi } = makeMockKubeConfig({
        createNamespacedStatefulSetImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ replication: { enabled: true, mode: 'async' } });

      await controller.reconcile(cluster);

      const createdSts = (mockAppsApi.createNamespacedStatefulSet as Mock).mock.calls[0][0]
        .body;
      const container = createdSts.spec?.template?.spec?.containers?.[0];
      const modeEnv = container?.env?.find(
        (e: { name: string }) => e.name === 'FIREBIRD_REPLICATION_MODE',
      );
      expect(modeEnv?.value).toBe('async');
    });

    it('updates status to Running after successful reconciliation with replication', async () => {
      const patchNamespacedCustomObjectStatusImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCustomApi } = makeMockKubeConfig({
        patchNamespacedCustomObjectStatusImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ replication: { enabled: true } });

      await controller.reconcile(cluster);

      const patchCalls = (mockCustomApi.patchNamespacedCustomObjectStatus as Mock).mock.calls;
      const lastPatch = patchCalls[patchCalls.length - 1][0];
      const patchedStatus = lastPatch.body[0].value;
      expect(patchedStatus.phase).toBe('Running');
    });
  });

  describe('reconcile() with replication disabled or absent', () => {
    it('does not create the replica service when replication is disabled', async () => {
      const { mockKubeConfig, mockCoreApi } = makeMockKubeConfig();
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ replication: { enabled: false } });

      await controller.reconcile(cluster);

      const createdNames = (mockCoreApi.createNamespacedService as Mock).mock.calls.map(
        (call: CreateServiceCall) => call[0].body?.metadata?.name,
      );
      expect(createdNames).not.toContain('test-cluster-replica');
    });

    it('does not create the replica service when replication spec is absent', async () => {
      const { mockKubeConfig, mockCoreApi } = makeMockKubeConfig();
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster(); // no replication spec

      await controller.reconcile(cluster);

      const createdNames = (mockCoreApi.createNamespacedService as Mock).mock.calls.map(
        (call: CreateServiceCall) => call[0].body?.metadata?.name,
      );
      expect(createdNames).not.toContain('test-cluster-replica');
    });

    it('does not read the replica service when replication is absent', async () => {
      const { mockKubeConfig, mockCoreApi } = makeMockKubeConfig();
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster();

      await controller.reconcile(cluster);

      const readNames = (mockCoreApi.readNamespacedService as Mock).mock.calls.map(
        (call: ReadServiceCall) => call[0].name,
      );
      expect(readNames).not.toContain('test-cluster-replica');
    });

    it('still creates headless and primary services when replication is disabled', async () => {
      const { mockKubeConfig, mockCoreApi } = makeMockKubeConfig();
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ replication: { enabled: false } });

      await controller.reconcile(cluster);

      const createdNames = (mockCoreApi.createNamespacedService as Mock).mock.calls.map(
        (call: CreateServiceCall) => call[0].body?.metadata?.name,
      );
      expect(createdNames).toContain('test-cluster-headless');
      expect(createdNames).toContain('test-cluster');
    });

    it('does not inject replication env vars when replication is disabled', async () => {
      const createNamespacedStatefulSetImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockAppsApi } = makeMockKubeConfig({
        createNamespacedStatefulSetImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ replication: { enabled: false } });

      await controller.reconcile(cluster);

      const createdSts = (mockAppsApi.createNamespacedStatefulSet as Mock).mock.calls[0][0]
        .body;
      const container = createdSts.spec?.template?.spec?.containers?.[0];
      const replicationEnv = container?.env?.find(
        (e: { name: string }) => e.name === 'FIREBIRD_REPLICATION_ENABLED',
      );
      expect(replicationEnv).toBeUndefined();
    });

    it('updates status to Running after successful reconciliation without replication', async () => {
      const patchNamespacedCustomObjectStatusImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCustomApi } = makeMockKubeConfig({
        patchNamespacedCustomObjectStatusImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster();

      await controller.reconcile(cluster);

      const patchCalls = (mockCustomApi.patchNamespacedCustomObjectStatus as Mock).mock.calls;
      const lastPatch = patchCalls[patchCalls.length - 1][0];
      const patchedStatus = lastPatch.body[0].value;
      expect(patchedStatus.phase).toBe('Running');
    });
  });

  describe('reconcile() error handling with replication', () => {
    it('throws and sets Degraded status when replica service creation fails', async () => {
      const replicaCreateError = new Error('replica service creation failed');
      const createNamespacedServiceImpl = vi.fn().mockImplementation(
        ({ body }: { body: { metadata?: { name?: string } } }) => {
          if (body?.metadata?.name === 'test-cluster-replica') {
            return Promise.reject(replicaCreateError);
          }
          return Promise.resolve({});
        },
      );
      const patchNamespacedCustomObjectStatusImpl = vi.fn().mockResolvedValue({});

      const { mockKubeConfig, mockCustomApi } = makeMockKubeConfig({
        createNamespacedServiceImpl,
        patchNamespacedCustomObjectStatusImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ replication: { enabled: true } });

      await expect(controller.reconcile(cluster)).rejects.toThrow(
        'replica service creation failed',
      );

      const patchCalls = (mockCustomApi.patchNamespacedCustomObjectStatus as Mock).mock.calls;
      const degradedPatch = patchCalls.find(
        (call: [{ body: Array<{ value: { phase?: string } }> }]) =>
          call[0].body[0].value.phase === 'Degraded',
      );
      expect(degradedPatch).toBeDefined();
    });

    it('sets status to Creating at the start of reconciliation', async () => {
      const patchNamespacedCustomObjectStatusImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCustomApi } = makeMockKubeConfig({
        patchNamespacedCustomObjectStatusImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ replication: { enabled: true } });

      await controller.reconcile(cluster);

      const firstPatch = (
        mockCustomApi.patchNamespacedCustomObjectStatus as Mock
      ).mock.calls[0][0];
      expect(firstPatch.body[0].value.phase).toBe('Creating');
    });
  });

  describe('reconcile() StatefulSet update with replication changes', () => {
    it('patches the StatefulSet when it already exists and needs updating', async () => {
      const outdatedSts = {
        spec: {
          replicas: 1,
          template: {
            spec: {
              containers: [{ image: 'firebirdsql/firebird:3.0' }],
            },
          },
        },
      };
      const readNamespacedStatefulSetImpl = vi.fn().mockResolvedValue(outdatedSts);
      const patchNamespacedStatefulSetImpl = vi.fn().mockResolvedValue({});

      const { mockKubeConfig, mockAppsApi } = makeMockKubeConfig({
        readNamespacedStatefulSetImpl,
        patchNamespacedStatefulSetImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      // Use a different image so the StatefulSet will need updating
      const cluster = makeCluster({
        imageName: 'firebirdsql/firebird:4.0',
        replication: { enabled: true },
      });

      await controller.reconcile(cluster);

      expect(mockAppsApi.patchNamespacedStatefulSet as Mock).toHaveBeenCalledTimes(1);
    });

    it('does not patch the StatefulSet when it is already up to date', async () => {
      const createNamespacedStatefulSetImpl = vi.fn().mockResolvedValue({});
      // Simulate StatefulSet not existing (404) so it gets created, not patched
      const readNamespacedStatefulSetImpl = vi.fn().mockRejectedValue(notFoundError);
      const patchNamespacedStatefulSetImpl = vi.fn().mockResolvedValue({});

      const { mockKubeConfig, mockAppsApi } = makeMockKubeConfig({
        readNamespacedStatefulSetImpl,
        createNamespacedStatefulSetImpl,
        patchNamespacedStatefulSetImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ replication: { enabled: true } });

      await controller.reconcile(cluster);

      expect(mockAppsApi.patchNamespacedStatefulSet as Mock).not.toHaveBeenCalled();
      expect(mockAppsApi.createNamespacedStatefulSet as Mock).toHaveBeenCalledTimes(1);
    });
  });

  describe('reconcile() replica service has correct shape', () => {
    it('creates replica service in the correct namespace', async () => {
      const createNamespacedServiceImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCoreApi } = makeMockKubeConfig({
        createNamespacedServiceImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ replication: { enabled: true } });

      await controller.reconcile(cluster);

      const replicaCreate = (mockCoreApi.createNamespacedService as Mock).mock.calls.find(
        (call: CreateServiceCall) =>
          call[0].body?.metadata?.name === 'test-cluster-replica',
      );
      expect(replicaCreate).toBeDefined();
      expect(replicaCreate[0].namespace).toBe('default');
    });

    it('creates replica service with database-replica component label', async () => {
      const createNamespacedServiceImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCoreApi } = makeMockKubeConfig({
        createNamespacedServiceImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ replication: { enabled: true } });

      await controller.reconcile(cluster);

      const replicaCreate = (mockCoreApi.createNamespacedService as Mock).mock.calls.find(
        (call: CreateServiceCall) =>
          call[0].body?.metadata?.name === 'test-cluster-replica',
      );
      expect(
        replicaCreate[0].body?.metadata?.labels?.['app.kubernetes.io/component'],
      ).toBe('database-replica');
    });

    it('creates replica service with ownerReference pointing to the FirebirdCluster', async () => {
      const createNamespacedServiceImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCoreApi } = makeMockKubeConfig({
        createNamespacedServiceImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ replication: { enabled: true } });

      await controller.reconcile(cluster);

      const replicaCreate = (mockCoreApi.createNamespacedService as Mock).mock.calls.find(
        (call: CreateServiceCall) => call[0].body?.metadata?.name === 'test-cluster-replica',
      );
      const ownerRef = replicaCreate[0].body?.metadata?.ownerReferences?.[0];
      expect(ownerRef?.kind).toBe('FirebirdCluster');
      expect(ownerRef?.uid).toBe('test-uid-1234');
    });

    it('creates journal archive CronJob when journalArchiveS3 is configured', async () => {
      const createNamespacedCronJobImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockBatchApi } = makeMockKubeConfig({
        createNamespacedCronJobImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({
        replication: {
          enabled: true,
          journalArchiveS3: {
            bucket: 'archive-bucket',
            secretRef: { name: 's3-secret' },
          },
        },
      });

      await controller.reconcile(cluster);

      expect(mockBatchApi.createNamespacedCronJob).toHaveBeenCalledTimes(1);
      const call = (mockBatchApi.createNamespacedCronJob as Mock).mock.calls[0][0];
      expect(call.body.metadata.name).toBe('test-cluster-journal-archive');
    });

    it('updates status with replicationStatus details when replication is enabled', async () => {
      const patchNamespacedCustomObjectStatusImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCustomApi } = makeMockKubeConfig({
        patchNamespacedCustomObjectStatusImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({
        instances: 3,
        replication: {
          enabled: true,
          mode: 'sync',
        },
      });

      await controller.reconcile(cluster);

      const statusCalls = (mockCustomApi.patchNamespacedCustomObjectStatus as Mock).mock.calls;
      const lastStatusCall = statusCalls[statusCalls.length - 1][0];
      const statusValue = lastStatusCall.body[0].value;
      expect(statusValue.replicationStatus).toEqual({
        primaryPod: 'test-cluster-0',
        activeReplicas: 2,
        syncReplicas: ['test-cluster-1', 'test-cluster-2'],
      });
    });
  });
});
