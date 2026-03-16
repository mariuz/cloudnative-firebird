// Mock the ESM-only @kubernetes/client-node package before any imports
jest.mock('@kubernetes/client-node', () => {
  const makeApiClient = jest.fn();
  class KubeConfig {
    makeApiClient = makeApiClient;
  }
  class AppsV1Api {}
  class CoreV1Api {}
  class CustomObjectsApi {}
  return { KubeConfig, AppsV1Api, CoreV1Api, CustomObjectsApi };
});

import { FirebirdClusterController } from '../src/controllers/firebirdcluster.controller';
import { FirebirdCluster } from '../src/types';
import { AppsV1Api, CoreV1Api, CustomObjectsApi, KubeConfig } from '@kubernetes/client-node';

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
  readNamespacedServiceImpl = jest.fn().mockRejectedValue(notFoundError),
  createNamespacedServiceImpl = jest.fn().mockResolvedValue({}),
  readNamespacedStatefulSetImpl = jest.fn().mockRejectedValue(notFoundError),
  createNamespacedStatefulSetImpl = jest.fn().mockResolvedValue({}),
  patchNamespacedStatefulSetImpl = jest.fn().mockResolvedValue({}),
  patchNamespacedCustomObjectStatusImpl = jest.fn().mockResolvedValue({}),
}: {
  readNamespacedServiceImpl?: jest.Mock;
  createNamespacedServiceImpl?: jest.Mock;
  readNamespacedStatefulSetImpl?: jest.Mock;
  createNamespacedStatefulSetImpl?: jest.Mock;
  patchNamespacedStatefulSetImpl?: jest.Mock;
  patchNamespacedCustomObjectStatusImpl?: jest.Mock;
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

  const mockCustomApi = {
    patchNamespacedCustomObjectStatus: patchNamespacedCustomObjectStatusImpl,
  };

  const mockKubeConfig = new KubeConfig();
  (mockKubeConfig.makeApiClient as jest.Mock).mockImplementation((ApiType: unknown) => {
    if (ApiType === CoreV1Api) return mockCoreApi;
    if (ApiType === AppsV1Api) return mockAppsApi;
    if (ApiType === CustomObjectsApi) return mockCustomApi;
    return {};
  });

  return {
    mockKubeConfig,
    mockCoreApi,
    mockAppsApi,
    mockCustomApi,
  };
}

describe('FirebirdClusterController – replication integration', () => {
  describe('reconcile() with replication enabled', () => {
    it('creates the replica service when replication is enabled', async () => {
      const { mockKubeConfig, mockCoreApi } = makeMockKubeConfig();
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ replication: { enabled: true } });

      await controller.reconcile(cluster);

      // Expect a service named "test-cluster-replica" to be created
      const createdNames = (mockCoreApi.createNamespacedService as jest.Mock).mock.calls.map(
        (call: CreateServiceCall) => call[0].body?.metadata?.name,
      );
      expect(createdNames).toContain('test-cluster-replica');
    });

    it('creates the headless and primary services alongside the replica service', async () => {
      const { mockKubeConfig, mockCoreApi } = makeMockKubeConfig();
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ replication: { enabled: true } });

      await controller.reconcile(cluster);

      const createdNames = (mockCoreApi.createNamespacedService as jest.Mock).mock.calls.map(
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

      const readCalls = (mockCoreApi.readNamespacedService as jest.Mock).mock.calls.map(
        (call: ReadServiceCall) => call[0].name,
      );
      expect(readCalls).toContain('test-cluster-replica');
    });

    it('does not create the replica service when it already exists', async () => {
      const readNamespacedServiceImpl = jest.fn().mockResolvedValue({}); // all services exist
      const createNamespacedServiceImpl = jest.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCoreApi } = makeMockKubeConfig({
        readNamespacedServiceImpl,
        createNamespacedServiceImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ replication: { enabled: true } });

      await controller.reconcile(cluster);

      const createdNames = (mockCoreApi.createNamespacedService as jest.Mock).mock.calls.map(
        (call: CreateServiceCall) => call[0].body?.metadata?.name,
      );
      expect(createdNames).not.toContain('test-cluster-replica');
    });

    it('creates a StatefulSet with FIREBIRD_REPLICATION_ENABLED=true', async () => {
      const createNamespacedStatefulSetImpl = jest.fn().mockResolvedValue({});
      const { mockKubeConfig, mockAppsApi } = makeMockKubeConfig({
        createNamespacedStatefulSetImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ replication: { enabled: true } });

      await controller.reconcile(cluster);

      const createdSts = (mockAppsApi.createNamespacedStatefulSet as jest.Mock).mock.calls[0][0]
        .body;
      const container = createdSts.spec?.template?.spec?.containers?.[0];
      const replicationEnv = container?.env?.find(
        (e: { name: string }) => e.name === 'FIREBIRD_REPLICATION_ENABLED',
      );
      expect(replicationEnv?.value).toBe('true');
    });

    it('creates a StatefulSet with FIREBIRD_REPLICATION_MODE=async by default', async () => {
      const createNamespacedStatefulSetImpl = jest.fn().mockResolvedValue({});
      const { mockKubeConfig, mockAppsApi } = makeMockKubeConfig({
        createNamespacedStatefulSetImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ replication: { enabled: true } });

      await controller.reconcile(cluster);

      const createdSts = (mockAppsApi.createNamespacedStatefulSet as jest.Mock).mock.calls[0][0]
        .body;
      const container = createdSts.spec?.template?.spec?.containers?.[0];
      const modeEnv = container?.env?.find(
        (e: { name: string }) => e.name === 'FIREBIRD_REPLICATION_MODE',
      );
      expect(modeEnv?.value).toBe('async');
    });

    it('creates a StatefulSet with FIREBIRD_REPLICATION_MODE=sync when mode is sync', async () => {
      const createNamespacedStatefulSetImpl = jest.fn().mockResolvedValue({});
      const { mockKubeConfig, mockAppsApi } = makeMockKubeConfig({
        createNamespacedStatefulSetImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ replication: { enabled: true, mode: 'sync' } });

      await controller.reconcile(cluster);

      const createdSts = (mockAppsApi.createNamespacedStatefulSet as jest.Mock).mock.calls[0][0]
        .body;
      const container = createdSts.spec?.template?.spec?.containers?.[0];
      const modeEnv = container?.env?.find(
        (e: { name: string }) => e.name === 'FIREBIRD_REPLICATION_MODE',
      );
      expect(modeEnv?.value).toBe('sync');
    });

    it('creates a StatefulSet with FIREBIRD_REPLICATION_MODE=async when mode is explicitly async', async () => {
      const createNamespacedStatefulSetImpl = jest.fn().mockResolvedValue({});
      const { mockKubeConfig, mockAppsApi } = makeMockKubeConfig({
        createNamespacedStatefulSetImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ replication: { enabled: true, mode: 'async' } });

      await controller.reconcile(cluster);

      const createdSts = (mockAppsApi.createNamespacedStatefulSet as jest.Mock).mock.calls[0][0]
        .body;
      const container = createdSts.spec?.template?.spec?.containers?.[0];
      const modeEnv = container?.env?.find(
        (e: { name: string }) => e.name === 'FIREBIRD_REPLICATION_MODE',
      );
      expect(modeEnv?.value).toBe('async');
    });

    it('updates status to Running after successful reconciliation with replication', async () => {
      const patchNamespacedCustomObjectStatusImpl = jest.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCustomApi } = makeMockKubeConfig({
        patchNamespacedCustomObjectStatusImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ replication: { enabled: true } });

      await controller.reconcile(cluster);

      const patchCalls = (mockCustomApi.patchNamespacedCustomObjectStatus as jest.Mock).mock.calls;
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

      const createdNames = (mockCoreApi.createNamespacedService as jest.Mock).mock.calls.map(
        (call: CreateServiceCall) => call[0].body?.metadata?.name,
      );
      expect(createdNames).not.toContain('test-cluster-replica');
    });

    it('does not create the replica service when replication spec is absent', async () => {
      const { mockKubeConfig, mockCoreApi } = makeMockKubeConfig();
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster(); // no replication spec

      await controller.reconcile(cluster);

      const createdNames = (mockCoreApi.createNamespacedService as jest.Mock).mock.calls.map(
        (call: CreateServiceCall) => call[0].body?.metadata?.name,
      );
      expect(createdNames).not.toContain('test-cluster-replica');
    });

    it('does not read the replica service when replication is absent', async () => {
      const { mockKubeConfig, mockCoreApi } = makeMockKubeConfig();
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster();

      await controller.reconcile(cluster);

      const readNames = (mockCoreApi.readNamespacedService as jest.Mock).mock.calls.map(
        (call: ReadServiceCall) => call[0].name,
      );
      expect(readNames).not.toContain('test-cluster-replica');
    });

    it('still creates headless and primary services when replication is disabled', async () => {
      const { mockKubeConfig, mockCoreApi } = makeMockKubeConfig();
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ replication: { enabled: false } });

      await controller.reconcile(cluster);

      const createdNames = (mockCoreApi.createNamespacedService as jest.Mock).mock.calls.map(
        (call: CreateServiceCall) => call[0].body?.metadata?.name,
      );
      expect(createdNames).toContain('test-cluster-headless');
      expect(createdNames).toContain('test-cluster');
    });

    it('does not inject replication env vars when replication is disabled', async () => {
      const createNamespacedStatefulSetImpl = jest.fn().mockResolvedValue({});
      const { mockKubeConfig, mockAppsApi } = makeMockKubeConfig({
        createNamespacedStatefulSetImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ replication: { enabled: false } });

      await controller.reconcile(cluster);

      const createdSts = (mockAppsApi.createNamespacedStatefulSet as jest.Mock).mock.calls[0][0]
        .body;
      const container = createdSts.spec?.template?.spec?.containers?.[0];
      const replicationEnv = container?.env?.find(
        (e: { name: string }) => e.name === 'FIREBIRD_REPLICATION_ENABLED',
      );
      expect(replicationEnv).toBeUndefined();
    });

    it('updates status to Running after successful reconciliation without replication', async () => {
      const patchNamespacedCustomObjectStatusImpl = jest.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCustomApi } = makeMockKubeConfig({
        patchNamespacedCustomObjectStatusImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster();

      await controller.reconcile(cluster);

      const patchCalls = (mockCustomApi.patchNamespacedCustomObjectStatus as jest.Mock).mock.calls;
      const lastPatch = patchCalls[patchCalls.length - 1][0];
      const patchedStatus = lastPatch.body[0].value;
      expect(patchedStatus.phase).toBe('Running');
    });
  });

  describe('reconcile() error handling with replication', () => {
    it('throws and sets Degraded status when replica service creation fails', async () => {
      const replicaCreateError = new Error('replica service creation failed');
      const createNamespacedServiceImpl = jest.fn().mockImplementation(
        ({ body }: { body: { metadata?: { name?: string } } }) => {
          if (body?.metadata?.name === 'test-cluster-replica') {
            return Promise.reject(replicaCreateError);
          }
          return Promise.resolve({});
        },
      );
      const patchNamespacedCustomObjectStatusImpl = jest.fn().mockResolvedValue({});

      const { mockKubeConfig, mockCustomApi } = makeMockKubeConfig({
        createNamespacedServiceImpl,
        patchNamespacedCustomObjectStatusImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ replication: { enabled: true } });

      await expect(controller.reconcile(cluster)).rejects.toThrow(
        'replica service creation failed',
      );

      const patchCalls = (mockCustomApi.patchNamespacedCustomObjectStatus as jest.Mock).mock.calls;
      const degradedPatch = patchCalls.find(
        (call: [{ body: Array<{ value: { phase?: string } }> }]) =>
          call[0].body[0].value.phase === 'Degraded',
      );
      expect(degradedPatch).toBeDefined();
    });

    it('sets status to Creating at the start of reconciliation', async () => {
      const patchNamespacedCustomObjectStatusImpl = jest.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCustomApi } = makeMockKubeConfig({
        patchNamespacedCustomObjectStatusImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ replication: { enabled: true } });

      await controller.reconcile(cluster);

      const firstPatch = (
        mockCustomApi.patchNamespacedCustomObjectStatus as jest.Mock
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
      const readNamespacedStatefulSetImpl = jest.fn().mockResolvedValue(outdatedSts);
      const patchNamespacedStatefulSetImpl = jest.fn().mockResolvedValue({});

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

      expect(mockAppsApi.patchNamespacedStatefulSet as jest.Mock).toHaveBeenCalledTimes(1);
    });

    it('does not patch the StatefulSet when it is already up to date', async () => {
      const createNamespacedStatefulSetImpl = jest.fn().mockResolvedValue({});
      // Simulate StatefulSet not existing (404) so it gets created, not patched
      const readNamespacedStatefulSetImpl = jest.fn().mockRejectedValue(notFoundError);
      const patchNamespacedStatefulSetImpl = jest.fn().mockResolvedValue({});

      const { mockKubeConfig, mockAppsApi } = makeMockKubeConfig({
        readNamespacedStatefulSetImpl,
        createNamespacedStatefulSetImpl,
        patchNamespacedStatefulSetImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ replication: { enabled: true } });

      await controller.reconcile(cluster);

      expect(mockAppsApi.patchNamespacedStatefulSet as jest.Mock).not.toHaveBeenCalled();
      expect(mockAppsApi.createNamespacedStatefulSet as jest.Mock).toHaveBeenCalledTimes(1);
    });
  });

  describe('reconcile() replica service has correct shape', () => {
    it('creates replica service in the correct namespace', async () => {
      const createNamespacedServiceImpl = jest.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCoreApi } = makeMockKubeConfig({
        createNamespacedServiceImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ replication: { enabled: true } });

      await controller.reconcile(cluster);

      const replicaCreate = (mockCoreApi.createNamespacedService as jest.Mock).mock.calls.find(
        (call: CreateServiceCall) =>
          call[0].body?.metadata?.name === 'test-cluster-replica',
      );
      expect(replicaCreate).toBeDefined();
      expect(replicaCreate[0].namespace).toBe('default');
    });

    it('creates replica service with database-replica component label', async () => {
      const createNamespacedServiceImpl = jest.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCoreApi } = makeMockKubeConfig({
        createNamespacedServiceImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ replication: { enabled: true } });

      await controller.reconcile(cluster);

      const replicaCreate = (mockCoreApi.createNamespacedService as jest.Mock).mock.calls.find(
        (call: CreateServiceCall) =>
          call[0].body?.metadata?.name === 'test-cluster-replica',
      );
      expect(
        replicaCreate[0].body?.metadata?.labels?.['app.kubernetes.io/component'],
      ).toBe('database-replica');
    });

    it('creates replica service with ownerReference pointing to the FirebirdCluster', async () => {
      const createNamespacedServiceImpl = jest.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCoreApi } = makeMockKubeConfig({
        createNamespacedServiceImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ replication: { enabled: true } });

      await controller.reconcile(cluster);

      const replicaCreate = (mockCoreApi.createNamespacedService as jest.Mock).mock.calls.find(
        (call: CreateServiceCall) => call[0].body?.metadata?.name === 'test-cluster-replica',
      );
      const ownerRef = replicaCreate[0].body?.metadata?.ownerReferences?.[0];
      expect(ownerRef?.kind).toBe('FirebirdCluster');
      expect(ownerRef?.uid).toBe('test-uid-1234');
    });
  });
});
