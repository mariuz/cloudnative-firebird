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
import { AppsV1Api, CoreV1Api, CustomObjectsApi, KubeConfig } from '@kubernetes/client-node';
import {
  makeCluster,
  makeClusterWithResources,
  makeClusterWithSecret,
  makeNamedCluster,
  notFoundError,
} from './helpers/factories';

/** Arguments passed to createNamespacedService / readNamespacedService mocks */
type CreateServiceCall = [
  { namespace: string; body: { metadata?: { name?: string } } },
];
type ReadServiceCall = [{ name: string; namespace: string }];

/** Arguments passed to createNamespacedStatefulSet / patchNamespacedStatefulSet mocks */
type CreateStatefulSetCall = [{ namespace: string; body: object }];

/** Arguments passed to patchNamespacedCustomObjectStatus */
type PatchStatusCall = [{ body: Array<{ value: { phase?: string } }> }];

/** Build a mock KubeConfig whose makeApiClient returns controllable fakes */
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

  return { mockKubeConfig, mockCoreApi, mockAppsApi, mockCustomApi };
}

// ---------------------------------------------------------------------------
// FirebirdClusterController – basic reconciliation
// ---------------------------------------------------------------------------
describe('FirebirdClusterController – basic reconciliation', () => {
  describe('reconcile() resource creation', () => {
    it('creates a headless service for a new cluster', async () => {
      const { mockKubeConfig, mockCoreApi } = makeMockKubeConfig();
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster();

      await controller.reconcile(cluster);

      const createdNames = (mockCoreApi.createNamespacedService as jest.Mock).mock.calls.map(
        (call: CreateServiceCall) => call[0].body?.metadata?.name,
      );
      expect(createdNames).toContain('test-cluster-headless');
    });

    it('creates a primary ClusterIP service for a new cluster', async () => {
      const { mockKubeConfig, mockCoreApi } = makeMockKubeConfig();
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster();

      await controller.reconcile(cluster);

      const createdNames = (mockCoreApi.createNamespacedService as jest.Mock).mock.calls.map(
        (call: CreateServiceCall) => call[0].body?.metadata?.name,
      );
      expect(createdNames).toContain('test-cluster');
    });

    it('creates both the headless and primary services for a new cluster', async () => {
      const { mockKubeConfig, mockCoreApi } = makeMockKubeConfig();
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster();

      await controller.reconcile(cluster);

      const createdNames = (mockCoreApi.createNamespacedService as jest.Mock).mock.calls.map(
        (call: CreateServiceCall) => call[0].body?.metadata?.name,
      );
      expect(createdNames).toContain('test-cluster-headless');
      expect(createdNames).toContain('test-cluster');
    });

    it('creates a StatefulSet for a new cluster', async () => {
      const createNamespacedStatefulSetImpl = jest.fn().mockResolvedValue({});
      const { mockKubeConfig, mockAppsApi } = makeMockKubeConfig({
        createNamespacedStatefulSetImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster();

      await controller.reconcile(cluster);

      expect(mockAppsApi.createNamespacedStatefulSet as jest.Mock).toHaveBeenCalledTimes(1);
      const call = (mockAppsApi.createNamespacedStatefulSet as jest.Mock)
        .mock.calls[0] as CreateStatefulSetCall;
      expect(call[0].namespace).toBe('default');
    });

    it('reads existing services before attempting to create them', async () => {
      const { mockKubeConfig, mockCoreApi } = makeMockKubeConfig();
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster();

      await controller.reconcile(cluster);

      const readNames = (mockCoreApi.readNamespacedService as jest.Mock).mock.calls.map(
        (call: ReadServiceCall) => call[0].name,
      );
      expect(readNames).toContain('test-cluster-headless');
      expect(readNames).toContain('test-cluster');
    });

    it('skips service creation when the service already exists', async () => {
      const readNamespacedServiceImpl = jest.fn().mockResolvedValue({}); // all exist
      const createNamespacedServiceImpl = jest.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCoreApi } = makeMockKubeConfig({
        readNamespacedServiceImpl,
        createNamespacedServiceImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster();

      await controller.reconcile(cluster);

      expect(mockCoreApi.createNamespacedService as jest.Mock).not.toHaveBeenCalled();
    });

    it('uses the correct namespace when creating resources', async () => {
      const { mockKubeConfig, mockCoreApi } = makeMockKubeConfig();
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeNamedCluster('my-cluster', 'production');

      await controller.reconcile(cluster);

      const call = (mockCoreApi.createNamespacedService as jest.Mock).mock
        .calls[0] as CreateServiceCall;
      expect(call[0].namespace).toBe('production');
    });
  });

  describe('reconcile() StatefulSet management', () => {
    it('patches the StatefulSet when it exists and replicas have changed', async () => {
      const outdatedSts = {
        spec: { replicas: 1, template: { spec: { containers: [{ image: 'old' }] } } },
      };
      const readNamespacedStatefulSetImpl = jest.fn().mockResolvedValue(outdatedSts);
      const patchNamespacedStatefulSetImpl = jest.fn().mockResolvedValue({});

      const { mockKubeConfig, mockAppsApi } = makeMockKubeConfig({
        readNamespacedStatefulSetImpl,
        patchNamespacedStatefulSetImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ instances: 3 }); // different replica count

      await controller.reconcile(cluster);

      expect(mockAppsApi.patchNamespacedStatefulSet as jest.Mock).toHaveBeenCalledTimes(1);
    });

    it('does not patch the StatefulSet when it is up to date', async () => {
      // StatefulSet does not exist (404) → gets created, not patched
      const readNamespacedStatefulSetImpl = jest.fn().mockRejectedValue(notFoundError);
      const createNamespacedStatefulSetImpl = jest.fn().mockResolvedValue({});
      const patchNamespacedStatefulSetImpl = jest.fn().mockResolvedValue({});

      const { mockKubeConfig, mockAppsApi } = makeMockKubeConfig({
        readNamespacedStatefulSetImpl,
        createNamespacedStatefulSetImpl,
        patchNamespacedStatefulSetImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster();

      await controller.reconcile(cluster);

      expect(mockAppsApi.patchNamespacedStatefulSet as jest.Mock).not.toHaveBeenCalled();
      expect(mockAppsApi.createNamespacedStatefulSet as jest.Mock).toHaveBeenCalledTimes(1);
    });

    it('creates a StatefulSet with correct ISC_PASSWORD when a superuser secret is set', async () => {
      const createNamespacedStatefulSetImpl = jest.fn().mockResolvedValue({});
      const { mockKubeConfig, mockAppsApi } = makeMockKubeConfig({
        createNamespacedStatefulSetImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeClusterWithSecret('my-superuser-secret');

      await controller.reconcile(cluster);

      const createdSts = (mockAppsApi.createNamespacedStatefulSet as jest.Mock).mock.calls[0][0]
        .body;
      const container = createdSts.spec?.template?.spec?.containers?.[0];
      const passwordEnv = container?.env?.find(
        (e: { name: string }) => e.name === 'ISC_PASSWORD',
      );
      expect(passwordEnv?.valueFrom?.secretKeyRef?.name).toBe('my-superuser-secret');
    });

    it('creates a StatefulSet with resource requests and limits when resources are specified', async () => {
      const createNamespacedStatefulSetImpl = jest.fn().mockResolvedValue({});
      const { mockKubeConfig, mockAppsApi } = makeMockKubeConfig({
        createNamespacedStatefulSetImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeClusterWithResources();

      await controller.reconcile(cluster);

      const createdSts = (mockAppsApi.createNamespacedStatefulSet as jest.Mock).mock.calls[0][0]
        .body;
      const container = createdSts.spec?.template?.spec?.containers?.[0];
      expect(container?.resources?.requests?.['cpu']).toBe('100m');
      expect(container?.resources?.requests?.['memory']).toBe('256Mi');
      expect(container?.resources?.limits?.['cpu']).toBe('500m');
      expect(container?.resources?.limits?.['memory']).toBe('512Mi');
    });
  });

  describe('reconcile() status lifecycle', () => {
    it('sets status to Creating at the start of reconciliation', async () => {
      const patchNamespacedCustomObjectStatusImpl = jest.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCustomApi } = makeMockKubeConfig({
        patchNamespacedCustomObjectStatusImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster();

      await controller.reconcile(cluster);

      const firstPatch = (
        mockCustomApi.patchNamespacedCustomObjectStatus as jest.Mock
      ).mock.calls[0] as PatchStatusCall;
      expect(firstPatch[0].body[0].value.phase).toBe('Creating');
    });

    it('sets status to Running after a successful reconciliation', async () => {
      const patchNamespacedCustomObjectStatusImpl = jest.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCustomApi } = makeMockKubeConfig({
        patchNamespacedCustomObjectStatusImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster();

      await controller.reconcile(cluster);

      const patchCalls = (mockCustomApi.patchNamespacedCustomObjectStatus as jest.Mock).mock.calls;
      const lastPatch = patchCalls[patchCalls.length - 1] as PatchStatusCall;
      expect(lastPatch[0].body[0].value.phase).toBe('Running');
    });

    it('includes a Ready=True condition after a successful reconciliation', async () => {
      const patchNamespacedCustomObjectStatusImpl = jest.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCustomApi } = makeMockKubeConfig({
        patchNamespacedCustomObjectStatusImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster();

      await controller.reconcile(cluster);

      const patchCalls = (mockCustomApi.patchNamespacedCustomObjectStatus as jest.Mock).mock.calls;
      const lastPatch = patchCalls[patchCalls.length - 1] as PatchStatusCall;
      const conditions = (lastPatch[0].body[0].value as { conditions?: Array<{ type: string; status: string }> }).conditions ?? [];
      const readyCondition = conditions.find((c) => c.type === 'Ready');
      expect(readyCondition?.status).toBe('True');
    });

    it('sets status to Degraded and re-throws when StatefulSet creation fails', async () => {
      const stsError = new Error('statefulset creation failed');
      const createNamespacedStatefulSetImpl = jest.fn().mockRejectedValue(stsError);
      const patchNamespacedCustomObjectStatusImpl = jest.fn().mockResolvedValue({});

      const { mockKubeConfig, mockCustomApi } = makeMockKubeConfig({
        createNamespacedStatefulSetImpl,
        patchNamespacedCustomObjectStatusImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster();

      await expect(controller.reconcile(cluster)).rejects.toThrow('statefulset creation failed');

      const patchCalls = (mockCustomApi.patchNamespacedCustomObjectStatus as jest.Mock).mock.calls;
      const degradedPatch = patchCalls.find(
        (call: PatchStatusCall) => call[0].body[0].value.phase === 'Degraded',
      );
      expect(degradedPatch).toBeDefined();
    });

    it('sets status to Degraded and re-throws when service creation fails', async () => {
      const svcError = new Error('service creation failed');
      const createNamespacedServiceImpl = jest.fn().mockRejectedValue(svcError);
      const patchNamespacedCustomObjectStatusImpl = jest.fn().mockResolvedValue({});

      const { mockKubeConfig, mockCustomApi } = makeMockKubeConfig({
        createNamespacedServiceImpl,
        patchNamespacedCustomObjectStatusImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster();

      await expect(controller.reconcile(cluster)).rejects.toThrow('service creation failed');

      const patchCalls = (mockCustomApi.patchNamespacedCustomObjectStatus as jest.Mock).mock.calls;
      const degradedPatch = patchCalls.find(
        (call: PatchStatusCall) => call[0].body[0].value.phase === 'Degraded',
      );
      expect(degradedPatch).toBeDefined();
    });

    it('includes a Ready=False condition in the Degraded status patch', async () => {
      const stsError = new Error('failure');
      const createNamespacedStatefulSetImpl = jest.fn().mockRejectedValue(stsError);
      const patchNamespacedCustomObjectStatusImpl = jest.fn().mockResolvedValue({});

      const { mockKubeConfig, mockCustomApi } = makeMockKubeConfig({
        createNamespacedStatefulSetImpl,
        patchNamespacedCustomObjectStatusImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster();

      await expect(controller.reconcile(cluster)).rejects.toThrow();

      const patchCalls = (mockCustomApi.patchNamespacedCustomObjectStatus as jest.Mock).mock.calls;
      const degradedPatch = patchCalls.find(
        (call: PatchStatusCall) => call[0].body[0].value.phase === 'Degraded',
      );
      const conditions = (degradedPatch[0].body[0].value as { conditions?: Array<{ type: string; status: string }> }).conditions ?? [];
      const readyCondition = conditions.find((c) => c.type === 'Ready');
      expect(readyCondition?.status).toBe('False');
    });
  });

  describe('updateStatus()', () => {
    it('calls patchNamespacedCustomObjectStatus with the correct API group and version', async () => {
      const patchNamespacedCustomObjectStatusImpl = jest.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCustomApi } = makeMockKubeConfig({
        patchNamespacedCustomObjectStatusImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster();

      await controller.updateStatus(cluster, { phase: 'Running' });

      const call = (mockCustomApi.patchNamespacedCustomObjectStatus as jest.Mock).mock.calls[0][0];
      expect(call.group).toBe('firebird.cloudnative-firebird.io');
      expect(call.version).toBe('v1');
      expect(call.plural).toBe('firebirdclusters');
    });

    it('calls patchNamespacedCustomObjectStatus with the cluster name and namespace', async () => {
      const patchNamespacedCustomObjectStatusImpl = jest.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCustomApi } = makeMockKubeConfig({
        patchNamespacedCustomObjectStatusImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeNamedCluster('prod-db', 'production');

      await controller.updateStatus(cluster, { phase: 'Running' });

      const call = (mockCustomApi.patchNamespacedCustomObjectStatus as jest.Mock).mock.calls[0][0];
      expect(call.name).toBe('prod-db');
      expect(call.namespace).toBe('production');
    });

    it('sends the status as a JSON Patch replace operation on /status', async () => {
      const patchNamespacedCustomObjectStatusImpl = jest.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCustomApi } = makeMockKubeConfig({
        patchNamespacedCustomObjectStatusImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster();

      await controller.updateStatus(cluster, { phase: 'Updating', instances: 2 });

      const call = (mockCustomApi.patchNamespacedCustomObjectStatus as jest.Mock).mock.calls[0][0];
      const patch = call.body[0];
      expect(patch.op).toBe('replace');
      expect(patch.path).toBe('/status');
      expect(patch.value.phase).toBe('Updating');
      expect(patch.value.instances).toBe(2);
    });

    it('does not throw when patchNamespacedCustomObjectStatus fails', async () => {
      const patchNamespacedCustomObjectStatusImpl = jest
        .fn()
        .mockRejectedValue(new Error('API error'));
      const { mockKubeConfig } = makeMockKubeConfig({
        patchNamespacedCustomObjectStatusImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster();

      await expect(controller.updateStatus(cluster, { phase: 'Running' })).resolves.toBeUndefined();
    });
  });
});
