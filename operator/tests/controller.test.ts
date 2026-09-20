import { describe, it, expect, vi, type Mock } from 'vitest';
import { FirebirdClusterController } from '../src/controllers/firebirdcluster.controller';
import {
  AppsV1Api,
  BatchV1Api,
  CoordinationV1Api,
  CoreV1Api,
  CustomObjectsApi,
  KubeConfig,
  NetworkingV1Api,
  PolicyV1Api,
} from '@kubernetes/client-node';
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
  readNamespacedServiceImpl = vi.fn().mockRejectedValue(notFoundError),
  createNamespacedServiceImpl = vi.fn().mockResolvedValue({}),
  readNamespacedConfigMapImpl = vi.fn().mockRejectedValue(notFoundError),
  createNamespacedConfigMapImpl = vi.fn().mockResolvedValue({}),
  patchNamespacedConfigMapImpl = vi.fn().mockResolvedValue({}),
  deleteNamespacedConfigMapImpl = vi.fn().mockResolvedValue({}),
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
  readNamespacedNetworkPolicyImpl = vi.fn().mockRejectedValue(notFoundError),
  createNamespacedNetworkPolicyImpl = vi.fn().mockResolvedValue({}),
  patchNamespacedNetworkPolicyImpl = vi.fn().mockResolvedValue({}),
  deleteNamespacedNetworkPolicyImpl = vi.fn().mockResolvedValue({}),
}: {
  readNamespacedServiceImpl?: Mock;
  createNamespacedServiceImpl?: Mock;
  readNamespacedConfigMapImpl?: Mock;
  createNamespacedConfigMapImpl?: Mock;
  patchNamespacedConfigMapImpl?: Mock;
  deleteNamespacedConfigMapImpl?: Mock;
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
  readNamespacedNetworkPolicyImpl?: Mock;
  createNamespacedNetworkPolicyImpl?: Mock;
  patchNamespacedNetworkPolicyImpl?: Mock;
  deleteNamespacedNetworkPolicyImpl?: Mock;
} = {}) {
  const mockCoreApi = {
    readNamespacedService: readNamespacedServiceImpl,
    createNamespacedService: createNamespacedServiceImpl,
    readNamespacedConfigMap: readNamespacedConfigMapImpl,
    createNamespacedConfigMap: createNamespacedConfigMapImpl,
    patchNamespacedConfigMap: patchNamespacedConfigMapImpl,
    deleteNamespacedConfigMap: deleteNamespacedConfigMapImpl,
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
  const mockNetworkingApi = {
    readNamespacedNetworkPolicy: readNamespacedNetworkPolicyImpl,
    createNamespacedNetworkPolicy: createNamespacedNetworkPolicyImpl,
    patchNamespacedNetworkPolicy: patchNamespacedNetworkPolicyImpl,
    deleteNamespacedNetworkPolicy: deleteNamespacedNetworkPolicyImpl,
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
    if (ApiType === NetworkingV1Api) return mockNetworkingApi as unknown as NetworkingV1Api;
    if (ApiType === CoordinationV1Api) return mockCoordinationApi as unknown as CoordinationV1Api;
    return {} as unknown as CoreV1Api;
  });

  return { mockKubeConfig, mockCoreApi, mockAppsApi, mockBatchApi, mockCustomApi, mockPolicyApi, mockNetworkingApi };
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

      const createdNames = (mockCoreApi.createNamespacedService as Mock).mock.calls.map(
        (call: CreateServiceCall) => call[0].body?.metadata?.name,
      );
      expect(createdNames).toContain('test-cluster-headless');
    });

    it('creates a primary ClusterIP service for a new cluster', async () => {
      const { mockKubeConfig, mockCoreApi } = makeMockKubeConfig();
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster();

      await controller.reconcile(cluster);

      const createdNames = (mockCoreApi.createNamespacedService as Mock).mock.calls.map(
        (call: CreateServiceCall) => call[0].body?.metadata?.name,
      );
      expect(createdNames).toContain('test-cluster');
    });

    it('creates both the headless and primary services for a new cluster', async () => {
      const { mockKubeConfig, mockCoreApi } = makeMockKubeConfig();
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster();

      await controller.reconcile(cluster);

      const createdNames = (mockCoreApi.createNamespacedService as Mock).mock.calls.map(
        (call: CreateServiceCall) => call[0].body?.metadata?.name,
      );
      expect(createdNames).toContain('test-cluster-headless');
      expect(createdNames).toContain('test-cluster');
    });

    it('creates a StatefulSet for a new cluster', async () => {
      const createNamespacedStatefulSetImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockAppsApi } = makeMockKubeConfig({
        createNamespacedStatefulSetImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster();

      await controller.reconcile(cluster);

      expect(mockAppsApi.createNamespacedStatefulSet as Mock).toHaveBeenCalledTimes(1);
      const call = (mockAppsApi.createNamespacedStatefulSet as Mock)
        .mock.calls[0] as CreateStatefulSetCall;
      expect(call[0].namespace).toBe('default');
    });

    it('reads existing services before attempting to create them', async () => {
      const { mockKubeConfig, mockCoreApi } = makeMockKubeConfig();
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster();

      await controller.reconcile(cluster);

      const readNames = (mockCoreApi.readNamespacedService as Mock).mock.calls.map(
        (call: ReadServiceCall) => call[0].name,
      );
      expect(readNames).toContain('test-cluster-headless');
      expect(readNames).toContain('test-cluster');
    });

    it('skips service creation when the service already exists', async () => {
      const readNamespacedServiceImpl = vi.fn().mockResolvedValue({}); // all exist
      const createNamespacedServiceImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCoreApi } = makeMockKubeConfig({
        readNamespacedServiceImpl,
        createNamespacedServiceImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster();

      await controller.reconcile(cluster);

      expect(mockCoreApi.createNamespacedService as Mock).not.toHaveBeenCalled();
    });

    it('uses the correct namespace when creating resources', async () => {
      const { mockKubeConfig, mockCoreApi } = makeMockKubeConfig();
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeNamedCluster('my-cluster', 'production');

      await controller.reconcile(cluster);

      const call = (mockCoreApi.createNamespacedService as Mock).mock
        .calls[0] as CreateServiceCall;
      expect(call[0].namespace).toBe('production');
    });
  });

  describe('reconcile() StatefulSet management', () => {
    it('patches the StatefulSet when it exists and replicas have changed', async () => {
      const outdatedSts = {
        spec: { replicas: 1, template: { spec: { containers: [{ image: 'old' }] } } },
      };
      const readNamespacedStatefulSetImpl = vi.fn().mockResolvedValue(outdatedSts);
      const patchNamespacedStatefulSetImpl = vi.fn().mockResolvedValue({});

      const { mockKubeConfig, mockAppsApi } = makeMockKubeConfig({
        readNamespacedStatefulSetImpl,
        patchNamespacedStatefulSetImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ instances: 3 }); // different replica count

      await controller.reconcile(cluster);

      expect(mockAppsApi.patchNamespacedStatefulSet as Mock).toHaveBeenCalledTimes(1);
    });

    it('does not patch the StatefulSet when it is up to date', async () => {
      // StatefulSet does not exist (404) → gets created, not patched
      const readNamespacedStatefulSetImpl = vi.fn().mockRejectedValue(notFoundError);
      const createNamespacedStatefulSetImpl = vi.fn().mockResolvedValue({});
      const patchNamespacedStatefulSetImpl = vi.fn().mockResolvedValue({});

      const { mockKubeConfig, mockAppsApi } = makeMockKubeConfig({
        readNamespacedStatefulSetImpl,
        createNamespacedStatefulSetImpl,
        patchNamespacedStatefulSetImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster();

      await controller.reconcile(cluster);

      expect(mockAppsApi.patchNamespacedStatefulSet as Mock).not.toHaveBeenCalled();
      expect(mockAppsApi.createNamespacedStatefulSet as Mock).toHaveBeenCalledTimes(1);
    });

    it('creates a StatefulSet with correct ISC_PASSWORD when a superuser secret is set', async () => {
      const createNamespacedStatefulSetImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockAppsApi } = makeMockKubeConfig({
        createNamespacedStatefulSetImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeClusterWithSecret('my-superuser-secret');

      await controller.reconcile(cluster);

      const createdSts = (mockAppsApi.createNamespacedStatefulSet as Mock).mock.calls[0][0]
        .body;
      const container = createdSts.spec?.template?.spec?.containers?.[0];
      const passwordEnv = container?.env?.find(
        (e: { name: string }) => e.name === 'ISC_PASSWORD',
      );
      expect(passwordEnv?.valueFrom?.secretKeyRef?.name).toBe('my-superuser-secret');
    });

    it('creates a StatefulSet with resource requests and limits when resources are specified', async () => {
      const createNamespacedStatefulSetImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockAppsApi } = makeMockKubeConfig({
        createNamespacedStatefulSetImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeClusterWithResources();

      await controller.reconcile(cluster);

      const createdSts = (mockAppsApi.createNamespacedStatefulSet as Mock).mock.calls[0][0]
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
      const patchNamespacedCustomObjectStatusImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCustomApi } = makeMockKubeConfig({
        patchNamespacedCustomObjectStatusImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster();

      await controller.reconcile(cluster);

      const firstPatch = (
        mockCustomApi.patchNamespacedCustomObjectStatus as Mock
      ).mock.calls[0] as PatchStatusCall;
      expect(firstPatch[0].body[0].value.phase).toBe('Creating');
    });

    it('sets status to Running after a successful reconciliation', async () => {
      const patchNamespacedCustomObjectStatusImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCustomApi } = makeMockKubeConfig({
        patchNamespacedCustomObjectStatusImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster();

      await controller.reconcile(cluster);

      const patchCalls = (mockCustomApi.patchNamespacedCustomObjectStatus as Mock).mock.calls;
      const lastPatch = patchCalls[patchCalls.length - 1] as PatchStatusCall;
      expect(lastPatch[0].body[0].value.phase).toBe('Running');
    });

    it('includes a Ready=True condition after a successful reconciliation', async () => {
      const patchNamespacedCustomObjectStatusImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCustomApi } = makeMockKubeConfig({
        patchNamespacedCustomObjectStatusImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster();

      await controller.reconcile(cluster);

      const patchCalls = (mockCustomApi.patchNamespacedCustomObjectStatus as Mock).mock.calls;
      const lastPatch = patchCalls[patchCalls.length - 1] as PatchStatusCall;
      const conditions = (lastPatch[0].body[0].value as { conditions?: Array<{ type: string; status: string }> }).conditions ?? [];
      const readyCondition = conditions.find((c) => c.type === 'Ready');
      expect(readyCondition?.status).toBe('True');
    });

    it('sets status to Degraded and re-throws when StatefulSet creation fails', async () => {
      const stsError = new Error('statefulset creation failed');
      const createNamespacedStatefulSetImpl = vi.fn().mockRejectedValue(stsError);
      const patchNamespacedCustomObjectStatusImpl = vi.fn().mockResolvedValue({});

      const { mockKubeConfig, mockCustomApi } = makeMockKubeConfig({
        createNamespacedStatefulSetImpl,
        patchNamespacedCustomObjectStatusImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster();

      await expect(controller.reconcile(cluster)).rejects.toThrow('statefulset creation failed');

      const patchCalls = (mockCustomApi.patchNamespacedCustomObjectStatus as Mock).mock.calls;
      const degradedPatch = patchCalls.find(
        (call: PatchStatusCall) => call[0].body[0].value.phase === 'Degraded',
      );
      expect(degradedPatch).toBeDefined();
    });

    it('sets status to Degraded and re-throws when service creation fails', async () => {
      const svcError = new Error('service creation failed');
      const createNamespacedServiceImpl = vi.fn().mockRejectedValue(svcError);
      const patchNamespacedCustomObjectStatusImpl = vi.fn().mockResolvedValue({});

      const { mockKubeConfig, mockCustomApi } = makeMockKubeConfig({
        createNamespacedServiceImpl,
        patchNamespacedCustomObjectStatusImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster();

      await expect(controller.reconcile(cluster)).rejects.toThrow('service creation failed');

      const patchCalls = (mockCustomApi.patchNamespacedCustomObjectStatus as Mock).mock.calls;
      const degradedPatch = patchCalls.find(
        (call: PatchStatusCall) => call[0].body[0].value.phase === 'Degraded',
      );
      expect(degradedPatch).toBeDefined();
    });

    it('sets status to Degraded and re-throws when spec validation fails', async () => {
      const patchNamespacedCustomObjectStatusImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCustomApi } = makeMockKubeConfig({
        patchNamespacedCustomObjectStatusImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const invalidCluster = makeCluster({ instances: 15 }); // > 10

      await expect(controller.reconcile(invalidCluster)).rejects.toThrow(/Must be an integer between 1 and 10/);

      const patchCalls = (mockCustomApi.patchNamespacedCustomObjectStatus as Mock).mock.calls;
      const degradedPatch = patchCalls.find(
        (call: PatchStatusCall) => call[0].body[0].value.phase === 'Degraded',
      );
      expect(degradedPatch).toBeDefined();
    });
  });

  describe('suspended cluster reconciliation', () => {
    it('sets phase to Paused and skips resource creation when suspended is true', async () => {
      const patchNamespacedCustomObjectStatusImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCoreApi, mockCustomApi } = makeMockKubeConfig({
        patchNamespacedCustomObjectStatusImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ suspended: true });

      await controller.reconcile(cluster);

      expect(mockCoreApi.createNamespacedService).not.toHaveBeenCalled();
      const patchCalls = (mockCustomApi.patchNamespacedCustomObjectStatus as Mock).mock.calls;
      const pausedPatch = patchCalls.find(
        (call: PatchStatusCall) => call[0].body[0].value.phase === 'Paused',
      );
      expect(pausedPatch).toBeDefined();
    });
  });

  describe('updateStatus()', () => {
    it('calls patchNamespacedCustomObjectStatus with the correct API group and version', async () => {
      const patchNamespacedCustomObjectStatusImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCustomApi } = makeMockKubeConfig({
        patchNamespacedCustomObjectStatusImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster();

      await controller.updateStatus(cluster, { phase: 'Running' });

      const call = (mockCustomApi.patchNamespacedCustomObjectStatus as Mock).mock.calls[0][0];
      expect(call.group).toBe('firebird.cloudnative-firebird.io');
      expect(call.version).toBe('v1');
      expect(call.plural).toBe('firebirdclusters');
    });

    it('calls patchNamespacedCustomObjectStatus with the cluster name and namespace', async () => {
      const patchNamespacedCustomObjectStatusImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCustomApi } = makeMockKubeConfig({
        patchNamespacedCustomObjectStatusImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeNamedCluster('prod-db', 'production');

      await controller.updateStatus(cluster, { phase: 'Running' });

      const call = (mockCustomApi.patchNamespacedCustomObjectStatus as Mock).mock.calls[0][0];
      expect(call.name).toBe('prod-db');
      expect(call.namespace).toBe('production');
    });

    it('sends the status as a JSON Patch replace operation on /status', async () => {
      const patchNamespacedCustomObjectStatusImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCustomApi } = makeMockKubeConfig({
        patchNamespacedCustomObjectStatusImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster();

      await controller.updateStatus(cluster, { phase: 'Updating', instances: 2 });

      const call = (mockCustomApi.patchNamespacedCustomObjectStatus as Mock).mock.calls[0][0];
      const patch = call.body[0];
      expect(patch.op).toBe('replace');
      expect(patch.path).toBe('/status');
      expect(patch.value.phase).toBe('Updating');
      expect(patch.value.instances).toBe(2);
    });

    it('does not throw when patchNamespacedCustomObjectStatus fails', async () => {
      const patchNamespacedCustomObjectStatusImpl = vi
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

  describe('reconcile() PodDisruptionBudget management', () => {
    it('creates PodDisruptionBudget when instances > 1', async () => {
      const createNamespacedPodDisruptionBudgetImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockPolicyApi } = makeMockKubeConfig({
        createNamespacedPodDisruptionBudgetImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ instances: 2 });

      await controller.reconcile(cluster);

      expect(mockPolicyApi.createNamespacedPodDisruptionBudget).toHaveBeenCalledTimes(1);
      const call = (mockPolicyApi.createNamespacedPodDisruptionBudget as Mock).mock.calls[0][0];
      expect(call.body.metadata.name).toBe('test-cluster-pdb');
    });

    it('deletes PodDisruptionBudget when instances <= 1 and PDB exists', async () => {
      const readNamespacedPodDisruptionBudgetImpl = vi.fn().mockResolvedValue({ metadata: { name: 'test-cluster-pdb' } });
      const deleteNamespacedPodDisruptionBudgetImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockPolicyApi } = makeMockKubeConfig({
        readNamespacedPodDisruptionBudgetImpl,
        deleteNamespacedPodDisruptionBudgetImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ instances: 1 });

      await controller.reconcile(cluster);

      expect(mockPolicyApi.deleteNamespacedPodDisruptionBudget).toHaveBeenCalledTimes(1);
    });
  });

  describe('reconcile() backup CronJob management', () => {
    it('creates backup CronJob when backup.enabled is true', async () => {
      const createNamespacedCronJobImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockBatchApi } = makeMockKubeConfig({
        createNamespacedCronJobImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ backup: { enabled: true, schedule: '0 3 * * *' } });

      await controller.reconcile(cluster);

      expect(mockBatchApi.createNamespacedCronJob).toHaveBeenCalledTimes(1);
      const call = (mockBatchApi.createNamespacedCronJob as Mock).mock.calls[0][0];
      expect(call.body.metadata.name).toBe('test-cluster-backup');
      expect(call.body.spec.schedule).toBe('0 3 * * *');
    });

    it('patches backup CronJob when schedule has changed', async () => {
      const existingCronJob = { spec: { schedule: '0 2 * * *', jobTemplate: { spec: { template: { spec: { containers: [{ image: 'firebirdsql/firebird:latest' }] } } } } } };
      const readNamespacedCronJobImpl = vi.fn().mockResolvedValue(existingCronJob);
      const patchNamespacedCronJobImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockBatchApi } = makeMockKubeConfig({
        readNamespacedCronJobImpl,
        patchNamespacedCronJobImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ backup: { enabled: true, schedule: '0 5 * * *' } });

      await controller.reconcile(cluster);

      expect(mockBatchApi.patchNamespacedCronJob).toHaveBeenCalledTimes(1);
      const call = (mockBatchApi.patchNamespacedCronJob as Mock).mock.calls[0][0];
      expect(call.name).toBe('test-cluster-backup');
    });

    it('deletes backup CronJob when backup.enabled is false and CronJob exists', async () => {
      const existingCronJob = { metadata: { name: 'test-cluster-backup' } };
      const readNamespacedCronJobImpl = vi.fn().mockImplementation(async ({ name }: { name: string }) => {
        if (name === 'test-cluster-backup') return existingCronJob;
        throw notFoundError;
      });
      const deleteNamespacedCronJobImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockBatchApi } = makeMockKubeConfig({
        readNamespacedCronJobImpl,
        deleteNamespacedCronJobImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ backup: { enabled: false } });

      await controller.reconcile(cluster);

      expect(mockBatchApi.deleteNamespacedCronJob).toHaveBeenCalledWith({
        name: 'test-cluster-backup',
        namespace: 'default',
      });
    });
  });

  describe('reconcile() PodMonitor management', () => {
    it('creates PodMonitor when enablePodMonitor is true', async () => {
      const createNamespacedCustomObjectImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCustomApi } = makeMockKubeConfig({
        createNamespacedCustomObjectImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ monitoring: { enablePodMonitor: true } });

      await controller.reconcile(cluster);

      expect(mockCustomApi.createNamespacedCustomObject).toHaveBeenCalledTimes(1);
      const call = (mockCustomApi.createNamespacedCustomObject as Mock).mock.calls[0][0];
      expect(call.group).toBe('monitoring.coreos.com');
      expect(call.plural).toBe('podmonitors');
      expect(call.body.metadata.name).toBe('test-cluster-podmonitor');
    });

    it('deletes PodMonitor when enablePodMonitor is false and PodMonitor exists', async () => {
      const getNamespacedCustomObjectImpl = vi.fn().mockResolvedValue({});
      const deleteNamespacedCustomObjectImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCustomApi } = makeMockKubeConfig({
        getNamespacedCustomObjectImpl,
        deleteNamespacedCustomObjectImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ monitoring: { enablePodMonitor: false } });

      await controller.reconcile(cluster);

      expect(mockCustomApi.deleteNamespacedCustomObject).toHaveBeenCalledTimes(1);
      const call = (mockCustomApi.deleteNamespacedCustomObject as Mock).mock.calls[0][0];
      expect(call.name).toBe('test-cluster-podmonitor');
    });
  });

  describe('reconcile() readyInstances tracking', () => {
    it('sets phase to Creating when readyReplicas is less than instances', async () => {
      const readNamespacedStatefulSetImpl = vi.fn().mockResolvedValue({
        status: { readyReplicas: 0 },
      });
      const patchNamespacedStatefulSetImpl = vi.fn().mockResolvedValue({
        status: { readyReplicas: 0 },
      });
      const patchNamespacedCustomObjectStatusImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCustomApi } = makeMockKubeConfig({
        readNamespacedStatefulSetImpl,
        patchNamespacedStatefulSetImpl,
        patchNamespacedCustomObjectStatusImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ instances: 3 });

      await controller.reconcile(cluster);

      const patchCalls = (mockCustomApi.patchNamespacedCustomObjectStatus as Mock).mock.calls;
      const lastPatch = patchCalls[patchCalls.length - 1] as PatchStatusCall;
      expect(lastPatch[0].body[0].value.phase).toBe('Creating');
    });

    it('sets phase to Running when readyReplicas equals instances', async () => {
      const readNamespacedStatefulSetImpl = vi.fn().mockResolvedValue({
        status: { readyReplicas: 3 },
      });
      const patchNamespacedCustomObjectStatusImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCustomApi } = makeMockKubeConfig({
        readNamespacedStatefulSetImpl,
        patchNamespacedCustomObjectStatusImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ instances: 3 });

      await controller.reconcile(cluster);

      const patchCalls = (mockCustomApi.patchNamespacedCustomObjectStatus as Mock).mock.calls;
      const lastPatch = patchCalls[patchCalls.length - 1] as PatchStatusCall;
      expect(lastPatch[0].body[0].value.phase).toBe('Running');
    });
  });

  describe('reconcile() ConfigMap management', () => {
    it('creates ConfigMap when custom config is specified', async () => {
      const createNamespacedConfigMapImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCoreApi } = makeMockKubeConfig({
        createNamespacedConfigMapImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ config: { settings: { DefaultCacheMem: '256M' } } });

      await controller.reconcile(cluster);

      expect(mockCoreApi.createNamespacedConfigMap).toHaveBeenCalledTimes(1);
    });

    it('deletes ConfigMap when config is no longer specified and ConfigMap exists', async () => {
      const readNamespacedConfigMapImpl = vi.fn().mockResolvedValue({ metadata: { name: 'test-cluster-config' } });
      const deleteNamespacedConfigMapImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockCoreApi } = makeMockKubeConfig({
        readNamespacedConfigMapImpl,
        deleteNamespacedConfigMapImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster(); // no config or bootstrap

      await controller.reconcile(cluster);

      expect(mockCoreApi.deleteNamespacedConfigMap).toHaveBeenCalledTimes(1);
    });
  });

  describe('reconcile() AutoSweep CronJob management', () => {
    it('creates AutoSweep CronJob when autoSweep.enabled is true', async () => {
      const createNamespacedCronJobImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockBatchApi } = makeMockKubeConfig({
        createNamespacedCronJobImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ autoSweep: { enabled: true, schedule: '0 4 * * *' } });

      await controller.reconcile(cluster);

      expect(mockBatchApi.createNamespacedCronJob).toHaveBeenCalledTimes(1);
      const call = (mockBatchApi.createNamespacedCronJob as Mock).mock.calls[0][0];
      expect(call.body.metadata.name).toBe('test-cluster-sweep');
    });

    it('deletes AutoSweep CronJob when autoSweep is disabled and CronJob exists', async () => {
      const existingCronJob = { metadata: { name: 'test-cluster-sweep' } };
      const readNamespacedCronJobImpl = vi.fn().mockImplementation(async ({ name }: { name: string }) => {
        if (name === 'test-cluster-sweep') return existingCronJob;
        throw notFoundError;
      });
      const deleteNamespacedCronJobImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockBatchApi } = makeMockKubeConfig({
        readNamespacedCronJobImpl,
        deleteNamespacedCronJobImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ autoSweep: { enabled: false } });

      await controller.reconcile(cluster);

      expect(mockBatchApi.deleteNamespacedCronJob).toHaveBeenCalledWith({
        name: 'test-cluster-sweep',
        namespace: 'default',
      });
    });
  });

  describe('reconcile() NetworkPolicy management', () => {
    it('creates NetworkPolicy when networkPolicy.enabled is true', async () => {
      const createNamespacedNetworkPolicyImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockNetworkingApi } = makeMockKubeConfig({
        createNamespacedNetworkPolicyImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ networkPolicy: { enabled: true } });

      await controller.reconcile(cluster);

      expect(mockNetworkingApi.createNamespacedNetworkPolicy).toHaveBeenCalledTimes(1);
      const call = (mockNetworkingApi.createNamespacedNetworkPolicy as Mock).mock.calls[0][0];
      expect(call.body.metadata.name).toBe('test-cluster-networkpolicy');
    });

    it('deletes NetworkPolicy when networkPolicy is disabled and policy exists', async () => {
      const readNamespacedNetworkPolicyImpl = vi.fn().mockResolvedValue({ metadata: { name: 'test-cluster-networkpolicy' } });
      const deleteNamespacedNetworkPolicyImpl = vi.fn().mockResolvedValue({});
      const { mockKubeConfig, mockNetworkingApi } = makeMockKubeConfig({
        readNamespacedNetworkPolicyImpl,
        deleteNamespacedNetworkPolicyImpl,
      });
      const controller = new FirebirdClusterController(mockKubeConfig);
      const cluster = makeCluster({ networkPolicy: { enabled: false } });

      await controller.reconcile(cluster);

      expect(mockNetworkingApi.deleteNamespacedNetworkPolicy).toHaveBeenCalledTimes(1);
    });
  });
});

