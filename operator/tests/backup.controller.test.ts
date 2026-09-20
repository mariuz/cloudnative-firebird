import { describe, it, expect, vi, beforeEach } from 'vitest';
import { FirebirdBackupController } from '../src/controllers/backup.controller';
import { KubeConfig, BatchV1Api, CustomObjectsApi } from '@kubernetes/client-node';
import { FirebirdBackup, FirebirdRestore, FirebirdCluster } from '../src/types';

describe('FirebirdBackupController', () => {
  let mockBatchApi: { readNamespacedJob: ReturnType<typeof vi.fn>; createNamespacedJob: ReturnType<typeof vi.fn> };
  let mockCustomApi: { getNamespacedCustomObject: ReturnType<typeof vi.fn>; patchNamespacedCustomObjectStatus: ReturnType<typeof vi.fn> };
  let mockKubeConfig: KubeConfig;
  let controller: FirebirdBackupController;

  const mockCluster: FirebirdCluster = {
    apiVersion: 'firebird.cloudnative-firebird.io/v1',
    kind: 'FirebirdCluster',
    metadata: { name: 'test-cluster', namespace: 'default' },
    spec: { instances: 1, storage: { size: '1Gi' } },
  };

  beforeEach(() => {
    mockBatchApi = {
      readNamespacedJob: vi.fn().mockRejectedValue({ response: { statusCode: 404 } }),
      createNamespacedJob: vi.fn().mockResolvedValue({}),
    };

    mockCustomApi = {
      getNamespacedCustomObject: vi.fn().mockResolvedValue(mockCluster),
      patchNamespacedCustomObjectStatus: vi.fn().mockResolvedValue({}),
    };

    mockKubeConfig = new KubeConfig();
    vi.spyOn(mockKubeConfig, 'makeApiClient').mockImplementation((apiClass: unknown) => {
      if (apiClass === BatchV1Api) return mockBatchApi as unknown as BatchV1Api;
      if (apiClass === CustomObjectsApi) return mockCustomApi as unknown as CustomObjectsApi;
      return {} as unknown as BatchV1Api;
    });

    controller = new FirebirdBackupController(mockKubeConfig);
  });

  it('reconciles FirebirdBackup by creating a Job and updating status to Completed', async () => {
    const backup: FirebirdBackup = {
      apiVersion: 'firebird.cloudnative-firebird.io/v1',
      kind: 'FirebirdBackup',
      metadata: { name: 'test-backup', namespace: 'default' },
      spec: { clusterName: 'test-cluster', type: 'logical' },
    };

    await controller.reconcileBackup(backup);

    expect(mockCustomApi.getNamespacedCustomObject).toHaveBeenCalledWith({
      group: 'firebird.cloudnative-firebird.io',
      version: 'v1',
      namespace: 'default',
      plural: 'firebirdclusters',
      name: 'test-cluster',
    });

    expect(mockBatchApi.createNamespacedJob).toHaveBeenCalledWith({
      namespace: 'default',
      body: expect.objectContaining({
        metadata: expect.objectContaining({ name: 'backup-test-backup' }),
      }),
    });

    expect(mockCustomApi.patchNamespacedCustomObjectStatus).toHaveBeenCalledWith(
      expect.objectContaining({
        plural: 'firebirdbackups',
        name: 'test-backup',
        body: expect.arrayContaining([
          expect.objectContaining({
            op: 'replace',
            path: '/status',
            value: expect.objectContaining({ phase: 'Completed' }),
          }),
        ]),
      }),
    );
  });

  it('skips reconciliation if FirebirdBackup is already in terminal state', async () => {
    const backup: FirebirdBackup = {
      apiVersion: 'firebird.cloudnative-firebird.io/v1',
      kind: 'FirebirdBackup',
      metadata: { name: 'test-backup', namespace: 'default' },
      spec: { clusterName: 'test-cluster' },
      status: { phase: 'Completed' },
    };

    await controller.reconcileBackup(backup);

    expect(mockBatchApi.createNamespacedJob).not.toHaveBeenCalled();
  });

  it('reconciles FirebirdRestore by creating a restore Job and updating status to Completed', async () => {
    const restore: FirebirdRestore = {
      apiVersion: 'firebird.cloudnative-firebird.io/v1',
      kind: 'FirebirdRestore',
      metadata: { name: 'test-restore', namespace: 'default' },
      spec: { clusterName: 'test-cluster', restoreType: 'logical' },
    };

    await controller.reconcileRestore(restore);

    expect(mockBatchApi.createNamespacedJob).toHaveBeenCalledWith({
      namespace: 'default',
      body: expect.objectContaining({
        metadata: expect.objectContaining({ name: 'restore-test-restore' }),
      }),
    });

    expect(mockCustomApi.patchNamespacedCustomObjectStatus).toHaveBeenCalledWith(
      expect.objectContaining({
        plural: 'firebirdrestores',
        name: 'test-restore',
        body: expect.arrayContaining([
          expect.objectContaining({
            op: 'replace',
            path: '/status',
            value: expect.objectContaining({ phase: 'Completed' }),
          }),
        ]),
      }),
    );
  });

  it('handles errors in FirebirdBackup by setting phase to Failed', async () => {
    const backup: FirebirdBackup = {
      apiVersion: 'firebird.cloudnative-firebird.io/v1',
      kind: 'FirebirdBackup',
      metadata: { name: 'invalid-backup', namespace: 'default' },
      spec: { clusterName: '' }, // empty clusterName causes ValidationError
    };

    await expect(controller.reconcileBackup(backup)).rejects.toThrow();

    expect(mockCustomApi.patchNamespacedCustomObjectStatus).toHaveBeenCalledWith(
      expect.objectContaining({
        plural: 'firebirdbackups',
        name: 'invalid-backup',
        body: expect.arrayContaining([
          expect.objectContaining({
            path: '/status',
            value: expect.objectContaining({ phase: 'Failed' }),
          }),
        ]),
      }),
    );
  });
});
