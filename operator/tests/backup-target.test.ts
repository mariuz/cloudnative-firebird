import { describe, it, expect, vi, Mock } from 'vitest';
import { KubeConfig, V1Pod } from '@kubernetes/client-node';
import { chooseBackupInstance } from '../src/utils/backup-target';
import { validateBackupSpec, validateClusterSpec } from '../src/utils/validation';
import { FirebirdBackupController } from '../src/controllers/backup.controller';
import { FirebirdBackup, FirebirdCluster } from '../src/types';

const s3 = { bucket: 'b' };
const cluster = (status: FirebirdCluster['status'] = {}): FirebirdCluster => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdCluster',
  metadata: { name: 'db', namespace: 'default', uid: 'c' },
  spec: { instances: 3, storage: { size: '1Gi' }, replication: { enabled: true } },
  status,
});
const pod = (name: string, ready = true): V1Pod => ({
  metadata: { name },
  status: { phase: 'Running', conditions: [{ type: 'Ready', status: ready ? 'True' : 'False' }] },
});
const pods = [pod('db-0'), pod('db-1'), pod('db-2'), pod('db-2-fencing')];
const choose = (opts: Partial<Parameters<typeof chooseBackupInstance>[0]>) =>
  chooseBackupInstance({ cluster: cluster(), primaryPod: 'db-0', pods, target: 'prefer-standby', s3, ...opts });

describe('backup target', () => {
  it('uses the lowest ready replica for logical S3 backups with prefer-standby', () => {
    expect(choose({})).toBe('db-1');
    expect(choose({ primaryPod: 'db-1' })).toBe('db-0');
  });

  it('uses the primary by default and for server-side backups; physical backups to S3 may use a replica', () => {
    expect(choose({ target: undefined })).toBe('db-0');
    expect(choose({ target: 'primary' })).toBe('db-0');
    expect(choose({ s3: undefined })).toBe('db-0');
    expect(choose({ type: 'physical', s3: undefined })).toBe('db-0');
    // nbackup runs in the replica's server, the file is copied through its segment server
    expect(choose({ type: 'physical' })).toBe('db-1');
  });

  it('skips unready, fenced, lagging and scaled-away replicas, falling back to the primary', () => {
    expect(choose({ pods: [pod('db-0'), pod('db-1', false), pod('db-2')] })).toBe('db-2');
    expect(choose({ cluster: cluster({ fencedInstances: ['db-1'] }) })).toBe('db-2');
    expect(choose({ cluster: cluster({ replicationStatus: { laggingReplicas: ['db-1', 'db-2'] } }) })).toBe('db-0');
    expect(choose({ pods: [pod('db-0'), pod('db-5')] })).toBe('db-0');
  });

  it('validates the target', () => {
    const backup: FirebirdBackup = {
      apiVersion: 'firebird.cloudnative-firebird.io/v1',
      kind: 'FirebirdBackup',
      metadata: { name: 'b', namespace: 'default' },
      spec: { clusterName: 'db', target: 'standby' as never },
    };
    expect(() => validateBackupSpec(backup)).toThrow(/target/);
    expect(() =>
      validateClusterSpec({ ...cluster(), spec: { ...cluster().spec, backup: { enabled: true, target: 'replica' as never } } }),
    ).toThrow(/target/);
  });

  it('runs a prefer-standby backup Job against the chosen replica and reports it', async () => {
    const notFound = Object.assign(new Error('Not Found'), { code: 404 });
    const api: Record<string, Mock> = {
      getNamespacedCustomObject: vi.fn().mockResolvedValue(cluster()),
      readNamespacedLease: vi.fn().mockResolvedValue({ spec: { holderIdentity: 'db-0' } }),
      listNamespacedPod: vi.fn().mockResolvedValue({ items: pods }),
      readNamespacedJob: vi.fn().mockRejectedValue(notFound),
      createNamespacedJob: vi.fn().mockImplementation(({ body }) => Promise.resolve(body)),
    };
    const kubeConfig = new KubeConfig();
    vi.spyOn(kubeConfig, 'makeApiClient').mockReturnValue(
      new Proxy({}, { get: (_t, p: string) => (api[p] ??= vi.fn().mockResolvedValue({})) }) as never,
    );
    await new FirebirdBackupController(kubeConfig).reconcileBackup({
      apiVersion: 'firebird.cloudnative-firebird.io/v1',
      kind: 'FirebirdBackup',
      metadata: { name: 'nightly', namespace: 'default', uid: 'b' },
      spec: { clusterName: 'db', s3, target: 'prefer-standby' },
    });
    const job = api.createNamespacedJob.mock.calls[0][0].body;
    const env = job.spec.template.spec.initContainers[0].env;
    expect(env.find((e: { name: string }) => e.name === 'FIREBIRD_HOST').value).toBe('db-1.db-headless');
    const status = api.patchNamespacedCustomObjectStatus.mock.calls.at(-1)[0].body[0].value;
    expect(status).toMatchObject({ phase: 'Running', instance: 'db-1' });
  });
});
