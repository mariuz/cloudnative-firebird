import { it, expect, vi, Mock } from 'vitest';
import { KubeConfig, V1Pod } from '@kubernetes/client-node';
import { FirebirdClusterController } from '../src/controllers/firebirdcluster.controller';
import { REVISION_LABEL } from '../src/utils/rolling-update';
import { buildStatefulSet } from '../src/utils/resources';
import { FirebirdCluster } from '../src/types';

const cluster: FirebirdCluster = {
  apiVersion: 'firebird.cloudnative-firebird.io/v1', kind: 'FirebirdCluster',
  metadata: { name: 'db', namespace: 'default', uid: 'c' },
  spec: { instances: 3, storage: { size: '1Gi' }, replication: { enabled: true, failover: { enabled: true, delaySeconds: 20 } } },
};
const pod = (name: string, revision: string, uid: string, ready = true): V1Pod => ({
  metadata: { name, uid, labels: { [REVISION_LABEL]: revision } },
  status: { phase: 'Running', conditions: [{ type: 'Ready', status: ready ? 'True' : 'False' }] },
});
/**
 * Against an API that keeps what is written: the reconcile right after the primary's restart
 * starts from a watch copy older than the restart record, and must not drop it (kind CI: the
 * failover timer started on the planned restart of the primary).
 */
it('keeps the planned primary restart across reconciles against a stateful API', async () => {
  let store: Record<string, unknown> = {};
  let pods = [pod('db-0', 'db-old', 'u0'), pod('db-1', 'db-new', 'u1'), pod('db-2', 'db-new', 'u2')];
  const existing = { ...buildStatefulSet(cluster), metadata: { name: 'db', generation: 2 }, status: { replicas: 3, updateRevision: 'db-new', currentRevision: 'db-old', observedGeneration: 2 } };
  const notFound = Object.assign(new Error('Not Found'), { code: 404 });
  const api: Record<string, Mock> = {
    listNamespacedPod: vi.fn().mockImplementation(() => Promise.resolve({ items: pods })),
    readNamespacedLease: vi.fn().mockResolvedValue({ spec: { holderIdentity: 'db-0' } }),
    readNamespacedConfigMap: vi.fn().mockResolvedValue({ data: {} }),
    readNamespacedStatefulSet: vi.fn().mockResolvedValue(existing),
    patchNamespacedStatefulSet: vi.fn().mockResolvedValue(existing),
    getNamespacedCustomObject: vi.fn().mockImplementation(() => Promise.resolve({ ...cluster, status: JSON.parse(JSON.stringify(store)) })),
    patchNamespacedCustomObjectStatus: vi.fn().mockImplementation(({ body }) => {
      for (const op of body) {
        if (op.path === '/status') store = JSON.parse(JSON.stringify(op.value));
        else store[op.path.split('/')[2]] = op.value;
      }
      return Promise.resolve({});
    }),
  };
  const calls: Record<string, Mock> = {};
  const fn = (m: string): Mock => (calls[m] ??= api[m] ?? (m.startsWith('read') || m.startsWith('get') ? vi.fn().mockRejectedValue(notFound) : m.startsWith('list') ? vi.fn().mockResolvedValue({ items: [] }) : vi.fn().mockResolvedValue({})));
  const kc = new KubeConfig();
  vi.spyOn(kc, 'makeApiClient').mockReturnValue(new Proxy({}, { get: (_t, p: string) => fn(p) }) as never);
  const c = new FirebirdClusterController(kc, vi.fn().mockRejectedValue(new Error('unreachable')));
  await c.reconcile({ ...cluster, status: { phase: 'Updating' } });
  expect(fn('deleteNamespacedPod').mock.calls.map((x) => x[0].name)).toContain('db-0');
  pods = [pod('db-0', 'db-new', 'u0-new', false), pod('db-1', 'db-new', 'u1'), pod('db-2', 'db-new', 'u2')];
  await c.reconcile({ ...cluster, status: { phase: 'Updating' } }); // watch copy: older status
  expect(store.primaryNotReadySince).toBeUndefined();
  expect((store.rollingUpdate as { primaryRestart?: object }).primaryRestart).toMatchObject({ pod: 'db-0', uid: 'u0' });
  expect(fn('createNamespacedJob').mock.calls.map((x) => x[0].body.metadata.name)).not.toContain('db-failover');
});
