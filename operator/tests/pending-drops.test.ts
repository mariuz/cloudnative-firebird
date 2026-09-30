import { describe, it, expect, vi, Mock } from 'vitest';
import { KubeConfig, V1Pod } from '@kubernetes/client-node';
import { FirebirdClusterController } from '../src/controllers/firebirdcluster.controller';
import {
  PENDING_DROP_USERS_ANNOTATION,
  addPendingDrops,
  buildPendingDropJob,
  parsePendingDrops,
  pendingDropsInitScript,
} from '../src/utils/pending-drops';
import { buildStatefulSet } from '../src/utils/resources';
import { logger } from '../src/utils/logger';
import { FirebirdCluster } from '../src/types';

const makeCluster = (spec: Partial<FirebirdCluster['spec']> = {}): FirebirdCluster => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdCluster',
  metadata: { name: 'db', namespace: 'default', uid: 'c' },
  spec: { instances: 2, storage: { size: '1Gi' }, ...spec },
});
const pod = (name: string, ready = true): V1Pod => ({
  metadata: { name },
  status: { phase: 'Running', conditions: [{ type: 'Ready', status: ready ? 'True' : 'False' }] },
});
const notFound = Object.assign(new Error('Not Found'), { code: 404 });

function setup(opts: { data: Record<string, string>; pods: V1Pod[]; users?: object[]; job?: object; pvcs?: string[] }) {
  const api: Record<string, Mock> = {
    readNamespacedConfigMap: vi.fn().mockResolvedValue({ metadata: { name: 'db-pending-user-drops' }, data: opts.data }),
    listNamespacedCustomObject: vi.fn().mockResolvedValue({ items: opts.users ?? [] }),
    listNamespacedPod: vi.fn().mockResolvedValue({ items: opts.pods }),
    listNamespacedPersistentVolumeClaim: vi.fn().mockResolvedValue({
      items: (opts.pvcs ?? ['firebird-data-db-0', 'firebird-data-db-1']).map((name) => ({ metadata: { name } })),
    }),
    readNamespacedJob: opts.job ? vi.fn().mockResolvedValue(opts.job) : vi.fn().mockRejectedValue(notFound),
    createNamespacedJob: vi.fn().mockResolvedValue({}),
    deleteNamespacedJob: vi.fn().mockResolvedValue({}),
    replaceNamespacedConfigMap: vi.fn().mockResolvedValue({}),
    deleteNamespacedConfigMap: vi.fn().mockResolvedValue({}),
  };
  const kubeConfig = new KubeConfig();
  vi.spyOn(kubeConfig, 'makeApiClient').mockReturnValue(
    new Proxy({}, { get: (_t, p: string) => api[p] ?? vi.fn().mockResolvedValue({}) }) as never,
  );
  const controller = new FirebirdClusterController(kubeConfig) as unknown as {
    reconcilePendingUserDrops(c: FirebirdCluster, l: typeof logger): Promise<void>;
  };
  return { api, run: (cluster = makeCluster()) => controller.reconcilePendingUserDrops(cluster, logger) };
}

describe('pending user drops', () => {
  it('records entries per instance and ignores invalid lines', () => {
    const data = addPendingDrops({ 'db-1': 'OLD 2026-01-01T00:00:00Z\n' }, ['db-1', 'db-2'], 'APP', 'T');
    expect(data).toEqual({ 'db-1': 'APP T\nOLD 2026-01-01T00:00:00Z\n', 'db-2': 'APP T\n' });
    expect(addPendingDrops(data, ['db-1'], 'APP', 'LATER')['db-1']).toBe(data['db-1']);
    expect(parsePendingDrops("APP T\nbad-name x\n'; drop\n\nAPP T2")).toEqual([{ username: 'APP', since: 'T' }]);
  });

  it('drops the pending users in the security-db-init container before the server starts', () => {
    const init = buildStatefulSet(makeCluster()).spec!.template.spec!.initContainers!.find((c) => c.name === 'security-db-init')!;
    const script = init.args![0];
    expect(script).toContain(pendingDropsInitScript('/var/lib/firebird/data/system/security.fdb'));
    expect(script.indexOf('pending_drops_security')).toBeLessThan(script.indexOf('chown -R'));
    expect(script).toContain('case "$u" in ""|*[!A-Z0-9_$]*) continue ;; esac');
    expect(init.env).toContainEqual({ name: 'POD_NAME', valueFrom: { fieldRef: { fieldPath: 'metadata.name' } } });
    expect(init.volumeMounts).toContainEqual({ name: 'pending-user-drops', mountPath: '/etc/firebird-pending-drops', readOnly: true });
    const volumes = buildStatefulSet(makeCluster()).spec!.template.spec!.volumes!;
    expect(volumes).toContainEqual({ name: 'pending-user-drops', configMap: { name: 'db-pending-user-drops', optional: true } });
  });

  it('drops through a Job once the instance is ready, revoking privileges without replication', async () => {
    const s = setup({ data: { 'db-1': 'APP T\nOLD T\n' }, pods: [pod('db-0'), pod('db-1')] });
    await s.run();
    const job = s.api.createNamespacedJob.mock.calls[0][0].body;
    expect(job.metadata.name).toBe('drop-users-db-1');
    expect(job.metadata.annotations[PENDING_DROP_USERS_ANNOTATION]).toBe('APP OLD');
    const env = Object.fromEntries(job.spec.template.spec.containers[0].env.map((e: { name: string; value?: string }) => [e.name, e.value]));
    expect(env.FIREBIRD_HOST).toBe('db-1.db-headless');
    expect(env.DROP_SQL).toContain('REVOKE ALL ON ALL FROM USER APP;');
    expect(env.DROP_SQL).toContain(`IF (EXISTS(SELECT 1 FROM SEC$USERS WHERE SEC$USER_NAME = 'APP')) THEN EXECUTE STATEMENT 'DROP USER APP';`);
    expect(s.api.replaceNamespacedConfigMap).not.toHaveBeenCalled();
    // with replication the replica's database is read-only: no revoke there
    const repl = buildPendingDropJob(makeCluster({ replication: { enabled: true } }), 'db-1', ['APP'], false);
    expect(repl.spec!.template.spec!.containers[0].env!.find((e) => e.name === 'DROP_SQL')!.value).not.toContain('REVOKE');
  });

  it('waits for an unready instance', async () => {
    const s = setup({ data: { 'db-1': 'APP T\n' }, pods: [pod('db-0'), pod('db-1', false)] });
    await s.run();
    expect(s.api.createNamespacedJob).not.toHaveBeenCalled();
    expect(s.api.replaceNamespacedConfigMap).not.toHaveBeenCalled();
  });

  it('clears the entries the completed Job dropped, and the ConfigMap once empty', async () => {
    const job = {
      metadata: { annotations: { [PENDING_DROP_USERS_ANNOTATION]: 'APP' } },
      status: { conditions: [{ type: 'Complete', status: 'True' }] },
    };
    const s = setup({ data: { 'db-1': 'APP T\nLATE T\n' }, pods: [pod('db-0'), pod('db-1')], job });
    await s.run();
    expect(s.api.deleteNamespacedJob).toHaveBeenCalled();
    expect(s.api.replaceNamespacedConfigMap.mock.calls[0][0].body.data).toEqual({ 'db-1': 'LATE T\n' });

    const last = setup({ data: { 'db-1': 'APP T\n' }, pods: [pod('db-0'), pod('db-1')], job });
    await last.run();
    expect(last.api.deleteNamespacedConfigMap).toHaveBeenCalledWith({ name: 'db-pending-user-drops', namespace: 'default' });
  });

  it('never drops a user a FirebirdUser declares again, and forgets instances scaled away with their volume', async () => {
    const again = { metadata: { name: 'app' }, spec: { clusterName: 'db', username: 'app', passwordSecret: { name: 'x' } } };
    const s = setup({ data: { 'db-1': 'APP T\n', 'db-5': 'OLD T\n' }, pods: [pod('db-0'), pod('db-1')], users: [again] });
    await s.run();
    expect(s.api.createNamespacedJob).not.toHaveBeenCalled();
    expect(s.api.deleteNamespacedConfigMap).toHaveBeenCalled();
  });
});
