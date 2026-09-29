import { describe, it, expect, vi, Mock } from 'vitest';
import { KubeConfig, V1Job } from '@kubernetes/client-node';
import {
  USER_FINALIZER,
  USER_JOB_ACTION_LABEL,
  USER_JOB_HASH_ANNOTATION,
  USER_JOB_TARGETS_ANNOTATION,
  buildUserJob,
  dropUserSql,
  firebirdUsername,
  grantSql,
  userJobName,
  userSpecHash,
  validateUserSpec,
} from '../src/utils/users';
import { FirebirdUserController, USER_DROP_WAIT_MS, USER_RETRY_DELAY_MS } from '../src/controllers/user.controller';
import { SECURITY_DB_PATH, buildStatefulSet } from '../src/utils/resources';
import { validateClusterSpec, ValidationError } from '../src/utils/validation';
import { FirebirdCluster, FirebirdUser } from '../src/types';

const makeCluster = (overrides: Partial<FirebirdCluster['spec']> = {}): FirebirdCluster => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdCluster',
  metadata: { name: 'db', namespace: 'default', uid: 'c-uid' },
  spec: { instances: 2, storage: { size: '1Gi' }, superuserSecret: { name: 'su' }, ...overrides },
});

const makeUser = (
  spec: Partial<FirebirdUser['spec']> = {},
  metadata: Partial<FirebirdUser['metadata']> = {},
  status?: FirebirdUser['status'],
): FirebirdUser => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdUser',
  metadata: { name: 'app-user', namespace: 'default', uid: 'u-uid', resourceVersion: '1', ...metadata },
  spec: { clusterName: 'db', passwordSecret: { name: 'app-pw' }, ...spec },
  ...(status ? { status } : {}),
});

const secret = { metadata: { uid: 's-uid', resourceVersion: '7' }, data: { password: 'c2VjcmV0' } };

describe('persistent security database', () => {
  it('seeds it on the instance volume and points the server and the entrypoint alias at it', () => {
    const pod = buildStatefulSet(makeCluster()).spec!.template.spec!;
    const init = pod.initContainers![0];
    expect(init.name).toBe('security-db-init');
    expect(init.args![0]).toContain('cp /opt/firebird/security[0-9]*.fdb "$d/security.fdb.tmp"');
    expect(init.args![0]).toContain(`'security.db = ${SECURITY_DB_PATH}'`);
    const main = pod.containers[0];
    expect(main.env).toContainEqual({ name: 'FIREBIRD_CONF_SecurityDatabase', value: '/var/lib/firebird/data/system/security.fdb' });
    expect(main.volumeMounts).toContainEqual({
      name: 'firebird-data',
      mountPath: '/opt/firebird/databases.conf',
      subPath: 'system/databases.conf',
    });
  });

  it('does not let config.settings override SecurityDatabase', () => {
    expect(() => validateClusterSpec(makeCluster({ config: { settings: { SecurityDatabase: '/x.fdb' } } }))).toThrow(
      /managed by the operator/,
    );
  });
});

describe('FirebirdUser spec', () => {
  it('derives the Firebird user name from the resource name', () => {
    expect(firebirdUsername(makeUser())).toBe('APP_USER');
    expect(firebirdUsername(makeUser({ username: 'reporting' }))).toBe('REPORTING');
  });

  it('validates names, roles and the reclaim policy', () => {
    expect(() => validateUserSpec(makeUser())).not.toThrow();
    expect(() => validateUserSpec(makeUser({ username: '1bad' }))).toThrow(ValidationError);
    expect(() => validateUserSpec(makeUser({ username: 'sysdba' }))).toThrow(/superuserSecret/);
    expect(() => validateUserSpec(makeUser({ roles: ['ok', "x'; drop"] }))).toThrow(/role/);
    expect(() => validateUserSpec(makeUser({ roles: ['rdb$admin'] }))).toThrow(/spec.admin/);
    expect(() => validateUserSpec(makeUser({ passwordSecret: { name: '' } }))).toThrow(/passwordSecret/);
    expect(() => validateUserSpec(makeUser({ reclaimPolicy: 'keep' as never }))).toThrow(/reclaimPolicy/);
  });

  it('hashes the spec and Secret version, not the password', () => {
    const a = userSpecHash(makeUser({ roles: ['b', 'a'] }), secret.metadata);
    expect(a).toBe(userSpecHash(makeUser({ roles: ['A', 'B', 'a'] }), secret.metadata));
    expect(a).not.toBe(userSpecHash(makeUser({ roles: ['a', 'b'] }), { ...secret.metadata, resourceVersion: '8' }));
    expect(a).not.toBe(userSpecHash(makeUser({ roles: ['a', 'b'], active: false }), secret.metadata));
  });

  it('keeps Job names within the pod name limit', () => {
    expect(userJobName(makeUser())).toBe('fbuser-app-user');
    const long = userJobName(makeUser({}, { name: 'x'.repeat(60) }));
    expect(long.length).toBeLessThanOrEqual(52);
  });
});

describe('user SQL and Jobs', () => {
  it('grants exactly the desired roles and revokes the others', () => {
    const sql = grantSql('APP', ['READER', 'WRITER']);
    expect(sql).toContain("IF (r NOT IN ('READER', 'WRITER')) THEN EXECUTE STATEMENT 'REVOKE \"' || r || '\" FROM USER APP'");
    expect(sql).toContain('GRANT READER TO USER APP;');
    expect(grantSql('APP', [])).toContain('IF (1 = 1)');
  });

  it('drops a user only if it exists', () => {
    expect(dropUserSql('APP')).toContain("IF (EXISTS(SELECT 1 FROM SEC$USERS WHERE SEC$USER_NAME = 'APP')) THEN");
  });

  it('applies the user through each instance, reading the password from the Secret in the Job', () => {
    const job = buildUserJob(makeCluster(), makeUser({ admin: true, roles: ['reader'] }), {
      action: 'apply',
      instances: ['db-0', 'db-1'],
      grantInstances: ['db-0'],
      hash: 'h1',
      targets: '[{"name":"db-0","volume":"v0"}]',
    });
    expect(job.metadata?.name).toBe('fbuser-app-user');
    expect(job.metadata?.annotations?.[USER_JOB_HASH_ANNOTATION]).toBe('h1');
    expect(job.metadata?.ownerReferences?.[0]).toMatchObject({ kind: 'FirebirdUser', uid: 'u-uid' });
    const c = job.spec!.template.spec!.containers[0];
    const env = Object.fromEntries((c.env ?? []).map((e) => [e.name, e.value ?? e.valueFrom]));
    expect(env.HOSTS).toBe('db-0.db-headless db-1.db-headless');
    expect(env.GRANT_HOSTS).toBe('db-0.db-headless');
    expect(env.FB_USER).toBe('APP_USER');
    expect(env.USER_CLAUSE).toBe('ACTIVE GRANT ADMIN ROLE');
    expect(env.PASSWORD).toEqual({ secretKeyRef: { name: 'app-pw', key: 'password' } });
    expect(c.args![0]).toContain(`pw=$(printf '%s' "$PASSWORD" | sed "s/'/''/g")`);
    expect(c.args![0]).not.toContain('secret');
  });

  it('drops the user and revokes its privileges', () => {
    const job = buildUserJob(makeCluster(), makeUser({ active: false }), {
      action: 'drop',
      instances: ['db-0'],
      grantInstances: ['db-0'],
      hash: '',
      targets: '[]',
    });
    const c = job.spec!.template.spec!.containers[0];
    expect(job.metadata?.labels?.[USER_JOB_ACTION_LABEL]).toBe('drop');
    expect(c.args![0]).toContain('REVOKE ALL ON ALL FROM USER $FB_USER;');
    expect((c.env ?? []).some((e) => e.name === 'PASSWORD')).toBe(false);
  });
});

describe('FirebirdUserController', () => {
  const notFound = Object.assign(new Error('Not Found'), { code: 404 });
  const readyPod = (name: string) => ({
    metadata: { name },
    status: { phase: 'Running', conditions: [{ type: 'Ready', status: 'True' }] },
  });
  const pvc = (pod: string) => ({ metadata: { name: `firebird-data-${pod}`, uid: `vol-${pod}` } });

  function setup(opts: { cluster?: FirebirdCluster; job?: V1Job; readyPods?: string[]; now?: number } = {}) {
    const cluster = opts.cluster ?? makeCluster();
    const api: Record<string, Mock> = {
      getNamespacedCustomObject: vi.fn().mockResolvedValue(cluster),
      patchNamespacedCustomObjectStatus: vi.fn().mockResolvedValue({}),
      patchNamespacedCustomObject: vi.fn().mockResolvedValue({}),
      readNamespacedSecret: vi.fn().mockResolvedValue(secret),
      readNamespacedJob: opts.job ? vi.fn().mockResolvedValue(opts.job) : vi.fn().mockRejectedValue(notFound),
      createNamespacedJob: vi.fn().mockResolvedValue({}),
      deleteNamespacedJob: vi.fn().mockResolvedValue({}),
      listNamespacedPod: vi.fn().mockResolvedValue({ items: (opts.readyPods ?? ['db-0', 'db-1']).map(readyPod) }),
      listNamespacedPersistentVolumeClaim: vi.fn().mockResolvedValue({ items: ['db-0', 'db-1'].map(pvc) }),
      readNamespacedLease: vi.fn().mockResolvedValue({ spec: { holderIdentity: 'db-1' } }),
      createNamespacedEvent: vi.fn().mockResolvedValue({}),
    };
    const kubeConfig = new KubeConfig();
    vi.spyOn(kubeConfig, 'makeApiClient').mockReturnValue(
      new Proxy({}, { get: (_t, p: string) => api[p] ?? vi.fn().mockResolvedValue({}) }) as never,
    );
    const controller = new FirebirdUserController(kubeConfig, () => opts.now ?? Date.parse('2026-09-27T12:00:00Z'));
    const status = () => {
      const calls = api.patchNamespacedCustomObjectStatus.mock.calls;
      return calls[calls.length - 1]?.[0].body[0].value;
    };
    const created = () => api.createNamespacedJob.mock.calls.map((c) => c[0].body as V1Job);
    return { controller, api, status, created };
  }

  const envOf = (job: V1Job) =>
    Object.fromEntries((job.spec!.template.spec!.containers[0].env ?? []).map((e) => [e.name, e.value]));

  it('applies a new user to every ready instance; grants on every instance without replication', async () => {
    const { controller, status, created } = setup();
    await controller.reconcileUser(makeUser({ roles: ['reader'] }));
    expect(created()).toHaveLength(1);
    expect(envOf(created()[0]).HOSTS).toBe('db-0.db-headless db-1.db-headless');
    expect(envOf(created()[0]).GRANT_HOSTS).toBe('db-0.db-headless db-1.db-headless');
    expect(JSON.parse(created()[0].metadata!.annotations![USER_JOB_TARGETS_ANNOTATION])).toEqual([
      { name: 'db-0', volume: 'vol-db-0' },
      { name: 'db-1', volume: 'vol-db-1' },
    ]);
    expect(status()).toMatchObject({ phase: 'Applying', username: 'APP_USER' });
  });

  it('grants roles on the primary only with replication', async () => {
    const { controller, created } = setup({ cluster: makeCluster({ replication: { enabled: true } }) });
    await controller.reconcileUser(makeUser({ roles: ['reader'] }));
    expect(envOf(created()[0]).GRANT_HOSTS).toBe('db-1.db-headless');
  });

  it('records a completed Job per instance and reports Applied', async () => {
    const user = makeUser();
    const hash = userSpecHash(user, secret.metadata);
    const job: V1Job = {
      metadata: {
        labels: { [USER_JOB_ACTION_LABEL]: 'apply' },
        annotations: {
          [USER_JOB_HASH_ANNOTATION]: hash,
          [USER_JOB_TARGETS_ANNOTATION]: '[{"name":"db-0","volume":"vol-db-0"},{"name":"db-1","volume":"vol-db-1"}]',
        },
      },
      status: { conditions: [{ type: 'Complete', status: 'True' }] },
    };
    const { controller, api, status, created } = setup({ job });
    await controller.reconcileUser(user);
    expect(api.deleteNamespacedJob).toHaveBeenCalled();
    expect(created()).toHaveLength(0);
    expect(status()).toMatchObject({
      phase: 'Applied',
      instances: [
        { name: 'db-0', volume: 'vol-db-0', hash },
        { name: 'db-1', volume: 'vol-db-1', hash },
      ],
    });
  });

  it('re-applies only to instances whose volume was replaced, and waits for unready ones', async () => {
    const user = makeUser();
    const hash = userSpecHash(user, secret.metadata);
    user.status = {
      instances: [
        { name: 'db-0', volume: 'vol-db-0', hash },
        { name: 'db-1', volume: 'old-volume', hash },
      ],
    };
    const { controller, created } = setup();
    await controller.reconcileUser(user);
    expect(envOf(created()[0]).HOSTS).toBe('db-1.db-headless');

    const { controller: c2, created: created2, status: status2 } = setup({ readyPods: ['db-0'] });
    await c2.reconcileUser(user);
    expect(created2()).toHaveLength(0);
    expect(status2()).toMatchObject({ phase: 'Pending', message: expect.stringContaining('db-1') });
  });

  it('backs off after a failed Job for the same spec', async () => {
    const user = makeUser();
    const hash = userSpecHash(user, secret.metadata);
    const failed: V1Job = {
      metadata: { labels: { [USER_JOB_ACTION_LABEL]: 'apply' }, annotations: { [USER_JOB_HASH_ANNOTATION]: hash } },
      status: { conditions: [{ type: 'Failed', status: 'True' }] },
    };
    const { controller, created, status } = setup({ job: failed });
    await controller.reconcileUser(user);
    expect(created()).toHaveLength(0);
    expect(status()).toMatchObject({ phase: 'Failed', failedHash: hash });

    const later = setup({ now: Date.parse('2026-09-27T12:00:00Z') + USER_RETRY_DELAY_MS + 1000 });
    await later.controller.reconcileUser({ ...user, status: status() });
    expect(later.created()).toHaveLength(1);
  });

  it('waits for a missing Secret or cluster', async () => {
    const s = setup();
    s.api.readNamespacedSecret.mockRejectedValue(notFound);
    await s.controller.reconcileUser(makeUser());
    expect(s.status()).toMatchObject({ phase: 'Pending', message: 'Secret app-pw not found' });

    const c = setup();
    c.api.getNamespacedCustomObject.mockRejectedValue(notFound);
    await c.controller.reconcileUser(makeUser());
    expect(c.status()).toMatchObject({ phase: 'Pending', message: 'FirebirdCluster db not found' });
  });

  it('adds the finalizer for reclaimPolicy delete, and drops the user on deletion', async () => {
    const s = setup();
    await s.controller.reconcileUser(makeUser({ reclaimPolicy: 'delete' }));
    expect(s.api.patchNamespacedCustomObject.mock.calls[0][0].body).toEqual([
      { op: 'test', path: '/metadata/resourceVersion', value: '1' },
      { op: 'add', path: '/metadata/finalizers', value: [USER_FINALIZER] },
    ]);

    const deleting = makeUser(
      { reclaimPolicy: 'delete' },
      { finalizers: [USER_FINALIZER], deletionTimestamp: '2026-09-27T12:00:00Z' },
    );
    const d = setup({ cluster: makeCluster({ replication: { enabled: true } }) });
    await d.controller.reconcileUser(deleting);
    const drop = d.created()[0];
    expect(drop.metadata?.labels?.[USER_JOB_ACTION_LABEL]).toBe('drop');
    expect(envOf(drop).HOSTS).toBe('db-0.db-headless db-1.db-headless');
    expect(envOf(drop).GRANT_HOSTS).toBe('db-1.db-headless');

    expect(JSON.parse(drop.metadata!.annotations![USER_JOB_TARGETS_ANNOTATION])).toEqual([
      { name: 'db-0', volume: 'vol-db-0' },
      { name: 'db-1', volume: 'vol-db-1' },
    ]);
    expect(d.status()).toMatchObject({ phase: 'Dropping' });

    const f = setup({ job: dropJob(['db-0', 'db-1']) });
    await f.controller.reconcileUser(deleting);
    expect(f.created()).toHaveLength(0);
    expect(f.api.patchNamespacedCustomObject.mock.calls[0][0].body[1]).toEqual({
      op: 'add',
      path: '/metadata/finalizers',
      value: [],
    });
  });

  const dropJob = (instances: string[]): V1Job => ({
    metadata: {
      labels: { [USER_JOB_ACTION_LABEL]: 'drop' },
      annotations: { [USER_JOB_TARGETS_ANNOTATION]: JSON.stringify(instances.map((name) => ({ name, volume: `vol-${name}` }))) },
    },
    status: { conditions: [{ type: 'Complete', status: 'True' }] },
  });
  const deletingHolder = (droppedFrom?: Array<{ name: string; volume: string }>) =>
    makeUser(
      { reclaimPolicy: 'delete' },
      { finalizers: [USER_FINALIZER], deletionTimestamp: '2026-09-27T12:00:00Z' },
      {
        instances: [
          { name: 'db-0', volume: 'vol-db-0', hash: 'h' },
          { name: 'db-1', volume: 'vol-db-1', hash: 'h' },
        ],
        ...(droppedFrom ? { droppedFrom } : {}),
      },
    );

  it('keeps the finalizer until an unready instance holding the user can drop it', async () => {
    // db-1 holds the user but is not ready: dropped from db-0 only, then waited for
    const first = setup({ readyPods: ['db-0'] });
    await first.controller.reconcileUser(deletingHolder());
    expect(envOf(first.created()[0]).HOSTS).toBe('db-0.db-headless');

    const waiting = setup({ readyPods: ['db-0'], job: dropJob(['db-0']) });
    await waiting.controller.reconcileUser(deletingHolder());
    expect(waiting.created()).toHaveLength(0);
    expect(waiting.api.patchNamespacedCustomObject).not.toHaveBeenCalled();
    expect(waiting.status()).toMatchObject({
      phase: 'Dropping',
      message: 'waiting for instance(s) to be ready to drop the user: db-1',
      droppedFrom: [{ name: 'db-0', volume: 'vol-db-0' }],
    });

    // db-1 is ready again: the user is dropped from it, then the finalizer is released
    const later = setup();
    await later.controller.reconcileUser(deletingHolder([{ name: 'db-0', volume: 'vol-db-0' }]));
    expect(envOf(later.created()[0]).HOSTS).toBe('db-1.db-headless');
    const done = setup({ job: dropJob(['db-1']) });
    await done.controller.reconcileUser(deletingHolder([{ name: 'db-0', volume: 'vol-db-0' }]));
    expect(done.api.patchNamespacedCustomObject.mock.calls[0][0].body[1].value).toEqual([]);
  });

  it('removes a running apply Job before dropping', async () => {
    const applying: V1Job = { metadata: { labels: { [USER_JOB_ACTION_LABEL]: 'apply' } }, status: { active: 1 } };
    const s = setup({ job: applying });
    await s.controller.reconcileUser(deletingHolder());
    expect(s.api.deleteNamespacedJob).toHaveBeenCalled();
    expect(s.created()).toHaveLength(0);
  });

  it('does not wait for unready instances that never had the user', async () => {
    const s = setup({ readyPods: ['db-0'], job: dropJob(['db-0']) });
    await s.controller.reconcileUser(
      makeUser({ reclaimPolicy: 'delete' }, { finalizers: [USER_FINALIZER], deletionTimestamp: '2026-09-27T12:00:00Z' }),
    );
    expect(s.api.patchNamespacedCustomObject.mock.calls[0][0].body[1].value).toEqual([]);
  });

  it(`keeps the user on instances unready for ${USER_DROP_WAIT_MS / 60000} minutes and releases the finalizer`, async () => {
    const s = setup({
      readyPods: ['db-0'],
      job: dropJob(['db-0']),
      now: Date.parse('2026-09-27T12:00:00Z') + USER_DROP_WAIT_MS,
    });
    await s.controller.reconcileUser(deletingHolder());
    expect(s.created()).toHaveLength(0);
    expect(s.api.patchNamespacedCustomObject.mock.calls[0][0].body[1].value).toEqual([]);
    const warning = s.api.createNamespacedEvent.mock.calls.map((c) => c[0].body).find((e) => e.type === 'Warning');
    expect(warning?.message).toMatch(/kept on db-1/);
  });

  it('releases the finalizer without dropping when the cluster is gone', async () => {
    const s = setup();
    s.api.getNamespacedCustomObject.mockRejectedValue(notFound);
    await s.controller.reconcileUser(
      makeUser({ reclaimPolicy: 'delete' }, { finalizers: [USER_FINALIZER], deletionTimestamp: '2026-09-27T12:00:00Z' }),
    );
    expect(s.created()).toHaveLength(0);
    expect(s.api.patchNamespacedCustomObject).toHaveBeenCalled();
  });
});
