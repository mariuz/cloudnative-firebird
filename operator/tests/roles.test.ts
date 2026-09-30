import { describe, it, expect, vi, type Mock } from 'vitest';
import { KubeConfig, V1Job } from '@kubernetes/client-node';
import {
  ROLE_FINALIZER,
  ROLE_JOB_ACTION_LABEL,
  ROLE_JOB_HASH_ANNOTATION,
  ROLE_JOB_TARGETS_ANNOTATION,
  applyRoleSql,
  buildRoleJob,
  dropRoleSql,
  firebirdRoleName,
  grantStatements,
  roleSpecHash,
  validateRoleSpec,
} from '../src/utils/roles';
import { FirebirdRoleController, ROLE_RETRY_DELAY_MS } from '../src/controllers/role.controller';
import { FirebirdCluster, FirebirdRole } from '../src/types';

const makeCluster = (replication = false): FirebirdCluster => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdCluster',
  metadata: { name: 'db', namespace: 'default', uid: 'c' },
  spec: { instances: 2, storage: { size: '1Gi' }, ...(replication ? { replication: { enabled: true } } : {}) },
});
const makeRole = (
  spec: Partial<FirebirdRole['spec']> = {},
  metadata: Partial<FirebirdRole['metadata']> = {},
  status?: FirebirdRole['status'],
): FirebirdRole => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdRole',
  metadata: { name: 'app-reader', namespace: 'default', uid: 'r-uid', resourceVersion: '1', ...metadata },
  spec: {
    clusterName: 'db',
    privileges: [
      { privileges: ['select', 'INSERT'], object: { kind: 'table', name: 'orders' } },
      { privileges: ['EXECUTE'], object: { kind: 'procedure', name: 'ship' } },
      { privileges: ['USAGE'], object: { kind: 'sequence', name: 'order_seq' } },
    ],
    ...spec,
  },
  ...(status ? { status } : {}),
});

describe('FirebirdRole SQL', () => {
  it('names the role after the resource, upper-cased', () => {
    expect(firebirdRoleName(makeRole())).toBe('APP_READER');
    expect(firebirdRoleName(makeRole({ roleName: 'Rpt' }))).toBe('RPT');
  });

  it('grants exactly the listed privileges after revoking what the role holds, in one transaction', () => {
    const sql = applyRoleSql(makeRole());
    expect(sql).toContain(`IF (NOT EXISTS(SELECT 1 FROM RDB$ROLES WHERE RDB$ROLE_NAME = 'APP_READER')) THEN EXECUTE STATEMENT 'CREATE ROLE APP_READER';`);
    const revoke = sql.indexOf('REVOKE ALL ON ALL FROM ROLE APP_READER;');
    const lastCommit = sql.lastIndexOf('COMMIT;');
    expect(revoke).toBeGreaterThan(sql.indexOf('COMMIT;'));
    for (const grant of [
      'GRANT INSERT, SELECT ON TABLE ORDERS TO ROLE APP_READER;',
      'GRANT EXECUTE ON PROCEDURE SHIP TO ROLE APP_READER;',
      'GRANT USAGE ON SEQUENCE ORDER_SEQ TO ROLE APP_READER;',
    ]) {
      expect(sql.indexOf(grant)).toBeGreaterThan(revoke);
      expect(sql.indexOf(grant)).toBeLessThan(lastCommit);
    }
    expect(grantStatements(makeRole({ privileges: [{ privileges: ['SELECT', 'ALL'], object: { kind: 'view', name: 'v' } }] }))).toEqual([
      'GRANT ALL ON TABLE V TO ROLE APP_READER;',
    ]);
    expect(dropRoleSql(makeRole())).toContain(`THEN EXECUTE STATEMENT 'DROP ROLE APP_READER';`);
  });

  it('hashes what is applied, independent of order and case', () => {
    const a = makeRole();
    const b = makeRole({ privileges: [...a.spec.privileges!].reverse().map((p) => ({ ...p, privileges: p.privileges.map((x) => x.toUpperCase()) })) });
    expect(roleSpecHash(a)).toBe(roleSpecHash(b));
    expect(roleSpecHash(a)).not.toBe(roleSpecHash(makeRole({ privileges: [] })));
  });

  it('validates names, kinds and privileges', () => {
    expect(() => validateRoleSpec(makeRole())).not.toThrow();
    expect(() => validateRoleSpec(makeRole({ roleName: 'rdb$admin' }))).toThrow(/system role/);
    expect(() => validateRoleSpec(makeRole({}, { name: 'public' }))).toThrow(/system role/);
    expect(() => validateRoleSpec(makeRole({ roleName: 'bad name' }))).toThrow(/Invalid Firebird role name/);
    expect(() =>
      validateRoleSpec(makeRole({ privileges: [{ privileges: ['SELECT'], object: { kind: 'procedure', name: 'p' } }] })),
    ).toThrow(/does not apply to a procedure/);
    expect(() =>
      validateRoleSpec(makeRole({ privileges: [{ privileges: ['SELECT'], object: { kind: 'table', name: 'x; drop' } }] })),
    ).toThrow(/regular identifier/);
  });

  it('grants on quoted, case-sensitive names as delimited identifiers', () => {
    const role = makeRole({
      privileges: [
        { privileges: ['SELECT'], object: { kind: 'table', name: 'Orders', quoted: true } },
        { privileges: ['INSERT', 'SELECT'], object: { kind: 'table', name: 'my table;x', quoted: true } },
        { privileges: ['ALL'], object: { kind: 'table', name: 'a"b', quoted: true } },
        { privileges: ['USAGE'], object: { kind: 'sequence', name: 'seqLower', quoted: true } },
        { privileges: ['SELECT'], object: { kind: 'table', name: 'orders' } },
      ],
    });
    expect(() => validateRoleSpec(role)).not.toThrow();
    expect(grantStatements(role)).toEqual([
      'GRANT ALL ON TABLE "a""b" TO ROLE APP_READER;',
      'GRANT INSERT, SELECT ON TABLE "my table;x" TO ROLE APP_READER;',
      'GRANT SELECT ON TABLE "Orders" TO ROLE APP_READER;',
      'GRANT SELECT ON TABLE ORDERS TO ROLE APP_READER;',
      'GRANT USAGE ON SEQUENCE "seqLower" TO ROLE APP_READER;',
    ]);
    // unquoted names keep their hash, so existing roles are not applied again
    expect(roleSpecHash(makeRole())).toBe(roleSpecHash(makeRole({ privileges: makeRole().spec.privileges!.map((p) => ({ ...p, object: { ...p.object, quoted: false } })) })));
    const bad = (name: string) =>
      validateRoleSpec(makeRole({ privileges: [{ privileges: ['SELECT'], object: { kind: 'table', name, quoted: true } }] }));
    expect(() => bad('')).toThrow(/quoted table name/);
    expect(() => bad(' lead')).toThrow(/leading or trailing/);
    expect(() => bad('trail ')).toThrow(/leading or trailing/);
    expect(() => bad('new\nline')).toThrow(/control characters/);
    expect(() => bad('x'.repeat(64))).toThrow(/63 characters/);
    expect(() => bad('ä'.repeat(63))).not.toThrow();
  });

  it('builds a Job that runs the SQL on the given instances', () => {
    const job = buildRoleJob(makeCluster(true), makeRole(), { action: 'apply', instances: ['db-1'], hash: 'h', targets: '[]' });
    const env = Object.fromEntries(job.spec!.template.spec!.containers[0].env!.map((e) => [e.name, e.value]));
    expect(env.HOSTS).toBe('db-1.db-headless');
    expect(env.ROLE_SQL).toBe(applyRoleSql(makeRole()));
    expect(job.metadata?.labels?.[ROLE_JOB_ACTION_LABEL]).toBe('apply');
    expect(job.spec!.template.spec!.securityContext?.runAsNonRoot).toBe(true);
  });
});

describe('FirebirdRoleController', () => {
  const notFound = Object.assign(new Error('Not Found'), { code: 404 });
  const readyPod = (name: string) => ({ metadata: { name }, status: { phase: 'Running', conditions: [{ type: 'Ready', status: 'True' }] } });

  function setup(opts: { cluster?: FirebirdCluster; job?: V1Job; readyPods?: string[]; now?: number } = {}) {
    const cluster = opts.cluster ?? makeCluster();
    const api: Record<string, Mock> = {
      getNamespacedCustomObject: vi.fn().mockResolvedValue(cluster),
      patchNamespacedCustomObjectStatus: vi.fn().mockResolvedValue({}),
      patchNamespacedCustomObject: vi.fn().mockResolvedValue({}),
      readNamespacedJob: opts.job ? vi.fn().mockResolvedValue(opts.job) : vi.fn().mockRejectedValue(notFound),
      createNamespacedJob: vi.fn().mockResolvedValue({}),
      deleteNamespacedJob: vi.fn().mockResolvedValue({}),
      listNamespacedPod: vi.fn().mockResolvedValue({ items: (opts.readyPods ?? ['db-0', 'db-1']).map(readyPod) }),
      listNamespacedPersistentVolumeClaim: vi.fn().mockResolvedValue({
        items: ['db-0', 'db-1'].map((p) => ({ metadata: { name: `firebird-data-${p}`, uid: `vol-${p}` } })),
      }),
      readNamespacedLease: vi.fn().mockResolvedValue({ spec: { holderIdentity: 'db-1' } }),
      createNamespacedEvent: vi.fn().mockResolvedValue({}),
    };
    const kubeConfig = new KubeConfig();
    vi.spyOn(kubeConfig, 'makeApiClient').mockReturnValue(
      new Proxy({}, { get: (_t, p: string) => api[p] ?? vi.fn().mockResolvedValue({}) }) as never,
    );
    const controller = new FirebirdRoleController(kubeConfig, () => opts.now ?? Date.parse('2026-09-30T12:00:00Z'));
    const status = () => {
      const calls = api.patchNamespacedCustomObjectStatus.mock.calls;
      return calls[calls.length - 1]?.[0].body[0].value;
    };
    const created = () => api.createNamespacedJob.mock.calls.map((c) => c[0].body as V1Job);
    return { controller, api, status, created };
  }
  const hostsOf = (job: V1Job) => job.spec!.template.spec!.containers[0].env!.find((e) => e.name === 'HOSTS')?.value;
  const done = (action: string, hash: string, targets: string[]): V1Job => ({
    metadata: {
      labels: { [ROLE_JOB_ACTION_LABEL]: action },
      annotations: {
        [ROLE_JOB_HASH_ANNOTATION]: hash,
        [ROLE_JOB_TARGETS_ANNOTATION]: JSON.stringify(targets.map((name) => ({ name, volume: `vol-${name}` }))),
      },
    },
    status: { conditions: [{ type: 'Complete', status: 'True' }] },
  });

  it('with replication, applies on the primary only and records the applied spec', async () => {
    const s = setup({ cluster: makeCluster(true) });
    await s.controller.reconcileRole(makeRole());
    expect(s.created()).toHaveLength(1);
    expect(hostsOf(s.created()[0])).toBe('db-1.db-headless');
    expect(s.status()).toMatchObject({ phase: 'Applying', roleName: 'APP_READER' });

    const hash = roleSpecHash(makeRole());
    const d = setup({ cluster: makeCluster(true), job: done('apply', hash, ['db-1']) });
    await d.controller.reconcileRole(makeRole());
    expect(d.created()).toHaveLength(0);
    expect(d.status()).toMatchObject({ phase: 'Applied', appliedHash: hash });
  });

  it('without replication, applies to every instance database and again on a new volume', async () => {
    const s = setup();
    await s.controller.reconcileRole(makeRole());
    expect(hostsOf(s.created()[0])).toBe('db-0.db-headless db-1.db-headless');

    const hash = roleSpecHash(makeRole());
    const stale = setup();
    await stale.controller.reconcileRole(
      makeRole({}, {}, {
        instances: [
          { name: 'db-0', hash, volume: 'vol-db-0' },
          { name: 'db-1', hash, volume: 'old-volume' },
        ],
      }),
    );
    expect(hostsOf(stale.created()[0])).toBe('db-1.db-headless');
  });

  it('waits for the primary, and retries a failed spec only after a delay', async () => {
    const waiting = setup({ cluster: makeCluster(true), readyPods: ['db-0'] });
    await waiting.controller.reconcileRole(makeRole());
    expect(waiting.created()).toHaveLength(0);
    expect(waiting.status()).toMatchObject({ phase: 'Pending', message: 'waiting for instance(s) to be ready: db-1' });

    const failed: V1Job = { ...done('apply', roleSpecHash(makeRole()), ['db-1']), status: { conditions: [{ type: 'Failed', status: 'True' }] } };
    const f = setup({ cluster: makeCluster(true), job: failed });
    await f.controller.reconcileRole(makeRole());
    expect(f.created()).toHaveLength(0);
    expect(f.status()).toMatchObject({ phase: 'Failed' });

    const later = setup({ cluster: makeCluster(true), now: Date.parse('2026-09-30T12:00:00Z') + ROLE_RETRY_DELAY_MS });
    await later.controller.reconcileRole(makeRole({}, {}, { failedHash: roleSpecHash(makeRole()), lastFailureTime: '2026-09-30T12:00:00.000Z' }));
    expect(later.created()).toHaveLength(1);
  });

  it('adds the finalizer for reclaimPolicy delete and drops the role on deletion', async () => {
    const s = setup();
    await s.controller.reconcileRole(makeRole({ reclaimPolicy: 'delete' }));
    expect(s.api.patchNamespacedCustomObject.mock.calls[0][0].body[1]).toEqual({
      op: 'add',
      path: '/metadata/finalizers',
      value: [ROLE_FINALIZER],
    });

    const deleting = makeRole({ reclaimPolicy: 'delete' }, { finalizers: [ROLE_FINALIZER], deletionTimestamp: '2026-09-30T12:00:00Z' });
    const d = setup({ cluster: makeCluster(true) });
    await d.controller.reconcileRole(deleting);
    const drop = d.created()[0];
    expect(drop.metadata?.labels?.[ROLE_JOB_ACTION_LABEL]).toBe('drop');
    expect(hostsOf(drop)).toBe('db-1.db-headless');

    const f = setup({ cluster: makeCluster(true), job: done('drop', '', []) });
    await f.controller.reconcileRole(deleting);
    expect(f.api.patchNamespacedCustomObject.mock.calls[0][0].body[1].value).toEqual([]);
  });

  it('keeps the role and releases the finalizer when the cluster is gone', async () => {
    const s = setup();
    s.api.getNamespacedCustomObject.mockRejectedValue(notFound);
    await s.controller.reconcileRole(
      makeRole({ reclaimPolicy: 'delete' }, { finalizers: [ROLE_FINALIZER], deletionTimestamp: '2026-09-30T12:00:00Z' }),
    );
    expect(s.created()).toHaveLength(0);
    expect(s.api.patchNamespacedCustomObject).toHaveBeenCalled();
  });
});
