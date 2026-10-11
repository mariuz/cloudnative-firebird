import { describe, it, expect, vi, Mock } from 'vitest';
import { KubeConfig, V1Job } from '@kubernetes/client-node';
import {
  buildDatabaseJob,
  DATABASE_FINALIZER,
  DATABASE_JOB_ACTION_LABEL,
  DATABASE_JOB_TARGETS_ANNOTATION,
  databaseClusterError,
  databaseJobScript,
  managedDatabaseName,
  validateDatabaseSpec,
} from '../src/utils/databases';
import { DATABASE_DROP_WAIT_MS, DATABASE_RETRY_DELAY_MS, FirebirdDatabaseController } from '../src/controllers/database.controller';
import { ValidationError } from '../src/utils/validation';
import { FirebirdCluster, FirebirdDatabase } from '../src/types';

const makeCluster = (overrides: Partial<FirebirdCluster['spec']> = {}): FirebirdCluster => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdCluster',
  metadata: { name: 'db', namespace: 'default', uid: 'c-uid' },
  spec: { instances: 2, storage: { size: '1Gi' }, superuserSecret: { name: 'su' }, ...overrides },
});

const makeDatabase = (
  spec: Partial<FirebirdDatabase['spec']> = {},
  metadata: Partial<FirebirdDatabase['metadata']> = {},
  status?: FirebirdDatabase['status'],
): FirebirdDatabase => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdDatabase',
  metadata: { name: 'sales', namespace: 'default', uid: 'd-uid', resourceVersion: '1', creationTimestamp: '2026-10-11T10:00:00Z', ...metadata },
  spec: { clusterName: 'db', ...spec },
  ...(status ? { status } : {}),
});

const envOf = (job: V1Job) => Object.fromEntries((job.spec!.template.spec!.containers[0].env ?? []).map((e) => [e.name, e.value ?? e.valueFrom]));

describe('FirebirdDatabase spec', () => {
  it('names the file after the resource unless databaseName is set', () => {
    expect(managedDatabaseName(makeDatabase())).toBe('sales.fdb');
    expect(managedDatabaseName(makeDatabase({ databaseName: 'Orders.FDB' }))).toBe('Orders.FDB');
  });

  it('validates the file name, page size, character set and reclaim policy', () => {
    expect(() => validateDatabaseSpec(makeDatabase({ pageSize: 16384, characterSet: 'WIN1252', reclaimPolicy: 'delete' }))).not.toThrow();
    expect(() => validateDatabaseSpec(makeDatabase({ databaseName: '../etc/passwd' }))).toThrow(ValidationError);
    expect(() => validateDatabaseSpec(makeDatabase({ databaseName: 'security.fdb' }))).toThrow(/security database/);
    expect(() => validateDatabaseSpec(makeDatabase({ pageSize: 1000 as never }))).toThrow(/pageSize/);
    expect(() => validateDatabaseSpec(makeDatabase({ characterSet: "UTF8'; drop" }))).toThrow(/characterSet/);
    expect(() => validateDatabaseSpec(makeDatabase({ reclaimPolicy: 'keep' as never }))).toThrow(/reclaimPolicy/);
    expect(() => validateDatabaseSpec(makeDatabase({ clusterName: '' }))).toThrow(/clusterName/);
  });

  it('refuses replicated clusters, the cluster database and a file another resource manages', () => {
    expect(databaseClusterError(makeDatabase(), makeCluster(), [])).toBeUndefined();
    expect(databaseClusterError(makeDatabase(), makeCluster({ replication: { enabled: true } }), [])).toMatch(/lost at a failover/);
    expect(databaseClusterError(makeDatabase({ databaseName: 'mydb.fdb' }), makeCluster(), [])).toMatch(/is the cluster database/);
    const older = makeDatabase({ databaseName: 'sales.fdb' }, { name: 'first', creationTimestamp: '2026-10-11T09:00:00Z' });
    const newer = makeDatabase();
    expect(databaseClusterError(newer, makeCluster(), [older, newer])).toBe('sales.fdb is managed by FirebirdDatabase first');
    expect(databaseClusterError(older, makeCluster(), [older, newer])).toBeUndefined();
    // another cluster's file of the same name is not a conflict
    expect(databaseClusterError(newer, makeCluster(), [{ ...older, spec: { clusterName: 'other' } }])).toBeUndefined();
  });
});

describe('database Jobs', () => {
  it('creates the database on each instance, adopting an existing one, with initSql from the environment', () => {
    const job = buildDatabaseJob(makeCluster(), makeDatabase({ pageSize: 16384, characterSet: 'win1252', initSql: "INSERT INTO t VALUES ('$x');" }), 'create', [
      { name: 'db-0', volume: 'v0' },
      { name: 'db-1', volume: 'v1' },
    ]);
    expect(job.metadata?.name).toBe('database-sales');
    expect(job.metadata?.labels?.[DATABASE_JOB_ACTION_LABEL]).toBe('create');
    expect(JSON.parse(job.metadata!.annotations![DATABASE_JOB_TARGETS_ANNOTATION])).toEqual([
      { name: 'db-0', volume: 'v0' },
      { name: 'db-1', volume: 'v1' },
    ]);
    expect(job.metadata?.ownerReferences?.[0]).toMatchObject({ kind: 'FirebirdDatabase', name: 'sales', blockOwnerDeletion: false });
    const env = envOf(job);
    expect(env).toMatchObject({
      DATABASE_PATH: '/var/lib/firebird/data/sales.fdb',
      HOSTS: 'db-0.db-headless db-1.db-headless',
      PAGE_SIZE: '16384',
      CHARACTER_SET: 'WIN1252',
      INIT_SQL: "INSERT INTO t VALUES ('$x');",
      ISC_USER: 'SYSDBA',
    });
    const script = job.spec!.template.spec!.containers[0].args![0];
    expect(script).toBe(databaseJobScript('create'));
    // initSql is never part of the shell text
    expect(script).not.toContain('INSERT');
    expect(script).toContain('adopted');
    expect(script).toContain('CREATE DATABASE');
    // a failed initSql drops the new database so that the retry runs it again
    expect(script).toMatch(/INIT_SQL.*\| isql -q -b "\$h:\$DATABASE_PATH"; then\n\s+echo 'DROP DATABASE;'/);
    // the default: no page size clause, UTF8
    const plain = envOf(buildDatabaseJob(makeCluster(), makeDatabase(), 'create', [{ name: 'db-0', volume: 'v0' }]));
    expect(plain.PAGE_SIZE).toBeUndefined();
    expect(plain.CHARACTER_SET).toBe('UTF8');
  });

  it('drops the database where it exists', () => {
    const job = buildDatabaseJob(makeCluster(), makeDatabase({ initSql: 'x' }), 'drop', [{ name: 'db-1', volume: 'v1' }]);
    expect(job.metadata?.labels?.[DATABASE_JOB_ACTION_LABEL]).toBe('drop');
    expect(envOf(job).INIT_SQL).toBeUndefined();
    expect(job.spec!.template.spec!.containers[0].args![0]).toContain('DROP DATABASE');
  });
});

describe('FirebirdDatabaseController', () => {
  const notFound = Object.assign(new Error('Not Found'), { code: 404 });
  const readyPod = (name: string) => ({ metadata: { name }, status: { phase: 'Running', conditions: [{ type: 'Ready', status: 'True' }] } });
  const pvc = (pod: string) => ({ metadata: { name: `firebird-data-${pod}`, uid: `vol-${pod}` } });
  const complete = (action: 'create' | 'drop', targets: Array<{ name: string; volume: string }>): V1Job => ({
    metadata: { labels: { [DATABASE_JOB_ACTION_LABEL]: action }, annotations: { [DATABASE_JOB_TARGETS_ANNOTATION]: JSON.stringify(targets) } },
    status: { conditions: [{ type: 'Complete', status: 'True' }] },
  });

  function setup(opts: { cluster?: FirebirdCluster | null; job?: V1Job; readyPods?: string[]; now?: number; others?: FirebirdDatabase[] } = {}) {
    const cluster = opts.cluster === undefined ? makeCluster() : opts.cluster;
    const api: Record<string, Mock> = {
      getNamespacedCustomObject: cluster ? vi.fn().mockResolvedValue(cluster) : vi.fn().mockRejectedValue(notFound),
      listNamespacedCustomObject: vi.fn().mockResolvedValue({ items: opts.others ?? [] }),
      patchNamespacedCustomObjectStatus: vi.fn().mockResolvedValue({}),
      patchNamespacedCustomObject: vi.fn().mockResolvedValue({}),
      readNamespacedJob: opts.job ? vi.fn().mockResolvedValue(opts.job) : vi.fn().mockRejectedValue(notFound),
      createNamespacedJob: vi.fn().mockResolvedValue({}),
      deleteNamespacedJob: vi.fn().mockResolvedValue({}),
      listNamespacedPod: vi.fn().mockResolvedValue({ items: (opts.readyPods ?? ['db-0', 'db-1']).map(readyPod) }),
      listNamespacedPersistentVolumeClaim: vi.fn().mockResolvedValue({ items: ['db-0', 'db-1'].map(pvc) }),
      createNamespacedEvent: vi.fn().mockResolvedValue({}),
    };
    const kubeConfig = new KubeConfig();
    vi.spyOn(kubeConfig, 'makeApiClient').mockReturnValue(new Proxy({}, { get: (_t, p: string) => api[p] ?? vi.fn().mockResolvedValue({}) }) as never);
    const controller = new FirebirdDatabaseController(kubeConfig, () => opts.now ?? Date.parse('2026-10-11T12:00:00Z'));
    const status = () => api.patchNamespacedCustomObjectStatus.mock.calls.at(-1)?.[0].body[0].value;
    const created = () => api.createNamespacedJob.mock.calls.map((c) => c[0].body as V1Job);
    const finalizers = () => api.patchNamespacedCustomObject.mock.calls.at(-1)?.[0].body[1].value;
    const events = () => api.createNamespacedEvent.mock.calls.map((c) => c[0].body.reason);
    return { controller, api, status, created, finalizers, events };
  }

  it('creates a new database on every ready instance', async () => {
    const { controller, status, created } = setup();
    await controller.reconcileDatabase(makeDatabase());
    expect(created()).toHaveLength(1);
    expect(envOf(created()[0]).HOSTS).toBe('db-0.db-headless db-1.db-headless');
    expect(status()).toMatchObject({ phase: 'Creating', path: '/var/lib/firebird/data/sales.fdb', jobName: 'database-sales' });
  });

  it('records a completed Job and reports Ready', async () => {
    const targets = [
      { name: 'db-0', volume: 'vol-db-0' },
      { name: 'db-1', volume: 'vol-db-1' },
    ];
    const { controller, status, created, api, events } = setup({ job: complete('create', targets) });
    await controller.reconcileDatabase(makeDatabase());
    expect(api.deleteNamespacedJob).toHaveBeenCalled();
    expect(created()).toHaveLength(0);
    expect(status()).toMatchObject({ phase: 'Ready', instances: targets });
    expect(status().jobName).toBeUndefined();
    expect(events()).toContain('DatabaseCreated');
  });

  it('creates it again on a replaced volume only, and waits for unready instances', async () => {
    const { controller, status, created } = setup({ readyPods: ['db-1'] });
    await controller.reconcileDatabase(
      makeDatabase({}, {}, { instances: [{ name: 'db-0', volume: 'vol-db-0' }, { name: 'db-1', volume: 'old-volume' }] }),
    );
    expect(envOf(created()[0]).HOSTS).toBe('db-1.db-headless');
    const waiting = setup({ readyPods: [] });
    await waiting.controller.reconcileDatabase(makeDatabase());
    expect(waiting.created()).toHaveLength(0);
    expect(waiting.status()).toMatchObject({ phase: 'Pending', message: expect.stringContaining('db-0, db-1') });
    expect(status()).toMatchObject({ phase: 'Creating' });
  });

  it('refuses a replicated cluster and a conflicting file, without a Job', async () => {
    const replicated = setup({ cluster: makeCluster({ replication: { enabled: true } }) });
    await replicated.controller.reconcileDatabase(makeDatabase());
    expect(replicated.created()).toHaveLength(0);
    expect(replicated.status()).toMatchObject({ phase: 'Failed', message: expect.stringContaining('replication') });
    expect(replicated.events()).toContain('DatabaseFailed');

    const older = makeDatabase({}, { name: 'first', creationTimestamp: '2026-10-01T00:00:00Z' }, { phase: 'Ready' });
    const conflict = setup({ others: [older] });
    await conflict.controller.reconcileDatabase(makeDatabase({ databaseName: 'first.fdb' }));
    expect(conflict.status()).toMatchObject({ phase: 'Failed', message: 'first.fdb is managed by FirebirdDatabase first' });
  });

  it('backs off after a failed Job, then retries', async () => {
    const failed: V1Job = { metadata: { labels: { [DATABASE_JOB_ACTION_LABEL]: 'create' } }, status: { conditions: [{ type: 'Failed', status: 'True' }] } };
    const first = setup({ job: failed });
    await first.controller.reconcileDatabase(makeDatabase());
    expect(first.created()).toHaveLength(0);
    expect(first.status()).toMatchObject({ phase: 'Failed', message: expect.stringContaining('retrying after') });
    expect(first.events()).toContain('DatabaseFailed');
    const later = setup({ now: Date.parse('2026-10-11T12:00:00Z') + DATABASE_RETRY_DELAY_MS + 1000 });
    await later.controller.reconcileDatabase(makeDatabase({}, {}, { lastFailureTime: '2026-10-11T12:00:00.000Z' }));
    expect(later.created()).toHaveLength(1);
  });

  it('waits for a missing or hibernated cluster', async () => {
    const missing = setup({ cluster: null });
    await missing.controller.reconcileDatabase(makeDatabase());
    expect(missing.status()).toMatchObject({ phase: 'Pending', message: 'FirebirdCluster db not found' });
    const hibernated = setup({ cluster: makeCluster({ hibernated: true }) });
    await hibernated.controller.reconcileDatabase(makeDatabase());
    expect(hibernated.status()).toMatchObject({ phase: 'Pending' });
    expect(hibernated.created()).toHaveLength(0);
  });

  it('adds the finalizer for reclaimPolicy delete, and drops the database on deletion', async () => {
    const create = setup();
    await create.controller.reconcileDatabase(makeDatabase({ reclaimPolicy: 'delete' }));
    expect(create.finalizers()).toEqual([DATABASE_FINALIZER]);

    const deleting = makeDatabase(
      { reclaimPolicy: 'delete' },
      { finalizers: [DATABASE_FINALIZER], deletionTimestamp: '2026-10-11T11:59:00Z' },
      { instances: [{ name: 'db-0', volume: 'vol-db-0' }, { name: 'db-1', volume: 'vol-db-1' }] },
    );
    const drop = setup();
    await drop.controller.reconcileDatabase(deleting);
    expect(drop.created()[0].metadata?.labels?.[DATABASE_JOB_ACTION_LABEL]).toBe('drop');
    expect(drop.status()).toMatchObject({ phase: 'Dropping' });

    const done = setup({ job: complete('drop', [{ name: 'db-0', volume: 'vol-db-0' }, { name: 'db-1', volume: 'vol-db-1' }]) });
    await done.controller.reconcileDatabase(deleting);
    expect(done.created()).toHaveLength(0);
    expect(done.finalizers()).toEqual([]);
    expect(done.events()).toContain('DatabaseDropped');
  });

  it('waits for an unready instance holding the database, then keeps its file', async () => {
    const deleting = (since: string) =>
      makeDatabase({ reclaimPolicy: 'delete' }, { finalizers: [DATABASE_FINALIZER], deletionTimestamp: since }, {
        instances: [{ name: 'db-0', volume: 'vol-db-0' }],
        droppedFrom: [{ name: 'db-1', volume: 'vol-db-1' }],
      });
    const waiting = setup({ readyPods: ['db-1'] });
    await waiting.controller.reconcileDatabase(deleting('2026-10-11T11:59:00Z'));
    expect(waiting.status()).toMatchObject({ phase: 'Dropping', message: expect.stringContaining('db-0') });
    expect(waiting.api.patchNamespacedCustomObject).not.toHaveBeenCalled();

    const late = setup({ readyPods: ['db-1'], now: Date.parse('2026-10-11T11:59:00Z') + DATABASE_DROP_WAIT_MS + 1000 });
    await late.controller.reconcileDatabase(deleting('2026-10-11T11:59:00Z'));
    expect(late.finalizers()).toEqual([]);
    expect(late.events()).toContain('DatabaseFailed');
  });

  it('keeps the files with reclaimPolicy retain, or when the cluster is gone', async () => {
    const retain = setup();
    await retain.controller.reconcileDatabase(makeDatabase({}, { finalizers: [DATABASE_FINALIZER], deletionTimestamp: '2026-10-11T11:00:00Z' }));
    expect(retain.created()).toHaveLength(0);
    expect(retain.finalizers()).toEqual([]);
    const gone = setup({ cluster: null });
    await gone.controller.reconcileDatabase(makeDatabase({ reclaimPolicy: 'delete' }, { finalizers: [DATABASE_FINALIZER], deletionTimestamp: '2026-10-11T11:00:00Z' }));
    expect(gone.finalizers()).toEqual([]);
  });
});
