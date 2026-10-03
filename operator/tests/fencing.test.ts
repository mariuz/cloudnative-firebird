import { describe, it, expect, vi, Mock } from 'vitest';
import { KubeConfig, V1Job } from '@kubernetes/client-node';
import { spawnSync } from 'child_process';
import { chmodSync, mkdtempSync, readFileSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import {
  FENCED_INSTANCES_ANNOTATION,
  FENCING_ACTION_LABEL,
  buildFencingJob,
  databaseOnlineCheck,
  desiredFencedInstances,
  planFencing,
} from '../src/utils/fencing';
import { buildStatefulSet, statefulSetNeedsUpdate } from '../src/utils/resources';
import { FirebirdClusterController } from '../src/controllers/firebirdcluster.controller';
import { ValidationError } from '../src/utils/validation';
import { FirebirdCluster } from '../src/types';

const makeCluster = (fenced?: string, overrides: Partial<FirebirdCluster['spec']> = {}): FirebirdCluster => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdCluster',
  metadata: {
    name: 'db',
    namespace: 'default',
    uid: 'uid-1',
    ...(fenced !== undefined ? { annotations: { [FENCED_INSTANCES_ANNOTATION]: fenced } } : {}),
  },
  spec: { instances: 3, storage: { size: '1Gi' }, ...overrides },
});

const job = (action: 'fence' | 'unfence', condition?: 'Complete' | 'Failed'): V1Job => ({
  metadata: { labels: { [FENCING_ACTION_LABEL]: action } },
  status: condition ? { conditions: [{ type: condition, status: 'True' }] } : {},
});

describe('fencedInstances annotation', () => {
  it('uses the CloudNativePG format: a JSON list of instance names', () => {
    expect(FENCED_INSTANCES_ANNOTATION).toBe('firebird.cloudnative-firebird.io/fencedInstances');
    expect(desiredFencedInstances(makeCluster())).toEqual([]);
    expect(desiredFencedInstances(makeCluster('[]'))).toEqual([]);
    expect(desiredFencedInstances(makeCluster('["db-1"]'))).toEqual(['db-1']);
    expect(desiredFencedInstances(makeCluster('["db-2","db-0"]'))).toEqual(['db-0', 'db-2']);
  });

  it('fences every instance with the "*" wildcard', () => {
    expect(desiredFencedInstances(makeCluster('["*"]'))).toEqual(['db-0', 'db-1', 'db-2']);
  });

  it('ignores instances beyond spec.instances', () => {
    expect(desiredFencedInstances(makeCluster('["db-5"]'))).toEqual([]);
  });

  it('rejects malformed values and other clusters\' instances', () => {
    expect(() => desiredFencedInstances(makeCluster('db-1'))).toThrow(ValidationError);
    expect(() => desiredFencedInstances(makeCluster('{"db-1":true}'))).toThrow(ValidationError);
    expect(() => desiredFencedInstances(makeCluster('[1]'))).toThrow(ValidationError);
    expect(() => desiredFencedInstances(makeCluster('["other-1"]'))).toThrow(/another cluster/);
  });
});

describe('fencing Jobs', () => {
  it('fences through the instance service manager with a full shutdown, idempotently', () => {
    const j = buildFencingJob(makeCluster(undefined, { databaseName: 'erp.fdb', superuserSecret: { name: 'su' } }), 'db-1', 'fence');
    expect(j.metadata?.name).toBe('db-1-fencing');
    expect(j.metadata?.labels?.[FENCING_ACTION_LABEL]).toBe('fence');
    expect(j.metadata?.ownerReferences?.[0]).toMatchObject({ kind: 'FirebirdCluster', uid: 'uid-1' });
    const c = j.spec!.template.spec!.containers[0];
    const args = c.args![0];
    expect(args).toContain('if echo "$hdr" | grep -q "full shutdown"; then echo "$FIREBIRD_HOST already fenced"');
    expect(args).toContain('prp_shutdown_mode prp_sm_full prp_force_shutdown 0');
    expect(c.env).toEqual(
      expect.arrayContaining([
        { name: 'FIREBIRD_HOST', value: 'db-1.db-headless' },
        { name: 'DATABASE_PATH', value: '/var/lib/firebird/data/erp.fdb' },
        { name: 'ISC_PASSWORD', valueFrom: { secretKeyRef: { name: 'su', key: 'password' } } },
      ]),
    );
    expect(j.spec?.template.spec?.volumes).toBeUndefined();
  });

  it('reads the database state from the header statistics, or from Firebird 6 refusing them', () => {
    // fbsvcmgr stand-in: header statistics answer per $STATS ("online", "full", "fb6", "down")
    const run = (action: 'fence' | 'unfence', stats: string) => {
      const dir = mkdtempSync(join(tmpdir(), 'fb-fence-'));
      const fake = join(dir, 'fbsvcmgr');
      writeFileSync(
        fake,
        [
          '#!/bin/sh',
          'case "$*" in *action_properties*) echo "$*" >> "$(dirname "$0")/calls"; exit 0 ;; esac',
          'case "$STATS" in',
          '  online) echo "	Attributes		force write" ;;',
          '  full) echo "	Attributes		force write, full shutdown" ;;',
          '  fb6) echo "database /var/lib/firebird/data/mydb.fdb shutdown"; exit 1 ;;',
          '  *) echo "Unable to complete network request to host"; exit 1 ;;',
          'esac',
          '',
        ].join('\n'),
      );
      chmodSync(fake, 0o755);
      const script = buildFencingJob(makeCluster(), 'db-1', action).spec!.template.spec!.containers[0].args![0];
      const r = spawnSync('sh', ['-c', script], {
        encoding: 'utf8',
        env: { ...process.env, PATH: `${dir}:${process.env.PATH}`, STATS: stats, FIREBIRD_HOST: 'db-1', DATABASE_PATH: '/db' },
      });
      let calls = '';
      try {
        calls = readFileSync(join(dir, 'calls'), 'utf8');
      } catch {
        // no service action
      }
      return { status: r.status, out: r.stdout + r.stderr, calls };
    };
    expect(run('fence', 'online')).toMatchObject({ status: 0, calls: expect.stringContaining('prp_sm_full') });
    expect(run('fence', 'full')).toMatchObject({ status: 0, out: expect.stringContaining('already fenced'), calls: '' });
    expect(run('fence', 'fb6')).toMatchObject({ status: 0, out: expect.stringContaining('already fenced'), calls: '' });
    expect(run('unfence', 'fb6')).toMatchObject({ status: 0, calls: expect.stringContaining('prp_online_mode prp_sm_normal') });
    expect(run('unfence', 'online')).toMatchObject({ status: 0, out: expect.stringContaining('already online'), calls: '' });
    // any other failure fails the Job
    const down = run('unfence', 'down');
    expect(down.status).not.toBe(0);
    expect(down.out).toContain('Unable to complete network request');
    expect(down.calls).toBe('');
  });

  it('unfences by bringing the database online', () => {
    const args = buildFencingJob(makeCluster(), 'db-0', 'unfence').spec!.template.spec!.containers[0].args![0];
    expect(args).toContain('prp_online_mode prp_sm_normal');
    expect(args).toContain('already online');
  });
});

describe('planFencing', () => {
  it('creates a Job only when the instance differs from the annotation', () => {
    expect(planFencing(false, false, undefined)).toEqual({ kind: 'none' });
    expect(planFencing(true, true, undefined)).toEqual({ kind: 'none' });
    expect(planFencing(true, false, undefined)).toEqual({ kind: 'create', action: 'fence' });
    expect(planFencing(false, true, undefined)).toEqual({ kind: 'create', action: 'unfence' });
  });

  it('never interrupts a running Job', () => {
    expect(planFencing(false, false, job('fence'))).toEqual({ kind: 'wait' });
  });

  it('records a finished Job for the action it performed, even if the annotation changed', () => {
    expect(planFencing(true, false, job('fence', 'Complete'))).toEqual({ kind: 'applied', action: 'fence' });
    expect(planFencing(false, false, job('fence', 'Complete'))).toEqual({ kind: 'applied', action: 'fence' });
    expect(planFencing(false, true, job('unfence', 'Complete'))).toEqual({ kind: 'applied', action: 'unfence' });
  });

  it('reports failed Jobs', () => {
    expect(planFencing(true, false, job('fence', 'Failed'))).toEqual({ kind: 'failed', action: 'fence' });
  });
});

describe('readiness probe', () => {
  it('requires the database to be online, so fenced instances leave the Services', () => {
    const probe = buildStatefulSet(makeCluster()).spec!.template.spec!.containers[0].readinessProbe!;
    expect(probe.exec?.command).toEqual(['/bin/sh', '-c', databaseOnlineCheck('/var/lib/firebird/data/mydb.fdb')]);
    expect(probe.exec?.command?.[2]).toContain("! echo \"$out\" | grep -q 'shutdown'");
    expect(probe.timeoutSeconds).toBe(5);
  });

  it('rolls existing StatefulSets that still use the TCP readiness probe', () => {
    const desired = buildStatefulSet(makeCluster());
    const old = JSON.parse(JSON.stringify(desired));
    old.spec.template.spec.containers[0].readinessProbe = { tcpSocket: { port: 3050 } };
    expect(statefulSetNeedsUpdate(old, desired)).toBe(true);
    expect(statefulSetNeedsUpdate(JSON.parse(JSON.stringify(desired)), desired)).toBe(false);
  });
});

describe('FirebirdClusterController – fencing', () => {
  const notFound = Object.assign(new Error('Not Found'), { code: 404 });

  function mockApi(overrides: Record<string, Mock> = {}) {
    const calls: Record<string, Mock> = {};
    const fn = (method: string): Mock => {
      if (!calls[method]) {
        calls[method] =
          overrides[method] ??
          (method.startsWith('read') || method.startsWith('get')
            ? vi.fn().mockRejectedValue(notFound)
            : method.startsWith('list')
              ? vi.fn().mockResolvedValue({ items: [] })
              : vi.fn().mockImplementation((req: { body?: unknown }) => Promise.resolve(req?.body ?? {})));
      }
      return calls[method];
    };
    const kubeConfig = new KubeConfig();
    vi.spyOn(kubeConfig, 'makeApiClient').mockReturnValue(new Proxy({}, { get: (_t, p: string) => fn(p) }) as never);
    return { controller: new FirebirdClusterController(kubeConfig), api: fn };
  }

  const lastStatus = (api: (m: string) => Mock) => {
    const calls = api('patchNamespacedCustomObjectStatus').mock.calls;
    return (calls[calls.length - 1][0] as { body: Array<{ value: Record<string, unknown> }> }).body[0].value;
  };

  const readyStatefulSet = (ready: number) =>
    vi.fn().mockResolvedValue({
      metadata: { name: 'db' },
      spec: { replicas: 3, podManagementPolicy: 'Parallel', template: { spec: { containers: [{ name: 'firebird' }] } } },
      status: { readyReplicas: ready },
    });

  it('starts a fence Job for each newly fenced instance', async () => {
    const { controller, api } = mockApi();
    await controller.reconcile(makeCluster('["db-1"]'));
    const created = api('createNamespacedJob').mock.calls.map((c) => (c[0] as { body: V1Job }).body);
    expect(created.map((j) => [j.metadata?.name, j.metadata?.labels?.[FENCING_ACTION_LABEL]])).toEqual([
      ['db-1-fencing', 'fence'],
    ]);
    const fenced = (lastStatus(api).conditions as Array<{ type: string; reason: string }>).find((c) => c.type === 'Fenced');
    expect(fenced?.reason).toBe('FencingInProgress');
  });

  it('records a completed Job in status.fencedInstances, deletes it and expects the instance to be unready', async () => {
    const { controller, api } = mockApi({
      readNamespacedJob: vi.fn().mockImplementation(({ name }: { name: string }) =>
        name === 'db-1-fencing' ? Promise.resolve(job('fence', 'Complete')) : Promise.reject(notFound),
      ),
      readNamespacedStatefulSet: readyStatefulSet(2),
      patchNamespacedStatefulSet: readyStatefulSet(2),
    });
    await controller.reconcile(makeCluster('["db-1"]'));
    expect(api('deleteNamespacedJob')).toHaveBeenCalledWith(expect.objectContaining({ name: 'db-1-fencing' }));
    const status = lastStatus(api);
    expect(status.fencedInstances).toEqual(['db-1']);
    expect(status.phase).toBe('Running');
    expect(status.phaseReason).toContain('1 instance(s) fenced');
    // scale subresource selector (HPA / VPA)
    expect(status.selector).toBe(
      'app.kubernetes.io/name=firebird,app.kubernetes.io/component=database,' +
        'app.kubernetes.io/managed-by=cloudnative-firebird-operator,firebird.cloudnative-firebird.io/cluster=db',
    );
  });

  it('unfences instances removed from the annotation', async () => {
    const { controller, api } = mockApi();
    const cluster = makeCluster('[]');
    cluster.status = { fencedInstances: ['db-2'] };
    await controller.reconcile(cluster);
    const created = api('createNamespacedJob').mock.calls.map((c) => (c[0] as { body: V1Job }).body);
    expect(created.map((j) => [j.metadata?.name, j.metadata?.labels?.[FENCING_ACTION_LABEL]])).toEqual([
      ['db-2-fencing', 'unfence'],
    ]);
  });

  it('keeps fencing state while hibernated and for instances removed by scaling down', async () => {
    const { controller, api } = mockApi();
    const cluster = makeCluster('["*"]', { instances: 1 });
    cluster.status = { fencedInstances: ['db-0', 'db-2'] };
    await controller.reconcile(cluster);
    expect(api('createNamespacedJob')).not.toHaveBeenCalled();
    expect(lastStatus(api).fencedInstances).toEqual(['db-0', 'db-2']);
  });

  it('marks the cluster Degraded for a malformed annotation', async () => {
    const { controller, api } = mockApi();
    await expect(controller.reconcile(makeCluster('db-1'))).rejects.toThrow(ValidationError);
    expect(lastStatus(api).phase).toBe('Degraded');
    expect(api('createNamespacedJob')).not.toHaveBeenCalled();
  });

  it('retries a Job that is still being deleted on the next reconcile', async () => {
    const { controller } = mockApi({
      createNamespacedJob: vi.fn().mockRejectedValue(Object.assign(new Error('exists'), { code: 409 })),
    });
    await expect(controller.reconcile(makeCluster('["db-0"]'))).resolves.toBeUndefined();
  });
});
