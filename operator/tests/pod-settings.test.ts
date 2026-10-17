import { describe, it, expect, vi, type Mock } from 'vitest';
import { KubeConfig, V1Container, V1PodSpec, V1StatefulSet } from '@kubernetes/client-node';
import { FirebirdClusterController } from '../src/controllers/firebirdcluster.controller';
import { FirebirdCluster } from '../src/types';
import { buildBenchJob } from '../src/plugin/bench';
import { buildBackupCronJob } from '../src/utils/backup';
import { buildFencingJob } from '../src/utils/fencing';
import { clusterPodSettings, inheritMetadata, missingInheritedMetadata, withInheritedMetadata, withPullPolicy } from '../src/utils/pod-settings';
import {
  buildHeadlessService,
  buildPodDisruptionBudget,
  buildReplicaService,
  buildService,
  buildStatefulSet,
  JOB_TEMPLATE_HASH_ANNOTATION,
  statefulSetNeedsUpdate,
  statefulSetPatchBody,
  withTemplateHash,
} from '../src/utils/resources';
import { validateClusterSpec, ValidationError } from '../src/utils/validation';
import { makeCluster, notFoundError } from './helpers/factories';

const settings: Partial<FirebirdCluster['spec']> = {
  priorityClassName: 'database-critical',
  schedulerName: 'custom-scheduler',
  imagePullPolicy: 'IfNotPresent',
  imagePullSecrets: [{ name: 'registry-credentials' }],
  inheritedMetadata: { labels: { team: 'payments' }, annotations: { 'sidecar.istio.io/inject': 'false' } },
};
const configured = (extra: Partial<FirebirdCluster['spec']> = {}) => makeCluster({ ...settings, ...extra });
const allContainers = (spec?: V1PodSpec): V1Container[] => [...(spec?.initContainers ?? []), ...(spec?.containers ?? [])];

describe('pod settings', () => {
  it('are empty without settings', () => {
    expect(clusterPodSettings(makeCluster())).toEqual({});
    const container = { name: 'c', image: 'i' };
    expect(withPullPolicy(makeCluster(), container)).toBe(container);
    expect(inheritMetadata(makeCluster(), { name: 'x', labels: { a: 'b' } })).toEqual({ name: 'x', labels: { a: 'b' } });
  });

  it('go on the instance pods: priority class, scheduler, pull secrets and the pull policy of every container', () => {
    // with segment TLS: its proxy sidecar (added after the instance containers) too
    const pod = buildStatefulSet(configured({ replication: { enabled: true }, segmentTLS: { enabled: true } })).spec!.template.spec!;
    expect(allContainers(pod).map((c) => c.name)).toContain('segment-tls');
    expect(pod.priorityClassName).toBe('database-critical');
    expect(pod.schedulerName).toBe('custom-scheduler');
    expect(pod.imagePullSecrets).toEqual([{ name: 'registry-credentials' }]);
    const containers = allContainers(pod);
    expect(containers.length).toBeGreaterThan(2);
    for (const c of containers) expect(c.imagePullPolicy, c.name).toBe('IfNotPresent');
  });

  it('keep the service account alongside', () => {
    expect(clusterPodSettings(configured({ serviceAccountName: 'workload' }))).toEqual({
      serviceAccountName: 'workload',
      priorityClassName: 'database-critical',
      schedulerName: 'custom-scheduler',
      imagePullSecrets: [{ name: 'registry-credentials' }],
    });
  });

  it('go on the Job pods (jobPodSpec) and the bench Job', () => {
    const tls = configured({ replication: { enabled: true }, segmentTLS: { enabled: true } });
    expect(allContainers(buildFencingJob(tls, 'test-cluster-0', 'fence').spec!.template.spec!).length).toBeGreaterThan(1);
    for (const job of [buildFencingJob(tls, 'test-cluster-0', 'fence'), buildFencingJob(configured(), 'test-cluster-0', 'fence'), buildBenchJob(configured(), { transactions: 1, clients: 1, databaseFile: 'db.fdb', jobName: 'b', keep: false })]) {
      const pod = job.spec!.template.spec!;
      expect(pod.priorityClassName).toBe('database-critical');
      expect(pod.schedulerName).toBe('custom-scheduler');
      expect(pod.imagePullSecrets).toEqual([{ name: 'registry-credentials' }]);
      for (const c of allContainers(pod)) expect(c.imagePullPolicy).toBe('IfNotPresent');
    }
    // the bench Job carries the inherited metadata itself; the operator's Jobs get it where they are created
    const bench = buildBenchJob(configured(), { transactions: 1, clients: 1, databaseFile: 'db.fdb', jobName: 'b', keep: false });
    expect(bench.metadata!.labels).toMatchObject({ team: 'payments', 'app.kubernetes.io/component': 'bench' });
    expect(bench.spec!.template.metadata!.annotations).toEqual({ 'sidecar.istio.io/inject': 'false' });
  });
});

describe('inherited metadata', () => {
  it('goes on the StatefulSet, its pods and its volume claims', () => {
    const sts = buildStatefulSet(configured());
    for (const metadata of [sts.metadata, sts.spec!.template.metadata, sts.spec!.volumeClaimTemplates![0].metadata]) {
      expect(metadata!.labels).toMatchObject({ team: 'payments', 'firebird.cloudnative-firebird.io/cluster': 'test-cluster' });
      expect(metadata!.annotations).toMatchObject({ 'sidecar.istio.io/inject': 'false' });
    }
    // the selector stays the operator's
    expect(sts.spec!.selector.matchLabels).not.toHaveProperty('team');
  });

  it("goes on the default Services and the PodDisruptionBudget, under the operator's own keys", () => {
    const cluster = configured({ instances: 2, replication: { enabled: true }, serviceAnnotations: { 'sidecar.istio.io/inject': 'true' } });
    for (const service of [buildService(cluster), buildReplicaService(cluster), buildHeadlessService(cluster)]) {
      expect(service.metadata!.labels).toMatchObject({ team: 'payments' });
      expect(service.spec!.selector).not.toHaveProperty('team');
    }
    // spec.serviceAnnotations win over inherited ones
    expect(buildService(cluster).metadata!.annotations).toEqual({ 'sidecar.istio.io/inject': 'true' });
    expect(buildReplicaService(cluster).metadata!.labels!['app.kubernetes.io/component']).toBe('database-replica');
    expect(buildHeadlessService(cluster).metadata!.annotations).toMatchObject({
      'sidecar.istio.io/inject': 'false',
      'service.alpha.kubernetes.io/tolerate-unready-endpoints': 'true',
    });
    expect(buildPodDisruptionBudget(cluster).metadata!.labels).toMatchObject({ team: 'payments' });
    expect(inheritMetadata(makeCluster({ inheritedMetadata: { labels: { a: 'inherited', b: 'x' } } }), { labels: { a: 'own' } }).labels).toEqual({ a: 'own', b: 'x' });
  });

  it('goes on Jobs and CronJobs, their Job templates and pods', () => {
    const job = withInheritedMetadata(configured(), buildFencingJob(configured(), 'test-cluster-0', 'fence'));
    expect(job.metadata!.labels).toMatchObject({ team: 'payments', 'app.kubernetes.io/component': expect.any(String) });
    expect(job.spec!.template.metadata!.labels).toMatchObject({ team: 'payments' });
    expect(job.spec!.template.metadata!.annotations).toMatchObject({ 'sidecar.istio.io/inject': 'false' });

    const cluster = configured({ backup: { enabled: true, schedule: '0 3 * * *' } });
    const plain = buildBackupCronJob(cluster, 'test-cluster-0');
    const cron = withTemplateHash(withInheritedMetadata(cluster, plain));
    expect(cron.metadata!.labels).toMatchObject({ team: 'payments' });
    expect(cron.spec!.jobTemplate.metadata!.labels).toMatchObject({ team: 'payments' });
    expect(cron.spec!.jobTemplate.spec!.template.metadata!.annotations).toMatchObject({ 'sidecar.istio.io/inject': 'false' });
    // a change of the inherited metadata changes the template hash, so the CronJob is updated
    expect(cron.metadata!.annotations![JOB_TEMPLATE_HASH_ANNOTATION]).not.toBe(plain.metadata!.annotations![JOB_TEMPLATE_HASH_ANNOTATION]);
    // nothing to inherit: the same object
    const bare = buildFencingJob(makeCluster(), 'test-cluster-0', 'fence');
    expect(withInheritedMetadata(makeCluster(), bare)).toBe(bare);
  });

  it('reports what an existing object lacks, leaving out keys the operator sets', () => {
    const cluster = configured();
    expect(missingInheritedMetadata(cluster, { labels: { team: 'payments' }, annotations: { 'sidecar.istio.io/inject': 'false' } })).toEqual({});
    expect(missingInheritedMetadata(cluster, { labels: { team: 'other' } })).toEqual({
      labels: { team: 'payments' },
      annotations: { 'sidecar.istio.io/inject': 'false' },
    });
    expect(missingInheritedMetadata(cluster, {}, { labels: { team: 'own' } })).toEqual({ annotations: { 'sidecar.istio.io/inject': 'false' } });
  });
});

describe('StatefulSet updates for the pod settings', () => {
  // the existing StatefulSet as the API server returns it: defaulted scheduler and pull policies
  const asStored = (sts: V1StatefulSet): V1StatefulSet => {
    const copy = JSON.parse(JSON.stringify(sts)) as V1StatefulSet;
    const pod = copy.spec!.template.spec!;
    pod.schedulerName ??= 'default-scheduler';
    for (const c of allContainers(pod)) c.imagePullPolicy ??= 'IfNotPresent';
    return copy;
  };

  it('are not needed when nothing changed, whatever the server defaulted', () => {
    expect(statefulSetNeedsUpdate(asStored(buildStatefulSet(makeCluster())), buildStatefulSet(makeCluster()))).toBe(false);
    expect(statefulSetNeedsUpdate(asStored(buildStatefulSet(configured())), buildStatefulSet(configured()))).toBe(false);
  });

  it.each<[string, Partial<FirebirdCluster['spec']>]>([
    ['priorityClassName', { priorityClassName: 'database-critical' }],
    ['schedulerName', { schedulerName: 'custom-scheduler' }],
    ['imagePullSecrets', { imagePullSecrets: [{ name: 'registry-credentials' }] }],
    ['imagePullPolicy', { imagePullPolicy: 'Always' }],
    ['inherited labels', { inheritedMetadata: { labels: { team: 'payments' } } }],
    ['inherited annotations', { inheritedMetadata: { annotations: { 'sidecar.istio.io/inject': 'false' } } }],
  ])('are needed when %s is set', (_field, extra) => {
    expect(statefulSetNeedsUpdate(asStored(buildStatefulSet(makeCluster())), buildStatefulSet(makeCluster(extra)))).toBe(true);
  });

  it('are needed when a pod setting is removed, and the patch removes it', () => {
    const existing = asStored(buildStatefulSet(configured()));
    const desired = buildStatefulSet(makeCluster());
    expect(statefulSetNeedsUpdate(existing, desired)).toBe(true);
    const pod = statefulSetPatchBody(existing, desired).spec!.template.spec! as unknown as Record<string, unknown>;
    expect(pod.priorityClassName).toBeNull();
    expect(pod.schedulerName).toBeNull();
    expect(pod.imagePullSecrets).toBeNull();
  });
});

describe('pod settings validation', () => {
  const check = (extra: Partial<FirebirdCluster['spec']>) => () => validateClusterSpec(makeCluster(extra));

  it('accepts valid settings', () => {
    expect(check({ ...settings, inheritedMetadata: { labels: { 'example.com/cost-center': '4711', empty: '' } } })).not.toThrow();
  });

  it.each<[Partial<FirebirdCluster['spec']>, string]>([
    [{ priorityClassName: 'Not_Valid' }, 'priorityClassName'],
    [{ schedulerName: ' ' }, 'schedulerName'],
    [{ imagePullPolicy: 'Sometimes' as never }, 'imagePullPolicy'],
    [{ imagePullSecrets: [{ name: '' }] }, 'imagePullSecrets'],
    [{ inheritedMetadata: { labels: { 'not a key': 'x' } } }, 'label key'],
    [{ inheritedMetadata: { labels: { team: 'not a value' } } }, 'label value'],
    [{ inheritedMetadata: { labels: { team: 'x'.repeat(64) } } }, 'label value'],
    [{ inheritedMetadata: { labels: { 'app.kubernetes.io/name': 'other' } } }, 'set by the operator'],
    [{ inheritedMetadata: { labels: { 'firebird.cloudnative-firebird.io/cluster': 'other' } } }, 'set by the operator'],
    [{ inheritedMetadata: { annotations: { 'firebird.cloudnative-firebird.io/reseed': 'volume' } } }, 'set by the operator'],
    [{ inheritedMetadata: { annotations: { 'bad key!': 'x' } } }, 'annotation key'],
  ])('rejects %j', (extra, message) => {
    expect(check(extra)).toThrow(ValidationError);
    expect(check(extra)).toThrow(message);
  });
});

type Overrides = Record<string, Mock>;
function makeMockKubeConfig(overrides: Overrides = {}) {
  const calls: Record<string, Mock> = {};
  const fn = (method: string): Mock => {
    if (!calls[method]) {
      calls[method] =
        overrides[method] ??
        (method.startsWith('read') || method.startsWith('get')
          ? vi.fn().mockRejectedValue(notFoundError)
          : method.startsWith('list')
            ? vi.fn().mockResolvedValue({ items: [] })
            : vi.fn().mockResolvedValue({}));
    }
    return calls[method];
  };
  const kubeConfig = new KubeConfig();
  vi.spyOn(kubeConfig, 'makeApiClient').mockReturnValue(new Proxy({}, { get: (_t, prop: string) => fn(prop) }) as never);
  return { kubeConfig, api: fn };
}

describe('cluster reconcile with inherited metadata', () => {
  it('creates the CronJobs with it', async () => {
    const { kubeConfig, api } = makeMockKubeConfig();
    await new FirebirdClusterController(kubeConfig).reconcile(configured({ backup: { enabled: true, schedule: '0 3 * * *' } }));
    const cron = api('createNamespacedCronJob').mock.calls.map((c) => c[0].body).find((b) => b.metadata.name.endsWith('-backup'));
    expect(cron?.metadata.labels).toMatchObject({ team: 'payments' });
    expect(cron?.spec.jobTemplate.spec.template.metadata.labels).toMatchObject({ team: 'payments' });
    expect(cron?.spec.jobTemplate.spec.template.spec.priorityClassName).toBe('database-critical');
  });

  it('adds it to existing volumes, Services and the PodDisruptionBudget', async () => {
    const cluster = configured({ instances: 2 });
    const service = (name: string) => ({ metadata: { name, namespace: 'default', labels: { 'app.kubernetes.io/name': 'firebird' } }, spec: { type: 'ClusterIP', selector: {} } });
    const { kubeConfig, api } = makeMockKubeConfig({
      listNamespacedPersistentVolumeClaim: vi.fn().mockResolvedValue({
        items: [
          { metadata: { name: 'firebird-data-test-cluster-0', labels: {} }, spec: { resources: { requests: { storage: '1Gi' } } }, status: { capacity: { storage: '1Gi' } } },
          {
            metadata: { name: 'firebird-data-test-cluster-1', labels: { team: 'payments' }, annotations: { 'sidecar.istio.io/inject': 'false' } },
            spec: { resources: { requests: { storage: '1Gi' } } },
            status: { capacity: { storage: '1Gi' } },
          },
        ],
      }),
      readNamespacedService: vi.fn().mockImplementation(async ({ name }) => service(name)),
      // volumes are reconciled once the StatefulSet exists
      readNamespacedStatefulSet: vi.fn().mockResolvedValue({ ...buildStatefulSet(cluster), status: { readyReplicas: 2 } }),
      readNamespacedPodDisruptionBudget: vi.fn().mockResolvedValue({ ...buildPodDisruptionBudget(makeCluster({ instances: 2 })) }),
    });
    await new FirebirdClusterController(kubeConfig).reconcile(cluster);

    const claims = api('patchNamespacedPersistentVolumeClaim').mock.calls.map((c) => c[0]).filter((c) => !Array.isArray(c.body));
    expect(claims).toEqual([
      { name: 'firebird-data-test-cluster-0', namespace: 'default', body: { metadata: { labels: { team: 'payments' }, annotations: { 'sidecar.istio.io/inject': 'false' } } } },
    ]);
    const services = api('patchNamespacedService').mock.calls.map((c) => c[0]).filter((c) => !Array.isArray(c.body));
    expect(services.map((s) => s.name).sort()).toEqual(['test-cluster', 'test-cluster-headless']);
    for (const s of services) expect(s.body.metadata.labels).toEqual({ team: 'payments' });
    expect(api('patchNamespacedPodDisruptionBudget').mock.calls[0]?.[0].body.metadata.labels).toMatchObject({ team: 'payments' });
  });
});
