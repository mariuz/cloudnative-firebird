import { describe, it, expect, vi, afterEach, type Mock } from 'vitest';
import { KubeConfig, V1Job, V1Pod } from '@kubernetes/client-node';
import { FirebirdClusterController } from '../src/controllers/firebirdcluster.controller';
import {
  buildImageCheckJob,
  buildMajorUpgradeJob,
  IMAGE_CHECK_CONTAINERS,
  imageChangeKind,
  imageCheckJobName,
  imageCheckResults,
  instancesStartedOn,
  majorUpgradeJobName,
  parseImageCheck,
  withEffectiveImage,
} from '../src/utils/major-upgrade';
import { buildStatefulSet } from '../src/utils/resources';
import { FirebirdCluster, MajorUpgradeStatus } from '../src/types';
import { makeCluster, notFoundError } from './helpers/factories';

const FB5 = 'firebirdsql/firebird:5';
const FB6 = 'firebirdsql/firebird:6';

const replicated = (overrides: Partial<FirebirdCluster['spec']> = {}): FirebirdCluster => {
  const cluster = makeCluster({ instances: 2, imageName: FB6, replication: { enabled: true }, ...overrides });
  cluster.status = { phase: 'Running', readyInstances: 2 };
  return cluster;
};
const upgrade = (phase: MajorUpgradeStatus['phase'], extra: Partial<MajorUpgradeStatus> = {}): MajorUpgradeStatus => ({
  from: FB5,
  to: FB6,
  fromOds: '13.1',
  toOds: '14.0',
  primary: 'test-cluster-0',
  phase,
  ...extra,
});
const checkPod = (from?: string, to?: string): V1Pod => ({
  metadata: { name: 'check' },
  status: {
    containerStatuses: [
      ...(from ? [{ name: IMAGE_CHECK_CONTAINERS.from, state: { terminated: { exitCode: 0, message: from } } }] : []),
      ...(to ? [{ name: IMAGE_CHECK_CONTAINERS.to, state: { terminated: { exitCode: 0, message: to } } }] : []),
    ] as never,
  },
});
const instance = (name: string, image: string, ready = true): V1Pod => ({
  metadata: { name },
  spec: { containers: [{ name: 'firebird', image }] },
  status: { phase: 'Running', conditions: [{ type: 'Ready', status: ready ? 'True' : 'False' }] },
});
const job = (outcome: 'Complete' | 'Failed' | 'Running'): V1Job => ({
  metadata: { name: 'j' },
  status: { conditions: outcome === 'Running' ? [] : [{ type: outcome, status: 'True' }] as never },
});

describe('major version upgrades', () => {
  it('reads the image check and compares on-disk structures', () => {
    expect(parseImageCheck('ODS 13.1 VERSION 5.0.4.1812')).toEqual({ ods: '13.1', version: '5.0.4.1812' });
    expect(parseImageCheck('garbage')).toBeUndefined();
    expect(parseImageCheck(undefined)).toBeUndefined();
    const v = (ods: string) => ({ ods, version: 'x' });
    // Firebird 4 to 5 opens ODS 13.0 as it is
    expect(imageChangeKind(v('13.0'), v('13.1'))).toBe('Compatible');
    expect(imageChangeKind(v('13.1'), v('13.0'))).toBe('Compatible');
    expect(imageChangeKind(v('13.1'), v('14.0'))).toBe('Upgrade');
    expect(imageChangeKind(v('14.0'), v('13.1'))).toBe('Refused');
    expect(imageCheckResults([checkPod('ODS 13.1 VERSION 5.0.4', 'ODS 14.0 VERSION 6.0.0')])).toEqual({
      from: { ods: '13.1', version: '5.0.4' },
      to: { ods: '14.0', version: '6.0.0' },
    });
  });

  it('checks both images in one Job, named after the pair', () => {
    const cluster = replicated();
    const built = buildImageCheckJob(cluster, FB5, FB6);
    expect(built.metadata?.name).toBe(imageCheckJobName(cluster, FB5, FB6));
    expect(imageCheckJobName(cluster, FB5, FB6)).not.toBe(imageCheckJobName(cluster, FB5, 'other'));
    const containers = built.spec!.template.spec!.containers;
    expect(containers.map((c) => [c.name, c.image])).toEqual([
      [IMAGE_CHECK_CONTAINERS.from, FB5],
      [IMAGE_CHECK_CONTAINERS.to, FB6],
    ]);
    expect(containers[0].command?.[2]).toContain('ODS version');
    expect(built.spec!.template.spec!.securityContext?.runAsNonRoot).toBe(true);
  });

  it('converts each volume with the old image, then the new one, by role', () => {
    const cluster = replicated({ tolerations: [{ key: 'db', operator: 'Exists' }] });
    const primary = buildMajorUpgradeJob(cluster, upgrade('Converting'), 0, 'firebird-data-test-cluster-0');
    const spec = primary.spec!.template.spec!;
    expect(primary.metadata?.name).toBe(majorUpgradeJobName(cluster, 0));
    expect(spec.initContainers?.[0].image).toBe(FB5);
    expect(spec.containers[0].image).toBe(FB6);
    const env = (c: { env?: Array<{ name: string; value?: string }> }) => Object.fromEntries((c.env ?? []).map((e) => [e.name, e.value]));
    expect(env(spec.initContainers![0])).toMatchObject({ MODE: 'backup', ROLE: 'primary', TARGET_ODS: '14' });
    expect(env(spec.containers[0])).toMatchObject({ MODE: 'restore', ROLE: 'primary', REPLICATION_DIR: '/var/lib/firebird/data/replication' });
    expect(spec.volumes?.[0]).toEqual({ name: 'firebird-data', persistentVolumeClaim: { claimName: 'firebird-data-test-cluster-0' } });
    expect(spec.tolerations).toEqual([{ key: 'db', operator: 'Exists' }]);
    const replica = buildMajorUpgradeJob(cluster, upgrade('Converting'), 1, 'firebird-data-test-cluster-1');
    expect(env(replica.spec!.template.spec!.containers[0]).ROLE).toBe('replica');
    const standalone = buildMajorUpgradeJob(replicated({ replication: undefined }), upgrade('Converting'), 1, 'c');
    expect(env(standalone.spec!.template.spec!.containers[0])).toMatchObject({ ROLE: 'standalone' });
    expect(env(standalone.spec!.template.spec!.containers[0]).REPLICATION_DIR).toBeUndefined();
  });

  it('holds the image or stops the cluster in the reconcile copy only', () => {
    const cluster = replicated();
    const held = withEffectiveImage(cluster, FB5, true);
    expect(held.spec.imageName).toBe(FB5);
    expect(held.spec.hibernated).toBe(true);
    expect(cluster.spec.imageName).toBe(FB6);
    expect(cluster.spec.hibernated).toBeUndefined();
    expect(held.status).toBe(cluster.status);
    expect(buildStatefulSet(held).spec?.replicas).toBe(0);
  });

  it('knows when every instance started on the new image', () => {
    const cluster = replicated();
    expect(instancesStartedOn(cluster, [instance('test-cluster-0', FB6)], FB6)).toBe(false);
    expect(instancesStartedOn(cluster, [instance('test-cluster-0', FB6), instance('test-cluster-1', FB5)], FB6)).toBe(false);
    expect(instancesStartedOn(cluster, [instance('test-cluster-0', FB6), instance('test-cluster-1', FB6, false)], FB6)).toBe(false);
    expect(instancesStartedOn(cluster, [instance('test-cluster-0', FB6), instance('test-cluster-1', FB6, false)], FB6, ['test-cluster-1'])).toBe(true);
    expect(instancesStartedOn(cluster, [instance('test-cluster-0', FB6), instance('test-cluster-1', FB6)], FB6)).toBe(true);
  });

  describe('in the controller', () => {
    afterEach(() => vi.unstubAllEnvs());

    function mockApi(overrides: Record<string, Mock> = {}) {
      const calls: Record<string, Mock> = {};
      const fn = (method: string): Mock =>
        (calls[method] ??=
          overrides[method] ??
          (method.startsWith('read') || method.startsWith('get')
            ? vi.fn().mockRejectedValue(notFoundError)
            : method.startsWith('list')
              ? vi.fn().mockResolvedValue({ items: [] })
              : vi.fn().mockResolvedValue({})));
      const kubeConfig = new KubeConfig();
      vi.spyOn(kubeConfig, 'makeApiClient').mockReturnValue(new Proxy({}, { get: (_t, p: string) => fn(p) }) as never);
      return { kubeConfig, api: fn };
    }
    /** The StatefulSet as it runs `image` */
    const running = (cluster: FirebirdCluster, image: string) => ({
      ...buildStatefulSet({ ...cluster, spec: { ...cluster.spec, imageName: image } }),
      status: { observedGeneration: 1, updateRevision: 'r1', readyReplicas: 2 },
    });
    const statusPatches = (api: (m: string) => Mock) =>
      api('patchNamespacedCustomObjectStatus').mock.calls.map((c) => c[0].body[0].value);
    const reasons = (api: (m: string) => Mock) => api('createNamespacedEvent').mock.calls.map((c) => c[0].body.reason);
    const patchedImage = (api: (m: string) => Mock) =>
      api('patchNamespacedStatefulSet').mock.calls.map(
        (c) => c[0].body.spec?.template?.spec?.containers?.find((x: { name: string }) => x.name === 'firebird')?.image,
      );

    it('checks a new image before the instances run it', async () => {
      const cluster = replicated();
      const { kubeConfig, api } = mockApi({
        getNamespacedCustomObject: vi.fn().mockResolvedValue({}),
        readNamespacedStatefulSet: vi.fn().mockResolvedValue(running(cluster, FB5)),
      });
      await new FirebirdClusterController(kubeConfig).reconcile(cluster);
      const created = api('createNamespacedJob').mock.calls.map((c) => c[0].body.metadata.name);
      expect(created).toContain(imageCheckJobName(cluster, FB5, FB6));
      expect(cluster.status?.imageCheck).toEqual({ from: FB5, to: FB6, phase: 'Checking' });
      expect(reasons(api)).toContain('ImageCheckStarted');
      // the instances keep their image meanwhile
      expect(patchedImage(api).every((image) => image === undefined || image === FB5)).toBe(true);
    });

    it('rolls out an image with the same on-disk structure as usual', async () => {
      const cluster = replicated({ imageName: 'firebirdsql/firebird:5.0.4' });
      cluster.status!.imageCheck = { from: FB5, to: 'firebirdsql/firebird:5.0.4', phase: 'Checking' };
      const { kubeConfig, api } = mockApi({
        getNamespacedCustomObject: vi.fn().mockResolvedValue({}),
        readNamespacedStatefulSet: vi.fn().mockResolvedValue(running(cluster, FB5)),
        readNamespacedJob: vi.fn().mockResolvedValue(job('Complete')),
        listNamespacedPod: vi.fn().mockResolvedValue({ items: [checkPod('ODS 13.1 VERSION 5.0.3', 'ODS 13.1 VERSION 5.0.4')] }),
      });
      await new FirebirdClusterController(kubeConfig).reconcile(cluster);
      expect(cluster.status?.imageCheck?.phase).toBe('Compatible');
      expect(reasons(api)).toContain('ImageChecked');
      expect(patchedImage(api)).toContain('firebirdsql/firebird:5.0.4');
      expect(api('deleteNamespacedJob')).toHaveBeenCalled();
    });

    it('refuses an older on-disk structure and keeps the image', async () => {
      const cluster = replicated({ imageName: FB5 });
      cluster.status!.imageCheck = { from: FB6, to: FB5, phase: 'Checking' };
      const { kubeConfig, api } = mockApi({
        getNamespacedCustomObject: vi.fn().mockResolvedValue({}),
        readNamespacedStatefulSet: vi.fn().mockResolvedValue(running(cluster, FB6)),
        readNamespacedJob: vi.fn().mockResolvedValue(job('Complete')),
        listNamespacedPod: vi.fn().mockResolvedValue({ items: [checkPod('ODS 14.0 VERSION 6.0.0', 'ODS 13.1 VERSION 5.0.4')] }),
      });
      await new FirebirdClusterController(kubeConfig).reconcile(cluster);
      expect(cluster.status?.imageCheck?.phase).toBe('Refused');
      expect(reasons(api)).toContain('ImageRefused');
      expect(patchedImage(api)).not.toContain(FB5);
      const last = statusPatches(api).at(-1);
      expect(last.conditions.find((c: { type: string }) => c.type === 'ImageChange')).toMatchObject({ reason: 'ImageRefused' });
    });

    it('keeps a failed check until its Job is deleted, then checks again', async () => {
      const cluster = replicated();
      cluster.status!.imageCheck = { from: FB5, to: FB6, phase: 'Checking' };
      const readJob = vi.fn().mockResolvedValue(job('Failed'));
      const { kubeConfig, api } = mockApi({
        getNamespacedCustomObject: vi.fn().mockResolvedValue({}),
        readNamespacedStatefulSet: vi.fn().mockResolvedValue(running(cluster, FB5)),
        readNamespacedJob: readJob,
        listNamespacedPod: vi.fn().mockResolvedValue({ items: [checkPod('ODS 13.1 VERSION 5.0.4')] }),
      });
      const controller = new FirebirdClusterController(kubeConfig);
      await controller.reconcile(cluster);
      expect(cluster.status?.imageCheck?.phase).toBe('Failed');
      expect(cluster.status?.imageCheck?.message).toMatch(/no result from firebirdsql\/firebird:6/);
      expect(api('deleteNamespacedJob').mock.calls.map((c) => c[0].name)).not.toContain(imageCheckJobName(cluster, FB5, FB6));
      expect(patchedImage(api)).not.toContain(FB6);
      readJob.mockRejectedValue(notFoundError);
      await controller.reconcile(cluster);
      expect(cluster.status?.imageCheck?.phase).toBe('Checking');
      expect(api('createNamespacedJob').mock.calls.map((c) => c[0].body.metadata.name)).toContain(imageCheckJobName(cluster, FB5, FB6));
    });

    it('starts a major upgrade for a newer on-disk structure: every instance stops', async () => {
      const cluster = replicated();
      cluster.status!.imageCheck = { from: FB5, to: FB6, phase: 'Checking' };
      const { kubeConfig, api } = mockApi({
        getNamespacedCustomObject: vi.fn().mockResolvedValue({}),
        readNamespacedStatefulSet: vi.fn().mockResolvedValue(running(cluster, FB5)),
        readNamespacedJob: vi.fn().mockImplementation(({ name }: { name: string }) =>
          name.includes('image-check') ? Promise.resolve(job('Complete')) : Promise.reject(notFoundError),
        ),
        listNamespacedPod: vi
          .fn()
          .mockImplementation(({ labelSelector }: { labelSelector: string }) =>
            Promise.resolve({
              items: labelSelector.startsWith('job-name')
                ? [checkPod('ODS 13.1 VERSION 5.0.4', 'ODS 14.0 VERSION 6.0.0')]
                : [instance('test-cluster-0', FB5), instance('test-cluster-1', FB5)],
            }),
          ),
      });
      await new FirebirdClusterController(kubeConfig).reconcile(cluster);
      expect(cluster.status?.majorUpgrade).toMatchObject({ from: FB5, to: FB6, fromOds: '13.1', toOds: '14.0', phase: 'Stopping' });
      expect(cluster.status?.imageCheck).toBeUndefined();
      expect(reasons(api)).toContain('MajorUpgradeStarted');
      const sts = api('patchNamespacedStatefulSet').mock.calls.at(-1)?.[0].body;
      expect(sts.spec.replicas).toBe(0);
      expect(statusPatches(api).at(-1)).toMatchObject({ phase: 'Upgrading' });
    });

    it('converts every volume once the instances stopped, then starts them on the new image', async () => {
      const cluster = replicated();
      cluster.status!.majorUpgrade = upgrade('Stopping');
      const jobs = new Map<string, V1Job>();
      const { kubeConfig, api } = mockApi({
        getNamespacedCustomObject: vi.fn().mockResolvedValue({}),
        readNamespacedStatefulSet: vi.fn().mockResolvedValue(running(cluster, FB5)),
        readNamespacedPersistentVolumeClaim: vi.fn().mockResolvedValue({}),
        readNamespacedJob: vi.fn().mockImplementation(({ name }: { name: string }) =>
          jobs.has(name) ? Promise.resolve(jobs.get(name)) : Promise.reject(notFoundError),
        ),
      });
      const controller = new FirebirdClusterController(kubeConfig);
      await controller.reconcile(cluster);
      expect(cluster.status?.majorUpgrade?.phase).toBe('Converting');
      const created = api('createNamespacedJob').mock.calls.map((c) => c[0].body);
      expect(created.map((j) => j.metadata.name)).toEqual([majorUpgradeJobName(cluster, 0), majorUpgradeJobName(cluster, 1)]);
      expect(created.map((j) => j.metadata.annotations['firebird.cloudnative-firebird.io/role'])).toEqual(['primary', 'replica']);
      expect(reasons(api)).toContain('MajorUpgradeConverting');

      // one done, one still running: still stopped
      jobs.set(majorUpgradeJobName(cluster, 0), job('Complete'));
      jobs.set(majorUpgradeJobName(cluster, 1), job('Running'));
      await controller.reconcile(cluster);
      expect(cluster.status?.majorUpgrade).toMatchObject({ phase: 'Converting', converted: ['test-cluster-0'] });

      // both done: the instances start on the new image
      jobs.set(majorUpgradeJobName(cluster, 1), job('Complete'));
      api('patchNamespacedStatefulSet').mockClear();
      await controller.reconcile(cluster);
      expect(cluster.status?.majorUpgrade?.phase).toBe('Starting');
      expect(statusPatches(api).at(-1).majorUpgrade.phase).toBe('Starting');
      expect(reasons(api)).toContain('MajorUpgradeStarting');
      const sts = api('patchNamespacedStatefulSet').mock.calls.at(-1)?.[0].body;
      expect(sts.spec.replicas).toBe(2);
      expect(patchedImage(api).at(-1)).toBe(FB6);
      expect(api('deleteNamespacedJob').mock.calls.map((c) => c[0].name)).toEqual([
        majorUpgradeJobName(cluster, 0),
        majorUpgradeJobName(cluster, 1),
      ]);
    });

    it('completes once every instance runs the new image', async () => {
      const cluster = replicated();
      cluster.status!.majorUpgrade = upgrade('Starting');
      const { kubeConfig, api } = mockApi({
        getNamespacedCustomObject: vi.fn().mockResolvedValue({}),
        readNamespacedStatefulSet: vi.fn().mockResolvedValue(running(cluster, FB6)),
        listNamespacedPod: vi.fn().mockResolvedValue({ items: [instance('test-cluster-0', FB6), instance('test-cluster-1', FB6)] }),
      });
      await new FirebirdClusterController(kubeConfig).reconcile(cluster);
      expect(cluster.status?.majorUpgrade?.phase).toBe('Completed');
      expect(reasons(api)).toContain('MajorUpgradeCompleted');
      // the reconcile's last status write keeps it completed
      expect(statusPatches(api).at(-1).phase).not.toBe('Upgrading');
      expect(statusPatches(api).at(-1).majorUpgrade.phase).toBe('Completed');
    });

    it('stays stopped when a conversion fails, and is abandoned when the image is set back', async () => {
      const cluster = replicated();
      cluster.status!.majorUpgrade = upgrade('Converting');
      const { kubeConfig, api } = mockApi({
        getNamespacedCustomObject: vi.fn().mockResolvedValue({}),
        readNamespacedStatefulSet: vi.fn().mockResolvedValue(running(cluster, FB5)),
        readNamespacedJob: vi.fn().mockResolvedValue(job('Failed')),
      });
      const controller = new FirebirdClusterController(kubeConfig);
      await controller.reconcile(cluster);
      expect(cluster.status?.majorUpgrade?.phase).toBe('Failed');
      expect(cluster.status?.majorUpgrade?.message).toMatch(/delete the Job to retry/);
      expect(reasons(api)).toContain('MajorUpgradeFailed');
      expect(api('patchNamespacedStatefulSet').mock.calls.at(-1)?.[0].body.spec.replicas).toBe(0);

      cluster.spec.imageName = FB5;
      await controller.reconcile(cluster);
      expect(cluster.status?.majorUpgrade).toBeUndefined();
      expect(reasons(api)).toContain('MajorUpgradeAbandoned');
    });
  });
});
