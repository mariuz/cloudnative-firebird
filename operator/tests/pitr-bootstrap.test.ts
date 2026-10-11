import { describe, it, expect, vi, type Mock } from 'vitest';
import { KubeConfig } from '@kubernetes/client-node';
import { FirebirdClusterController } from '../src/controllers/firebirdcluster.controller';
import { FirebirdCluster } from '../src/types';
import { buildBootstrapInitContainers, buildRecoveryBootstrapJob, bootstrapVolumes } from '../src/utils/backup';
import { validateClusterSpec } from '../src/utils/validation';
import { makeCluster, notFoundError } from './helpers/factories';

const s3 = { bucket: 'backups', prefix: 'prod', secretRef: { name: 'bk' } };
const journals = { bucket: 'backups', prefix: 'prod', secretRef: { name: 'jr' } };
const pitrCluster = (spec: Partial<FirebirdCluster['spec']> = {}) =>
  makeCluster({
    instances: 2,
    replication: { enabled: true },
    bootstrap: {
      recovery: {
        sourcePath: 'nbackup-l0-a.nbk',
        incrementalPaths: ['nbackup-l1-b.nbk'],
        s3,
        pointInTime: { targetTime: '2026-10-04T10:15:00Z', journalS3: journals },
      },
    },
    ...spec,
  });

/** API clients whose calls default to: read → 404, list → empty, others → {} */
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

describe('point-in-time bootstrap', () => {
  it('recovers the first volume with a Job, in place of the bootstrap init containers', () => {
    const cluster = pitrCluster();
    expect(buildBootstrapInitContainers(cluster)).toEqual([]);
    expect(bootstrapVolumes(cluster)).toEqual([]);
    const job = buildRecoveryBootstrapJob(cluster);
    expect(job.metadata?.name).toBe('test-cluster-pitr-recovery');
    expect(job.metadata?.ownerReferences?.[0]).toMatchObject({ kind: 'FirebirdCluster', name: 'test-cluster' });
    const pod = job.spec!.template.spec!;
    expect(pod.volumes).toContainEqual({ name: 'firebird-data', persistentVolumeClaim: { claimName: 'firebird-data-test-cluster-0' } });
    expect(pod.securityContext).toMatchObject({ runAsUser: 84, runAsNonRoot: true, fsGroup: 84 });
    expect(pod.initContainers![0].args![0]).toContain(`s3 cp 's3://backups/prod/nbackup-l1-b.nbk' /work/chain/1.nbk`);
    const [firebird, fetch] = pod.containers;
    expect(firebird.args![0]).toBe('set -eu; . /etc/firebird-operator/pitr-restore.sh');
    const env = Object.fromEntries((firebird.env ?? []).map((e) => [e.name, e.value]));
    // with replication the replication init container takes the recovered database from here
    expect(env).toMatchObject({ LOCAL_TARGET: '/var/lib/firebird/data/.bootstrap.fdb', CHAIN_COUNT: '2', TARGET_TIME: '20261004T101500Z' });
    expect(firebird.volumeMounts).toContainEqual({ name: 'firebird-data', mountPath: '/var/lib/firebird/data' });
    expect(fetch.env).toContainEqual(expect.objectContaining({ valueFrom: { secretKeyRef: { name: 'jr', key: 'AWS_ACCESS_KEY_ID' } } }));
    // without replication straight into the database file
    const single = buildRecoveryBootstrapJob(pitrCluster({ instances: 1, replication: undefined }));
    expect(single.spec!.template.spec!.containers[0].env).toContainEqual({ name: 'LOCAL_TARGET', value: '/var/lib/firebird/data/mydb.fdb' });
  });

  it('validates the source, the journal archive and the instances', () => {
    expect(() => validateClusterSpec(pitrCluster())).not.toThrow();
    const recovery = pitrCluster().spec.bootstrap!.recovery!;
    expect(() => validateClusterSpec(pitrCluster({ bootstrap: { recovery: { ...recovery, pointInTime: {} } } }))).toThrow('journalS3 is required');
    expect(() => validateClusterSpec(pitrCluster({ bootstrap: { recovery: { ...recovery, s3: undefined } } }))).toThrow('needs s3 and sourcePath');
    expect(() => validateClusterSpec(pitrCluster({ replication: undefined }))).toThrow('needs replication');
    expect(() =>
      validateClusterSpec(pitrCluster({ replication: { enabled: true, journalArchiveS3: { bucket: 'backups', prefix: '/prod/' } } })),
    ).toThrow('own replication.journalArchiveS3');
    expect(() =>
      validateClusterSpec(pitrCluster({ replication: { enabled: true, journalArchiveS3: { bucket: 'backups', prefix: 'recovered' } } })),
    ).not.toThrow();
    expect(() =>
      validateClusterSpec(pitrCluster({ bootstrap: { recovery: { ...recovery, pointInTime: { journalS3: journals, targetSegment: 0 } } } })),
    ).toThrow('targetSegment');
    expect(() =>
      validateClusterSpec(makeCluster({ bootstrap: { recovery: { sourcePath: 'b.fbk', incrementalPaths: ['l1.nbk'] } } })),
    ).toThrow('point-in-time recovery only');
  });

  it('creates the volume and the Job, and no StatefulSet until the Job completed', async () => {
    const cluster = pitrCluster();
    const { kubeConfig, api } = mockApi();
    const controller = new FirebirdClusterController(kubeConfig);
    await controller.reconcile(cluster);
    const claim = api('createNamespacedPersistentVolumeClaim').mock.calls[0][0].body;
    expect(claim.metadata.name).toBe('firebird-data-test-cluster-0');
    expect(claim.metadata.labels).toMatchObject({ 'firebird.cloudnative-firebird.io/cluster': 'test-cluster' });
    expect(claim.spec).toMatchObject({ accessModes: ['ReadWriteOnce'], resources: { requests: { storage: '1Gi' } } });
    expect(api('createNamespacedJob').mock.calls[0][0].body.metadata.name).toBe('test-cluster-pitr-recovery');
    expect(api('createNamespacedStatefulSet')).not.toHaveBeenCalled();
    // the ConfigMap carrying the scripts exists before the Job
    expect(api('createNamespacedConfigMap')).toHaveBeenCalled();

    // running: waits
    api('readNamespacedPersistentVolumeClaim').mockResolvedValue({});
    api('readNamespacedJob').mockResolvedValue({ status: { active: 1 } });
    await controller.reconcile(cluster);
    expect(api('createNamespacedJob')).toHaveBeenCalledTimes(1);
    expect(api('createNamespacedPersistentVolumeClaim')).toHaveBeenCalledTimes(1);
    expect(api('createNamespacedStatefulSet')).not.toHaveBeenCalled();

    // failed: the cluster is Degraded until the Job is deleted
    api('readNamespacedJob').mockResolvedValue({ status: { conditions: [{ type: 'Failed', status: 'True' }] } });
    await expect(controller.reconcile(cluster)).rejects.toThrow('delete the Job to retry');

    // completed: the StatefulSet is created and adopts the volume
    api('readNamespacedJob').mockResolvedValue({ status: { conditions: [{ type: 'Complete', status: 'True' }] } });
    await controller.reconcile(cluster);
    expect(api('createNamespacedStatefulSet')).toHaveBeenCalledTimes(1);
  });

  it('leaves a cluster whose StatefulSet exists alone', async () => {
    const { kubeConfig, api } = mockApi({
      readNamespacedStatefulSet: vi.fn().mockResolvedValue({ metadata: { name: 'test-cluster' }, spec: { podManagementPolicy: 'Parallel' }, status: {} }),
    });
    await new FirebirdClusterController(kubeConfig).reconcile(pitrCluster());
    expect(api('createNamespacedJob')).not.toHaveBeenCalledWith(
      expect.objectContaining({ body: expect.objectContaining({ metadata: expect.objectContaining({ name: 'test-cluster-pitr-recovery' }) }) }),
    );
    expect(api('createNamespacedPersistentVolumeClaim')).not.toHaveBeenCalled();
  });
});

describe('bootstrap from a volume snapshot', () => {
  const snapshotCluster = () =>
    makeCluster({ instances: 2, replication: { enabled: true }, bootstrap: { recovery: { volumeSnapshot: { name: 'nightly' } } } });

  it('waits for the snapshot, restores the first volume from it, and starts the instances after the Job', async () => {
    const snapshot = { metadata: { name: 'nightly' }, spec: { source: {} }, status: { readyToUse: false } };
    const { kubeConfig, api } = mockApi({
      getNamespacedCustomObject: vi.fn().mockImplementation(({ plural }) =>
        plural === 'volumesnapshots' ? Promise.resolve(snapshot) : Promise.reject(notFoundError),
      ),
    });
    const controller = new FirebirdClusterController(kubeConfig);
    await controller.reconcile(snapshotCluster());
    expect(api('createNamespacedPersistentVolumeClaim')).not.toHaveBeenCalled();
    const waiting = api('patchNamespacedCustomObjectStatus').mock.calls.at(-1)?.[0].body;
    expect(JSON.stringify(waiting)).toContain('Waiting for VolumeSnapshot nightly to be ready to use');

    // larger than the volume asked for
    Object.assign(snapshot.status, { readyToUse: true, restoreSize: '2Gi' });
    await controller.reconcile(snapshotCluster());
    expect(api('createNamespacedPersistentVolumeClaim')).not.toHaveBeenCalled();
    expect(JSON.stringify(api('patchNamespacedCustomObjectStatus').mock.calls.at(-1)?.[0].body)).toContain('smaller than the restore size 2Gi');

    Object.assign(snapshot.status, { restoreSize: '1Gi' });
    await controller.reconcile(snapshotCluster());
    const claim = api('createNamespacedPersistentVolumeClaim').mock.calls[0][0].body;
    expect(claim.metadata.name).toBe('firebird-data-test-cluster-0');
    expect(claim.spec.dataSource).toEqual({ apiGroup: 'snapshot.storage.k8s.io', kind: 'VolumeSnapshot', name: 'nightly' });
    expect(claim.spec.resources).toEqual({ requests: { storage: '1Gi' } });
    expect(api('createNamespacedJob').mock.calls[0][0].body.metadata.name).toBe('test-cluster-snapshot-recovery');
    expect(api('createNamespacedStatefulSet')).not.toHaveBeenCalled();

    api('readNamespacedPersistentVolumeClaim').mockResolvedValue({});
    api('readNamespacedJob').mockResolvedValue({ status: { conditions: [{ type: 'Complete', status: 'True' }] } });
    await controller.reconcile(snapshotCluster());
    expect(api('createNamespacedStatefulSet')).toHaveBeenCalledTimes(1);
    const sts = api('createNamespacedStatefulSet').mock.calls[0][0].body;
    expect(sts.spec.template.spec.initContainers.map((c: { name: string }) => c.name)).not.toContain('bootstrap-restore');
  });
});
