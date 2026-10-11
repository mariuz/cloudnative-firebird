import { describe, it, expect, vi, type Mock } from 'vitest';
import { KubeConfig, V1Pod } from '@kubernetes/client-node';
import { FirebirdClusterController } from '../src/controllers/firebirdcluster.controller';
import {
  buildArchiveFollowerContainer,
  parseFollowState,
  replicaClusterErrors,
  replicaClusterFollowing,
  replicaPromotionToken,
} from '../src/utils/replica-cluster';
import { buildBootstrapInitContainers, buildRecoveryBootstrapJob } from '../src/utils/backup';
import { buildStatefulSet } from '../src/utils/resources';
import { validateClusterSpec } from '../src/utils/validation';
import { FirebirdCluster, ReplicaClusterStatus } from '../src/types';
import { makeCluster, notFoundError } from './helpers/factories';

const source = { bucket: 'dr', prefix: 'prod', endpoint: 'http://s3:9000', secretRef: { name: 's3' } };
const replicaCluster = (overrides: Partial<FirebirdCluster['spec']> = {}): FirebirdCluster =>
  makeCluster({
    instances: 1,
    replication: { enabled: true },
    replica: { enabled: true, journalS3: source, pollIntervalSeconds: 10 },
    bootstrap: { recovery: { sourcePath: 'nbackup-l0.nbk', s3: source } },
    ...overrides,
  });
const env = (c: { env?: Array<{ name: string; value?: string }> }) => Object.fromEntries((c.env ?? []).map((e) => [e.name, e.value]));

describe('replica clusters', () => {
  it('validates the source, the bootstrap and the single instance', () => {
    expect(replicaClusterErrors(replicaCluster())).toEqual([]);
    expect(() => validateClusterSpec(replicaCluster())).not.toThrow();
    expect(replicaClusterErrors(replicaCluster({ replication: undefined }))).toContainEqual(expect.stringMatching(/replication.enabled/));
    expect(replicaClusterErrors(replicaCluster({ bootstrap: undefined }))).toContainEqual(expect.stringMatching(/bootstrap.recovery/));
    expect(
      replicaClusterErrors(
        replicaCluster({ bootstrap: { recovery: { sourcePath: 'l0.nbk', s3: source, pointInTime: { targetSegment: 5 } } } }),
      ),
    ).toContainEqual(expect.stringMatching(/pointInTime is not allowed/));
    expect(replicaClusterErrors(replicaCluster({ instances: 3 }))).toContainEqual(expect.stringMatching(/one instance/));
    // more instances once promoted
    expect(replicaClusterErrors(replicaCluster({ instances: 3, replica: { enabled: false, journalS3: source } }))).toEqual([]);
    expect(
      replicaClusterErrors(replicaCluster({ replication: { enabled: true, journalArchiveS3: { bucket: 'dr', prefix: 'prod' } } })),
    ).toContainEqual(expect.stringMatching(/must not be the source/));
    expect(replicaClusterErrors(replicaCluster({ replica: { enabled: true, journalS3: source, pollIntervalSeconds: 1 } }))).toContainEqual(
      expect.stringMatching(/pollIntervalSeconds/),
    );
    expect(() => validateClusterSpec(replicaCluster({ instances: 2 }))).toThrow(/one instance/);
  });

  it('runs the archive follower beside the instance while it follows', () => {
    const cluster = replicaCluster();
    const follower = buildArchiveFollowerContainer(cluster);
    expect(follower.image).toBe('amazon/aws-cli:2.37.4');
    expect(follower.command?.[2]).toContain('follow.state');
    expect(env(follower)).toMatchObject({
      SOURCE_DIR: '/var/lib/firebird/data/replication/source',
      REPLICATION_DIR: '/var/lib/firebird/data/replication',
      JOURNALS: 's3://dr/prod/journals/',
      AWS: "aws --endpoint-url 'http://s3:9000'",
      POLL_SECONDS: '10',
    });
    const names = (c: FirebirdCluster) => buildStatefulSet(c).spec!.template.spec!.containers.map((x) => x.name);
    expect(names(cluster)).toContain('archive-follower');
    expect(names(replicaCluster({ replica: { enabled: false, journalS3: source } }))).not.toContain('archive-follower');
    expect(names(makeCluster({ replication: { enabled: true } }))).not.toContain('archive-follower');
    expect(replicaClusterFollowing(cluster)).toBe(true);
  });

  it('bootstraps from the source chain and archive, leaving the database a replica in place', () => {
    const cluster = replicaCluster();
    const job = buildRecoveryBootstrapJob(cluster);
    const containers = job.spec!.template.spec!.containers;
    const restore = containers.find((c) => c.name === 'firebird-restore')!;
    expect(env(restore)).toMatchObject({
      LOCAL_TARGET: '/var/lib/firebird/data/mydb.fdb',
      REPLICA_SOURCE: '/var/lib/firebird/data/replication/source',
    });
    expect(env(restore).TARGET_TIME).toBeUndefined();
    expect(env(restore).TARGET_SEGMENT).toBeUndefined();
    const fetch = containers.find((c) => c.name === 'journal-fetch')!;
    expect(fetch.args?.join(' ')).toContain('s3://dr/prod/journals/');
    // no bootstrap init containers: the Job does it, and keeps it after the promotion
    expect(buildBootstrapInitContainers(cluster)).toEqual([]);
    expect(buildBootstrapInitContainers(replicaCluster({ replica: { enabled: false, journalS3: source } }))).toEqual([]);
  });

  it("reads the follower's state from the segment server", () => {
    expect(parseFollowState('OK applied=12 archived=15')).toEqual({ applied: 12, archived: 15 });
    expect(parseFollowState('OK applied=6 archived=10 restart=8')).toEqual({ applied: 6, archived: 10, restart: 8 });
    expect(parseFollowState('OK applied=3 archived=0 error=listing failed: denied')).toEqual({
      applied: 3,
      archived: 0,
      error: 'listing failed: denied',
    });
    expect(parseFollowState('OK none')).toBeUndefined();
    expect(parseFollowState(undefined)).toBeUndefined();
    expect(replicaPromotionToken(replicaCluster())).toMatch(/^replica-cluster-/);
  });

  it("creates the segment TLS Secret before the bootstrap Job, whose proxy sidecar mounts it", async () => {
    const cluster = replicaCluster({ segmentTLS: { enabled: true } });
    cluster.status = { phase: 'Creating' };
    const order: string[] = [];
    const calls: Record<string, Mock> = {};
    const overrides: Record<string, Mock> = {
      getNamespacedCustomObject: vi.fn().mockResolvedValue({}),
      createNamespacedSecret: vi.fn().mockImplementation(async () => { order.push('secret'); return {}; }),
      createNamespacedJob: vi.fn().mockImplementation(async ({ body }: { body: { metadata: { name: string } } }) => {
        order.push(`job:${body.metadata.name}`);
        return {};
      }),
    };
    const fn = (m: string): Mock =>
      (calls[m] ??=
        overrides[m] ??
        (m.startsWith('read') || m.startsWith('get')
          ? vi.fn().mockRejectedValue(notFoundError)
          : m.startsWith('list')
            ? vi.fn().mockResolvedValue({ items: [] })
            : vi.fn().mockResolvedValue({})));
    const kubeConfig = new KubeConfig();
    vi.spyOn(kubeConfig, 'makeApiClient').mockReturnValue(new Proxy({}, { get: (_t, p: string) => fn(p) }) as never);
    await new FirebirdClusterController(kubeConfig).reconcile(cluster);
    expect(order).toEqual(['secret', 'job:test-cluster-pitr-recovery']);
    const job = fn('createNamespacedJob').mock.calls[0][0].body;
    const mounts = JSON.stringify(job.spec.template.spec.volumes);
    expect(mounts).toContain('test-cluster-segment-tls');
  });

  describe('in the controller', () => {
    const readyPod = (uid: string): V1Pod => ({
      // created from the StatefulSet's current revision: no rolling update
      metadata: { name: 'test-cluster-0', uid, labels: { 'controller-revision-hash': 'r1' } },
      status: { phase: 'Running', conditions: [{ type: 'Ready', status: 'True' }] },
    });
    function setup(options: { pod?: V1Pod; follow?: string; stored?: ReplicaClusterStatus; cluster?: FirebirdCluster }) {
      const cluster = options.cluster ?? replicaCluster();
      cluster.status = { phase: 'Running', readyInstances: 1, ...(options.stored ? { replicaCluster: options.stored } : {}) };
      const calls: Record<string, Mock> = {};
      const overrides: Record<string, Mock> = {
        getNamespacedCustomObject: vi.fn().mockResolvedValue({}),
        // the cluster exists already: no recovery bootstrap
        readNamespacedStatefulSet: vi.fn().mockResolvedValue({
          ...buildStatefulSet(cluster),
          status: { observedGeneration: 1, updateRevision: 'r1', readyReplicas: 1 },
        }),
        readNamespacedPod: options.pod ? vi.fn().mockResolvedValue(options.pod) : vi.fn().mockRejectedValue(notFoundError),
      };
      const fn = (m: string): Mock =>
        (calls[m] ??=
          overrides[m] ??
          (m.startsWith('read') || m.startsWith('get')
            ? vi.fn().mockRejectedValue(notFoundError)
            : m.startsWith('list')
              ? vi.fn().mockResolvedValue({ items: options.pod ? [options.pod] : [] })
              : vi.fn().mockResolvedValue({})));
      const kubeConfig = new KubeConfig();
      vi.spyOn(kubeConfig, 'makeApiClient').mockReturnValue(new Proxy({}, { get: (_t, p: string) => fn(p) }) as never);
      const segment = vi.fn().mockImplementation(async (_h: string, _p: number, request: string) =>
        request.endsWith(' FOLLOW') ? [options.follow ?? 'OK none'] : ['ERR unknown'],
      );
      return { cluster, controller: new FirebirdClusterController(kubeConfig, segment), fn, segment };
    }
    const lastStatus = (fn: (m: string) => Mock) => fn('patchNamespacedCustomObjectStatus').mock.calls.at(-1)?.[0].body[0].value;
    const reasons = (fn: (m: string) => Mock) => fn('createNamespacedEvent').mock.calls.map((c) => c[0].body.reason);

    it('reports how far the replica follows the source', async () => {
      const s = setup({ pod: readyPod('p1'), follow: 'OK applied=40 archived=42' });
      await s.controller.reconcile(s.cluster);
      expect(lastStatus(s.fn).replicaCluster).toEqual({ phase: 'Following', appliedSegment: 40, archivedSegment: 42 });
      expect(s.fn('deleteNamespacedPod')).not.toHaveBeenCalled();
    });

    it('restarts the instance across a failover of the source, and warns about a problem', async () => {
      const s = setup({ pod: readyPod('p1'), follow: 'OK applied=6 archived=10 restart=8' });
      await s.controller.reconcile(s.cluster);
      expect(s.fn('deleteNamespacedPod').mock.calls.map((c) => c[0].name)).toContain('test-cluster-0');
      expect(reasons(s.fn)).toContain('ReplicaClusterRepositioning');
      expect(lastStatus(s.fn).replicaCluster.message).toMatch(/continue after its archive's segment 8/);

      const e = setup({ pod: readyPod('p1'), follow: 'OK applied=8 archived=10 error=cannot follow the new lineage' });
      await e.controller.reconcile(e.cluster);
      expect(reasons(e.fn)).toContain('ReplicaClusterFollowFailed');
      expect(e.fn('deleteNamespacedPod')).not.toHaveBeenCalled();
    });

    it('promotes once replica.enabled is false: the promote directive, then a restart', async () => {
      const promoted = replicaCluster({ replica: { enabled: false, journalS3: source } });
      const s = setup({ cluster: promoted, pod: readyPod('p1'), stored: { phase: 'Following', appliedSegment: 40, archivedSegment: 40 } });
      await s.controller.reconcile(s.cluster);
      const cm = s.fn('createNamespacedConfigMap').mock.calls[0]?.[0].body ?? s.fn('patchNamespacedConfigMap').mock.calls[0]?.[0].body;
      expect(cm.data.promote).toBe(`test-cluster-0 ${replicaPromotionToken(promoted)}\n`);
      expect(s.fn('deleteNamespacedPod').mock.calls.map((c) => c[0].name)).toContain('test-cluster-0');
      expect(s.cluster.status?.replicaCluster).toMatchObject({ phase: 'Promoting', promotingPodUid: 'p1' });
      expect(reasons(s.fn)).toContain('ReplicaClusterPromoting');
      // not the follower anymore
      expect(s.segment.mock.calls.some((c) => String(c[2]).endsWith(' FOLLOW'))).toBe(false);

      // the new pod is ready: promoted
      const done = setup({ cluster: promoted, pod: readyPod('p2'), stored: s.cluster.status!.replicaCluster! });
      await done.controller.reconcile(done.cluster);
      expect(lastStatus(done.fn).replicaCluster).toMatchObject({ phase: 'Promoted' });
      expect(lastStatus(done.fn).replicaCluster.promotingPodUid).toBeUndefined();
      expect(reasons(done.fn)).toContain('ReplicaClusterPromoted');
      expect(done.fn('deleteNamespacedPod')).not.toHaveBeenCalled();
    });
  });
});
