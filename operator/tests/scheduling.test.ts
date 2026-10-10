import { describe, it, expect, vi, type Mock } from 'vitest';
import { KubeConfig, V1Pod } from '@kubernetes/client-node';
import { FirebirdClusterController } from '../src/controllers/firebirdcluster.controller';
import {
  instanceAffinity,
  instanceLabelSelector,
  instanceTopologySpreadConstraints,
  POD_ANTI_AFFINITY_ANNOTATION,
  podAntiAffinityEnabled,
} from '../src/utils/scheduling';
import { buildStatefulSet, canonicalJson, statefulSetNeedsUpdate, statefulSetPatchBody, withRemovals } from '../src/utils/resources';
import { planRollingUpdate, REVISION_LABEL, staleUnschedulablePod } from '../src/utils/rolling-update';
import { validateClusterSpec } from '../src/utils/validation';
import { FirebirdCluster } from '../src/types';
import { makeCluster, notFoundError } from './helpers/factories';

const recorded = (value: string | undefined, spec: Partial<FirebirdCluster['spec']> = {}): FirebirdCluster => {
  const cluster = makeCluster({ instances: 3, ...spec });
  cluster.metadata.annotations = value ? { [POD_ANTI_AFFINITY_ANNOTATION]: value } : {};
  return cluster;
};
const term = (cluster: FirebirdCluster, topologyKey = 'kubernetes.io/hostname') => ({
  labelSelector: instanceLabelSelector(cluster),
  topologyKey,
});

describe('spreading the instances', () => {
  it('repels the instances of new clusters, not of pinned ones, unless the owner decides', () => {
    expect(podAntiAffinityEnabled(recorded('enabled'))).toBe(true);
    expect(podAntiAffinityEnabled(recorded('pinned'))).toBe(false);
    expect(podAntiAffinityEnabled(recorded(undefined))).toBe(false);
    expect(podAntiAffinityEnabled(recorded('pinned', { podAntiAffinity: { enabled: true } }))).toBe(true);
    expect(podAntiAffinityEnabled(recorded('enabled', { podAntiAffinity: { enabled: false } }))).toBe(false);
  });

  it('prefers one instance per node, or requires it, on the topology key', () => {
    const cluster = recorded('enabled');
    expect(instanceLabelSelector(cluster)).toEqual({
      matchLabels: { 'firebird.cloudnative-firebird.io/cluster': 'test-cluster', 'app.kubernetes.io/component': 'database' },
    });
    expect(instanceAffinity(cluster)).toEqual({
      podAntiAffinity: { preferredDuringSchedulingIgnoredDuringExecution: [{ weight: 100, podAffinityTerm: term(cluster) }] },
    });
    const required = recorded('enabled', { podAntiAffinity: { type: 'required', topologyKey: 'topology.kubernetes.io/zone' } });
    expect(instanceAffinity(required)).toEqual({
      podAntiAffinity: { requiredDuringSchedulingIgnoredDuringExecution: [term(required, 'topology.kubernetes.io/zone')] },
    });
    expect(instanceAffinity(recorded('pinned'))).toBeUndefined();
  });

  it("keeps the owner's affinity rules", () => {
    const own = {
      nodeAffinity: { requiredDuringSchedulingIgnoredDuringExecution: { nodeSelectorTerms: [{ matchExpressions: [{ key: 'db', operator: 'Exists' }] }] } },
      podAntiAffinity: { preferredDuringSchedulingIgnoredDuringExecution: [{ weight: 10, podAffinityTerm: { topologyKey: 'rack' } }] },
    };
    const cluster = recorded('enabled', { affinity: own });
    const affinity = instanceAffinity(cluster)!;
    expect(affinity.nodeAffinity).toEqual(own.nodeAffinity);
    expect(affinity.podAntiAffinity?.preferredDuringSchedulingIgnoredDuringExecution).toEqual([
      { weight: 10, podAffinityTerm: { topologyKey: 'rack' } },
      { weight: 100, podAffinityTerm: term(cluster) },
    ]);
    // pinned: exactly the owner's
    expect(instanceAffinity(recorded('pinned', { affinity: own }))).toEqual(own);
  });

  it('passes topology spread constraints, with the instance selector when they have none', () => {
    const own = { matchLabels: { app: 'other' } };
    const cluster = recorded('pinned', {
      topologySpreadConstraints: [
        { maxSkew: 1, topologyKey: 'topology.kubernetes.io/zone', whenUnsatisfiable: 'ScheduleAnyway' },
        { maxSkew: 2, topologyKey: 'kubernetes.io/hostname', whenUnsatisfiable: 'DoNotSchedule', labelSelector: own },
      ],
    });
    expect(instanceTopologySpreadConstraints(cluster)).toEqual([
      { maxSkew: 1, topologyKey: 'topology.kubernetes.io/zone', whenUnsatisfiable: 'ScheduleAnyway', labelSelector: instanceLabelSelector(cluster) },
      { maxSkew: 2, topologyKey: 'kubernetes.io/hostname', whenUnsatisfiable: 'DoNotSchedule', labelSelector: own },
    ]);
    expect(instanceTopologySpreadConstraints(recorded('pinned', { topologySpreadConstraints: [] }))).toBeUndefined();
    const podSpec = buildStatefulSet(cluster).spec!.template.spec!;
    expect(podSpec.topologySpreadConstraints).toHaveLength(2);
    expect(podSpec.affinity).toBeUndefined();
    expect(buildStatefulSet(recorded('enabled')).spec!.template.spec!.affinity?.podAntiAffinity).toBeDefined();
  });

  it('updates the StatefulSet for a scheduling change, not for the order of keys the API server returns', () => {
    const cluster = recorded('enabled', { topologySpreadConstraints: [{ maxSkew: 1, topologyKey: 'zone', whenUnsatisfiable: 'ScheduleAnyway' }] });
    const desired = buildStatefulSet(cluster);
    const reordered = JSON.parse(JSON.stringify(desired));
    const podSpec = reordered.spec.template.spec;
    // as the API server returns it: its own key order
    podSpec.affinity = JSON.parse(canonicalJson(podSpec.affinity));
    podSpec.topologySpreadConstraints = podSpec.topologySpreadConstraints.map((c: Record<string, unknown>) =>
      Object.fromEntries(Object.entries(c).reverse()),
    );
    expect(statefulSetNeedsUpdate(reordered, desired)).toBe(false);
    expect(statefulSetNeedsUpdate(buildStatefulSet(recorded('pinned')), desired)).toBe(true);
    expect(canonicalJson({ b: 1, a: { d: [{ y: 1, x: 2 }], c: 3 } })).toBe('{"a":{"c":3,"d":[{"x":2,"y":1}]},"b":1}');
  });

  it('removes the scheduling keys a merge patch would keep (required turned preferred, a node selector dropped)', () => {
    expect(withRemovals({ a: { b: 1, c: 2 }, d: [1] }, { a: { b: 3 } })).toEqual({ a: { b: 3, c: null }, d: null });
    expect(withRemovals({ a: 1 }, [2])).toEqual([2]);
    const existing = buildStatefulSet(recorded('enabled', { podAntiAffinity: { type: 'required' }, nodeSelector: { disk: 'ssd' } }));
    const desired = buildStatefulSet(recorded('enabled'));
    const pod = statefulSetPatchBody(existing, desired).spec!.template.spec! as unknown as Record<string, unknown>;
    expect(pod.nodeSelector).toBeNull();
    expect(pod.affinity).toEqual({
      podAntiAffinity: {
        preferredDuringSchedulingIgnoredDuringExecution: desired.spec!.template.spec!.affinity!.podAntiAffinity!.preferredDuringSchedulingIgnoredDuringExecution,
        requiredDuringSchedulingIgnoredDuringExecution: null,
      },
    });
    // nothing to remove: the desired object as it is
    expect(statefulSetPatchBody(desired, desired).spec!.template.spec!.affinity).toEqual(desired.spec!.template.spec!.affinity);
  });

  it('rejects an unknown anti-affinity type or an empty topology key', () => {
    expect(() => validateClusterSpec(recorded('enabled', { podAntiAffinity: { type: 'always' as never } }))).toThrow(/preferred or required/);
    expect(() => validateClusterSpec(recorded('enabled', { podAntiAffinity: { topologyKey: ' ' } }))).toThrow(/topologyKey/);
    expect(() => validateClusterSpec(recorded('enabled', { podAntiAffinity: { type: 'required' } }))).not.toThrow();
  });

  it('recreates an outdated pod no node took at once, instead of waiting for it', () => {
    const pod = (name: string, revision: string, scheduled = true): V1Pod => ({
      metadata: { name, labels: { [REVISION_LABEL]: revision } },
      spec: { containers: [], ...(scheduled ? { nodeName: 'node-1' } : {}) },
      status: scheduled
        ? { phase: 'Running', conditions: [{ type: 'Ready', status: 'True' }] }
        : { phase: 'Pending', conditions: [{ type: 'PodScheduled', status: 'False', reason: 'Unschedulable' }] },
    });
    const cluster = makeCluster({ instances: 3, replication: { enabled: true } });
    const statefulSet = { metadata: { name: 'db', generation: 2 }, status: { updateRevision: 'new', observedGeneration: 2 } };
    const plan = planRollingUpdate({
      cluster,
      statefulSet,
      pods: [pod('test-cluster-0', 'old'), pod('test-cluster-1', 'old'), pod('test-cluster-2', 'old', false)],
      primaryPod: 'test-cluster-0',
      fenced: [],
    });
    expect(plan?.restart).toBe('test-cluster-2');
    expect(plan?.message).toMatch(/never scheduled/);
    // a pod of the new revision no node took does not hold back the outdated ones: their old
    // required anti-affinity may be what keeps it off the node
    const next = planRollingUpdate({
      cluster,
      statefulSet,
      pods: [pod('test-cluster-0', 'old'), pod('test-cluster-1', 'new'), pod('test-cluster-2', 'new', false)],
      primaryPod: 'test-cluster-0',
      fenced: [],
    });
    expect(next?.restart).toBe('test-cluster-0');
    // a pod that is scheduled but not ready yet is still waited for
    const starting = pod('test-cluster-2', 'new');
    starting.status = { phase: 'Running', conditions: [{ type: 'Ready', status: 'False' }] };
    const waiting = planRollingUpdate({
      cluster,
      statefulSet,
      pods: [pod('test-cluster-0', 'old'), pod('test-cluster-1', 'new'), starting],
      primaryPod: 'test-cluster-0',
      fenced: [],
    });
    expect(waiting?.restart).toBeUndefined();
  });

  it('has a pod found unschedulable before the other instances changed scheduled again', () => {
    const pending = (found: string): V1Pod => ({
      metadata: { name: 'test-cluster-1', creationTimestamp: new Date('2026-10-10T10:00:00Z') },
      spec: { containers: [] },
      status: { phase: 'Pending', conditions: [{ type: 'PodScheduled', status: 'False', reason: 'Unschedulable', lastTransitionTime: new Date(found) }] },
    });
    const running = (created: string): V1Pod => ({
      metadata: { name: 'test-cluster-0', creationTimestamp: new Date(created) },
      spec: { containers: [], nodeName: 'node-1' },
      status: { phase: 'Running' },
    });
    // the other instance was recreated after the finding (its old rule may be gone)
    expect(staleUnschedulablePod([running('2026-10-10T10:01:00Z'), pending('2026-10-10T10:00:05Z')])).toBe('test-cluster-1');
    // found after every other pod was created: nothing changed since, left to the scheduler
    expect(staleUnschedulablePod([running('2026-10-10T09:00:00Z'), pending('2026-10-10T10:00:05Z')])).toBeUndefined();
    // a pod still being scheduled, or one a node took
    const fresh = pending('2026-10-10T10:00:05Z');
    fresh.status!.conditions = [];
    expect(staleUnschedulablePod([running('2026-10-10T10:01:00Z'), fresh])).toBeUndefined();
    expect(staleUnschedulablePod([running('2026-10-10T10:01:00Z'), running('2026-10-10T10:02:00Z')])).toBeUndefined();
  });

  describe('in the controller', () => {
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
    const annotationPatches = (api: (m: string) => Mock) =>
      api('patchNamespacedCustomObject')
        .mock.calls.map((c) => c[0].body?.metadata?.annotations?.[POD_ANTI_AFFINITY_ANNOTATION])
        .filter(Boolean);

    it('records the default for a new cluster, and its StatefulSet repels the instances', async () => {
      const cluster = recorded(undefined);
      cluster.metadata.annotations = { 'firebird.cloudnative-firebird.io/primary-lease': 'pinned' };
      const { kubeConfig, api } = mockApi({ getNamespacedCustomObject: vi.fn().mockResolvedValue({}) });
      await new FirebirdClusterController(kubeConfig).reconcile(cluster);
      expect(annotationPatches(api)).toEqual(['enabled']);
      expect(api('createNamespacedEvent').mock.calls.map((c) => c[0].body.reason)).toContain('PodAntiAffinityDefaulted');
      const created = api('createNamespacedStatefulSet').mock.calls[0][0].body;
      expect(created.spec.template.spec.affinity.podAntiAffinity.preferredDuringSchedulingIgnoredDuringExecution).toHaveLength(1);
    });

    it('pins an existing cluster, whose instances keep their scheduling', async () => {
      const cluster = recorded(undefined);
      cluster.metadata.annotations = { 'firebird.cloudnative-firebird.io/primary-lease': 'pinned' };
      const existing = { ...buildStatefulSet(recorded('pinned')), status: { observedGeneration: 1, updateRevision: 'r1' } };
      const { kubeConfig, api } = mockApi({
        getNamespacedCustomObject: vi.fn().mockResolvedValue({}),
        readNamespacedStatefulSet: vi.fn().mockResolvedValue(existing),
      });
      await new FirebirdClusterController(kubeConfig).reconcile(cluster);
      expect(annotationPatches(api)).toEqual(['pinned']);
      expect(api('patchNamespacedStatefulSet')).not.toHaveBeenCalled();
    });
  });
});
