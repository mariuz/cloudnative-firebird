import { describe, it, expect } from 'vitest';
import { V1PodSpec, V1StatefulSet } from '@kubernetes/client-node';
// the client's own deserializer, which every read of the operator goes through
import { ObjectSerializer } from '@kubernetes/client-node/dist/gen/models/ObjectSerializer.js';
import { FirebirdCluster } from '../src/types';
import { buildStatefulSet, statefulSetUpdateReason } from '../src/utils/resources';
import { makeCluster } from './helpers/factories';

/**
 * The StatefulSet as the operator reads it back after writing it: stored by the API server (with
 * its defaults: empty resources, a field reference's apiVersion, probe timings, the grace period)
 * and deserialized by the client (its objects' keys in the client's own order)
 */
function readBack(sts: V1StatefulSet): V1StatefulSet {
  const stored = JSON.parse(JSON.stringify(sts)) as V1StatefulSet;
  const pod = stored.spec!.template.spec! as V1PodSpec;
  pod.terminationGracePeriodSeconds ??= 30;
  for (const c of [...(pod.initContainers ?? []), ...pod.containers]) {
    c.resources ??= {};
    c.terminationMessagePath ??= '/dev/termination-log';
    c.imagePullPolicy ??= 'IfNotPresent';
    for (const e of c.env ?? []) if (e.valueFrom?.fieldRef) e.valueFrom.fieldRef.apiVersion ??= 'v1';
    for (const probe of [c.livenessProbe, c.readinessProbe, c.startupProbe]) {
      if (!probe) continue;
      probe.initialDelaySeconds ??= 0;
      probe.periodSeconds ??= 10;
      probe.timeoutSeconds ??= 1;
      probe.successThreshold ??= 1;
      probe.failureThreshold ??= 3;
    }
  }
  // the client's key order: the reverse of the operator's, to tell the two apart in any case
  const reversed = JSON.parse(JSON.stringify(stored, (_k, v: unknown) =>
    v && typeof v === 'object' && !Array.isArray(v) ? Object.fromEntries(Object.entries(v).reverse()) : v));
  return ObjectSerializer.deserialize(reversed, 'V1StatefulSet') as V1StatefulSet;
}

describe('the StatefulSet read back from the API server', () => {
  const clusters: Array<[string, Partial<FirebirdCluster['spec']>]> = [
    ['a single instance', {}],
    ['replication', { instances: 3, replication: { enabled: true } }],
    ['replication with failover and segment TLS', {
      instances: 3,
      replication: { enabled: true, failover: { enabled: true } },
      segmentTLS: { enabled: true },
    }],
    ['a superuser Secret', { superuserSecret: { name: 'fb-superuser' } }],
    ['resources', { resources: { requests: { cpu: '500m', memory: '1024Mi' }, limits: { memory: '2Gi' } } }],
    ['the firebird user', { runAsFirebirdUser: true }],
    ['own variables', { env: [{ name: 'NODE', valueFrom: { fieldRef: { fieldPath: 'spec.nodeName' } } }] }],
  ];

  it.each(clusters)('is current for %s', (_name, spec) => {
    const cluster = makeCluster(spec as Partial<FirebirdCluster['spec']>);
    expect(statefulSetUpdateReason(readBack(buildStatefulSet(cluster)), buildStatefulSet(cluster))).toBeUndefined();
  });

  it('still needs an update when what the operator sets changes', () => {
    const before = readBack(buildStatefulSet(makeCluster()));
    expect(statefulSetUpdateReason(before, buildStatefulSet(makeCluster({ resources: { requests: { cpu: '1' } } })))).toBe('resources');
    expect(statefulSetUpdateReason(before, buildStatefulSet(makeCluster({ env: [{ name: 'A', value: 'b' }] })))).toBe('env');
    expect(statefulSetUpdateReason(before, buildStatefulSet(makeCluster({ securityContext: { readOnlyRootFilesystem: true } }))))
      .toBe('container securityContext');
  });
});
