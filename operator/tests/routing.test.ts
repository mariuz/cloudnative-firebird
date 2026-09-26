import { describe, it, expect } from 'vitest';
import { V1Pod } from '@kubernetes/client-node';
import {
  READ_ROUTABLE_LABEL,
  REPLICATION_LAG_ANNOTATION,
  ROLE_LABEL,
  computeReadRouting,
  isPodReady,
  podRoutingLabelPatch,
  replicationLagSeconds,
} from '../src/utils/routing';

const makePod = (
  name: string,
  { ready = true, lag, labels }: { ready?: boolean; lag?: string; labels?: Record<string, string> } = {},
): V1Pod => ({
  metadata: {
    name,
    ...(labels ? { labels } : {}),
    ...(lag !== undefined ? { annotations: { [REPLICATION_LAG_ANNOTATION]: lag } } : {}),
  },
  status: {
    phase: 'Running',
    conditions: [{ type: 'Ready', status: ready ? 'True' : 'False' }],
  },
});

describe('isPodReady', () => {
  it('is true for a running pod with Ready=True', () => {
    expect(isPodReady(makePod('p'))).toBe(true);
  });

  it('is false when Ready=False', () => {
    expect(isPodReady(makePod('p', { ready: false }))).toBe(false);
  });

  it('is false for a terminating pod', () => {
    const pod = makePod('p');
    pod.metadata!.deletionTimestamp = new Date();
    expect(isPodReady(pod)).toBe(false);
  });
});

describe('replicationLagSeconds', () => {
  it('parses the lag annotation', () => {
    expect(replicationLagSeconds(makePod('p', { lag: '12.5' }))).toBe(12.5);
  });

  it.each([undefined, '', 'abc', '-3'])('returns null for %j', (lag) => {
    expect(replicationLagSeconds(makePod('p', { lag }))).toBeNull();
  });
});

describe('computeReadRouting', () => {
  it('routes reads to ready replicas within the lag threshold', () => {
    const plan = computeReadRouting(
      [makePod('db-0'), makePod('db-1', { lag: '2' }), makePod('db-2', { lag: '5' })],
      'db-0',
      { enabled: true, maxLagSeconds: 10 },
    );
    expect(plan.readRoutablePods).toEqual(['db-1', 'db-2']);
    expect(plan.laggingReplicas).toEqual([]);
    expect(plan.decisions.find((d) => d.name === 'db-0')).toEqual({ name: 'db-0', role: 'primary', readRoutable: false });
  });

  it('excludes lagging and unready replicas', () => {
    const plan = computeReadRouting(
      [makePod('db-0'), makePod('db-1', { lag: '120' }), makePod('db-2', { ready: false }), makePod('db-3', { lag: '1' })],
      'db-0',
      { enabled: true, maxLagSeconds: 30 },
    );
    expect(plan.readRoutablePods).toEqual(['db-3']);
    expect(plan.laggingReplicas).toEqual(['db-1']);
  });

  it('uses the default 30s threshold', () => {
    const plan = computeReadRouting(
      [makePod('db-0'), makePod('db-1', { lag: '31' }), makePod('db-2', { lag: '30' })],
      'db-0',
      { enabled: true },
    );
    expect(plan.readRoutablePods).toEqual(['db-2']);
    expect(plan.laggingReplicas).toEqual(['db-1']);
  });

  it('routes replicas without a lag report on readiness alone', () => {
    const plan = computeReadRouting([makePod('db-0'), makePod('db-1')], 'db-0', { enabled: true });
    expect(plan.readRoutablePods).toEqual(['db-1']);
  });

  it('falls back to the primary when no replica is eligible', () => {
    const plan = computeReadRouting(
      [makePod('db-0'), makePod('db-1', { lag: '300' })],
      'db-0',
      { enabled: true },
    );
    expect(plan.readRoutablePods).toEqual(['db-0']);
  });

  it('does not fall back to the primary when disabled', () => {
    const plan = computeReadRouting(
      [makePod('db-0'), makePod('db-1', { lag: '300' })],
      'db-0',
      { enabled: true, fallbackToPrimary: false },
    );
    expect(plan.readRoutablePods).toEqual([]);
  });

  it('does not fall back to an unready primary', () => {
    const plan = computeReadRouting([makePod('db-0', { ready: false })], 'db-0', { enabled: true });
    expect(plan.readRoutablePods).toEqual([]);
  });

  it('honours a failed-over primary', () => {
    const plan = computeReadRouting([makePod('db-0'), makePod('db-1')], 'db-1', { enabled: true });
    expect(plan.decisions.find((d) => d.name === 'db-1')?.role).toBe('primary');
    expect(plan.readRoutablePods).toEqual(['db-0']);
  });
});

describe('podRoutingLabelPatch', () => {
  it('adds escaped label paths for missing labels', () => {
    const patch = podRoutingLabelPatch(makePod('db-1', { labels: { app: 'x' } }), {
      name: 'db-1',
      role: 'replica',
      readRoutable: true,
    });
    expect(patch).toEqual([
      { op: 'add', path: '/metadata/labels/firebird.cloudnative-firebird.io~1role', value: 'replica' },
      { op: 'add', path: '/metadata/labels/firebird.cloudnative-firebird.io~1read-routable', value: 'true' },
    ]);
  });

  it('only patches labels that changed', () => {
    const pod = makePod('db-1', { labels: { [ROLE_LABEL]: 'replica', [READ_ROUTABLE_LABEL]: 'true' } });
    expect(podRoutingLabelPatch(pod, { name: 'db-1', role: 'replica', readRoutable: false })).toEqual([
      { op: 'add', path: '/metadata/labels/firebird.cloudnative-firebird.io~1read-routable', value: 'false' },
    ]);
  });

  it('returns no operations when labels are up to date', () => {
    const pod = makePod('db-1', { labels: { [ROLE_LABEL]: 'replica', [READ_ROUTABLE_LABEL]: 'true' } });
    expect(podRoutingLabelPatch(pod, { name: 'db-1', role: 'replica', readRoutable: true })).toEqual([]);
  });

  it('creates the labels map when the pod has none', () => {
    expect(podRoutingLabelPatch(makePod('db-0'), { name: 'db-0', role: 'primary', readRoutable: false })).toEqual([
      { op: 'add', path: '/metadata/labels', value: { [ROLE_LABEL]: 'primary', [READ_ROUTABLE_LABEL]: 'false' } },
    ]);
  });
});
