import { describe, it, expect } from 'vitest';
import { V1Pod, V1PodTemplateSpec } from '@kubernetes/client-node';
import { inPlaceResize, resizeApplied, resizeInfeasible } from '../src/utils/in-place';

const template = (resources: object, extra: object = {}): V1PodTemplateSpec => ({
  metadata: { labels: { app: 'db' } },
  spec: {
    containers: [
      { name: 'firebird', image: 'firebird:5', resources, ...extra },
      { name: 'segment-server', image: 'firebird:5' },
    ],
  },
});

describe('in-place resize decisions', () => {
  const base = { requests: { cpu: '250m', memory: '512Mi' }, limits: { cpu: '1', memory: '1Gi' } };

  it('resizes CPU changes and memory increases of the same QoS class', () => {
    expect(inPlaceResize(template(base), template({ ...base, limits: { cpu: '2', memory: '1Gi' } }))).toEqual([
      { name: 'firebird', resources: { ...base, limits: { cpu: '2', memory: '1Gi' } } },
    ]);
    expect(inPlaceResize(template(base), template({ requests: { cpu: '250m', memory: '768Mi' }, limits: { cpu: '1', memory: '2Gi' } }))).toHaveLength(1);
    expect(inPlaceResize(template(base), template(base))).toEqual([]);
  });

  it('restarts for a lower or new memory limit, a QoS class change, or any other change', () => {
    expect(inPlaceResize(template(base), template({ ...base, limits: { cpu: '1', memory: '768Mi' } }))).toBeUndefined();
    expect(inPlaceResize(template({ requests: { cpu: '250m' } }), template({ requests: { cpu: '250m' }, limits: { memory: '1Gi' } }))).toBeUndefined();
    // Burstable -> Guaranteed (every container with equal requests and limits; a container without
    // resources keeps a pod Burstable either way)
    const guaranteed = { requests: { cpu: '1', memory: '1Gi' }, limits: { cpu: '1', memory: '1Gi' } };
    const single = (resources: object): V1PodTemplateSpec => ({ spec: { containers: [{ name: 'firebird', image: 'firebird:5', resources }] } });
    expect(inPlaceResize(single(base), single(guaranteed))).toBeUndefined();
    expect(inPlaceResize(template(base), template(guaranteed))).toHaveLength(1);
    expect(inPlaceResize(template(base), template({ ...base, limits: { cpu: '2', memory: '1Gi' } }, { env: [{ name: 'X', value: '1' }] }))).toBeUndefined();
    const otherImage = template(base);
    otherImage.spec!.containers[1].image = 'firebird:6';
    expect(inPlaceResize(template(base), otherImage)).toBeUndefined();
  });

  it('tells when the kubelet applied the resize, and when it refused it', () => {
    const want = [{ name: 'firebird', resources: { requests: { cpu: '500m', memory: '512Mi' }, limits: { cpu: '2', memory: '1Gi' } } }];
    const pod = (resources: object, conditions: object[] = []): V1Pod => ({
      status: { containerStatuses: [{ name: 'firebird', resources, ready: true, restartCount: 0, image: '', imageID: '' }], conditions } as never,
    });
    // quantities compare by value
    expect(resizeApplied(pod({ requests: { cpu: '0.5', memory: '536870912' }, limits: { cpu: '2000m', memory: '1Gi' } }), want)).toBe(true);
    expect(resizeApplied(pod({ requests: { cpu: '250m', memory: '512Mi' }, limits: { cpu: '1', memory: '1Gi' } }), want)).toBe(false);
    expect(resizeApplied({ status: {} }, want)).toBe(false);
    expect(resizeInfeasible(pod({}, [{ type: 'PodResizePending', status: 'True', reason: 'Infeasible' }]))).toBe(true);
    expect(resizeInfeasible(pod({}, [{ type: 'PodResizePending', status: 'True', reason: 'Deferred' }]))).toBe(false);
  });
});
