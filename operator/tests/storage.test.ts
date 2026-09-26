import { describe, it, expect } from 'vitest';
import { V1PersistentVolumeClaim } from '@kubernetes/client-node';
import { parseQuantity, planVolumeExpansion } from '../src/utils/storage';

const makePvc = (
  requested: string,
  capacity?: string,
  conditions: Array<{ type: string; status: string }> = [],
): V1PersistentVolumeClaim => ({
  metadata: { name: 'firebird-data-test-cluster-0' },
  spec: { resources: { requests: { storage: requested } } },
  status: {
    ...(capacity ? { capacity: { storage: capacity } } : {}),
    conditions,
  },
});

describe('parseQuantity', () => {
  it.each([
    ['1Gi', 2 ** 30],
    ['512Mi', 512 * 2 ** 20],
    ['1.5Ti', 1.5 * 2 ** 40],
    ['10G', 10e9],
    ['500M', 500e6],
    ['100k', 100e3],
    ['1e9', 1e9],
    ['1024', 1024],
  ])('parses %s', (input, expected) => {
    expect(parseQuantity(input)).toBe(expected);
  });

  it.each(['', 'abc', '10GB', '-1Gi', 'Gi'])('returns null for invalid quantity %j', (input) => {
    expect(parseQuantity(input)).toBeNull();
  });

  it('returns null for undefined', () => {
    expect(parseQuantity(undefined)).toBeNull();
  });

  it('compares binary and decimal suffixes correctly', () => {
    expect(parseQuantity('1Gi')!).toBeGreaterThan(parseQuantity('1G')!);
  });
});

describe('planVolumeExpansion', () => {
  it('requests expansion when the desired size is larger', () => {
    const plan = planVolumeExpansion(makePvc('1Gi', '1Gi'), '5Gi');
    expect(plan.expand).toBe(true);
    expect(plan.status.state).toBe('Resizing');
    expect(plan.status.requestedSize).toBe('5Gi');
    expect(plan.status.capacity).toBe('1Gi');
  });

  it('treats equivalent quantities as equal', () => {
    const plan = planVolumeExpansion(makePvc('1024Mi', '1024Mi'), '1Gi');
    expect(plan.expand).toBe(false);
    expect(plan.status.state).toBe('Ready');
  });

  it('rejects shrinking', () => {
    const plan = planVolumeExpansion(makePvc('10Gi', '10Gi'), '5Gi');
    expect(plan.expand).toBe(false);
    expect(plan.status.state).toBe('ShrinkRejected');
    expect(plan.status.message).toContain('Cannot shrink');
  });

  it('reports Resizing while capacity lags behind the request', () => {
    const plan = planVolumeExpansion(makePvc('5Gi', '1Gi'), '5Gi');
    expect(plan.expand).toBe(false);
    expect(plan.status.state).toBe('Resizing');
  });

  it('reports a pending file system resize', () => {
    const plan = planVolumeExpansion(
      makePvc('5Gi', '5Gi', [{ type: 'FileSystemResizePending', status: 'True' }]),
      '5Gi',
    );
    expect(plan.status.state).toBe('Resizing');
    expect(plan.status.message).toContain('file system resize');
  });

  it('reports Ready once capacity matches', () => {
    const plan = planVolumeExpansion(makePvc('5Gi', '5Gi'), '5Gi');
    expect(plan).toEqual({
      expand: false,
      status: { name: 'firebird-data-test-cluster-0', requestedSize: '5Gi', capacity: '5Gi', state: 'Ready' },
    });
  });
});
