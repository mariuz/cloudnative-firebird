import { describe, it, expect } from 'vitest';
import { canonicalStatus, keepTransitionTimes } from '../src/utils/status-diff';

describe('status comparison (CloudNativePG #11789)', () => {
  it('compares statuses whatever their key order, without undefined values, with array order', () => {
    expect(canonicalStatus({ a: 1, b: { d: 2, c: [1, 2] } })).toBe(canonicalStatus({ b: { c: [1, 2], d: 2 }, a: 1, e: undefined }));
    expect(canonicalStatus({ c: [1, 2] })).not.toBe(canonicalStatus({ c: [2, 1] }));
    expect(canonicalStatus(undefined)).toBe(canonicalStatus({}));
  });

  it('keeps the time of a condition whose status is unchanged, by type', () => {
    const previous = [
      { type: 'Ready', status: 'True', reason: 'A', lastTransitionTime: 't0' },
      { type: 'Progressing', status: 'True', reason: 'B', lastTransitionTime: 't0' },
    ];
    expect(
      keepTransitionTimes(previous, [
        { type: 'Progressing', status: 'False', reason: 'B', lastTransitionTime: 't1' },
        { type: 'Ready', status: 'True', reason: 'other', lastTransitionTime: 't1' },
        { type: 'Fenced', status: 'False', reason: 'C', lastTransitionTime: 't1' },
      ]),
    ).toEqual([
      { type: 'Progressing', status: 'False', reason: 'B', lastTransitionTime: 't1' },
      { type: 'Ready', status: 'True', reason: 'other', lastTransitionTime: 't0' },
      { type: 'Fenced', status: 'False', reason: 'C', lastTransitionTime: 't1' },
    ]);
    expect(keepTransitionTimes(undefined, undefined)).toBeUndefined();
    expect(keepTransitionTimes([], previous)).toBe(previous);
  });
});
