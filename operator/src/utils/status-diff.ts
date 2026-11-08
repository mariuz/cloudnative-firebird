/**
 * Status writes that change nothing are skipped (CloudNativePG #11789): the cluster reconcile
 * compares the status it would write with the one stored, and conditions keep their
 * lastTransitionTime while their status is unchanged, so an unchanged cluster compares equal.
 */

interface Condition {
  type: string;
  status: string;
  lastTransitionTime?: string;
}

/**
 * The conditions to write, each keeping the lastTransitionTime of the previous condition of its
 * type while its status is the same (Kubernetes API conventions: the time the status last changed;
 * a new reason or message is not a transition)
 */
export function keepTransitionTimes<C extends Condition>(previous: C[] | undefined, next: C[] | undefined): C[] | undefined {
  if (!next || !previous?.length) return next;
  return next.map((c) => {
    const before = previous.find((p) => p.type === c.type);
    return before && before.status === c.status && before.lastTransitionTime && before.lastTransitionTime !== c.lastTransitionTime
      ? { ...c, lastTransitionTime: before.lastTransitionTime }
      : c;
  });
}

/**
 * A JSON form of a status for comparison: object keys sorted, undefined values left out (as the
 * API server stores them), array order kept
 */
export function canonicalStatus(value: unknown): string {
  return JSON.stringify(value ?? {}, (_key, v: unknown) =>
    v && typeof v === 'object' && !Array.isArray(v)
      ? Object.fromEntries(Object.entries(v as Record<string, unknown>).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)))
      : v,
  );
}
