import { describe, it, expect, vi, type Mock } from 'vitest';
import { KubeConfig } from '@kubernetes/client-node';
import { FirebirdClusterController } from '../src/controllers/firebirdcluster.controller';
import { FirebirdCluster } from '../src/types';
import { notMigratingToPrimaryLease } from '../src/utils/primary-lease-migration';
import { notMigrating } from '../src/utils/segment-tls-migration';
import { RECONCILIATION_LOOP_ANNOTATION, reconciliationPaused } from '../src/utils/suspension';
import { makeCluster, notFoundError } from './helpers/factories';

const annotated = (value: string, cluster = makeCluster()): FirebirdCluster => ({
  ...cluster,
  metadata: { ...cluster.metadata, annotations: { ...cluster.metadata.annotations, [RECONCILIATION_LOOP_ANNOTATION]: value } },
});

function mocks(overrides: Record<string, Mock> = {}) {
  const calls: Record<string, Mock> = {};
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
  const events = () => fn('createNamespacedEvent').mock.calls.map((c) => c[0].body);
  const statuses = () => fn('patchNamespacedCustomObjectStatus').mock.calls.map((c) => c[0].body[0].value);
  return { controller: new FirebirdClusterController(kubeConfig), fn, events, statuses };
}

describe('pausing the reconciliation', () => {
  it('is spec.suspended or the reconciliationLoop annotation set to disabled', () => {
    expect(reconciliationPaused(makeCluster())).toBeUndefined();
    expect(reconciliationPaused(makeCluster({ suspended: true }))).toBe('Reconciliation suspended by spec.suspended');
    expect(reconciliationPaused(annotated('disabled'))).toBe(`Reconciliation disabled by the ${RECONCILIATION_LOOP_ANNOTATION} annotation`);
    expect(reconciliationPaused(annotated(' Disabled '))).toBeDefined();
    expect(reconciliationPaused(annotated('enabled'))).toBeUndefined();
    expect(reconciliationPaused(annotated(''))).toBeUndefined();
    const disabled = makeCluster();
    disabled.metadata.annotations = { ...disabled.metadata.annotations, 'firebird.cloudnative-firebird.io/reconciliationDisabled': 'true' };
    expect(reconciliationPaused(disabled)).toBe('Reconciliation disabled by the firebird.cloudnative-firebird.io/reconciliationDisabled annotation');
  });

  it('changes nothing, records Paused with an event once, even for an invalid spec', async () => {
    const m = mocks();
    // an invalid spec (no storage size) is not reconciled either while paused
    const cluster = annotated('disabled', makeCluster({ storage: { size: '' } }));
    await m.controller.reconcile(cluster);
    for (const create of ['createNamespacedStatefulSet', 'createNamespacedService', 'createNamespacedConfigMap']) {
      expect(m.fn(create)).not.toHaveBeenCalled();
    }
    expect(m.fn('patchNamespacedStatefulSet')).not.toHaveBeenCalled();
    expect(m.statuses()).toEqual([
      expect.objectContaining({
        phase: 'Paused',
        phaseReason: `Reconciliation disabled by the ${RECONCILIATION_LOOP_ANNOTATION} annotation`,
        conditions: [expect.objectContaining({ type: 'Paused', status: 'True', reason: 'ReconciliationPaused' })],
      }),
    ]);
    expect(m.events().map((e) => [e.type, e.reason])).toEqual([['Normal', 'ReconciliationPaused']]);

    // already recorded: nothing written again (every resync reconciles it)
    const again = mocks();
    await again.controller.reconcile({ ...cluster, status: { phase: 'Paused', phaseReason: m.statuses()[0].phaseReason } });
    expect(again.statuses()).toEqual([]);
    expect(again.events()).toEqual([]);

    // paused for another reason: recorded again
    const other = mocks();
    await other.controller.reconcile({ ...annotated('disabled', makeCluster({ suspended: true })), status: { phase: 'Paused', phaseReason: m.statuses()[0].phaseReason } });
    expect(other.statuses()[0].phaseReason).toBe('Reconciliation suspended by spec.suspended');
  });

  it('resumes with an event and reconciles again', async () => {
    const m = mocks({ getNamespacedCustomObject: vi.fn().mockResolvedValue({}) });
    await m.controller.reconcile({ ...makeCluster(), status: { phase: 'Paused', phaseReason: 'Reconciliation suspended by spec.suspended' } });
    expect(m.events().map((e) => e.reason)).toContain('ReconciliationResumed');
    expect(m.fn('createNamespacedStatefulSet')).toHaveBeenCalled();
    expect(m.statuses().at(-1).phase).not.toBe('Paused');
  });

  it('holds back the segment TLS and primary Lease migrations', () => {
    const running = (c: FirebirdCluster): FirebirdCluster => ({
      ...c,
      metadata: { ...c.metadata, annotations: { ...c.metadata.annotations, 'firebird.cloudnative-firebird.io/segment-tls-migration': 'pinned' } },
      spec: { ...c.spec, segmentTLS: { enabled: false }, replication: { enabled: true, failover: { enabled: true } } },
      status: { phase: 'Running', readyInstances: c.spec.instances },
    });
    expect(notMigrating(running(annotated('disabled')), 'all', true)).toBe('hibernated or paused');
    expect(notMigratingToPrimaryLease(running(annotated('disabled')), 'pinned', true)).toBe('hibernated or paused');
  });
});
