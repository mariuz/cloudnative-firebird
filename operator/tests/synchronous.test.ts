import { describe, it, expect } from 'vitest';
import { V1Job, V1Pod } from '@kubernetes/client-node';
import { FirebirdCluster, SynchronousStatus } from '../src/types';
import { buildSyncStandbyJob, planSynchronous, SyncPlanInput } from '../src/utils/synchronous';
import { buildReplicationConf } from '../src/utils/replication';
import { REPLICATION_LAG_ANNOTATION } from '../src/utils/routing';

const makeCluster = (replication: object = {}, instances = 3): FirebirdCluster => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdCluster',
  metadata: { name: 'db', namespace: 'default', uid: 'c' },
  spec: { instances, storage: { size: '1Gi' }, replication: { enabled: true, mode: 'sync', ...replication } },
});

/** lag: the replication-lag-seconds annotation, null for none (not measured) */
const pod = (name: string, ready = true, lag: string | null = '0'): V1Pod => ({
  metadata: { name, uid: `uid-${name}`, annotations: lag === null ? {} : { [REPLICATION_LAG_ANNOTATION]: lag } },
  status: { phase: 'Running', conditions: [{ type: 'Ready', status: ready ? 'True' : 'False' }] },
});

const now = Date.parse('2026-10-03T10:00:00Z');
const ago = (s: number) => new Date(now - s * 1000).toISOString();
const attached: SynchronousStatus = { standby: 'db-1', primary: 'db-0', phase: 'Attached', time: ago(600) };
const done = (type: 'Complete' | 'Failed'): V1Job => ({ status: { conditions: [{ type, status: 'True' }] } });

const plan = (over: Partial<SyncPlanInput> = {}) =>
  planSynchronous({
    cluster: makeCluster(),
    primaryPod: 'db-0',
    pods: [pod('db-0'), pod('db-1'), pod('db-2')],
    fenced: [],
    reseeding: [],
    now,
    ...over,
  });

describe('planSynchronous: attaching', () => {
  it('attaches the lowest-ordinal ready replica that has caught up', () => {
    const step = plan({ pods: [pod('db-0'), pod('db-1', true, '120'), pod('db-2', true, '1')] });
    expect(step).toMatchObject({ kind: 'start', action: 'attach', standby: 'db-2', status: { phase: 'Attaching', primary: 'db-0', standby: 'db-2' } });
    expect(plan()).toMatchObject({ kind: 'start', action: 'attach', standby: 'db-1' });
  });

  it('attaches nothing without a candidate, a ready primary, or while busy', () => {
    // unmeasured lag, not ready, fenced, being re-seeded, scaled away
    expect(plan({ pods: [pod('db-0'), pod('db-1', true, null), pod('db-2', false)] }).kind).toBe('none');
    expect(plan({ fenced: ['db-1', 'db-2'] }).kind).toBe('none');
    expect(plan({ reseeding: ['db-1', 'db-2'] }).kind).toBe('none');
    expect(plan({ cluster: makeCluster({}, 1) }).kind).toBe('none');
    expect(plan({ pods: [pod('db-0', false), pod('db-1'), pod('db-2')] }).kind).toBe('none');
    expect(plan({ fenced: ['db-0'] }).kind).toBe('none');
    expect(plan({ busy: 'a switchover is requested' }).kind).toBe('none');
    // asynchronous mode
    expect(plan({ cluster: makeCluster({ mode: 'async' }) })).toEqual({ kind: 'none', status: undefined });
  });

  it('retries a failed attach after a minute', () => {
    const failed: SynchronousStatus = { ...attached, phase: 'Failed', time: ago(10) };
    expect(plan({ status: failed })).toEqual({ kind: 'none', status: failed });
    expect(plan({ status: { ...failed, time: ago(61) } }).kind).toBe('start');
  });

  it('records the Job outcome', () => {
    const attaching: SynchronousStatus = { ...attached, phase: 'Attaching', time: ago(5) };
    expect(plan({ status: attaching, job: {} })).toEqual({ kind: 'none', status: attaching });
    expect(plan({ status: attaching, job: done('Complete'), jobOutcome: 'attached' })).toMatchObject({
      kind: 'finished',
      status: { phase: 'Attached', standby: 'db-1', primary: 'db-0' },
      event: 'SyncStandbyAttached',
    });
    // undone cleanly: nothing to re-seed; otherwise (or unknown) the standby is re-seeded
    expect(plan({ status: attaching, job: done('Failed'), jobOutcome: 'failed clean' })).toMatchObject({
      kind: 'finished',
      status: { phase: 'Failed' },
      reseed: undefined,
      event: 'SyncStandbyFailed',
    });
    expect(plan({ status: attaching, job: done('Failed'), jobOutcome: 'failed' })).toMatchObject({ reseed: 'db-1' });
    expect(plan({ status: attaching, job: done('Failed') })).toMatchObject({ reseed: 'db-1' });
    // a failed detach leaves the standby attached (the primary may still replicate to it): retried a minute later
    const failedDetach = plan({ status: { ...attaching, phase: 'Detaching' }, job: done('Failed') });
    expect(failedDetach).toMatchObject({ kind: 'finished', status: { phase: 'Attached', retryAfter: new Date(now + 60_000).toISOString() } });
    const retrying = failedDetach.kind === 'finished' ? failedDetach.status : attached;
    expect(plan({ status: retrying, fenced: ['db-1'] }).kind).toBe('none');
    const retry = plan({ status: retrying, fenced: ['db-1'], now: now + 61_000 });
    expect(retry).toMatchObject({ kind: 'start', action: 'detach' });
    expect(retry.kind === 'start' && retry.status.retryAfter).toBeUndefined();
    expect(plan({ status: attaching })).toMatchObject({ kind: 'finished', status: { phase: 'Failed' } });
  });
});

describe('planSynchronous: attached', () => {
  it('keeps the standby attached while nothing calls for a detach', () => {
    expect(plan({ status: attached })).toEqual({ kind: 'none', status: attached });
    // a primary restart keeps its standby
    expect(plan({ status: attached, pods: [pod('db-0', false), pod('db-1'), pod('db-2')] })).toEqual({ kind: 'none', status: attached });
  });

  it.each([
    ['synchronous replication is turned off', { cluster: makeCluster({ mode: 'async' }) }, 'synchronous replication is off'],
    ['the standby is scaled away', { cluster: makeCluster({}, 1) }, 'being removed'],
    ['the standby is fenced', { fenced: ['db-1'] }, 'fenced'],
    ['the standby is to be re-seeded', { reseeding: ['db-1'] }, 're-seeded'],
    ['a switchover is requested', { busy: 'a switchover is requested' }, 'a switchover is requested'],
  ])('detaches the standby when %s', (_what, over, reason) => {
    const step = plan({ status: attached, ...over });
    expect(step).toMatchObject({ kind: 'start', action: 'detach', standby: 'db-1', status: { phase: 'Detaching' } });
    expect(step.kind === 'start' && step.status.message).toContain(reason);
  });

  it('with dataDurability required, keeps a standby that is not ready (writes wait for it)', () => {
    const down = [pod('db-0'), pod('db-1', false), pod('db-2')];
    const step = plan({ status: { ...attached, unavailableSince: ago(3600) }, pods: down });
    expect(step).toMatchObject({ kind: 'none', status: { phase: 'Attached', unavailableSince: ago(3600) } });
  });

  it('with dataDurability preferred, detaches a standby unavailable for standbyUnavailableSeconds', () => {
    const cluster = makeCluster({ synchronous: { dataDurability: 'preferred', standbyUnavailableSeconds: 20 } });
    const down = [pod('db-0'), pod('db-1', false), pod('db-2')];
    // just went down: the time is recorded
    expect(plan({ cluster, status: attached, pods: down })).toMatchObject({ kind: 'none', status: { unavailableSince: ago(0) } });
    expect(plan({ cluster, status: { ...attached, unavailableSince: ago(10) }, pods: down }).kind).toBe('none');
    const step = plan({ cluster, status: { ...attached, unavailableSince: ago(25) }, pods: down });
    expect(step).toMatchObject({ kind: 'start', action: 'detach' });
    expect(step.kind === 'start' && step.status.message).toContain('has not been ready for 25s');
    // back in time: forgotten
    expect(plan({ cluster, status: { ...attached, unavailableSince: ago(10) } })).toEqual({ kind: 'none', status: attached });
  });

  it('records a detach, and re-seeds a standby that could not be reached', () => {
    const detaching: SynchronousStatus = { ...attached, phase: 'Detaching', time: ago(5) };
    expect(plan({ status: detaching, job: done('Complete'), jobOutcome: 'detached\n' })).toMatchObject({
      kind: 'finished',
      status: { phase: 'Detached' },
      reseed: undefined,
    });
    expect(plan({ status: detaching, job: done('Complete'), jobOutcome: 'detached unreachable' })).toMatchObject({
      kind: 'finished',
      status: { phase: 'Detached' },
      reseed: 'db-1',
    });
  });

  it('forgets a standby attached to a former primary (failover)', () => {
    expect(plan({ status: attached, primaryPod: 'db-1', pods: [pod('db-0', false), pod('db-1'), pod('db-2')] })).toMatchObject({
      kind: 'start',
      action: 'attach',
      standby: 'db-2',
      status: { primary: 'db-1' },
    });
  });
});

describe('buildSyncStandbyJob', () => {
  it('runs sync-standby.pl against the primary and the standby', () => {
    const job = buildSyncStandbyJob(makeCluster(), 'attach', 'db-0', 'db-1');
    expect(job.metadata?.name).toBe('db-sync-standby');
    const c = job.spec!.template.spec!.containers[0];
    expect(c.command).toEqual(['perl', '/etc/firebird-operator/sync-standby.pl']);
    const env = Object.fromEntries(c.env!.map((e) => [e.name, e.value]));
    expect(env).toMatchObject({ ACTION: 'attach', PRIMARY: 'db-0.db-headless', STANDBY: 'db-1.db-headless', DATABASE_PATH: '/var/lib/firebird/data/mydb.fdb' });
  });
});

describe('replication.conf for synchronous replication', () => {
  it('includes the sync_replica file, and makes replication errors fail the commit in mode sync', () => {
    const sync = buildReplicationConf(makeCluster(), '/var/lib/firebird/data/db.fdb', '/var/lib/firebird/data');
    expect(sync).toContain('    include /var/lib/firebird/data/replication/sync.conf\n}');
    expect(sync).toContain('report_errors = true');
    expect(sync).toContain('disable_on_error = false');
    const async = buildReplicationConf(makeCluster({ mode: 'async' }), '/var/lib/firebird/data/db.fdb', '/var/lib/firebird/data');
    expect(async).toContain('include /var/lib/firebird/data/replication/sync.conf');
    expect(async).not.toContain('report_errors');
  });
});
