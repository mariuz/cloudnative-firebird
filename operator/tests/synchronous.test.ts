import { describe, it, expect } from 'vitest';
import { V1Job, V1Pod } from '@kubernetes/client-node';
import { FirebirdCluster, SynchronousStatus } from '../src/types';
import {
  attachedStandbys,
  buildSyncStandbyJob,
  planSynchronous,
  SyncPlanInput,
  synchronousMembers,
  synchronousNumber,
  syncUnsupportedReason,
} from '../src/utils/synchronous';
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
// a status written before v0.61.0 (no standbys list): read as the one standby attached
const attached: SynchronousStatus = { standby: 'db-1', primary: 'db-0', phase: 'Attached', time: ago(600) };
/** what the operator stores for it */
const kept: SynchronousStatus = { ...attached, standbys: ['db-1'] };
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
    expect(plan({ status: failed })).toEqual({ kind: 'none', status: { ...failed, standbys: [] } });
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
    expect(plan({ status: attached })).toEqual({ kind: 'none', status: kept });
    expect(plan({ status: kept })).toEqual({ kind: 'none', status: kept });
    // a primary restart keeps its standby
    expect(plan({ status: kept, pods: [pod('db-0', false), pod('db-1'), pod('db-2')] })).toEqual({ kind: 'none', status: kept });
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
    // the time a status before v0.61.0 recorded is kept, per standby
    const step = plan({ status: { ...attached, unavailableSince: ago(3600) }, pods: down });
    expect(step).toMatchObject({ kind: 'none', status: { phase: 'Attached', unavailable: { 'db-1': ago(3600) } } });
    expect(step.status?.unavailableSince).toBeUndefined();
  });

  it('with dataDurability preferred, detaches a standby unavailable for standbyUnavailableSeconds', () => {
    const cluster = makeCluster({ synchronous: { dataDurability: 'preferred', standbyUnavailableSeconds: 20 } });
    const down = [pod('db-0'), pod('db-1', false), pod('db-2')];
    // just went down: the time is recorded
    expect(plan({ cluster, status: kept, pods: down })).toMatchObject({ kind: 'none', status: { unavailable: { 'db-1': ago(0) } } });
    expect(plan({ cluster, status: { ...kept, unavailable: { 'db-1': ago(10) } }, pods: down }).kind).toBe('none');
    const step = plan({ cluster, status: { ...kept, unavailable: { 'db-1': ago(25) } }, pods: down });
    expect(step).toMatchObject({ kind: 'start', action: 'detach' });
    expect(step.kind === 'start' && step.status.message).toContain('has not been ready for 25s');
    // back in time: forgotten
    expect(plan({ cluster, status: { ...kept, unavailable: { 'db-1': ago(10) } } })).toEqual({ kind: 'none', status: kept });
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

describe('planSynchronous: rolling updates', () => {
  it('hands the standby over to another replica before the rolling update restarts it', () => {
    const step = plan({ status: attached, rollingTarget: 'db-1' });
    expect(step).toMatchObject({ kind: 'start', action: 'detach', standby: 'db-1' });
    expect(step.kind === 'start' && step.status.message).toContain('restarted by the rolling update');
    // then attaches the other replica, never the one about to be restarted
    const detached: SynchronousStatus = { ...attached, phase: 'Detached', time: ago(1) };
    expect(plan({ status: detached, rollingTarget: 'db-1' })).toMatchObject({ kind: 'start', action: 'attach', standby: 'db-2' });
    expect(plan({ rollingTarget: 'db-1' })).toMatchObject({ kind: 'start', action: 'attach', standby: 'db-2' });
    // a standby that is not restarted next stays
    expect(plan({ status: attached, rollingTarget: 'db-2' })).toEqual({ kind: 'none', status: kept });
  });

  it('with dataDurability required and no other replica, restarts the standby attached', () => {
    const pods = [pod('db-0'), pod('db-1')];
    expect(plan({ cluster: makeCluster({}, 2), pods, status: attached, rollingTarget: 'db-1' })).toEqual({ kind: 'none', status: kept });
    // another replica that has not caught up cannot take over
    const lagging = [pod('db-0'), pod('db-1'), pod('db-2', true, '300')];
    expect(plan({ pods: lagging, status: attached, rollingTarget: 'db-1' })).toEqual({ kind: 'none', status: kept });
  });

  it('with dataDurability required and detachForUpdates, detaches the only standby before its restart', () => {
    const cluster = makeCluster({ synchronous: { detachForUpdates: true } }, 2);
    const pods = [pod('db-0'), pod('db-1')];
    expect(plan({ cluster, pods, status: attached, rollingTarget: 'db-1' })).toMatchObject({
      kind: 'start',
      action: 'detach',
      standby: 'db-1',
    });
    // attached again once restarted; a standby that is not restarted next stays, and one that is
    // only unavailable still holds writes (required)
    expect(plan({ cluster, pods, status: { ...attached, phase: 'Detached' } })).toMatchObject({ kind: 'start', action: 'attach', standby: 'db-1' });
    expect(plan({ cluster, pods, status: attached })).toEqual({ kind: 'none', status: kept });
    expect(plan({ cluster, pods: [pod('db-0'), pod('db-1', false)], status: attached }).kind).toBe('none');
    // another replica ready to take over: still a handover
    const three = makeCluster({ synchronous: { detachForUpdates: true } });
    expect(plan({ cluster: three, status: attached, rollingTarget: 'db-1' })).toMatchObject({ kind: 'start', action: 'detach', standby: 'db-1' });
  });

  it('with dataDurability preferred, detaches the standby before its restart', () => {
    const cluster = makeCluster({ synchronous: { dataDurability: 'preferred' } }, 2);
    const pods = [pod('db-0'), pod('db-1')];
    expect(plan({ cluster, pods, status: attached, rollingTarget: 'db-1' })).toMatchObject({ kind: 'start', action: 'detach' });
    // and attaches nothing until it was restarted
    expect(plan({ cluster, pods, status: { ...attached, phase: 'Detached' }, rollingTarget: 'db-1' }).kind).toBe('none');
    expect(plan({ cluster, pods, status: { ...attached, phase: 'Detached' } })).toMatchObject({ kind: 'start', action: 'attach', standby: 'db-1' });
  });
});

describe('planSynchronous: several standbys (synchronous.number)', () => {
  const two = makeCluster({ synchronous: { number: 2 } });
  const one: SynchronousStatus = { standby: 'db-1', primary: 'db-0', standbys: ['db-1'], phase: 'Attached', time: ago(600) };
  const both: SynchronousStatus = { ...one, standby: 'db-2', standbys: ['db-1', 'db-2'] };

  it('attaches standbys one at a time up to the number', () => {
    expect(plan({ cluster: two })).toMatchObject({ kind: 'start', action: 'attach', standby: 'db-1', status: { standbys: [] } });
    const second = plan({ cluster: two, status: one });
    expect(second).toMatchObject({ kind: 'start', action: 'attach', standby: 'db-2', status: { phase: 'Attaching', standbys: ['db-1'] } });
    const attaching = second.kind === 'start' ? second.status : one;
    expect(plan({ cluster: two, status: attaching, job: done('Complete'), jobOutcome: 'attached' })).toMatchObject({
      kind: 'finished',
      status: { phase: 'Attached', standby: 'db-2', standbys: ['db-1', 'db-2'] },
    });
    expect(plan({ cluster: two, status: both })).toEqual({ kind: 'none', status: both });
    // a failed attach of the second leaves the first attached, retried a minute later
    const failed = plan({ cluster: two, status: attaching, job: done('Failed'), jobOutcome: 'failed clean' });
    expect(failed).toMatchObject({ kind: 'finished', status: { phase: 'Failed', standbys: ['db-1'] } });
    const failedStatus = failed.kind === 'finished' ? failed.status : one;
    expect(plan({ cluster: two, status: failedStatus })).toEqual({ kind: 'none', status: failedStatus });
    expect(plan({ cluster: two, status: failedStatus, now: now + 61_000 })).toMatchObject({ kind: 'start', action: 'attach', standby: 'db-2' });
  });

  it('detaches the standby that needs it, keeping the others', () => {
    const step = plan({ cluster: two, status: both, fenced: ['db-2'] });
    expect(step).toMatchObject({ kind: 'start', action: 'detach', standby: 'db-2', status: { phase: 'Detaching', standbys: ['db-1', 'db-2'] } });
    const detaching = step.kind === 'start' ? step.status : both;
    expect(plan({ cluster: two, status: detaching, job: done('Complete'), jobOutcome: 'detached' })).toMatchObject({
      kind: 'finished',
      status: { phase: 'Attached', standbys: ['db-1'] },
    });
    // the last one: Detached
    const last: SynchronousStatus = { ...one, standby: 'db-1', phase: 'Detaching' };
    expect(plan({ cluster: two, status: last, job: done('Complete'), jobOutcome: 'detached' })).toMatchObject({
      kind: 'finished',
      status: { phase: 'Detached', standbys: [] },
    });
  });

  it('detaches the highest ordinal when the number is lowered', () => {
    const step = plan({ cluster: makeCluster({ synchronous: { number: 1 } }), status: both });
    expect(step).toMatchObject({ kind: 'start', action: 'detach', standby: 'db-2' });
    expect(step.kind === 'start' && step.status.message).toContain('more synchronous standbys than synchronous.number (1)');
  });

  it('tracks each standby\'s unavailability on its own', () => {
    const pods = [pod('db-0'), pod('db-1'), pod('db-2', false)];
    expect(plan({ cluster: two, status: both, pods })).toMatchObject({ kind: 'none', status: { unavailable: { 'db-2': ago(0) } } });
    const preferred = makeCluster({ synchronous: { number: 2, dataDurability: 'preferred', standbyUnavailableSeconds: 20 } });
    expect(plan({ cluster: preferred, status: { ...both, unavailable: { 'db-2': ago(30) } }, pods })).toMatchObject({
      kind: 'start',
      action: 'detach',
      standby: 'db-2',
    });
  });

  it('builds the Job with the standbys that stay attached', () => {
    const env = (others: string[]) =>
      Object.fromEntries(buildSyncStandbyJob(two, 'attach', 'db-0', 'db-2', others).spec!.template.spec!.containers[0].env!.map((e) => [e.name, e.value]));
    expect(env(['db-1']).OTHERS).toBe('db-1.db-headless');
    expect(env([]).OTHERS).toBe('');
  });

  it('reads the standbys of statuses written before v0.61.0', () => {
    expect(attachedStandbys(attached)).toEqual(['db-1']);
    expect(attachedStandbys({ ...attached, phase: 'Detaching' })).toEqual(['db-1']);
    expect(attachedStandbys({ ...attached, phase: 'Attaching' })).toEqual([]);
    expect(synchronousMembers({ ...attached, phase: 'Attaching' })).toEqual(['db-1']);
    expect(synchronousMembers({ ...both, standby: 'db-3', phase: 'Attaching' })).toEqual(['db-1', 'db-2', 'db-3']);
    expect(synchronousNumber(makeCluster({ synchronous: { number: 5 } }))).toBe(2);
  });
});

describe('planSynchronous on Firebird 4', () => {
  const reason = syncUnsupportedReason('4.0.7')!;

  it('names the engines that commit without an unreachable sync_replica', () => {
    expect(reason).toContain('Firebird 4.0.7 commits while a synchronous replica is unreachable');
    expect(syncUnsupportedReason('5.0.4')).toBeUndefined();
    expect(syncUnsupportedReason('6.0.0')).toBeUndefined();
    expect(syncUnsupportedReason(undefined)).toBeUndefined();
  });

  it('attaches nothing until the primary\'s version is known, keeping attached standbys', () => {
    const blocked = "the primary's Firebird version is not known yet";
    expect(plan({ attachBlocked: blocked }).kind).toBe('none');
    expect(plan({ attachBlocked: blocked, status: kept })).toEqual({ kind: 'none', status: kept });
  });

  it('attaches nothing, detaches an attached standby, and says why', () => {
    const none = plan({ unsupported: reason });
    expect(none).toMatchObject({ kind: 'none', status: { phase: 'Failed', primary: 'db-0', standbys: [], message: reason } });
    // the time stays while the reason does
    const stored = none.status!;
    expect(plan({ unsupported: reason, status: stored, now: now + 600_000 })).toEqual({ kind: 'none', status: stored });
    const step = plan({ unsupported: reason, status: kept });
    expect(step).toMatchObject({ kind: 'start', action: 'detach', standby: 'db-1' });
    expect(step.kind === 'start' && step.status.message).toContain('Firebird 4.0.7');
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
