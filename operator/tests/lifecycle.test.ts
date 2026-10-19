import { describe, it, expect } from 'vitest';
import { spawnSync } from 'child_process';
import { chmodSync, mkdtempSync, readFileSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { V1StatefulSet } from '@kubernetes/client-node';
import { FirebirdCluster } from '../src/types';
import { instanceProbes, lifecycleErrors, smartShutdownInProgress, smartShutdownScript } from '../src/utils/lifecycle';
import { buildStatefulSet, statefulSetNeedsUpdate, statefulSetPatchBody } from '../src/utils/resources';
import { validateClusterSpec } from '../src/utils/validation';
import { makeCluster } from './helpers/factories';

const firebird = (cluster: FirebirdCluster) => buildStatefulSet(cluster).spec!.template.spec!.containers.find((c) => c.name === 'firebird')!;

describe('instance probes', () => {
  it('keep the defaults without settings: no startup probe, no hook, Kubernetes grace period', () => {
    const c = firebird(makeCluster());
    expect(c.livenessProbe).toEqual({ tcpSocket: { port: 3050 }, initialDelaySeconds: 30, periodSeconds: 10, failureThreshold: 5 });
    expect(c.readinessProbe).toMatchObject({ initialDelaySeconds: 5, periodSeconds: 5, timeoutSeconds: 5, failureThreshold: 3 });
    expect(c.readinessProbe!.exec!.command![2]).toContain('fbsvcmgr');
    expect(c.startupProbe).toBeUndefined();
    expect(c.lifecycle).toBeUndefined();
    expect(buildStatefulSet(makeCluster()).spec!.template.spec!.terminationGracePeriodSeconds).toBeUndefined();
  });

  it('take the timings of spec.probes over the defaults', () => {
    const c = firebird(makeCluster({ probes: { liveness: { periodSeconds: 20, timeoutSeconds: 3 }, readiness: { failureThreshold: 6, successThreshold: 2 } } }));
    expect(c.livenessProbe).toEqual({ tcpSocket: { port: 3050 }, initialDelaySeconds: 30, periodSeconds: 20, timeoutSeconds: 3, failureThreshold: 5 });
    expect(c.readinessProbe).toMatchObject({ periodSeconds: 5, failureThreshold: 6, successThreshold: 2 });
  });

  it('add a startup probe for startDelay, or for probes.startup alone', () => {
    expect(instanceProbes(makeCluster({ startDelay: 600 }), ['true']).startupProbe).toEqual({ tcpSocket: { port: 3050 }, periodSeconds: 10, failureThreshold: 60 });
    expect(instanceProbes(makeCluster({ startDelay: 5 }), ['true']).startupProbe!.failureThreshold).toBe(1);
    expect(instanceProbes(makeCluster({ startDelay: 95 }), ['true']).startupProbe!.failureThreshold).toBe(10);
    expect(instanceProbes(makeCluster({ probes: { startup: { periodSeconds: 5 } } }), ['true']).startupProbe).toEqual({
      tcpSocket: { port: 3050 },
      periodSeconds: 5,
      failureThreshold: 30,
    });
    expect(firebird(makeCluster({ startDelay: 300 })).startupProbe!.failureThreshold).toBe(30);
  });

  it('set the grace period and the smart shutdown hook', () => {
    const cluster = makeCluster({ stopDelay: 120, smartShutdownTimeout: 60 });
    expect(buildStatefulSet(cluster).spec!.template.spec!.terminationGracePeriodSeconds).toBe(120);
    const command = firebird(cluster).lifecycle!.preStop!.exec!.command!;
    expect(command.slice(0, 2)).toEqual(['/bin/sh', '-c']);
    expect(command[2]).toContain('localhost:/var/lib/firebird/data/');
    expect(command[2]).toContain('+ 60 ))');
  });
});

// each test runs the script in a shell, with real sleeps: slow on a loaded CI runner
describe('the smart shutdown script', { timeout: 20000 }, () => {
  // a fake isql: answers remote=<n> with the counts listed in a file, one per call
  const run = (counts: string[], timeout: number) => {
    const dir = mkdtempSync(join(tmpdir(), 'smart-shutdown-'));
    writeFileSync(join(dir, 'counts'), counts.join('\n') + '\n');
    writeFileSync(
      join(dir, 'isql'),
      `#!/bin/sh\ncat > "${dir}/last.sql"\nn=$(head -1 "${dir}/counts"); sed -i 1d "${dir}/counts"\necho "$n" >> "${dir}/calls"\n[ "$n" = down ] && exit 1\n[ -n "$n" ] && printf '\\n=============\\nremote=%s\\n' "$n"\nexit 0\n`,
    );
    chmodSync(join(dir, 'isql'), 0o755);
    // poll every second instead of SMART_SHUTDOWN_POLL, for the test's speed
    const script = smartShutdownScript('/var/lib/firebird/data/db.fdb', timeout).replace(/sleep \d+/, 'sleep 1').replace('>/proc/1/fd/1', `>${dir}/log`);
    const started = Date.now();
    const result = spawnSync('sh', ['-c', script], { env: { ...process.env, PATH: `${dir}:${process.env.PATH}` }, encoding: 'utf8' });
    return {
      status: result.status,
      seconds: (Date.now() - started) / 1000,
      log: readFileSync(join(dir, 'log'), 'utf8'),
      calls: readFileSync(join(dir, 'calls'), 'utf8').trim().split('\n'),
      sql: readFileSync(join(dir, 'last.sql'), 'utf8'),
    };
  };

  it('stops at once without remote attachments', () => {
    const r = run(['0'], 30);
    expect(r.status).toBe(0);
    expect(r.calls).toEqual(['0']);
    expect(r.log).toContain('no remote attachments');
    // remote user attachments only: the local sidecars and probes do not count
    expect(r.sql).toContain('MON$SYSTEM_FLAG = 0');
    expect(r.sql).toContain("NOT STARTING WITH '127.'");
    expect(r.sql).toContain("<> '::1'");
    expect(r.sql).toContain('CURRENT_CONNECTION');
  });

  it('waits while clients are attached', () => {
    const r = run(['2', '1', '0'], 30);
    expect(r.status).toBe(0);
    expect(r.calls).toEqual(['2', '1', '0']);
    expect(r.log).toContain('waiting for 2 remote attachments');
    expect(r.log).toContain('no remote attachments');
    expect(r.seconds).toBeGreaterThanOrEqual(1.5);
  });

  it('gives up after the timeout', () => {
    const r = run(['3', '3', '3', '3', '3', '3', '3', '3'], 2);
    expect(r.status).toBe(0);
    expect(r.log).toContain('3 remote attachments after 2s, stopping anyway');
    expect(r.seconds).toBeLessThan(5);
  });

  it('does not wait for a server that does not answer', () => {
    const r = run(['down'], 30);
    expect(r.status).toBe(0);
    expect(r.log).toContain('does not answer');
  });
});

describe('the smart shutdown window of a pod being deleted', () => {
  const now = Date.parse('2026-10-17T12:00:00Z');
  // deleted secondsAgo with a grace period of grace seconds (deletionTimestamp: when it ends)
  const deleted = (secondsAgo: number, grace = 60) => ({
    metadata: { name: 'db-0', deletionTimestamp: new Date(now + (grace - secondsAgo) * 1000), deletionGracePeriodSeconds: grace },
  });
  const cluster = makeCluster({ stopDelay: 60, smartShutdownTimeout: 30 });

  it('lasts the timeout and a margin from the deletion', () => {
    expect(smartShutdownInProgress(cluster, deleted(5), now)).toBe(true);
    expect(smartShutdownInProgress(cluster, deleted(39), now)).toBe(true);
    expect(smartShutdownInProgress(cluster, deleted(41), now)).toBe(false);
  });

  it('does not apply to pods not being deleted, or without a timeout', () => {
    expect(smartShutdownInProgress(cluster, { metadata: { name: 'db-0' } }, now)).toBe(false);
    expect(smartShutdownInProgress(cluster, undefined, now)).toBe(false);
    expect(smartShutdownInProgress(makeCluster(), deleted(5), now)).toBe(false);
  });

  it('takes the grace period from the pod spec when the deletion does not carry it', () => {
    const pod = { metadata: { name: 'db-0', deletionTimestamp: new Date(now + 50_000) }, spec: { containers: [], terminationGracePeriodSeconds: 60 } };
    // deleted 10 s ago
    expect(smartShutdownInProgress(cluster, pod, now)).toBe(true);
    expect(smartShutdownInProgress(cluster, pod, now + 31_000)).toBe(false);
  });
});

describe('StatefulSet updates for probes and shutdown', () => {
  // as the API server stores it: every probe field defaulted, the grace period too
  const asStored = (sts: V1StatefulSet): V1StatefulSet => {
    const copy = JSON.parse(JSON.stringify(sts)) as V1StatefulSet;
    const pod = copy.spec!.template.spec!;
    pod.terminationGracePeriodSeconds ??= 30;
    for (const c of pod.containers) {
      for (const probe of [c.livenessProbe, c.readinessProbe, c.startupProbe]) {
        if (!probe) continue;
        probe.initialDelaySeconds ??= 0;
        probe.periodSeconds ??= 10;
        probe.timeoutSeconds ??= 1;
        probe.successThreshold ??= 1;
        probe.failureThreshold ??= 3;
      }
    }
    return copy;
  };

  it('are not needed for an unchanged cluster, whatever the server defaulted', () => {
    for (const cluster of [makeCluster(), makeCluster({ stopDelay: 90, smartShutdownTimeout: 30, startDelay: 300, probes: { readiness: { periodSeconds: 7 } } })]) {
      expect(statefulSetNeedsUpdate(asStored(buildStatefulSet(cluster)), buildStatefulSet(cluster))).toBe(false);
    }
  });

  it.each<[string, Partial<FirebirdCluster['spec']>]>([
    ['stopDelay', { stopDelay: 90 }],
    ['smartShutdownTimeout', { smartShutdownTimeout: 20 }],
    ['startDelay', { startDelay: 300 }],
    ['probes.liveness', { probes: { liveness: { failureThreshold: 9 } } }],
    ['probes.readiness', { probes: { readiness: { periodSeconds: 7 } } }],
  ])('are needed when %s changes', (_field, extra) => {
    expect(statefulSetNeedsUpdate(asStored(buildStatefulSet(makeCluster())), buildStatefulSet(makeCluster(extra)))).toBe(true);
    // and back
    expect(statefulSetNeedsUpdate(asStored(buildStatefulSet(makeCluster(extra))), buildStatefulSet(makeCluster()))).toBe(true);
  });

  it('remove the grace period when stopDelay is unset', () => {
    const existing = asStored(buildStatefulSet(makeCluster({ stopDelay: 90 })));
    const pod = statefulSetPatchBody(existing, buildStatefulSet(makeCluster())).spec!.template.spec! as unknown as Record<string, unknown>;
    expect(pod.terminationGracePeriodSeconds).toBeNull();
  });
});

describe('probe and shutdown validation', () => {
  it('accepts valid settings', () => {
    expect(lifecycleErrors(makeCluster({ stopDelay: 120, smartShutdownTimeout: 60, startDelay: 600, probes: { readiness: { successThreshold: 2, initialDelaySeconds: 0 } } }).spec)).toEqual([]);
    for (const switchoverDelay of [0, 30, 3600]) expect(lifecycleErrors(makeCluster({ switchoverDelay }).spec)).toEqual([]);
    expect(() => validateClusterSpec(makeCluster({ smartShutdownTimeout: 20 }))).not.toThrow();
  });

  it.each<[Partial<FirebirdCluster['spec']>, string]>([
    [{ stopDelay: 0 }, 'stopDelay must be an integer'],
    [{ stopDelay: 1.5 }, 'stopDelay must be an integer'],
    [{ smartShutdownTimeout: 30 }, 'must be below stopDelay (30)'],
    [{ stopDelay: 60, smartShutdownTimeout: 90 }, 'must be below stopDelay (60)'],
    [{ startDelay: -1 }, 'startDelay must be an integer'],
    [{ switchoverDelay: -1 }, 'switchoverDelay must be an integer of at least 0'],
    [{ switchoverDelay: 2.5 }, 'switchoverDelay must be an integer of at least 0'],
    [{ probes: { liveness: { periodSeconds: 0 } } }, 'probes.liveness.periodSeconds'],
    [{ probes: { readiness: { initialDelaySeconds: -1 } } }, 'probes.readiness.initialDelaySeconds'],
    [{ probes: { liveness: { successThreshold: 2 } } }, 'probes.liveness.successThreshold must be 1'],
    [{ probes: { startup: { successThreshold: 3 } } }, 'probes.startup.successThreshold must be 1'],
  ])('rejects %j', (extra, message) => {
    expect(() => validateClusterSpec(makeCluster(extra))).toThrow(message);
  });
});
