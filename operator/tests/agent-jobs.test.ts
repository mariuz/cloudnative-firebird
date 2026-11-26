import { describe, it, expect } from 'vitest';
import { V1Container, V1Job, V1PodSpec } from '@kubernetes/client-node';
import { AGENT_ANNOTATION } from '../src/utils/agent';
import { buildBackupJob, buildJournalArchiveCronJob, buildRestoreJob } from '../src/utils/backup';
import { buildMajorUpgradeJob } from '../src/utils/major-upgrade';
import { buildFailoverJob, buildPromoteJob, buildSwitchoverJob } from '../src/utils/switchover';
import { buildSyncStandbyJob } from '../src/utils/synchronous';
import { JOB_SCRIPTS } from '../src/utils/replication';
import { FirebirdBackup, FirebirdCluster, FirebirdRestore } from '../src/types';
import { makeCluster } from './helpers/factories';

/**
 * The Jobs run the Rust agent (since v0.170.0 on every cluster): the binary is installed into the
 * Job's pod, and every program the Job runs is the agent's command (agent/fb-agent/src/jobs.rs).
 * The commands' own tests are in jobs-conformance.test.ts and the tests of their Jobs.
 */
const s3 = { bucket: 'bkt', prefix: 'fb/', secretRef: { name: 's3-creds' }, endpoint: 'http://minio:9000' };
const annotated = (cluster: FirebirdCluster, value: string): FirebirdCluster => ({
  ...cluster,
  metadata: { ...cluster.metadata, annotations: { ...cluster.metadata.annotations, [AGENT_ANNOTATION]: value } },
});
const backup = (spec: Partial<FirebirdBackup['spec']> = {}): FirebirdBackup => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdBackup',
  metadata: { name: 'b1', namespace: 'default', uid: 'backup-uid' },
  spec: { clusterName: 'test-cluster', ...spec },
});
const restore = (spec: Partial<FirebirdRestore['spec']> = {}): FirebirdRestore => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdRestore',
  metadata: { name: 'r1', namespace: 'default', uid: 'restore-uid' },
  spec: { clusterName: 'test-cluster', ...spec },
});

/** Every Job the operator runs for a cluster that uses them all, by name */
function jobs(cluster: FirebirdCluster): Record<string, V1PodSpec> {
  const pod = (job: V1Job) => job.spec!.template.spec!;
  const chain = { type: 'physical' as const, path: 'nbackup-l0-a.nbk', incrementalPaths: ['nbackup-l1-b.nbk'] };
  return {
    switchover: pod(buildSwitchoverJob(cluster, { from: 'test-cluster-0', target: 'test-cluster-1', replicas: ['test-cluster-2'], archived: 9 })),
    promote: pod(buildPromoteJob(cluster, 'test-cluster-1', 9)),
    failover: pod(buildFailoverJob(cluster, ['test-cluster-1', 'test-cluster-2'])),
    'sync-standby': pod(buildSyncStandbyJob(cluster, 'attach', 'test-cluster-0', 'test-cluster-1')),
    'logical backup': pod(buildBackupJob(backup(), cluster, 'test-cluster-1', true)),
    'nbackup of a replica to S3': pod(buildBackupJob(backup({ type: 'physical', level: 1, s3 }), cluster, 'test-cluster-1', true)),
    'backup to S3 with retention': pod(buildBackupJob(backup({ s3, retention: { policy: '7d' } } as Partial<FirebirdBackup['spec']>), cluster, 'test-cluster-0')),
    'nbackup restore': pod(buildRestoreJob(restore({ restoreType: 'physical' }), cluster, chain, 'test-cluster-0')),
    'point-in-time recovery': pod(
      buildRestoreJob(restore({ restoreType: 'physical', pointInTime: { targetTime: '2026-10-01T12:15:30Z' } }), cluster, { ...chain, s3 }, 'test-cluster-0'),
    ),
    'journal archive': buildJournalArchiveCronJob(cluster, 'test-cluster-0')!.spec!.jobTemplate.spec!.template.spec!,
    'major upgrade': pod(
      buildMajorUpgradeJob(cluster, { from: 'firebirdsql/firebird:5', to: 'firebirdsql/firebird:6', fromOds: '13.1', toOds: '14.0', primary: 'test-cluster-0', phase: 'Converting' }, 1, 'firebird-data-test-cluster-1'),
    ),
  };
}
const containers = (spec: V1PodSpec): V1Container[] => [...(spec.initContainers ?? []), ...spec.containers];
const commandLine = (c: V1Container) => [...(c.command ?? []), ...(c.args ?? [])].join(' ');

describe('the Jobs of a cluster run the Rust agent', () => {
  const spec = {
    instances: 3,
    replication: {
      enabled: true,
      failover: { enabled: true },
      journalArchiveS3: s3,
      pruneAppliedSegments: true,
      synchronous: { enabled: true },
    },
    segmentTLS: { enabled: true },
  } as Partial<FirebirdCluster['spec']>;

  it('run the agent\'s commands, whatever the annotation of before v0.170.0 says', () => {
    const rust = jobs(makeCluster(spec));
    // the Jobs that only run Firebird's tools are left as they are
    const without = Object.keys(rust).filter((name) => !containers(rust[name]).some((c) => c.name === 'agent-install'));
    // (a backup to S3 runs the agent's object store client since v0.192.0)
    expect(without).toEqual(['logical backup', 'nbackup restore']);
    for (const name of without) {
      for (const c of containers(rust[name])) expect(c.env ?? [], `${name}/${c.name}`).not.toContainEqual(expect.objectContaining({ name: 'FB_AGENT' }));
    }
    for (const [name, pod] of Object.entries(rust).filter(([name]) => !without.includes(name))) {
      const names = containers(pod).map((c) => c.name);
      // installed after the native sidecars (the segment TLS proxy), before the Job's own containers
      expect(names, name).toContain('agent-install');
      const sidecars = (pod.initContainers ?? []).filter((c) => c.restartPolicy === 'Always').length;
      expect(pod.initContainers![sidecars].name, name).toBe('agent-install');
      expect(pod.volumes, name).toContainEqual({ name: 'fb-agent', emptyDir: {} });
      for (const c of containers(pod).filter((c) => c.name !== 'agent-install' && c.restartPolicy !== 'Always')) {
        // no Perl, and the binary where the commands look for it
        expect(commandLine(c), `${name}/${c.name}`).not.toMatch(/\bperl\b|\.pl\b/);
        expect(c.env, `${name}/${c.name}`).toContainEqual({ name: 'FB_AGENT', value: '/opt/fb-agent/fb-agent' });
        expect(c.volumeMounts, `${name}/${c.name}`).toContainEqual({ name: 'fb-agent', mountPath: '/opt/fb-agent', readOnly: true });
      }
    }
    // each program as the agent's command
    const command = (job: string, container: string) => containers(rust[job]).find((c) => c.name === container)!;
    expect(command('switchover', 'switchover').command).toEqual(['/opt/fb-agent/fb-agent', 'switchover']);
    expect(command('failover', 'failover').command).toEqual(['/opt/fb-agent/fb-agent', 'failover']);
    expect(command('sync-standby', 'sync-standby').command).toEqual(['/opt/fb-agent/fb-agent', 'sync-standby']);
    expect(command('journal archive', 'fetch-segments').command).toEqual(['/opt/fb-agent/fb-agent', 'fetch-segments']);
    expect(commandLine(command('promote', 'promote'))).toContain('reply=$(/opt/fb-agent/fb-agent request "$TARGET" PROMOTE 9)');
    expect(commandLine(command('nbackup of a replica to S3', 'firebird-backup'))).toContain('out=$(/opt/fb-agent/fb-agent backup-file nbackup 1 "$f")');
    expect(commandLine(command('major upgrade', 'restore'))).toContain('"$FB_AGENT" set-repl-seq');
    // the annotation that made a cluster opt in is accepted and changes nothing
    for (const value of ['rust', 'perl']) {
      expect(JSON.parse(JSON.stringify(jobs(annotated(makeCluster(spec), value)))), value).toEqual(JSON.parse(JSON.stringify(rust)));
    }
  });

  it('point-in-time recovery: the script runs the agent', () => {
    const script = JOB_SCRIPTS['pitr-restore.sh'];
    expect(script).toContain('backup_file() { "$FB_AGENT" backup-file "$@"; }');
    expect(script).toContain('pitr_plan() { "$FB_AGENT" pitr-plan "$@"; }');
    expect(script).toContain('cut_segment() { "$FB_AGENT" cut-segment "$1" "$2"; }');
    expect(script.split('\n').filter((l) => /\bperl\b/.test(l) && !l.trimStart().startsWith('#'))).toEqual([]);
  });
});
