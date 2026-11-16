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
 * The Jobs of a cluster that runs the Rust agent: the binary is installed into the Job's pod, and
 * every Perl program the Job ran is the agent's command (agent/fb-agent/src/jobs.rs). The
 * conformance of each command with its script is in their tests (tests/helpers/implementations.ts).
 */
const s3 = { bucket: 'bkt', prefix: 'fb/', secretRef: { name: 's3-creds' }, endpoint: 'http://minio:9000' };
const withAgent = (cluster: FirebirdCluster): FirebirdCluster => ({
  ...cluster,
  metadata: { ...cluster.metadata, annotations: { ...cluster.metadata.annotations, [AGENT_ANNOTATION]: 'rust' } },
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

describe('the Jobs of a cluster with the Rust agent', () => {
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

  it('run the agent\'s commands where they ran the Perl scripts', () => {
    const perl = jobs(makeCluster(spec));
    const rust = jobs(withAgent(makeCluster(spec)));
    // without the annotation the Jobs are as they were: Perl
    for (const [name, pod] of Object.entries(perl)) {
      expect(containers(pod).map((c) => c.name), name).not.toContain('agent-install');
    }
    expect(Object.values(perl).some((pod) => containers(pod).some((c) => /\bperl\b/.test(commandLine(c))))).toBe(true);
    for (const [name, pod] of Object.entries(rust)) {
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
    // everything else in the Jobs as without the annotation
    const strip = (pod: V1PodSpec) =>
      JSON.stringify(pod)
        .split('/opt/fb-agent/fb-agent backup-file')
        .join('perl /etc/firebird-operator/backup-file.pl')
        .split('/opt/fb-agent/fb-agent request')
        .join('perl /etc/firebird-operator/segment-request.pl');
    const some = <T>(list?: T[]) => (list?.length ? list : undefined);
    for (const name of Object.keys(perl)) {
      const pod = JSON.parse(strip(rust[name])) as V1PodSpec;
      const plain = (c: V1Container) => ({
        ...c,
        command: c.command?.[0] === '/opt/fb-agent/fb-agent' && c.command.length === 2 ? undefined : c.command?.map((t) => t.split('"$FB_AGENT" set-repl-seq').join('perl "$SCRIPT_DIR/set-repl-seq.pl"')),
        env: some(c.env?.filter((e) => e.name !== 'FB_AGENT')),
        volumeMounts: some(c.volumeMounts?.filter((m) => m.name !== 'fb-agent')),
      });
      const without = {
        ...pod,
        // the segment TLS proxy is the agent's too since v0.164.0 (tests/agent.test.ts)
        initContainers: pod.initContainers?.filter((c) => c.name !== 'agent-install' && c.restartPolicy !== 'Always').map(plain),
        containers: pod.containers.map(plain),
        volumes: pod.volumes?.filter((v) => v.name !== 'fb-agent'),
      };
      const before = {
        ...perl[name],
        initContainers: perl[name].initContainers?.filter((c) => c.restartPolicy !== 'Always').map((c) => ({ ...plain(c), command: c.command?.[0] === 'perl' && c.command.length === 2 ? undefined : c.command })),
        containers: perl[name].containers.map((c) => ({ ...plain(c), command: c.command?.[0] === 'perl' && c.command.length === 2 ? undefined : c.command })),
      };
      expect(JSON.parse(JSON.stringify(without)), name).toEqual(JSON.parse(JSON.stringify(before)));
    }
  });

  it('point-in-time recovery: the script asks for the agent itself', () => {
    const script = JOB_SCRIPTS['pitr-restore.sh'];
    expect(script).toContain('backup_file() { if [ -n "${FB_AGENT:-}" ]; then "$FB_AGENT" backup-file "$@"; else perl "$SCRIPT_DIR/backup-file.pl" "$@"; fi; }');
    // every Perl run goes through those three
    const runs = script.split('\n').filter((l) => /\bperl\b/.test(l) && !l.trimStart().startsWith('#'));
    expect(runs).toHaveLength(3);
    expect(runs.every((l) => /^(backup_file\(\)|pitr_plan\(\)|  perl -e )/.test(l))).toBe(true);
  });
});
