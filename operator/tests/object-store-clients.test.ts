import { describe, it, expect } from 'vitest';
import { V1Container, V1Job, V1PodSpec } from '@kubernetes/client-node';
import {
  buildBackupCronJob,
  buildBackupJob,
  buildBootstrapInitContainers,
  buildJournalArchiveCronJob,
  buildRecoveryBootstrapJob,
  buildRestoreJob,
  buildScheduledBackupCronJob,
} from '../src/utils/backup';
import { buildArchiveFollowerContainer, FOLLOWER_AGENT_CLIENT, FOLLOWER_AWS_CLIENT } from '../src/utils/replica-cluster';
import { withAgent } from '../src/utils/agent';
import { validateS3Encryption } from '../src/utils/validation';
import { spawnSync } from 'child_process';
import { FirebirdBackup, FirebirdCluster, FirebirdRestore, FirebirdScheduledBackup, S3BackupConfiguration } from '../src/types';

/**
 * docs/object-stores.md, phase 1b: every pod and container that reads or writes an object store,
 * for a given S3 configuration. With the aws client (the default) they are pinned by a snapshot
 * taken before the object store builder existed, so that it changes nothing for them.
 */
const cluster = (spec: Partial<FirebirdCluster['spec']>, status?: FirebirdCluster['status']): FirebirdCluster => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdCluster',
  metadata: { name: 'db', namespace: 'default', uid: 'cluster-uid' },
  spec: { instances: 3, storage: { size: '1Gi' }, ...spec },
  ...(status ? { status } : {}),
});
const backup = (spec: Partial<FirebirdBackup['spec']>): FirebirdBackup => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdBackup',
  metadata: { name: 'b1', namespace: 'default', uid: 'backup-uid' },
  spec: { clusterName: 'db', ...spec },
});
const scheduled = (spec: Partial<FirebirdScheduledBackup['spec']>): FirebirdScheduledBackup => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdScheduledBackup',
  metadata: { name: 'nightly', namespace: 'default', uid: 'sb-uid' },
  spec: { clusterName: 'db', schedule: '0 2 * * *', ...spec } as FirebirdScheduledBackup['spec'],
});
const restore = (spec: Partial<FirebirdRestore['spec']>): FirebirdRestore => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdRestore',
  metadata: { name: 'r1', namespace: 'default', uid: 'restore-uid' },
  spec: { clusterName: 'db', ...spec },
});
const pod = (job: V1Job): V1PodSpec => job.spec!.template.spec!;

/** Every place an object store is used, for one S3 configuration */
export function objectStorePods(s3: S3BackupConfiguration): Record<string, V1PodSpec | V1Container | V1Container[]> {
  const replicated = cluster({ replication: { enabled: true, journalArchiveS3: s3 } }, { replicatedDatabases: ['sales.fdb'] });
  const chain = { type: 'physical' as const, path: 'nbackup-l0-a.nbk', incrementalPaths: ['nbackup-l1-b.nbk'], s3 };
  const replicaCluster = cluster({
    instances: 1,
    replication: { enabled: true },
    replica: { enabled: true, journalS3: s3, pollIntervalSeconds: 10 },
    bootstrap: { recovery: { sourcePath: 'nbackup-l0.nbk', s3, pointInTime: { journalS3: s3 } } },
  });
  return {
    'logical backup': pod(buildBackupJob(backup({ s3 }), cluster({}), 'db-0')),
    'nbackup of a replica': pod(buildBackupJob(backup({ type: 'physical', level: 1, s3 }), replicated, 'db-1', true)),
    'scheduled logical backup with retention': buildScheduledBackupCronJob(
      scheduled({ s3, retentionPolicy: '7d' }),
      cluster({}),
      'db-0',
    ).spec!.jobTemplate.spec!.template.spec!,
    'scheduled nbackup with retention': buildScheduledBackupCronJob(
      scheduled({ s3, type: 'physical', retentionPolicy: '4w' } as Partial<FirebirdScheduledBackup['spec']>),
      cluster({}),
      'db-0',
    ).spec!.jobTemplate.spec!.template.spec!,
    'cluster backup schedule': buildBackupCronJob(cluster({ backup: { enabled: true, schedule: '0 3 * * *', s3 } }), 'db-0')
      .spec!.jobTemplate.spec!.template.spec!,
    'logical restore': pod(buildRestoreJob(restore({}), cluster({}), { type: 'logical', path: 'backup-b1.fbk', s3 }, 'db-0')),
    'nbackup restore': pod(buildRestoreJob(restore({ restoreType: 'physical' }), cluster({}), chain, 'db-0')),
    'point-in-time recovery': pod(
      buildRestoreJob(restore({ restoreType: 'physical', pointInTime: { targetTime: '2026-10-01T12:15:30Z' } }), replicated, chain, 'db-0'),
    ),
    "point-in-time recovery of a declarative database": pod(
      buildRestoreJob(
        restore({ restoreType: 'physical', pointInTime: { targetSegment: 12 } }),
        replicated,
        { ...chain, database: 'sales.fdb' },
        'db-0',
      ),
    ),
    'journal archive': buildJournalArchiveCronJob(replicated, 'db-0')!.spec!.jobTemplate.spec!.template.spec!,
    'recovery bootstrap': pod(buildRecoveryBootstrapJob(replicaCluster)),
    'bootstrap from a logical backup': buildBootstrapInitContainers(cluster({ bootstrap: { recovery: { s3, sourcePath: 'nightly/x.fbk' } } })),
    'replica cluster archive follower': buildArchiveFollowerContainer(replicaCluster),
  };
}

const configurations: Record<string, S3BackupConfiguration> = {
  'keys, endpoint and prefix': { bucket: 'bkt', prefix: '/fb/', endpoint: 'http://minio:9000', secretRef: { name: 's3-creds' } },
  'workload identity, region': { bucket: 'bkt', region: 'eu-west-1' },
  'SSE-KMS': { bucket: 'bkt', prefix: 'p', secretRef: { name: 's3-creds' }, encryption: 'aws:kms', kmsKeyId: 'alias/fb' },
  'SSE-C': { bucket: 'bkt', prefix: 'p', secretRef: { name: 's3-creds' }, sseCustomerKey: { name: 'ssec' } },
  'client image': { bucket: 'bkt', clientImage: 'example.com/aws-cli:2' },
};

describe('the aws client (client: aws, the default until v0.192.0, deprecated)', () => {
  for (const [name, s3] of Object.entries(configurations)) {
    it(`is what it was before the object store builder: ${name}`, () => {
      expect(objectStorePods({ ...s3, client: 'aws' })).toMatchSnapshot();
    });
  }
});

describe('the default client (since v0.192.0, docs/object-stores.md phase 5)', () => {
  for (const [name, s3] of Object.entries(configurations)) {
    it(`is the agent at every place: ${name}`, () => {
      // the configurations name no client
      expect(s3.client).toBeUndefined();
      expect(objectStorePods(s3)).toEqual(objectStorePods({ ...s3, client: 'agent' }));
    });
  }
});

const containersOf = (x: V1PodSpec | V1Container | V1Container[]): V1Container[] =>
  Array.isArray(x) ? x : 'containers' in x ? [...(x.initContainers ?? []), ...x.containers] : [x as V1Container];
const script = (c: V1Container) => [...(c.command ?? []), ...(c.args ?? [])].join(' ');
const env = (c: V1Container, name: string) => c.env?.find((e) => e.name === name)?.value;

describe('the agent client (s3.client: agent)', () => {
  for (const [name, base] of Object.entries(configurations)) {
    const s3: S3BackupConfiguration = { ...base, client: 'agent' };
    const pods = objectStorePods(s3);
    it(`runs fb-agent objects in the instance image, never the aws CLI: ${name}`, () => {
      for (const [where, spec] of Object.entries(pods)) {
        const all = containersOf(spec);
        const stores = all.filter((c) => env(c, 'OBJECT_STORE') === 's3');
        expect(stores.length, where).toBeGreaterThan(0);
        for (const c of all) {
          expect(script(c), `${where}: ${c.name}`).not.toMatch(/\baws (--endpoint-url .* )?s3 /);
          expect(c.image, `${where}: ${c.name}`).not.toMatch(/aws-cli/);
        }
        for (const c of stores) {
          // a Job's script names the binary, the follower's the FB_AGENT the installer sets
          expect(script(c), `${where}: ${c.name}`).toMatch(/\/opt\/fb-agent\/fb-agent objects|"\$FB_AGENT" objects/);
          expect(c.image, `${where}: ${c.name}`).toBe('firebirdsql/firebird:latest');
          expect(env(c, 'AWS_BUCKET')).toBe('bkt');
          expect(env(c, 'OBJECT_PREFIX')).toBe(base.prefix ? base.prefix.replace(/^\/+|\/+$/g, '') : undefined);
          // AWS's endpoint by the bucket's host name, as the aws CLI; an endpoint of its own by path
          expect(env(c, 'AWS_ENDPOINT')).toBe(base.endpoint);
          expect(env(c, 'AWS_VIRTUAL_HOSTED_STYLE_REQUEST')).toBe(base.endpoint ? undefined : 'true');
          expect(env(c, 'AWS_DEFAULT_REGION')).toBe(base.region ?? 'us-east-1');
          expect(env(c, 'AWS_SERVER_SIDE_ENCRYPTION')).toBe(base.encryption);
          expect(env(c, 'AWS_SSE_KMS_KEY_ID')).toBe(base.kmsKeyId);
          const keyFile = env(c, 'OBJECT_SSE_C_KEY_FILE');
          if (keyFile) {
            // the key is mounted where the agent reads it
            expect((c.volumeMounts ?? []).some((m) => keyFile.startsWith(`${m.mountPath}/`)), `${where}: ${c.name}`).toBe(true);
          } else {
            // only a listing goes without it
            expect(!base.sseCustomerKey || c.name === 'list-uploaded', `${where}: ${c.name}`).toBe(true);
          }
          expect(c.env?.some((e) => e.name === 'AWS_ACCESS_KEY_ID')).toBe(Boolean(base.secretRef));
        }
        // a Job's containers have the binary
        if (!Array.isArray(spec) && 'containers' in spec && where !== 'replica cluster archive follower') {
          for (const c of stores) expect(c.volumeMounts?.some((m) => m.mountPath === '/opt/fb-agent'), `${where}: ${c.name}`).toBe(true);
        }
      }
    });
  }

  it('keeps every key relative to the prefix, and the listings the scripts parse as the aws CLI prints them', () => {
    const pods = objectStorePods({ ...configurations['keys, endpoint and prefix'], client: 'agent' });
    const all = (where: string) => containersOf(pods[where]).map(script).join('\n');
    expect(all('logical backup')).toContain(`fb-agent objects put "/work/$f" "$f"`);
    expect(all('nbackup of a replica')).toContain(`then /opt/fb-agent/fb-agent objects put "/work/$f.ctl" "$f.ctl"; fi`);
    expect(all('scheduled nbackup with retention')).toContain(`{ /opt/fb-agent/fb-agent objects list ''; } > /work/all || true`);
    expect(all('scheduled nbackup with retention')).toContain(`fb-agent objects rm "$k.ctl" >/dev/null 2>&1 || true`);
    expect(all('logical restore')).toContain(`fb-agent objects get 'backup-b1.fbk' /work/backup.fbk`);
    expect(all('point-in-time recovery')).toContain(`fb-agent objects list --long 'journals/' > $W/journals.tmp`);
    expect(all('point-in-time recovery')).toContain(`fb-agent objects exists 'nbackup-l1-b.nbk.ctl'`);
    expect(all('point-in-time recovery of a declarative database')).toContain(`list --long 'journals/sales.fdb/'`);
    expect(all('journal archive')).toContain(`fb-agent objects sync /work/segments/ 'journals/'`);
    expect(all('journal archive')).toContain(`fb-agent objects sync $d/segments/ 'journals/'"$f/"`);
    expect(all('journal archive')).toContain(`fb-agent objects list 'journals/'"$f/" > $d/uploaded || : > $d/uploaded`);
    expect(all('bootstrap from a logical backup')).toContain(`fb-agent objects get 'nightly/x.fbk' /work/backup.fbk`);
  });

  it("runs the replica cluster's follower through the agent, the script otherwise the same", () => {
    const aws = buildArchiveFollowerContainer(cluster({ instances: 1, replica: { enabled: true, journalS3: { bucket: 'bkt', client: 'aws' } } }));
    const agent = buildArchiveFollowerContainer(cluster({ instances: 1, replica: { enabled: true, journalS3: { bucket: 'bkt', client: 'agent' } } }));
    expect(agent.command![2]).toContain(FOLLOWER_AGENT_CLIENT);
    expect(agent.command![2]).not.toContain(FOLLOWER_AWS_CLIENT);
    expect(agent.command![2].replace(FOLLOWER_AGENT_CLIENT, FOLLOWER_AWS_CLIENT)).toBe(aws.command![2]);
    expect(env(agent, 'JOURNALS')).toBe('journals/');
    // the instance pod's agent installer gives it the binary
    const pod = withAgent({ containers: [agent, aws] });
    expect(pod.containers[0].volumeMounts?.some((m) => m.mountPath === '/opt/fb-agent')).toBe(true);
    expect(pod.containers[1].volumeMounts?.some((m) => m.mountPath === '/opt/fb-agent')).toBeFalsy();
  });

  it('writes scripts the shell parses, with either client', () => {
    for (const client of ['aws', 'agent'] as const) {
      for (const [where, spec] of Object.entries(objectStorePods({ ...configurations['SSE-C'], client }))) {
        for (const c of containersOf(spec)) {
          const text = c.command?.[0] === '/bin/sh' ? (c.command[2] ?? c.args?.[0]) : undefined;
          if (!text) continue;
          const checked = spawnSync('sh', ['-n', '-c', text]);
          expect(checked.status, `${client}: ${where}: ${c.name}: ${checked.stderr}`).toBe(0);
        }
      }
    }
  });

  it('refuses another client', () => {
    expect(() => validateS3Encryption('spec.backup.s3', { bucket: 'b', client: 'rclone' as never })).toThrow(/client "rclone" is invalid/);
    expect(() => validateS3Encryption('spec.backup.s3', { bucket: 'b', client: 'agent' })).not.toThrow();
  });
});
