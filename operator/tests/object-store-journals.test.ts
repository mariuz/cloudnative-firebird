import { describe, it, expect } from 'vitest';
import { V1Container, V1PodSpec } from '@kubernetes/client-node';
import { spawnSync } from 'child_process';
import {
  buildJournalArchiveCronJob,
  buildRecoveryBootstrapJob,
  buildRestoreJob,
  pointInTimeSourceError,
} from '../src/utils/backup';
import { buildStatefulSet } from '../src/utils/resources';
import { objectStoreOf } from '../src/utils/object-store';
import { replicaClusterErrors } from '../src/utils/replica-cluster';
import { validateClusterSpec, validateRestoreSpec } from '../src/utils/validation';
import { AzureBlobConfiguration, FirebirdCluster, FirebirdRestore, GCSConfiguration } from '../src/types';

/**
 * docs/object-stores.md, phase 3: the journal archive (replication.journalArchiveAzureBlob,
 * journalArchiveGCS) and point-in-time recovery's journals (pointInTime.journalAzureBlob,
 * journalGCS), declarative databases' journals/<file>/ included, through fb-agent objects
 */
const cluster = (spec: Partial<FirebirdCluster['spec']>, status?: FirebirdCluster['status']): FirebirdCluster => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdCluster',
  metadata: { name: 'db', namespace: 'default', uid: 'cluster-uid' },
  spec: { instances: 3, storage: { size: '1Gi' }, ...spec },
  ...(status ? { status } : {}),
});
const restore = (spec: Partial<FirebirdRestore['spec']>): FirebirdRestore => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdRestore',
  metadata: { name: 'r1', namespace: 'default', uid: 'restore-uid' },
  spec: { clusterName: 'db', ...spec },
});

const azure: AzureBlobConfiguration = { storageAccount: 'account1', container: 'journals', prefix: 'prod', secretRef: { name: 'azure-creds' } };
const azureIdentity: AzureBlobConfiguration = { storageAccount: 'account1', container: 'journals' };
const gcs: GCSConfiguration = { bucket: 'journals', prefix: 'prod', secretRef: { name: 'gcs-creds' } };

const containersOf = (x: V1PodSpec): V1Container[] => [...(x.initContainers ?? []), ...x.containers];
const script = (c: V1Container) => [...(c.command ?? []), ...(c.args ?? [])].join(' ');
const value = (c: V1Container, name: string) => c.env?.find((e) => e.name === name)?.value;

/** Every store container of the pod runs the agent against the store, its key mounted from a volume of the pod */
function expectAgentStore(pod: V1PodSpec, kind: 'azure' | 'gcs', where: string, containers?: string[]) {
  const all = containersOf(pod);
  const stores = all.filter((c) => value(c, 'OBJECT_STORE') === kind && (!containers || containers.includes(c.name)));
  expect(stores.length, where).toBeGreaterThan(0);
  for (const c of all) {
    expect(script(c), `${where}: ${c.name}`).not.toMatch(/\baws (--endpoint-url .* )?s3 /);
    expect(c.image ?? '', `${where}: ${c.name}`).not.toMatch(/aws-cli/);
  }
  for (const c of stores) {
    expect(script(c), `${where}: ${c.name}`).toMatch(/\/opt\/fb-agent\/fb-agent objects/);
    expect(value(c, 'OBJECT_PREFIX'), `${where}: ${c.name}`).toBe('prod');
    for (const m of c.volumeMounts ?? []) expect(pod.volumes!.map((v) => v.name), `${where}: ${c.name}`).toContain(m.name);
    const key = value(c, 'GOOGLE_SERVICE_ACCOUNT');
    if (kind === 'gcs') expect(c.volumeMounts!.some((m) => key && key.startsWith(`${m.mountPath}/`)), `${where}: ${c.name}`).toBe(true);
  }
  return stores;
}

describe('the journal archive and point-in-time recovery on Azure Blob and GCS (phase 3)', () => {
  for (const [name, archive, kind] of [
    ['Azure Blob', { journalArchiveAzureBlob: azure }, 'azure'],
    ['GCS', { journalArchiveGCS: gcs }, 'gcs'],
  ] as const) {
    it(`archives the segments to ${name}, the declarative databases' under journals/<file>/`, () => {
      const replicated = cluster({ replication: { enabled: true, ...archive } }, { replicatedDatabases: ['sales.fdb'] });
      const job = buildJournalArchiveCronJob(replicated, 'db-0')!;
      const pod = job.spec!.jobTemplate.spec!.template.spec!;
      const stores = expectAgentStore(pod, kind, 'journal archive');
      const all = stores.map(script).join('\n');
      expect(all).toContain(`fb-agent objects sync /work/segments/ 'journals/'`);
      expect(all).toContain(`fb-agent objects sync $d/segments/ 'journals/'"$f/"`);
      expect(all).toContain(`fb-agent objects list 'journals/'"$f/" > $d/uploaded || : > $d/uploaded`);
      for (const c of containersOf(pod)) {
        if (c.command?.[0] !== '/bin/sh') continue;
        const text = c.command.length > 2 ? c.command[2] : c.args![0];
        expect(spawnSync('sh', ['-n', '-c', text]).status, c.name).toBe(0);
      }
    });

    it(`tells the instances there is an archive in ${name} (recovery points, pruning what was uploaded)`, () => {
      const set = buildStatefulSet(cluster({ replication: { enabled: true, pruneAppliedSegments: true, ...archive } }));
      const env = set.spec!.template.spec!.containers.flatMap((c) => c.env ?? []);
      expect(env.find((e) => e.name === 'RECOVERY_POINTS')?.value).toBe('true');
      expect(env.find((e) => e.name === 'ARCHIVE_UPLOAD')?.value).toBe('true');
    });

    it(`recovers to a point in time from the cluster's archive in ${name}`, () => {
      const replicated = cluster({ replication: { enabled: true, ...archive } });
      const r = restore({ restoreType: 'physical', backupPath: 'nbackup-l0-a.nbk', pointInTime: { targetSegment: 12 } });
      const source = { type: 'physical' as const, path: 'nbackup-l0-a.nbk', s3: { bucket: 'backups', client: 'agent' as const } };
      expect(pointInTimeSourceError(r, replicated, source)).toBeUndefined();
      const job = buildRestoreJob(r, replicated, source, 'db-0');
      const [fetch] = expectAgentStore(job.spec!.template.spec!, kind, 'point-in-time recovery', ['journal-fetch']);
      expect(script(fetch)).toContain(`fb-agent objects list --long 'journals/' > $W/journals.tmp`);
    });
  }

  it("takes the restore's own journal archive over the cluster's", () => {
    const replicated = cluster({ replication: { enabled: true, journalArchiveS3: { bucket: 'j' } } });
    const r = restore({ restoreType: 'physical', backupPath: 'x.nbk', pointInTime: { journalAzureBlob: azure } });
    const job = buildRestoreJob(r, replicated, { type: 'physical', path: 'x.nbk', s3: { bucket: 'backups', client: 'agent' } }, 'db-0');
    expectAgentStore(job.spec!.template.spec!, 'azure', 'restore', ['journal-fetch']);
  });

  it('bootstraps a cluster to a point in time from a journal archive in Azure Blob, through workload identity', () => {
    const recovering = cluster({
      instances: 1,
      bootstrap: {
        recovery: { sourcePath: 'nbackup-l0.nbk', gcs: { ...gcs, bucket: 'backups' }, pointInTime: { journalAzureBlob: { ...azureIdentity, prefix: 'prod' } } },
      },
    });
    const job = buildRecoveryBootstrapJob(recovering);
    const pod = job.spec!.template.spec!;
    expectAgentStore(pod, 'azure', 'recovery bootstrap', ['journal-fetch']);
    expectAgentStore(pod, 'gcs', 'recovery bootstrap', ['download']);
    expect(job.spec!.template.metadata!.labels!['azure.workload.identity/use']).toBe('true');
    expect(() => validateClusterSpec(recovering)).not.toThrow();
  });

  it('refuses two archives, and recovering from its own archive', () => {
    expect(() =>
      validateClusterSpec(cluster({ replication: { enabled: true, journalArchiveS3: { bucket: 'j' }, journalArchiveGCS: gcs } })),
    ).toThrow(/spec.replication: journalArchiveS3 and journalArchiveGCS exclude each other/);
    expect(() =>
      validateClusterSpec(cluster({ replication: { enabled: true, journalArchiveAzureBlob: { ...azure, storageAccount: 'Bad_Name' } } })),
    ).toThrow(/spec.replication.journalArchiveAzureBlob.storageAccount/);
    expect(() =>
      validateRestoreSpec(restore({ restoreType: 'physical', backupPath: 'x.nbk', gcs, pointInTime: { journalS3: { bucket: 'j' }, journalAzureBlob: azure } })),
    ).toThrow(/spec.pointInTime: journalS3 and journalAzureBlob exclude each other/);
    expect(() =>
      validateClusterSpec(
        cluster({
          instances: 1,
          replication: { enabled: true, journalArchiveAzureBlob: azure },
          bootstrap: { recovery: { sourcePath: 'x.nbk', azureBlob: { ...azure, container: 'backups' }, pointInTime: { journalAzureBlob: { ...azure, secretRef: undefined } } } },
        }),
      ),
    ).toThrow(/would archive its segments over the ones it recovers from/);
    // the same container under another account is another archive
    expect(() =>
      validateClusterSpec(
        cluster({
          instances: 1,
          replication: { enabled: true, journalArchiveAzureBlob: azure },
          bootstrap: { recovery: { sourcePath: 'x.nbk', azureBlob: { ...azure, container: 'backups' }, pointInTime: { journalAzureBlob: { ...azure, storageAccount: 'account2' } } } },
        }),
      ),
    ).not.toThrow();
    expect(pointInTimeSourceError(restore({ pointInTime: {} }), cluster({}), { type: 'physical', path: 'x.nbk', s3: objectStoreOf({ gcs })! })).toMatch(
      /journalArchiveGCS/,
    );
  });

  it("keeps a replica cluster from following its own archive, whatever the archive's store", () => {
    const errors = replicaClusterErrors(
      cluster({
        instances: 1,
        replication: { enabled: true, journalArchiveGCS: { bucket: 'dr', prefix: 'src' } },
        replica: { enabled: true, journalS3: { bucket: 'dr', prefix: 'src' } },
        bootstrap: { recovery: { sourcePath: 'x.nbk', s3: { bucket: 'dr' } } },
      }),
    );
    // a GCS archive is not the S3 one the replica follows, even with the same names
    expect(errors.some((e) => e.includes('must not be the source'))).toBe(false);
  });
});
