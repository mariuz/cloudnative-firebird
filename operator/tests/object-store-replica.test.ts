import { describe, it, expect } from 'vitest';
import { V1Container } from '@kubernetes/client-node';
import { spawnSync } from 'child_process';
import { buildRecoveryBootstrapJob } from '../src/utils/backup';
import { buildStatefulSet } from '../src/utils/resources';
import { buildArchiveFollowerContainer, FOLLOWER_AGENT_CLIENT, replicaClusterErrors } from '../src/utils/replica-cluster';
import { replicaSourceRefusal } from '../src/utils/replication';
import { validateClusterSpec } from '../src/utils/validation';
import { AzureBlobConfiguration, FirebirdCluster, GCSConfiguration, ReplicaClusterConfiguration } from '../src/types';

/**
 * docs/object-stores.md, phase 4: replica clusters following an archive in Azure Blob or GCS
 * (replica.journalAzureBlob, journalGCS), bootstrapped from a chain in any store
 */
const cluster = (name: string, spec: Partial<FirebirdCluster['spec']>, status?: FirebirdCluster['status']): FirebirdCluster => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdCluster',
  metadata: { name, namespace: 'default', uid: `${name}-uid` },
  spec: { instances: 1, storage: { size: '1Gi' }, ...spec },
  ...(status ? { status } : {}),
});
const azure: AzureBlobConfiguration = { storageAccount: 'account1', container: 'drarchive', prefix: 'src', secretRef: { name: 'azure-creds' } };
const gcs: GCSConfiguration = { bucket: 'dr', prefix: 'src', secretRef: { name: 'gcs-creds' } };
const replicaCluster = (replica: Omit<ReplicaClusterConfiguration, 'enabled'>, bootstrap: Record<string, unknown> = { azureBlob: { ...azure, prefix: 'src-backups' } }) =>
  cluster('dr', {
    replication: { enabled: true },
    replica: { enabled: true, pollIntervalSeconds: 10, ...replica },
    bootstrap: { recovery: { sourcePath: 'nbackup-l0-src-base.nbk', ...bootstrap } },
  });
const value = (c: V1Container, name: string) => c.env?.find((e) => e.name === name)?.value;

describe('replica clusters following an archive in Azure Blob or GCS (phase 4)', () => {
  for (const [name, replica, kind] of [
    ['Azure Blob', { journalAzureBlob: azure }, 'azure'],
    ['GCS', { journalGCS: gcs }, 'gcs'],
  ] as const) {
    it(`follows the source's archive in ${name} through the agent`, () => {
      const dr = replicaCluster(replica);
      expect(() => validateClusterSpec(dr)).not.toThrow();
      const follower = buildArchiveFollowerContainer(dr);
      expect(follower.command![2]).toContain(FOLLOWER_AGENT_CLIENT);
      expect(follower.image).toBe('firebirdsql/firebird:latest');
      expect(value(follower, 'OBJECT_STORE')).toBe(kind);
      expect(value(follower, 'OBJECT_PREFIX')).toBe('src');
      expect(value(follower, 'JOURNALS')).toBe('journals/');
      expect(follower.env!.some((e) => e.name.startsWith('AWS_') || e.name === 'S3_ENDPOINT')).toBe(false);
      expect(spawnSync('sh', ['-n', '-c', follower.command![2]]).status).toBe(0);

      // in the instance pod: the binary installed for it, its key from a volume of the pod
      const pod = buildStatefulSet(dr).spec!.template.spec!;
      const inPod = pod.containers.find((c) => c.name === 'archive-follower')!;
      expect(inPod.volumeMounts!.some((m) => m.mountPath === '/opt/fb-agent')).toBe(true);
      for (const m of inPod.volumeMounts!) {
        expect([...pod.volumes!.map((v) => v.name), 'firebird-data'], m.name).toContain(m.name);
      }
      if (kind === 'gcs') {
        const key = value(inPod, 'GOOGLE_SERVICE_ACCOUNT')!;
        expect(inPod.volumeMounts!.some((m) => key.startsWith(`${m.mountPath}/`))).toBe(true);
      }
    });

    it(`bootstraps from the chain and the archive, both in ${name}`, () => {
      const chain = kind === 'azure' ? { azureBlob: { ...azure, prefix: 'src-backups' } } : { gcs: { ...gcs, prefix: 'src-backups' } };
      const job = buildRecoveryBootstrapJob(replicaCluster(replica, chain));
      const containers = [...(job.spec!.template.spec!.initContainers ?? []), ...job.spec!.template.spec!.containers];
      expect(value(containers.find((c) => c.name === 'download')!, 'OBJECT_STORE')).toBe(kind);
      expect(value(containers.find((c) => c.name === 'journal-fetch')!, 'OBJECT_STORE')).toBe(kind);
      for (const c of containers) expect([...(c.command ?? []), ...(c.args ?? [])].join(' ')).not.toMatch(/\baws (--endpoint-url .* )?s3 /);
    });
  }

  it("labels the instance pods for AKS workload identity while the follower reads Azure without a Secret, not after the promotion", () => {
    const identity = { journalAzureBlob: { storageAccount: 'account1', container: 'drarchive' } };
    const following = buildStatefulSet(replicaCluster(identity));
    expect(following.spec!.template.metadata!.labels!['azure.workload.identity/use']).toBe('true');
    expect(following.spec!.selector.matchLabels!['azure.workload.identity/use']).toBeUndefined();
    const keyed = buildStatefulSet(replicaCluster({ journalAzureBlob: azure }, { gcs }));
    expect(keyed.spec!.template.metadata!.labels!['azure.workload.identity/use']).toBeUndefined();
    const promoted = replicaCluster(identity, { gcs });
    promoted.spec.replica!.enabled = false;
    expect(buildStatefulSet(promoted).spec!.template.metadata!.labels!['azure.workload.identity/use']).toBeUndefined();
  });

  it('needs exactly one archive, and not its own', () => {
    expect(replicaClusterErrors(replicaCluster({}))).toContain(
      "replica: exactly one of journalS3, journalAzureBlob and journalGCS (the source cluster's journal archive)",
    );
    expect(() => validateClusterSpec(replicaCluster({ journalS3: { bucket: 'dr' }, journalGCS: gcs }))).toThrow(
      /spec.replica: journalS3 and journalGCS exclude each other/,
    );
    expect(() => validateClusterSpec(replicaCluster({ journalAzureBlob: { ...azure, container: 'DR' } }))).toThrow(
      /spec.replica.journalAzureBlob.container/,
    );
    const own = replicaCluster({ journalAzureBlob: azure });
    own.spec.replication!.journalArchiveAzureBlob = { ...azure, secretRef: undefined };
    expect(replicaClusterErrors(own).join('; ')).toMatch(/must not be the source's journal archive/);
    // the chain in no store at all is refused
    expect(replicaClusterErrors(cluster('dr', { replication: { enabled: true }, replica: { enabled: true, journalGCS: gcs }, bootstrap: { recovery: { sourcePath: 'x.nbk' } } })).join('; ')).toMatch(
      /nbackup chain in an object store/,
    );
  });

  it('refuses to follow an archive in any store that a cluster replicates declarative databases into', () => {
    const source = cluster('src', { instances: 2, replication: { enabled: true, journalArchiveAzureBlob: azure } }, { replicatedDatabases: ['sales.fdb'] });
    expect(replicaSourceRefusal(replicaCluster({ journalAzureBlob: azure }), [source])).toMatch(/replicates declarative databases/);
    // the same names in another store are another archive
    expect(replicaSourceRefusal(replicaCluster({ journalGCS: { bucket: 'dr', prefix: 'src' } }), [source])).toBeUndefined();
  });
});
