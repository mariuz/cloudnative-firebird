import { describe, it, expect } from 'vitest';
import { V1Container, V1Job, V1PodSpec } from '@kubernetes/client-node';
import { spawnSync } from 'child_process';
import {
  backupLocation,
  bootstrapPodLabels,
  bootstrapVolumes,
  buildBackupCronJob,
  buildBackupJob,
  buildBootstrapInitContainers,
  buildRecoveryBootstrapJob,
  buildRestoreJob,
  buildScheduledBackupCronJob,
} from '../src/utils/backup';
import { objectStoreOf, objectUri } from '../src/utils/object-store';
import { buildStatefulSet } from '../src/utils/resources';
import { validateBackupSpec, validateClusterSpec, validateRestoreSpec, validateScheduledBackupSpec } from '../src/utils/validation';
import {
  AzureBlobConfiguration,
  FirebirdBackup,
  FirebirdCluster,
  FirebirdRestore,
  FirebirdScheduledBackup,
  GCSConfiguration,
} from '../src/types';

/**
 * docs/object-stores.md, phase 2: backups, retention, restores and bootstrap on Azure Blob and
 * GCS, always through fb-agent objects (azureBlob and gcs beside every s3 field of those places)
 */
const cluster = (spec: Partial<FirebirdCluster['spec']>): FirebirdCluster => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdCluster',
  metadata: { name: 'db', namespace: 'default', uid: 'cluster-uid' },
  spec: { instances: 3, storage: { size: '1Gi' }, ...spec },
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

type Fields = { azureBlob: AzureBlobConfiguration } | { gcs: GCSConfiguration };
type Place = { template?: Record<string, string>; spec: V1PodSpec | V1Container[] };

/** Every phase 2 place, for one store's fields: the pod template's labels and its spec */
function places(fields: Fields): Record<string, Place> {
  const job = (j: V1Job): Place => ({ template: j.spec!.template.metadata?.labels ?? {}, spec: j.spec!.template.spec! });
  const cron = (c: ReturnType<typeof buildBackupCronJob>): Place => ({
    template: c.spec!.jobTemplate.spec!.template.metadata?.labels ?? {},
    spec: c.spec!.jobTemplate.spec!.template.spec!,
  });
  const store = objectStoreOf(fields)!;
  const chain = { type: 'physical' as const, path: 'nbackup-l0-a.nbk', incrementalPaths: ['nbackup-l1-b.nbk'], s3: store };
  const replicated = cluster({ replication: { enabled: true, journalArchiveS3: { bucket: 'journals', client: 'agent' } } });
  const recovery = cluster({
    instances: 1,
    bootstrap: { recovery: { sourcePath: 'nbackup-l0.nbk', ...fields, pointInTime: { journalS3: { bucket: 'journals', client: 'agent' } } } },
  });
  const bootstrap = cluster({ bootstrap: { recovery: { sourcePath: 'nightly/x.fbk', ...fields } } });
  return {
    'logical backup': job(buildBackupJob(backup({ ...fields }), cluster({}), 'db-0')),
    'nbackup': job(buildBackupJob(backup({ type: 'physical', ...fields }), cluster({}), 'db-0')),
    'scheduled nbackup with retention': cron(
      buildScheduledBackupCronJob(scheduled({ type: 'physical', retentionPolicy: '4w', ...fields } as Partial<FirebirdScheduledBackup['spec']>), cluster({}), 'db-0'),
    ),
    'cluster backup schedule': cron(buildBackupCronJob(cluster({ backup: { enabled: true, schedule: '0 3 * * *', retentionPolicy: '7d', ...fields } }), 'db-0')),
    'logical restore': job(buildRestoreJob(restore({ backupPath: 'backup-b1.fbk', ...fields }), cluster({}), { type: 'logical', path: 'backup-b1.fbk', s3: store }, 'db-0')),
    'nbackup restore': job(buildRestoreJob(restore({ restoreType: 'physical' }), cluster({}), chain, 'db-0')),
    'point-in-time recovery (the chain only)': job(
      buildRestoreJob(restore({ restoreType: 'physical', pointInTime: { targetSegment: 12 } }), replicated, chain, 'db-0'),
    ),
    'recovery bootstrap (the chain only)': job(buildRecoveryBootstrapJob(recovery)),
    'bootstrap from a logical backup': { template: bootstrapPodLabels(bootstrap), spec: buildBootstrapInitContainers(bootstrap) },
  };
}

const containersOf = (x: V1PodSpec | V1Container[]): V1Container[] => (Array.isArray(x) ? x : [...(x.initContainers ?? []), ...x.containers]);
const script = (c: V1Container) => [...(c.command ?? []), ...(c.args ?? [])].join(' ');
const env = (c: V1Container, name: string) => c.env?.find((e) => e.name === name);
const value = (c: V1Container, name: string) => env(c, name)?.value;

const azure: AzureBlobConfiguration = { storageAccount: 'account1', container: 'backups', prefix: '/prod/', secretRef: { name: 'azure-creds' } };
const gcs: GCSConfiguration = { bucket: 'fbk', prefix: 'prod', secretRef: { name: 'gcs-creds' } };

describe('Azure Blob and GCS stores (phase 2)', () => {
  for (const [name, fields, kind] of [
    ['Azure Blob with an account key or SAS', { azureBlob: azure }, 'azure'],
    ['Azure Blob through workload identity, Azurite', { azureBlob: { storageAccount: 'devstoreaccount1', container: 'fbk', endpoint: 'http://azurite:10000/devstoreaccount1' } }, 'azure'],
    ['GCS with a service account key', { gcs }, 'gcs'],
    ['GCS through workload identity, an endpoint', { gcs: { bucket: 'fbk', endpoint: 'http://fake-gcs:4443' } }, 'gcs'],
  ] as [string, Fields, 'azure' | 'gcs'][]) {
    const config = 'azureBlob' in fields ? fields.azureBlob : fields.gcs;
    it(`runs every transfer through fb-agent objects, with the store's own settings: ${name}`, () => {
      for (const [where, place] of Object.entries(places(fields))) {
        const all = containersOf(place.spec);
        const stores = all.filter((c) => value(c, 'OBJECT_STORE') === kind);
        expect(stores.length, where).toBeGreaterThan(0);
        for (const c of all) {
          expect(script(c), `${where}: ${c.name}`).not.toMatch(/\baws (--endpoint-url .* )?s3 /);
          expect(c.image, `${where}: ${c.name}`).not.toMatch(/aws-cli/);
        }
        for (const c of stores) {
          expect(script(c), `${where}: ${c.name}`).toMatch(/\/opt\/fb-agent\/fb-agent objects/);
          expect(c.image).toBe('firebirdsql/firebird:latest');
          expect(c.env?.some((e) => e.name.startsWith('AWS_')), `${where}: ${c.name}`).toBe(false);
          expect(value(c, 'OBJECT_PREFIX')).toBe(config.prefix ? 'prod' : undefined);
          if (kind === 'azure') {
            const a = config as AzureBlobConfiguration;
            expect(value(c, 'AZURE_STORAGE_ACCOUNT_NAME')).toBe(a.storageAccount);
            expect(value(c, 'AZURE_CONTAINER_NAME')).toBe(a.container);
            expect(value(c, 'AZURE_STORAGE_ENDPOINT')).toBe(a.endpoint);
            // either key of the Secret: the one it holds is set, the other left out
            for (const key of ['AZURE_STORAGE_ACCOUNT_KEY', 'AZURE_STORAGE_SAS_KEY']) {
              expect(env(c, key)?.valueFrom?.secretKeyRef).toEqual(a.secretRef ? { name: a.secretRef.name, key, optional: true } : undefined);
            }
          } else {
            const g = config as GCSConfiguration;
            expect(value(c, 'GOOGLE_BUCKET')).toBe(g.bucket);
            expect(value(c, 'GOOGLE_BASE_URL')).toBe(g.endpoint);
            const keyFile = value(c, 'GOOGLE_SERVICE_ACCOUNT');
            expect(Boolean(keyFile)).toBe(Boolean(g.secretRef));
            if (keyFile) {
              const mount = c.volumeMounts!.find((m) => keyFile.startsWith(`${m.mountPath}/`))!;
              expect(mount, `${where}: ${c.name}`).toBeDefined();
              // the pod has the Secret's volume (the instance pod's for the bootstrap init containers)
              const volumes = Array.isArray(place.spec) ? bootstrapVolumes(cluster({ bootstrap: { recovery: { sourcePath: 'x', gcs: g } } })) : place.spec.volumes!;
              const volume = volumes.find((v) => v.name === mount.name)!;
              expect(volume.secret).toEqual({ secretName: g.secretRef!.name, items: [{ key: 'APPLICATION_CREDENTIALS', path: 'key.json' }], defaultMode: 0o444 });
            }
          }
          if (!Array.isArray(place.spec)) expect(c.volumeMounts?.some((m) => m.mountPath === '/opt/fb-agent'), `${where}: ${c.name}`).toBe(true);
        }
        // AKS workload identity's label, on the pods reaching Azure without a Secret only
        const identity = kind === 'azure' && !config.secretRef;
        expect(place.template?.['azure.workload.identity/use'], where).toBe(identity ? 'true' : undefined);
      }
    });

    it(`writes scripts the shell parses: ${name}`, () => {
      for (const [where, place] of Object.entries(places(fields))) {
        for (const c of containersOf(place.spec)) {
          if (c.command?.[0] !== '/bin/sh') continue;
          const text = c.command[1] === '-c' && c.command.length > 2 ? c.command[2] : c.args![0];
          const parsed = spawnSync('sh', ['-n', '-c', text]);
          expect(parsed.status, `${where}: ${c.name}: ${parsed.stderr}`).toBe(0);
        }
      }
    });
  }

  it('keeps the point-in-time journals on S3 (phase 3) while the chain comes from the store', () => {
    const recovery = places({ azureBlob: azure })['point-in-time recovery (the chain only)'];
    const all = containersOf(recovery.spec);
    expect(value(all.find((c) => c.name === 'download')!, 'OBJECT_STORE')).toBe('azure');
    expect(value(all.find((c) => c.name === 'journal-fetch')!, 'OBJECT_STORE')).toBe('s3');
  });

  it('names the commands and keys as for S3, relative to the prefix', () => {
    const all = (where: string) => containersOf(places({ gcs })[where].spec).map(script).join('\n');
    expect(all('logical backup')).toContain(`fb-agent objects put "/work/$f" "$f"`);
    expect(all('scheduled nbackup with retention')).toContain(`{ /opt/fb-agent/fb-agent objects list ''; } > /work/all || true`);
    expect(all('logical restore')).toContain(`fb-agent objects get 'backup-b1.fbk' /work/backup.fbk`);
    expect(all('nbackup restore')).toContain(`fb-agent objects get 'nbackup-l1-b.nbk' /work/restore-r1-1.nbk`);
    expect(all('bootstrap from a logical backup')).toContain(`fb-agent objects get 'nightly/x.fbk' /work/backup.fbk`);
  });

  it('gives the backups their store URI as their location', () => {
    expect(backupLocation('backup-b1.fbk', objectStoreOf({ azureBlob: azure }))).toBe('azure://account1/backups/prod/backup-b1.fbk');
    expect(backupLocation('backup-b1.fbk', objectStoreOf({ gcs }))).toBe('gs://fbk/prod/backup-b1.fbk');
    expect(backupLocation('backup-b1.fbk', objectStoreOf({ gcs: { bucket: 'fbk' } }))).toBe('gs://fbk/backup-b1.fbk');
    expect(objectUri({ bucket: 'b', prefix: 'p/' }, 'k')).toBe('s3://b/p/k');
  });

  it('refuses more than one store, and invalid names', () => {
    expect(() => validateBackupSpec(backup({ s3: { bucket: 'b' }, azureBlob: azure }))).toThrow(/s3 and azureBlob exclude each other/);
    expect(() => validateScheduledBackupSpec(scheduled({ azureBlob: azure, gcs }))).toThrow(/azureBlob and gcs exclude each other/);
    expect(() => validateRestoreSpec(restore({ backupPath: 'x.fbk', gcs: { bucket: '' } }))).toThrow(/spec.gcs.bucket is required/);
    expect(() => validateRestoreSpec(restore({ backupPath: 'x.fbk', azureBlob: { ...azure, storageAccount: 'My_Account' } }))).toThrow(/storageAccount/);
    expect(() => validateRestoreSpec(restore({ backupPath: 'x.fbk', azureBlob: { ...azure, container: 'Backups' } }))).toThrow(/container/);
    expect(() =>
      validateClusterSpec(cluster({ backup: { enabled: true, schedule: '0 2 * * *', gcs, azureBlob: azure } })),
    ).toThrow(/spec.backup: azureBlob and gcs exclude each other/);
    expect(() =>
      validateClusterSpec(cluster({ bootstrap: { recovery: { sourcePath: 'x.fbk', s3: { bucket: 'b' }, gcs } } })),
    ).toThrow(/spec.bootstrap.recovery: s3 and gcs exclude each other/);
    // a key outside the data directory is fine in a store, as with s3
    expect(() => validateRestoreSpec(restore({ backupPath: 'daily/../x.fbk', gcs }))).not.toThrow();
    expect(() => validateClusterSpec(cluster({ bootstrap: { recovery: { azureBlob: azure } } }))).not.toThrow();
    expect(() => validateBackupSpec(backup({ type: 'volumeSnapshot', gcs }))).toThrow(/spec.gcs is not used by volume snapshots/);
  });
  it("bootstraps an instance from the store: the binary installed first, the key mounted, workload identity's label", () => {
    for (const fields of [{ gcs }, { azureBlob: { storageAccount: 'account1', container: 'backups' } }] as Fields[]) {
      const set = buildStatefulSet(cluster({ instances: 1, bootstrap: { recovery: { sourcePath: 'x.fbk', ...fields } } }));
      const template = set.spec!.template;
      const init = template.spec!.initContainers!;
      const download = init.findIndex((c) => c.name === 'bootstrap-download');
      const installer = init.findIndex((c) => (c.volumeMounts ?? []).some((m) => m.mountPath === '/agent'));
      expect(download).toBeGreaterThan(installer);
      expect(installer).toBeGreaterThanOrEqual(0);
      expect(init[download].volumeMounts!.some((m) => m.mountPath === '/opt/fb-agent')).toBe(true);
      const volumes = [...template.spec!.volumes!, ...(set.spec!.volumeClaimTemplates ?? []).map((t) => t.metadata!)].map((v) => v.name);
      for (const m of init[download].volumeMounts!) expect(volumes, m.name).toContain(m.name);
      expect(template.metadata!.labels!['azure.workload.identity/use']).toBe('azureBlob' in fields ? 'true' : undefined);
      // the selector is the cluster's, not the bootstrap's
      expect(set.spec!.selector.matchLabels!['azure.workload.identity/use']).toBeUndefined();
    }
  });
});
