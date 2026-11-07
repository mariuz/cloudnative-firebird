import { describe, it, expect } from 'vitest';
import { V1Container, V1Job, V1PodSpec } from '@kubernetes/client-node';
import { readFileSync } from 'fs';
import { join } from 'path';
import {
  buildBackupJob,
  buildBootstrapInitContainers,
  buildJournalArchiveCronJob,
  buildRestoreJob,
  s3TransferArgs,
} from '../src/utils/backup';
import { buildArchiveFollowerContainer, replicaClusterErrors } from '../src/utils/replica-cluster';
import { buildStatefulSet } from '../src/utils/resources';
import { validateBackupSpec, validateClusterSpec, validateRestoreSpec } from '../src/utils/validation';
import { FirebirdBackup, FirebirdCluster, FirebirdRestore, S3BackupConfiguration } from '../src/types';
import { makeCluster } from './helpers/factories';

/**
 * Server-side encryption of the S3 objects (CloudNativePG #11775): SSE-S3 or SSE-KMS on uploads,
 * SSE-C (the key from a Secret) on uploads and downloads
 */
const base = { bucket: 'bkt', prefix: 'fb/', secretRef: { name: 's3-creds' } };
const sseS3: S3BackupConfiguration = { ...base, encryption: 'AES256' };
const sseKms: S3BackupConfiguration = { ...base, encryption: 'aws:kms', kmsKeyId: 'alias/fb backups' };
const sseC: S3BackupConfiguration = { ...base, sseCustomerKey: { name: 'fb-sse' } };
const SSE_C_ARGS = '--sse-c AES256 --sse-c-key fileb:///var/run/s3-sse-c/s3/key';

const backup = (s3: S3BackupConfiguration, spec: Partial<FirebirdBackup['spec']> = {}): FirebirdBackup => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdBackup',
  metadata: { name: 'b1', namespace: 'default', uid: 'backup-uid' },
  spec: { clusterName: 'db', s3, ...spec },
});
const restore = (spec: Partial<FirebirdRestore['spec']> = {}): FirebirdRestore => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdRestore',
  metadata: { name: 'r1', namespace: 'default', uid: 'restore-uid' },
  spec: { clusterName: 'db', ...spec },
});
const podOf = (job: V1Job) => job.spec!.template.spec!;
const script = (c: V1Container) => c.args![0];
const keyVolume = (pod: V1PodSpec, name = 's3-sse-c') => pod.volumes?.find((v) => v.name === name);

describe('S3 server-side encryption arguments', () => {
  it('sets SSE-S3 and SSE-KMS on uploads only, and SSE-C both ways', () => {
    expect(s3TransferArgs(base, 'upload')).toBe('');
    expect(s3TransferArgs(sseS3, 'upload')).toBe(' --sse AES256');
    expect(s3TransferArgs(sseS3, 'download')).toBe('');
    expect(s3TransferArgs(sseKms, 'upload')).toBe(` --sse aws:kms --sse-kms-key-id 'alias/fb backups'`);
    expect(s3TransferArgs({ ...base, encryption: 'aws:kms' }, 'upload')).toBe(' --sse aws:kms');
    expect(s3TransferArgs(sseKms, 'download')).toBe('');
    expect(s3TransferArgs(sseC, 'upload')).toBe(` ${SSE_C_ARGS}`);
    expect(s3TransferArgs(sseC, 'download')).toBe(` ${SSE_C_ARGS}`);
    expect(s3TransferArgs(sseC, 'download', 'chain')).toBe(' --sse-c AES256 --sse-c-key fileb:///var/run/s3-sse-c/chain/key');
  });
});

describe('backups', () => {
  it('upload with SSE-S3 or SSE-KMS, without a key volume', () => {
    const pod = podOf(buildBackupJob(backup(sseKms), makeCluster()));
    expect(script(pod.containers[0])).toContain(`aws s3 cp --sse aws:kms --sse-kms-key-id 'alias/fb backups' "/work/$f" "s3://bkt/fb/$f"`);
    expect(pod.volumes).toEqual([{ name: 'work', emptyDir: {} }]);
  });

  it('upload with SSE-C, the key mounted from its Secret, also the position of a replica nbackup', () => {
    const cluster = makeCluster({ replication: { enabled: true } });
    const pod = podOf(buildBackupJob(backup(sseC, { type: 'physical', target: 'prefer-standby' }), cluster, 'db-1', true));
    const upload = pod.containers[0];
    expect(script(upload)).toContain(`aws s3 cp ${SSE_C_ARGS} "/work/$f.ctl"`);
    expect(script(upload)).toContain(`aws s3 cp ${SSE_C_ARGS} "/work/$f" "s3://bkt/fb/$f"`);
    expect(upload.volumeMounts).toContainEqual({ name: 's3-sse-c', mountPath: '/var/run/s3-sse-c/s3', readOnly: true });
    expect(keyVolume(pod)).toEqual({
      name: 's3-sse-c',
      secret: { secretName: 'fb-sse', items: [{ key: 'SSE_C_KEY', path: 'key' }], defaultMode: 0o444 },
    });
    // only the upload container reads it
    expect(pod.initContainers!.flatMap((c) => c.volumeMounts ?? []).map((m) => m.name)).not.toContain('s3-sse-c');
  });

  it('read the key under the configured Secret key', () => {
    const pod = podOf(buildBackupJob(backup({ ...base, sseCustomerKey: { name: 'fb-sse', key: 'aes' } }), makeCluster()));
    expect(keyVolume(pod)?.secret?.items).toEqual([{ key: 'aes', path: 'key' }]);
  });
});

describe('restores', () => {
  it('download an SSE-C object with the key, and an SSE-S3 one without arguments', () => {
    const logical = podOf(buildRestoreJob(restore(), makeCluster(), { type: 'logical', path: 'x.fbk', s3: sseC }));
    expect(script(logical.initContainers![0])).toContain(`aws s3 cp ${SSE_C_ARGS} 's3://bkt/fb/x.fbk' /work/backup.fbk`);
    expect(keyVolume(logical)).toBeDefined();
    const physical = podOf(
      buildRestoreJob(restore(), makeCluster({ replication: { enabled: true } }), { type: 'physical', path: 'l0.nbk', s3: sseC }, 'db-1'),
    );
    expect(script(physical.initContainers![0])).toContain(`aws s3 cp ${SSE_C_ARGS} 's3://bkt/fb/l0.nbk'`);
    expect(keyVolume(physical)).toBeDefined();
    const plain = podOf(buildRestoreJob(restore(), makeCluster(), { type: 'logical', path: 'x.fbk', s3: sseS3 }));
    expect(script(plain.initContainers![0])).toContain(`set -eu; aws s3 cp 's3://bkt/fb/x.fbk' /work/backup.fbk`);
    expect(keyVolume(plain)).toBeUndefined();
  });

  it('to a point in time read the chain and the journal archive each with its own key', () => {
    const journals = { bucket: 'arch', prefix: 'prod', sseCustomerKey: { name: 'arch-sse' } };
    const cluster = makeCluster({ replication: { enabled: true, journalArchiveS3: journals } });
    const chain = { type: 'physical' as const, path: 'l0.nbk', s3: sseC };
    const pod = podOf(buildRestoreJob(restore({ restoreType: 'physical', pointInTime: { targetSegment: 9 } }), cluster, chain, 'db-1'));
    const download = pod.initContainers![0];
    expect(script(download)).toContain(`s3 cp --sse-c AES256 --sse-c-key fileb:///var/run/s3-sse-c/chain/key 's3://bkt/fb/l0.nbk' /work/chain/0.nbk`);
    expect(script(download)).toContain(`s3 cp --sse-c AES256 --sse-c-key fileb:///var/run/s3-sse-c/chain/key 's3://bkt/fb/l0.nbk.ctl'`);
    expect(download.volumeMounts?.map((m) => m.name)).toContain('chain-sse-c');
    const fetch = pod.containers.find((c) => c.name === 'journal-fetch')!;
    expect(script(fetch)).toContain(
      `s3 cp --only-show-errors --sse-c AES256 --sse-c-key fileb:///var/run/s3-sse-c/journals/key 's3://arch/prod/journals/'"$s"`,
    );
    expect(fetch.volumeMounts?.map((m) => m.name)).toContain('journals-sse-c');
    expect(keyVolume(pod, 'chain-sse-c')?.secret?.secretName).toBe('fb-sse');
    expect(keyVolume(pod, 'journals-sse-c')?.secret?.secretName).toBe('arch-sse');
  });
});

describe('the journal archive', () => {
  it('syncs the segments with the encryption setting', () => {
    const pod = (s3: S3BackupConfiguration) =>
      buildJournalArchiveCronJob(makeCluster({ replication: { enabled: true, journalArchiveS3: s3 } }), 'db-0')!.spec!.jobTemplate.spec!.template.spec!;
    const upload = (p: V1PodSpec) => [...(p.initContainers ?? []), ...p.containers].find((c) => c.name === 'upload')!;
    expect(script(upload(pod(sseS3)))).toBe(`set -eu; aws s3 sync --sse AES256 /work/segments/ 's3://bkt/fb/journals/'`);
    const c = pod(sseC);
    expect(script(upload(c))).toBe(`set -eu; aws s3 sync ${SSE_C_ARGS} /work/segments/ 's3://bkt/fb/journals/'`);
    expect(upload(c).volumeMounts?.map((m) => m.name)).toContain('s3-sse-c');
    expect(keyVolume(c)).toBeDefined();
  });

  it('is followed by a replica cluster with the archive key', () => {
    const journalS3 = { bucket: 'dr', prefix: 'prod', sseCustomerKey: { name: 'dr-sse' } };
    const cluster = makeCluster({
      instances: 1,
      replication: { enabled: true },
      replica: { enabled: true, journalS3 },
      bootstrap: { recovery: { sourcePath: 'l0.nbk', s3: { bucket: 'dr', prefix: 'prod', sseCustomerKey: { name: 'dr-sse' } } } },
    });
    expect(replicaClusterErrors(cluster)).toEqual([]);
    const follower = buildArchiveFollowerContainer(cluster);
    expect(follower.env).toContainEqual({ name: 'S3_SSE_C_KEY_FILE', value: '/var/run/s3-sse-c/follow/key' });
    expect(follower.volumeMounts).toContainEqual({ name: 'follow-sse-c', mountPath: '/var/run/s3-sse-c/follow', readOnly: true });
    // the bootstrap download has its own volume: both can be in the pod
    const pod = buildStatefulSet(cluster).spec!.template.spec!;
    expect(keyVolume(pod, 'follow-sse-c')?.secret?.secretName).toBe('dr-sse');
    expect(keyVolume(pod, 's3-sse-c')?.secret?.secretName).toBe('dr-sse');
    const names = pod.volumes!.map((v) => v.name);
    expect(new Set(names).size).toBe(names.length);
    const follow = readFileSync(join(__dirname, '..', 'src', 'replication', 'archive-follower.sh'), 'utf8');
    expect(follow).toContain('aws_s3 cp --only-show-errors ${S3_SSE_C_KEY_FILE:+--sse-c AES256 --sse-c-key "fileb://$S3_SSE_C_KEY_FILE"} "$JOURNALS$name"');
    // no key volume without SSE-C
    const plain = buildStatefulSet(makeCluster({ ...cluster.spec, replica: { enabled: true, journalS3: { bucket: 'dr' } } }));
    expect(plain.spec!.template.spec!.volumes!.map((v) => v.name)).not.toContain('follow-sse-c');
    expect(buildArchiveFollowerContainer(makeCluster({ ...cluster.spec, replica: { enabled: true, journalS3: { bucket: 'dr' } } })).env).not.toContainEqual(
      expect.objectContaining({ name: 'S3_SSE_C_KEY_FILE' }),
    );
  });

  it('bootstraps a cluster from an SSE-C backup', () => {
    const cluster = makeCluster({ bootstrap: { recovery: { s3: sseC } } });
    const [download] = buildBootstrapInitContainers(cluster);
    expect(script(download)).toContain(`aws s3 cp ${SSE_C_ARGS} 's3://bkt/fb/backup.fbk' /work/backup.fbk`);
    expect(download.volumeMounts?.map((m) => m.name)).toContain('s3-sse-c');
    expect(keyVolume(buildStatefulSet(cluster).spec!.template.spec!)?.secret?.secretName).toBe('fb-sse');
  });
});

describe('validation', () => {
  const cluster = (s3: S3BackupConfiguration): FirebirdCluster => makeCluster({ backup: { enabled: true, s3 } });

  it('accepts each mode', () => {
    for (const s3 of [base, sseS3, sseKms, sseC]) {
      expect(() => validateClusterSpec(cluster(s3))).not.toThrow();
      expect(() => validateBackupSpec(backup(s3))).not.toThrow();
    }
  });

  it('rejects an unknown mode, a KMS key without aws:kms, SSE-C with encryption, and a Secret without a name', () => {
    expect(() => validateBackupSpec(backup({ ...base, encryption: 'aes' as never }))).toThrow(/spec.s3.encryption "aes" is invalid/);
    expect(() => validateClusterSpec(cluster({ ...base, encryption: 'AES256', kmsKeyId: 'k' }))).toThrow(/spec.backup.s3.kmsKeyId needs encryption "aws:kms"/);
    expect(() => validateBackupSpec(backup({ ...sseC, encryption: 'AES256' }))).toThrow(/cannot be combined/);
    expect(() => validateBackupSpec(backup({ ...base, sseCustomerKey: { name: ' ' } }))).toThrow(/sseCustomerKey.name is required/);
    expect(() => validateBackupSpec(backup({ ...base, sseCustomerKey: { name: 'k', key: 'a/b' } }))).toThrow(/not a valid Secret key/);
    expect(() =>
      validateClusterSpec(makeCluster({ replication: { enabled: true, journalArchiveS3: { ...base, encryption: 'x' as never } } })),
    ).toThrow(/spec.replication.journalArchiveS3.encryption/);
    expect(() => validateClusterSpec(makeCluster({ bootstrap: { recovery: { s3: { ...sseC, encryption: 'AES256' } } } }))).toThrow(
      /spec.bootstrap.recovery.s3.sseCustomerKey and/,
    );
    expect(() => validateRestoreSpec(restore({ s3: { ...base, kmsKeyId: 'k' }, backupPath: 'x.fbk' }))).toThrow(/kmsKeyId/);
  });
});

describe('the CRD schemas', () => {
  it('carry the encryption fields in every S3 configuration', () => {
    for (const [file, count] of [['firebirdbackup', 1], ['firebirdcluster', 5], ['firebirdrestore', 2], ['firebirdscheduledbackup', 1]] as const) {
      const crd = readFileSync(join(__dirname, '..', '..', 'config', 'crds', `${file}.yaml`), 'utf8');
      expect(crd.match(/^ *clientImage:$/gm)?.length).toBe(count);
      expect(crd.match(/^ *sseCustomerKey:$/gm)?.length).toBe(count);
      expect(crd.match(/enum: \["AES256", "aws:kms"\]/g)?.length).toBe(count);
    }
  });
});
