import { describe, it, expect } from 'vitest';
import { buildStatefulSet } from '../src/utils/resources';
import { FirebirdCluster } from '../src/types';

const cluster = (replication: NonNullable<FirebirdCluster['spec']['replication']>): FirebirdCluster => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdCluster',
  metadata: { name: 'db', namespace: 'default', uid: 'c' },
  spec: { instances: 2, storage: { size: '1Gi' }, replication },
});
const serverEnv = (c: FirebirdCluster) =>
  Object.fromEntries(
    buildStatefulSet(c)
      .spec!.template.spec!.containers.find((x) => x.name === 'segment-server')!
      .env!.map((e) => [e.name, e.value]),
  );

describe('pruneAppliedSegments', () => {
  it('tells the segment server to prune applied (and uploaded) segments only when enabled', () => {
    const s3 = { bucket: 'b', secretRef: { name: 's' } };
    expect(serverEnv(cluster({ enabled: true }))).not.toHaveProperty('PRUNE_APPLIED');
    expect(serverEnv(cluster({ enabled: true, journalArchiveS3: s3 }))).not.toHaveProperty('ARCHIVE_UPLOAD');
    expect(serverEnv(cluster({ enabled: true, pruneAppliedSegments: true }))).toMatchObject({ PRUNE_APPLIED: 'true' });
    expect(serverEnv(cluster({ enabled: true, pruneAppliedSegments: true }))).not.toHaveProperty('ARCHIVE_UPLOAD');
    expect(serverEnv(cluster({ enabled: true, pruneAppliedSegments: true, journalArchiveS3: s3 }))).toMatchObject({
      PRUNE_APPLIED: 'true',
      ARCHIVE_UPLOAD: 'true',
    });
  });
});
