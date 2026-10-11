import { V1Container } from '@kubernetes/client-node';
import { FirebirdCluster } from '../types';
import { s3ClientEnv, s3ClientImage, s3Uri } from './backup';
import { FIREBIRD_DATA_DIR } from './resources';
import { replicationDirectories, UPGRADE_SCRIPTS } from './replication';

/**
 * Replica clusters (CloudNativePG replica clusters, spec.replica).
 *
 * A replica cluster follows another cluster, the source, through the source's journal archive
 * in S3 (replication.journalArchiveS3), for disaster recovery in another namespace or Kubernetes
 * cluster: nothing connects to the source itself. Its one instance, the designated primary, is a
 * read-only replica of the source database:
 * - bootstrap: the point-in-time recovery Job (pitr-restore.sh) restores the source's nbackup
 *   chain (bootstrap.recovery) and replays every archived segment, but leaves the database a
 *   replica, with its replica control file in the instance's journal_source_directory;
 * - following: the archive-follower sidecar (archive-follower.sh, in the S3 client image)
 *   downloads the segments after the replica's position into that directory, where the Firebird
 *   replica server applies them. Across a failover in the source (a lineage marker in the
 *   archive), it stops at the promoted replica's segment and asks for a restart, which the
 *   operator makes: the replication init container moves the position past the lost primary's
 *   segments. The segment server reports the follower's state (FOLLOW) for status.replicaCluster;
 * - promotion: replica.enabled false makes the instance a primary of its own, once: the promote
 *   directive of a switchover, applied offline by the replication init container when the
 *   operator restarts it (the journal continues after the source's last segment applied).
 *
 * Writes to the database fail while it follows (FirebirdRole, restores into it: they wait).
 */

export function replicaClusterFollowing(cluster: FirebirdCluster): boolean {
  return cluster.spec.replica?.enabled === true;
}

export const DEFAULT_FOLLOW_POLL_SECONDS = 30;

/** The archive-follower sidecar of a following replica cluster's instance pods */
export function buildArchiveFollowerContainer(cluster: FirebirdCluster): V1Container {
  const replica = cluster.spec.replica!;
  const dirs = replicationDirectories(cluster, FIREBIRD_DATA_DIR);
  return {
    name: 'archive-follower',
    image: s3ClientImage(replica.journalS3),
    command: ['/bin/sh', '-c', UPGRADE_SCRIPTS['archive-follower.sh']],
    env: [
      ...s3ClientEnv(replica.journalS3),
      { name: 'SOURCE_DIR', value: dirs.source },
      { name: 'REPLICATION_DIR', value: dirs.base },
      { name: 'JOURNALS', value: s3Uri(replica.journalS3, 'journals/') },
      ...(replica.journalS3.endpoint ? [{ name: 'S3_ENDPOINT', value: replica.journalS3.endpoint }] : []),
      { name: 'POLL_SECONDS', value: String(replica.pollIntervalSeconds ?? DEFAULT_FOLLOW_POLL_SECONDS) },
      // the aws CLI may write a cache under HOME, whatever user the container runs as
      { name: 'HOME', value: '/tmp' },
    ],
    resources: { requests: { cpu: '10m', memory: '64Mi' } },
    volumeMounts: [{ name: 'firebird-data', mountPath: FIREBIRD_DATA_DIR }],
  };
}

/** The follower's state from the segment server (FOLLOW: "OK applied=<n> archived=<n> [restart=<U>|error=...]") */
export interface FollowState {
  applied: number;
  archived: number;
  restart?: number;
  error?: string;
}

export function parseFollowState(reply: string | undefined): FollowState | undefined {
  const match = /^OK applied=(\d+) archived=(\d+)(?: restart=(\d+))?(?: error=(.*))?$/.exec((reply ?? '').trim());
  if (!match) return undefined;
  return {
    applied: Number(match[1]),
    archived: Number(match[2]),
    ...(match[3] !== undefined ? { restart: Number(match[3]) } : {}),
    ...(match[4] !== undefined ? { error: match[4] } : {}),
  };
}

/** The promote directive's token: one promotion per cluster */
export function replicaPromotionToken(cluster: FirebirdCluster): string {
  return `replica-cluster-${(cluster.metadata.uid ?? cluster.metadata.name).replace(/[^A-Za-z0-9-]/g, '')}`;
}

/** Problems with spec.replica, for validateClusterSpec */
export function replicaClusterErrors(cluster: FirebirdCluster): string[] {
  const spec = cluster.spec;
  const replica = spec.replica;
  if (!replica) return [];
  const errors: string[] = [];
  if (!replica.journalS3?.bucket) errors.push('replica.journalS3.bucket is required (the source cluster\'s journal archive)');
  const poll = replica.pollIntervalSeconds;
  if (poll !== undefined && (!Number.isInteger(poll) || poll < 5 || poll > 3600)) {
    errors.push('replica.pollIntervalSeconds must be an integer from 5 to 3600');
  }
  if (!spec.replication?.enabled) errors.push('a replica cluster needs replication.enabled (its instance is a replica of the source)');
  const recovery = spec.bootstrap?.recovery;
  if (!recovery?.sourcePath || !recovery.s3) {
    errors.push('a replica cluster bootstraps from the source\'s nbackup chain in S3: bootstrap.recovery with sourcePath and s3');
  }
  if (recovery?.pointInTime) errors.push('a replica cluster follows the whole archive: bootstrap.recovery.pointInTime is not allowed');
  if (replica.enabled && spec.instances !== 1) {
    errors.push('a replica cluster runs one instance while it follows the source (scale it after the promotion)');
  }
  const own = spec.replication?.journalArchiveS3;
  if (own && replica.journalS3 && own.bucket === replica.journalS3.bucket && (own.prefix ?? '') === (replica.journalS3.prefix ?? '')) {
    errors.push('replication.journalArchiveS3 must not be the source\'s journal archive (replica.journalS3): the promoted cluster would archive over it');
  }
  return errors;
}
