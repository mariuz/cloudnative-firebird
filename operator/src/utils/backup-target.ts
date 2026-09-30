import { V1Pod } from '@kubernetes/client-node';
import { FirebirdCluster, S3BackupConfiguration } from '../types';
import { instancePods } from './resources';
import { isPodReady } from './routing';

/**
 * Where a backup runs (CloudNativePG `target`):
 *
 * - `primary` (default): the current primary.
 * - `prefer-standby`: a replica when one qualifies, else the primary. Only backups to S3 can run
 *   on a replica: gbak reads the read-only replica and streams the backup to the Job pod, and
 *   nbackup runs in the replica's server (verified with Firebird 5 on a read-only replica), the
 *   file copied through its segment server. Server-side files belong on the primary's volume, so
 *   those backups always use the primary. An nbackup chain lives in the backup history of the
 *   database it was taken on: a level 1 or 2 on another instance finds no base there.
 *
 * The replica is the ready, unfenced, non-lagging one with the lowest ordinal, so scheduled
 * backups keep the same replica (and an unchanged CronJob) while it stays healthy.
 */
export type BackupTarget = 'primary' | 'prefer-standby';

export function chooseBackupInstance(options: {
  cluster: FirebirdCluster;
  primaryPod: string;
  pods: V1Pod[];
  target?: BackupTarget;
  type?: 'logical' | 'physical';
  s3?: S3BackupConfiguration;
}): string {
  const { cluster, primaryPod, pods, target, s3 } = options;
  if (target !== 'prefer-standby' || !s3) return primaryPod;
  const name = cluster.metadata.name;
  const fenced = cluster.status?.fencedInstances ?? [];
  const lagging = cluster.status?.replicationStatus?.laggingReplicas ?? [];
  const ordinal = (pod: string) => Number(pod.slice(name.length + 1));
  const replica = instancePods(pods, name)
    .filter((p) => {
      const pod = p.metadata?.name ?? '';
      return pod !== primaryPod && isPodReady(p) && !fenced.includes(pod) && !lagging.includes(pod) &&
        ordinal(pod) < cluster.spec.instances;
    })
    .map((p) => p.metadata!.name!)
    .sort((a, b) => ordinal(a) - ordinal(b))[0];
  return replica ?? primaryPod;
}
