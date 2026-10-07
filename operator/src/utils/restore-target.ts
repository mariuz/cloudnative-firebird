import { V1Secret } from '@kubernetes/client-node';
import { FirebirdCluster } from '../types';
import { instanceHost, SEGMENT_PORT } from './replication';
import { SegmentClient, segmentRequest } from './replication-lag';

/**
 * Restores never overwrite a database: the restore Job's engine call refuses an existing target
 * (nrest "File exists", gbak without -rep). Only the instance sees its data directory, so the
 * operator asks the primary's segment server (or, without replication, its backup file server)
 * with EXISTS before the restore Job is created, and the admission webhook does so for new
 * restores: an existing target is refused up front instead of failing the Job.
 */

/** The SYSDBA password (the segment servers' token): the superuser Secret's, or the image default */
export function superuserPasswordFrom(cluster: FirebirdCluster, secret: V1Secret | undefined): string | undefined {
  if (!cluster.spec.superuserSecret?.name) return 'masterkey'; // the pods' ISC_PASSWORD without a superuser Secret
  const data = secret?.data?.password;
  return data ? Buffer.from(data, 'base64').toString('utf8') : undefined;
}

/**
 * Whether the file `target` exists in the data directory of the instance `pod`: undefined when
 * that cannot be told (no password, no answer, a segment server of an earlier version).
 */
export async function restoreTargetExists(
  cluster: FirebirdCluster,
  pod: string,
  password: string | undefined,
  target: string,
  client: SegmentClient = segmentRequest,
  timeoutMs?: number,
): Promise<boolean | undefined> {
  if (!password) return undefined;
  try {
    const [reply] = await client(instanceHost(cluster, pod), SEGMENT_PORT, `${password} EXISTS ${target}`, timeoutMs);
    if (reply === 'OK yes') return true;
    if (reply === 'OK no') return false;
    return undefined;
  } catch {
    return undefined;
  }
}

/** Why a restore into an existing target is refused */
export function existingTargetMessage(target: string, pod: string): string {
  return (
    `targetDatabase ${target} already exists on ${pod}; restores never overwrite a database ` +
    '(choose another targetDatabase, or drop that database first)'
  );
}
