import { createHash } from 'crypto';
import { V1Job, V1PersistentVolumeClaim } from '@kubernetes/client-node';
import {
  API_GROUP,
  DEFAULT_FIREBIRD_IMAGE,
  FirebirdBackup,
  FirebirdCluster,
  FirebirdScheduledBackup,
  RESOURCE_KIND,
  VolumeSnapshotConfiguration,
} from '../types';
import { instanceDataClaimName, ownerReference, retentionSeconds } from './backup';
import { instanceHost, replicationDirectories, replicationEnabled } from './replication';
import {
  clusterLabels,
  databaseName,
  FIREBIRD_DATA_DIR,
  instancePodSecurityContext,
  instanceSecurityContext,
  jobPodSpec,
  serviceAccount,
  superuserClientEnv,
} from './resources';

/**
 * Volume snapshot backups (CloudNativePG's volumeSnapshot backup method) and bootstrap.
 *
 * Backup (FirebirdBackup type volumeSnapshot), driven by the backup controller:
 * 1. a Job locks the primary's database for backup (ALTER DATABASE BEGIN BACKUP, as nbackup -L):
 *    the database file stops changing, its changes go to the delta file next to it;
 * 2. the operator creates a VolumeSnapshot of the primary's volume (the FirebirdBackup's name),
 *    and waits until it is cut (status.creationTime, or readyToUse);
 * 3. a Job unlocks the database (ALTER DATABASE END BACKUP: the delta is merged back);
 * 4. the backup completes once the snapshot is readyToUse.
 * A snapshot that reports an error, or is not cut within lockTimeoutSeconds, fails the backup; the
 * database is unlocked either way (a snapshot not cut yet is deleted first, so that it is not cut
 * later from the unlocked volume). A finalizer keeps a FirebirdBackup deleted meanwhile until the
 * database is unlocked.
 *
 * The snapshot holds the whole volume: the database file as of the lock (plus the delta, which is
 * discarded), the security database, and the cluster's replication state. Bootstrap
 * (bootstrap.recovery.volumeSnapshot) creates the first instance's volume from it, and a Job
 * fixes up the database (nbackup -F, as for a copy taken under nbackup -L), turns it into a
 * standalone database (no publication, no replica mode) and discards the source's replication
 * state; the instance then starts on it like on a restored backup. With replication it becomes the
 * new primary's database and the other instances are seeded from it.
 *
 * Scheduled (FirebirdScheduledBackup type volumeSnapshot): the operator creates a FirebirdBackup
 * at each scheduled time (UTC) and applies the retention policy to the backups it created,
 * deleting their VolumeSnapshots with them.
 */

export const SNAPSHOT_API_GROUP = 'snapshot.storage.k8s.io';
export const SNAPSHOT_API_VERSION = 'v1';
export const SNAPSHOT_PLURAL = 'volumesnapshots';

/** How long the database may stay locked while the snapshot is cut, by default */
export const DEFAULT_LOCK_TIMEOUT_SECONDS = 600;

/** Keeps a FirebirdBackup until the database it locked is unlocked */
export const SNAPSHOT_FINALIZER = `${API_GROUP}/unlock-database`;

/** The FirebirdBackup a VolumeSnapshot was taken for */
export const BACKUP_LABEL = `${API_GROUP}/backup`;

/** The FirebirdScheduledBackup that created a FirebirdBackup */
export const SCHEDULED_BACKUP_LABEL = `${API_GROUP}/scheduled-backup`;

/** The fields of a VolumeSnapshot the operator uses */
export interface VolumeSnapshot {
  apiVersion?: string;
  kind?: string;
  metadata: {
    name: string;
    namespace?: string;
    labels?: Record<string, string>;
    annotations?: Record<string, string>;
  };
  spec: {
    volumeSnapshotClassName?: string;
    source: { persistentVolumeClaimName?: string; volumeSnapshotContentName?: string };
  };
  status?: {
    creationTime?: string;
    readyToUse?: boolean;
    restoreSize?: string;
    error?: { message?: string; time?: string };
  };
}

/** A label value of at most 63 characters: the name, or its start and a hash */
export function labelValue(name: string): string {
  if (name.length <= 63) return name;
  return `${name.slice(0, 54).replace(/[-._]+$/, '')}-${createHash('sha256').update(name).digest('hex').slice(0, 8)}`;
}

/** A Job name of at most 63 characters (the job-name label of its pods) */
export function boundedJobName(prefix: string, name: string, suffix = ''): string {
  const full = `${prefix}-${name}${suffix ? `-${suffix}` : ''}`;
  if (full.length <= 63) return full;
  const hash = createHash('sha256').update(full).digest('hex').slice(0, 8);
  const room = 63 - prefix.length - (suffix ? suffix.length + 1 : 0) - hash.length - 2;
  return `${prefix}-${name.slice(0, room).replace(/[-.]+$/, '')}-${hash}${suffix ? `-${suffix}` : ''}`;
}

/** Job locking or unlocking the database for a volume snapshot backup */
export function snapshotLockJobName(backup: FirebirdBackup, action: 'lock' | 'unlock'): string {
  return boundedJobName('backup', backup.metadata.name, action);
}

/** Ordinal of an instance pod ("<cluster>-<n>") */
function ordinal(cluster: FirebirdCluster, pod: string): number {
  const n = Number(pod.slice(cluster.metadata.name.length + 1));
  if (!pod.startsWith(`${cluster.metadata.name}-`) || !Number.isInteger(n) || n < 0) {
    throw new Error(`${pod} is not an instance of cluster ${cluster.metadata.name}`);
  }
  return n;
}

/** The PersistentVolumeClaim of an instance pod */
export function instanceClaimName(cluster: FirebirdCluster, pod: string): string {
  return instanceDataClaimName(cluster, ordinal(cluster, pod));
}

/**
 * Shell run against the instance (isql over the network). MON$BACKUP_STATE: 0 normal, 1 locked
 * (stalled), 2 merging. Locking refuses a database already in backup mode (an nbackup or another
 * snapshot backup holds it: unlocking would end their lock); unlocking does nothing unless it is
 * locked.
 */
export function lockScript(action: 'lock' | 'unlock'): string {
  const state =
    `state=$(echo 'SET LIST ON; SELECT MON$BACKUP_STATE AS S FROM MON$DATABASE;' | isql -q "$FIREBIRD_HOST:$DATABASE_PATH" | awk '$1 == "S" { print $2 }'); ` +
    '[ -n "$state" ] || { echo "could not read the backup state of $DATABASE_PATH on $FIREBIRD_HOST" >&2; exit 1; }; ';
  return action === 'lock'
    ? 'set -eu; ' +
        state +
        'if [ "$state" != 0 ]; then echo "$DATABASE_PATH is in backup mode already (state $state): an nbackup or another snapshot backup is running" >&2; exit 1; fi; ' +
        `echo 'ALTER DATABASE BEGIN BACKUP;' | isql -q -b "$FIREBIRD_HOST:$DATABASE_PATH"; ` +
        'echo "$DATABASE_PATH locked for backup on $FIREBIRD_HOST"'
    : 'set -eu; ' +
        state +
        'if [ "$state" = 1 ]; then ' +
        `echo 'ALTER DATABASE END BACKUP;' | isql -q -b "$FIREBIRD_HOST:$DATABASE_PATH"; ` +
        'echo "$DATABASE_PATH unlocked on $FIREBIRD_HOST"; ' +
        'else echo "$DATABASE_PATH is not locked for backup (state $state)"; fi'
  ;
}

/** Job locking or unlocking the database of the instance a snapshot backup is taken on */
export function buildSnapshotLockJob(backup: FirebirdBackup, cluster: FirebirdCluster, instance: string, action: 'lock' | 'unlock'): V1Job {
  const { name, namespace = 'default', uid } = backup.metadata;
  const labels = { ...clusterLabels(cluster.metadata.name), 'app.kubernetes.io/component': 'snapshot-backup', [BACKUP_LABEL]: labelValue(name) };
  return {
    apiVersion: 'batch/v1',
    kind: 'Job',
    metadata: {
      name: snapshotLockJobName(backup, action),
      namespace,
      labels,
      ownerReferences: [ownerReference('FirebirdBackup', name, uid)],
    },
    spec: {
      // a lock is not retried: a failed attempt may have found another backup's lock
      backoffLimit: action === 'lock' ? 0 : 3,
      template: {
        metadata: { labels },
        spec: jobPodSpec(cluster, {
          restartPolicy: 'Never',
          containers: [
            {
              name: action,
              image: cluster.spec.imageName ?? DEFAULT_FIREBIRD_IMAGE,
              command: ['/bin/sh', '-c'],
              args: [lockScript(action)],
              env: [
                ...superuserClientEnv(cluster),
                { name: 'FIREBIRD_HOST', value: instanceHost(cluster, instance) },
                { name: 'DATABASE_PATH', value: `${FIREBIRD_DATA_DIR}/${databaseName(cluster)}` },
              ],
            },
          ],
        }),
      },
    },
  };
}

/** The VolumeSnapshot of a snapshot backup, of the instance's volume */
export function buildVolumeSnapshot(backup: FirebirdBackup, cluster: FirebirdCluster, instance: string): VolumeSnapshot {
  const config: VolumeSnapshotConfiguration = backup.spec.volumeSnapshot ?? {};
  return {
    apiVersion: `${SNAPSHOT_API_GROUP}/${SNAPSHOT_API_VERSION}`,
    kind: 'VolumeSnapshot',
    metadata: {
      name: backup.metadata.name,
      namespace: backup.metadata.namespace ?? 'default',
      labels: { ...(config.labels ?? {}), ...clusterLabels(cluster.metadata.name), 'app.kubernetes.io/component': 'snapshot-backup', [BACKUP_LABEL]: labelValue(backup.metadata.name) },
      annotations: {
        ...(config.annotations ?? {}),
        [`${API_GROUP}/instance`]: instance,
        [`${API_GROUP}/database`]: databaseName(cluster),
      },
    },
    spec: {
      ...(config.className ? { volumeSnapshotClassName: config.className } : {}),
      source: { persistentVolumeClaimName: instanceClaimName(cluster, instance) },
    },
  };
}

/** What a VolumeSnapshot reports: cut (the database may be unlocked), ready, or failed */
export function snapshotProgress(snapshot: VolumeSnapshot): { taken: boolean; ready: boolean; error?: string } {
  const status = snapshot.status ?? {};
  const ready = status.readyToUse === true;
  return {
    taken: ready || Boolean(status.creationTime),
    ready,
    ...(status.error?.message ? { error: status.error.message } : {}),
  };
}

/** Whether the database has been locked longer than the backup allows without the snapshot being cut */
export function lockTimedOut(backup: FirebirdBackup, lockedAt: string | undefined, now: Date): boolean {
  if (!lockedAt) return false;
  const limit = (backup.spec.volumeSnapshot?.lockTimeoutSeconds ?? DEFAULT_LOCK_TIMEOUT_SECONDS) * 1000;
  return now.getTime() - new Date(lockedAt).getTime() > limit;
}

/** Validation of the volumeSnapshot settings of a backup spec */
export function volumeSnapshotSpecErrors(
  spec: { type?: string; volumeSnapshot?: VolumeSnapshotConfiguration; level?: number; s3?: unknown; verify?: boolean; target?: string },
  path: string,
): string[] {
  if (spec.type !== 'volumeSnapshot') {
    return spec.volumeSnapshot ? [`${path}.volumeSnapshot is only used by type volumeSnapshot`] : [];
  }
  const errors: string[] = [];
  if (spec.level !== undefined) errors.push(`${path}.level is for physical backups; a volume snapshot copies the whole volume`);
  if (spec.s3) errors.push(`${path}.s3 is not used by volume snapshots (the CSI driver stores them)`);
  if (spec.verify) errors.push(`${path}.verify is for logical backups`);
  if (spec.target === 'prefer-standby') errors.push(`${path}.target prefer-standby: volume snapshots are taken of the primary`);
  const timeout = spec.volumeSnapshot?.lockTimeoutSeconds;
  if (timeout !== undefined && (!Number.isInteger(timeout) || timeout < 30 || timeout > 86400)) {
    errors.push(`${path}.volumeSnapshot.lockTimeoutSeconds must be between 30 and 86400`);
  }
  return errors;
}

/** Name of the FirebirdBackup a schedule creates for a scheduled time */
export function scheduledSnapshotBackupName(scheduled: FirebirdScheduledBackup, time: Date): string {
  const stamp = time.toISOString().replace(/[-:T]/g, '').slice(0, 12);
  // the lock and unlock Jobs are named after it; boundedJobName keeps them valid
  return `${scheduled.metadata.name.slice(0, 253 - 13)}-${stamp}`;
}

/** The FirebirdBackup a schedule creates */
export function buildScheduledSnapshotBackup(scheduled: FirebirdScheduledBackup, time: Date): FirebirdBackup {
  const spec = scheduled.spec;
  return {
    apiVersion: 'firebird.cloudnative-firebird.io/v1',
    kind: 'FirebirdBackup',
    metadata: {
      name: scheduledSnapshotBackupName(scheduled, time),
      namespace: scheduled.metadata.namespace ?? 'default',
      labels: { ...clusterLabels(spec.clusterName), 'app.kubernetes.io/component': 'snapshot-backup', [SCHEDULED_BACKUP_LABEL]: labelValue(scheduled.metadata.name) },
    },
    spec: {
      clusterName: spec.clusterName,
      type: 'volumeSnapshot',
      ...(spec.volumeSnapshot ? { volumeSnapshot: spec.volumeSnapshot } : {}),
    },
  };
}

/**
 * The backups of a schedule its retention policy expires: finished (completed or failed) before
 * the retention window, never the newest completed one
 */
export function expiredSnapshotBackups(backups: FirebirdBackup[], policy: string | undefined, now: Date): FirebirdBackup[] {
  if (!policy) return [];
  const cutoff = now.getTime() - retentionSeconds(policy) * 1000;
  const finished = (b: FirebirdBackup) => b.status?.completionTime ?? (b.status?.phase === 'Failed' ? b.status.startTime : undefined);
  const completed = backups
    .filter((b) => b.status?.phase === 'Completed' && b.status.completionTime)
    .sort((a, b) => new Date(b.status!.completionTime!).getTime() - new Date(a.status!.completionTime!).getTime());
  const newest = completed[0]?.metadata.name;
  return backups.filter((b) => {
    if (b.metadata.name === newest) return false;
    if (b.status?.phase !== 'Completed' && b.status?.phase !== 'Failed') return false;
    const at = finished(b) ?? (b.metadata.creationTimestamp ? new Date(b.metadata.creationTimestamp).toISOString() : undefined);
    return at !== undefined && new Date(at).getTime() < cutoff;
  });
}

/** Name of the Job that prepares a new cluster's database from a volume snapshot */
export function snapshotRecoveryJobName(cluster: FirebirdCluster): string {
  return boundedJobName(cluster.metadata.name, 'snapshot', 'recovery');
}

/** The first instance's volume, created from the snapshot under the StatefulSet's claim name */
export function buildSnapshotRecoveryClaim(cluster: FirebirdCluster, template: V1PersistentVolumeClaim): V1PersistentVolumeClaim {
  const source = cluster.spec.bootstrap!.recovery!.volumeSnapshot!;
  return {
    apiVersion: 'v1',
    kind: 'PersistentVolumeClaim',
    metadata: { ...template.metadata, name: instanceDataClaimName(cluster, 0), namespace: cluster.metadata.namespace ?? 'default' },
    spec: {
      ...template.spec,
      dataSource: { apiGroup: SNAPSHOT_API_GROUP, kind: 'VolumeSnapshot', name: source.name },
    },
  };
}

/**
 * Shell turning the database of a volume restored from a snapshot into the new cluster's: it runs
 * with the embedded engine (nothing else has the volume). A database locked for backup is fixed
 * up (nbackup -F: the database as of the lock; the delta file is discarded), brought online,
 * made a standalone database (replica mode and publication off: the replication init container
 * enables publication on the new primary), and moved to TARGET_PATH. The source's replication
 * state and work files are removed. A marker makes a retried Job a no-op.
 */
export const SNAPSHOT_RECOVERY_SCRIPT = [
  'set -eu',
  'FB=/opt/firebird/bin',
  `D=${FIREBIRD_DATA_DIR}`,
  'M="$D/.snapshot-recovered"',
  'if [ -f "$M" ]; then echo "recovered from the snapshot already"; exit 0; fi',
  'fail() { echo "volume snapshot recovery failed: $*" >&2; exit 1; }',
  'if [ ! -f "$SOURCE_DATABASE" ]; then ls -la "$D" >&2 || true; fail "no database $SOURCE_DATABASE on the volume restored from the snapshot (bootstrap.recovery.volumeSnapshot.databaseName)"; fi',
  // the source's offline work files and replication state (seeds, journals, control files)
  'rm -rf "$D/.bootstrap.fdb" "$D/.init.fdb" "$D/.init.fdb.ctl" "$D/.init.fdb.kind" "$D/.switchover.fdb" "$D/.enable-replication.fdb" "$D/.pitr-recovery"',
  'for r in $REPLICATION_DIRS; do rm -rf "$r"; done',
  'if [ "$TARGET_PATH" != "$SOURCE_DATABASE" ] && [ -e "$TARGET_PATH" ]; then fail "$TARGET_PATH exists on the snapshot\'s volume"; fi',
  'attrs=$("$FB/gstat" -h "$SOURCE_DATABASE" | sed -n "s/^[[:space:]]*Attributes[[:space:]]*//p")',
  'case "$attrs" in',
  '  *"backup lock"*)',
  '    rm -f "$SOURCE_DATABASE.delta"',
  '    "$FB/nbackup" -F "$SOURCE_DATABASE"',
  '    echo "the database as of its backup lock (the snapshot\'s delta file discarded)" ;;',
  '  *"backup merge"*)',
  '    fail "the snapshot was taken while the database merged its backup changes: take another snapshot backup" ;;',
  '  *)',
  '    rm -f "$SOURCE_DATABASE.delta"',
  '    echo "note: the database was not locked for backup when the snapshot was taken; it is used as a crash-consistent copy" ;;',
  'esac',
  'case "$attrs" in *shutdown*) "$FB/gfix" -user SYSDBA -online "$SOURCE_DATABASE" ;; esac',
  '"$FB/gfix" -user SYSDBA -replica none "$SOURCE_DATABASE"',
  'echo "ALTER DATABASE DISABLE PUBLICATION; COMMIT;" | "$FB/isql" -q -b -user SYSDBA "$SOURCE_DATABASE"',
  'if [ "$TARGET_PATH" != "$SOURCE_DATABASE" ]; then mv "$SOURCE_DATABASE" "$TARGET_PATH"; fi',
  '[ "$(id -u)" -ne 0 ] || chown firebird:firebird "$TARGET_PATH"',
  'touch "$M"',
  'echo "database restored from the volume snapshot at $TARGET_PATH"',
].join('\n');

/**
 * Job preparing a new cluster's database on its first instance's volume, created from the
 * snapshot (bootstrap.recovery.volumeSnapshot), before the StatefulSet exists. It runs with the
 * instances' security context: the snapshot's files belong to the instance's users.
 */
export function buildSnapshotRecoveryJob(cluster: FirebirdCluster): V1Job {
  const { name, namespace = 'default', uid } = cluster.metadata;
  const source = cluster.spec.bootstrap!.recovery!.volumeSnapshot!;
  const labels = { ...clusterLabels(name), 'app.kubernetes.io/component': 'snapshot-recovery' };
  const target = replicationEnabled(cluster) ? `${FIREBIRD_DATA_DIR}/.bootstrap.fdb` : `${FIREBIRD_DATA_DIR}/${databaseName(cluster)}`;
  // the source's replication state: the default location, and this cluster's when on the volume
  const dirs = new Set([`${FIREBIRD_DATA_DIR}/replication`]);
  const own = replicationDirectories(cluster, FIREBIRD_DATA_DIR).base;
  if (own.startsWith(`${FIREBIRD_DATA_DIR}/`)) dirs.add(own);
  return {
    apiVersion: 'batch/v1',
    kind: 'Job',
    metadata: { name: snapshotRecoveryJobName(cluster), namespace, labels, ownerReferences: [ownerReference(RESOURCE_KIND, name, uid)] },
    spec: {
      backoffLimit: 2,
      template: {
        metadata: { labels },
        spec: {
          restartPolicy: 'Never',
          ...serviceAccount(cluster),
          securityContext: instancePodSecurityContext(cluster),
          ...(cluster.spec.nodeSelector ? { nodeSelector: cluster.spec.nodeSelector } : {}),
          ...(cluster.spec.tolerations ? { tolerations: cluster.spec.tolerations } : {}),
          containers: [
            {
              name: 'snapshot-recovery',
              image: cluster.spec.imageName ?? DEFAULT_FIREBIRD_IMAGE,
              command: ['/bin/sh', '-c'],
              args: [SNAPSHOT_RECOVERY_SCRIPT],
              env: [
                { name: 'SOURCE_DATABASE', value: `${FIREBIRD_DATA_DIR}/${source.databaseName ?? databaseName(cluster)}` },
                { name: 'TARGET_PATH', value: target },
                { name: 'REPLICATION_DIRS', value: [...dirs].join(' ') },
              ],
              securityContext: instanceSecurityContext(cluster),
              volumeMounts: [{ name: 'firebird-data', mountPath: FIREBIRD_DATA_DIR }],
            },
          ],
          volumes: [{ name: 'firebird-data', persistentVolumeClaim: { claimName: instanceDataClaimName(cluster, 0) } }],
        },
      },
    },
  };
}

/** Why a FirebirdRestore cannot read a volume snapshot backup */
export function volumeSnapshotRestoreMessage(backupName: string): string {
  return `FirebirdBackup ${backupName} is a volume snapshot: bootstrap a new cluster from it (bootstrap.recovery.volumeSnapshot.name: ${backupName})`;
}

/** Validation of bootstrap.recovery.volumeSnapshot */
export function volumeSnapshotRecoveryErrors(cluster: FirebirdCluster): string[] {
  const recovery = cluster.spec.bootstrap?.recovery;
  const source = recovery?.volumeSnapshot;
  if (!source) return [];
  const errors: string[] = [];
  const path = 'spec.bootstrap.recovery.volumeSnapshot';
  if (!source.name || !/^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$/.test(source.name)) errors.push(`${path}.name must be the name of a VolumeSnapshot`);
  if (source.databaseName !== undefined && !/^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(source.databaseName)) {
    errors.push(`${path}.databaseName must be a file name (letters, digits, ".", "_", "-")`);
  }
  if (recovery.sourcePath || recovery.s3 || recovery.pointInTime || recovery.incrementalPaths?.length) {
    errors.push(`${path} is mutually exclusive with sourcePath, s3, incrementalPaths and pointInTime`);
  }
  if (cluster.spec.replica) errors.push(`${path}: a replica cluster bootstraps from an nbackup in S3 (bootstrap.recovery.sourcePath)`);
  return errors;
}
