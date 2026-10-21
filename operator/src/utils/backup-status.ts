import { FirebirdBackup, FirebirdBackupType } from '../types';

/**
 * The cluster's backup summary in its status (CloudNativePG's lastSuccessfulBackup,
 * lastFailedBackup, firstRecoverabilityPoint and the ByMethod maps), from the FirebirdBackup
 * resources of the cluster that still exist: the first recoverability point is the oldest
 * completed backup's completion time (deleting that backup moves it), the last successful and
 * failed backups the latest completion times by outcome, each also per type (logical, physical,
 * volumeSnapshot). Shown by kubectl firebird status.
 */
export interface BackupSummary {
  lastSuccessfulBackup?: string;
  lastSuccessfulBackupByMethod?: Partial<Record<FirebirdBackupType, string>>;
  lastFailedBackup?: string;
  firstRecoverabilityPoint?: string;
  firstRecoverabilityPointByMethod?: Partial<Record<FirebirdBackupType, string>>;
}

const time = (b: FirebirdBackup): string | undefined => b.status?.completionTime ?? b.status?.startTime;

export function backupSummary(backups: FirebirdBackup[], clusterName: string): BackupSummary {
  const own = backups.filter((b) => b.spec?.clusterName === clusterName && !b.metadata?.deletionTimestamp);
  const completed = own.filter((b) => b.status?.phase === 'Completed' && time(b)).sort((a, b) => time(a)!.localeCompare(time(b)!));
  const failed = own.filter((b) => b.status?.phase === 'Failed' && time(b)).sort((a, b) => time(a)!.localeCompare(time(b)!));
  const summary: BackupSummary = {};
  if (completed.length > 0) {
    summary.firstRecoverabilityPoint = time(completed[0]);
    summary.lastSuccessfulBackup = time(completed[completed.length - 1]);
    const first: Partial<Record<FirebirdBackupType, string>> = {};
    const last: Partial<Record<FirebirdBackupType, string>> = {};
    for (const b of completed) {
      const type = b.spec.type ?? 'logical';
      first[type] ??= time(b);
      last[type] = time(b);
    }
    summary.firstRecoverabilityPointByMethod = first;
    summary.lastSuccessfulBackupByMethod = last;
  }
  if (failed.length > 0) summary.lastFailedBackup = time(failed[failed.length - 1]);
  return summary;
}
