import { describe, it, expect, vi, type Mock } from 'vitest';
import { KubeConfig } from '@kubernetes/client-node';
import { FirebirdClusterController } from '../src/controllers/firebirdcluster.controller';
import { backupSummary } from '../src/utils/backup-status';
import { nextCronTime } from '../src/utils/cron';
import { FirebirdBackup } from '../src/types';
import { makeCluster, notFoundError } from './helpers/factories';

const backup = (name: string, cluster: string, phase: string, at: string, type?: FirebirdBackup['spec']['type'], extra: Partial<FirebirdBackup['metadata']> = {}): FirebirdBackup => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdBackup',
  metadata: { name, namespace: 'default', ...extra },
  spec: { clusterName: cluster, ...(type ? { type } : {}) },
  status: { phase: phase as 'Completed', ...(phase === 'Completed' ? { startTime: at, completionTime: at } : { startTime: at }) },
});

describe('the backup summary of the cluster status (lastSuccessfulBackup, firstRecoverabilityPoint)', () => {
  it('comes from the completed and failed backups of the cluster, overall and per type', () => {
    const backups = [
      backup('b1', 'db', 'Completed', '2026-10-01T00:00:00Z'),
      backup('b2', 'db', 'Completed', '2026-10-03T00:00:00Z', 'physical'),
      backup('b3', 'db', 'Completed', '2026-10-05T00:00:00Z'),
      backup('b4', 'db', 'Failed', '2026-10-06T00:00:00Z', 'volumeSnapshot'),
      backup('b5', 'db', 'Running', '2026-10-07T00:00:00Z'),
      backup('other', 'elsewhere', 'Completed', '2026-10-08T00:00:00Z'),
      backup('gone', 'db', 'Completed', '2026-09-01T00:00:00Z', 'logical', { deletionTimestamp: '2026-10-08T00:00:00Z' }),
    ];
    expect(backupSummary(backups, 'db')).toEqual({
      firstRecoverabilityPoint: '2026-10-01T00:00:00Z',
      lastSuccessfulBackup: '2026-10-05T00:00:00Z',
      firstRecoverabilityPointByMethod: { logical: '2026-10-01T00:00:00Z', physical: '2026-10-03T00:00:00Z' },
      lastSuccessfulBackupByMethod: { logical: '2026-10-05T00:00:00Z', physical: '2026-10-03T00:00:00Z' },
      lastFailedBackup: '2026-10-06T00:00:00Z',
    });
    // no backups: no fields (the status drops them)
    expect(backupSummary([], 'db')).toEqual({});
    expect(backupSummary([backup('b', 'db', 'Pending', '2026-10-01T00:00:00Z')], 'db')).toEqual({});
  });

  it('is written with the cluster status, and left out when the backups cannot be listed', async () => {
    const run = async (list: Mock) => {
      const calls: Record<string, Mock> = {};
      const fn = (m: string): Mock =>
        (calls[m] ??=
          m === 'listNamespacedCustomObject'
            ? list
            : m.startsWith('read') || m.startsWith('get')
              ? vi.fn().mockRejectedValue(notFoundError)
              : m.startsWith('list')
                ? vi.fn().mockResolvedValue({ items: [] })
                : vi.fn().mockResolvedValue({}));
      const kubeConfig = new KubeConfig();
      vi.spyOn(kubeConfig, 'makeApiClient').mockReturnValue(new Proxy({}, { get: (_t, p: string) => fn(p) }) as never);
      await new FirebirdClusterController(kubeConfig).reconcile(makeCluster());
      return fn('patchNamespacedCustomObjectStatus').mock.calls.map((c) => c[0].body[0].value).at(-1);
    };
    const listed = vi.fn().mockImplementation(({ plural }: { plural: string }) =>
      Promise.resolve({ items: plural === 'firebirdbackups' ? [backup('b1', 'test-cluster', 'Completed', '2026-10-01T00:00:00Z'), backup('b2', 'test-cluster', 'Failed', '2026-10-02T00:00:00Z')] : [] }),
    );
    expect(await run(listed)).toMatchObject({ lastSuccessfulBackup: '2026-10-01T00:00:00Z', firstRecoverabilityPoint: '2026-10-01T00:00:00Z', lastFailedBackup: '2026-10-02T00:00:00Z' });
    expect(listed).toHaveBeenCalledWith(expect.objectContaining({ plural: 'firebirdbackups', namespace: 'default' }));
    const status = await run(vi.fn().mockRejectedValue(new Error('forbidden')));
    expect(status.phase).toBeDefined();
    expect(status.lastSuccessfulBackup).toBeUndefined();
  });
});

describe('nextCronTime', () => {
  it('finds the next matching minute after a time', () => {
    expect(nextCronTime('0 2 * * *', new Date('2026-10-20T01:30:00Z'))?.toISOString()).toBe('2026-10-20T02:00:00.000Z');
    expect(nextCronTime('0 2 * * *', new Date('2026-10-20T02:00:00Z'))?.toISOString()).toBe('2026-10-21T02:00:00.000Z');
    expect(nextCronTime('*/15 * * * *', new Date('2026-10-20T02:07:30Z'))?.toISOString()).toBe('2026-10-20T02:15:00.000Z');
    expect(nextCronTime('0 0 30 2 *', new Date('2026-10-20T00:00:00Z'))).toBeUndefined();
  });
});
