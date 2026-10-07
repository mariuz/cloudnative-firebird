import { describe, it, expect, vi } from 'vitest';
import { FirebirdCluster } from '../src/types';
import { restoreTargetExists, superuserPasswordFrom } from '../src/utils/restore-target';

const cluster = (superuser?: string): FirebirdCluster => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdCluster',
  metadata: { name: 'db', namespace: 'default' },
  spec: { instances: 1, storage: { size: '1Gi' }, ...(superuser ? { superuserSecret: { name: superuser } } : {}) },
});

describe('restore targets', () => {
  it('signs with the superuser Secret, or the image default without one', () => {
    expect(superuserPasswordFrom(cluster(), undefined)).toBe('masterkey');
    expect(superuserPasswordFrom(cluster('su'), { data: { password: Buffer.from('s3cret').toString('base64') } })).toBe('s3cret');
    expect(superuserPasswordFrom(cluster('su'), undefined)).toBeUndefined();
    expect(superuserPasswordFrom(cluster('su'), { data: {} })).toBeUndefined();
  });

  it('asks the instance segment server with EXISTS, and tells nothing when it cannot', async () => {
    const client = vi.fn().mockResolvedValue(['OK yes']);
    expect(await restoreTargetExists(cluster(), 'db-0', 'pw', 'x.fdb', client, 1500)).toBe(true);
    expect(client).toHaveBeenCalledWith('db-0.db-headless', 3051, 'pw EXISTS x.fdb', 1500);
    client.mockResolvedValue(['OK no']);
    expect(await restoreTargetExists(cluster(), 'db-0', 'pw', 'x.fdb', client)).toBe(false);
    client.mockResolvedValue(['ERR not available without replication']);
    expect(await restoreTargetExists(cluster(), 'db-0', 'pw', 'x.fdb', client)).toBeUndefined();
    client.mockRejectedValue(new Error('timed out'));
    expect(await restoreTargetExists(cluster(), 'db-0', 'pw', 'x.fdb', client)).toBeUndefined();
    client.mockClear();
    expect(await restoreTargetExists(cluster(), 'db-0', undefined, 'x.fdb', client)).toBeUndefined();
    expect(client).not.toHaveBeenCalled();
  });
});
