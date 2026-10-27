import { describe, it, expect, vi } from 'vitest';
import { CustomObjectsApi } from '@kubernetes/client-node';
import { followOwnWrite, replaceStatus, StaleCopyError } from '../src/utils/status-write';

/** The conditional status write shared by the backup, user, role and database controllers */
describe('replaceStatus', () => {
  const api = (impl: () => Promise<unknown>) => ({ patchNamespacedCustomObjectStatus: vi.fn().mockImplementation(impl) }) as unknown as CustomObjectsApi & {
    patchNamespacedCustomObjectStatus: ReturnType<typeof vi.fn>;
  };

  it('tests the copy\'s resourceVersion, then replaces the status, and takes the new version', async () => {
    const a = api(async () => ({ metadata: { resourceVersion: '12' } }));
    const object = { metadata: { name: 'u', namespace: 'apps', resourceVersion: '11' } };
    await replaceStatus(a, 'firebirdusers', object, { phase: 'Applied' });
    expect(a.patchNamespacedCustomObjectStatus).toHaveBeenCalledWith({
      group: 'firebird.cloudnative-firebird.io',
      version: 'v1',
      namespace: 'apps',
      plural: 'firebirdusers',
      name: 'u',
      body: [
        { op: 'test', path: '/metadata/resourceVersion', value: '11' },
        { op: 'replace', path: '/status', value: { phase: 'Applied' } },
      ],
    });
    expect(object.metadata.resourceVersion).toBe('12');
  });

  it('refuses a write from an older copy with a StaleCopyError', async () => {
    const a = api(async () => {
      throw Object.assign(new Error('the server rejected our request due to an error in our request'), { code: 422 });
    });
    const err = await replaceStatus(a, 'firebirdroles', { metadata: { name: 'r', resourceVersion: '3' } }, {}).catch((e) => e);
    expect(err).toBeInstanceOf(StaleCopyError);
    expect(err.version).toBe('3');
    expect(err.message).toBe('firebirdroles/r changed since resourceVersion 3 was read: status not written from the older copy');
  });

  it('writes a copy without a version unconditionally, and passes other errors on', async () => {
    const a = api(async () => ({}));
    await replaceStatus(a, 'firebirddatabases', { metadata: { name: 'd' } }, { phase: 'Ready' });
    expect(a.patchNamespacedCustomObjectStatus.mock.calls[0][0].body).toEqual([{ op: 'replace', path: '/status', value: { phase: 'Ready' } }]);
    const unversioned = api(async () => {
      throw Object.assign(new Error('the server rejected our request due to an error in our request'), { code: 422 });
    });
    await expect(replaceStatus(unversioned, 'firebirddatabases', { metadata: { name: 'd' } }, {})).rejects.not.toBeInstanceOf(StaleCopyError);
    const forbidden = api(async () => {
      throw Object.assign(new Error('forbidden'), { code: 403 });
    });
    await expect(replaceStatus(forbidden, 'firebirddatabases', { metadata: { name: 'd', resourceVersion: '1' } }, {})).rejects.toThrow('forbidden');
  });

  it('follows the version of the reconcile\'s own write', () => {
    const object = { metadata: { name: 'u', resourceVersion: '1' } };
    followOwnWrite(object, { metadata: { resourceVersion: '2' } });
    expect(object.metadata.resourceVersion).toBe('2');
    followOwnWrite(object, undefined);
    expect(object.metadata.resourceVersion).toBe('2');
  });
});
