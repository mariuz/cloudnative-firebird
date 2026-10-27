import { CustomObjectsApi } from '@kubernetes/client-node';
import { API_GROUP, API_VERSION } from '../types';

/**
 * A status write refused because the object changed after the copy the reconcile works from was
 * read: that reconcile stops, and the one of the newer copy (its watch event) decides instead
 */
export class StaleCopyError extends Error {
  constructor(
    object: string,
    readonly version: string,
  ) {
    super(`${object} changed since resourceVersion ${version} was read: status not written from the older copy`);
    this.name = 'StaleCopyError';
  }
}

/** The fields of a namespaced custom object a status write needs, kept up to date by it */
export interface VersionedObject {
  metadata: { name: string; namespace?: string; resourceVersion?: string };
}

/** HTTP status of a Kubernetes API error */
function statusCode(err: unknown): number | undefined {
  const e = err as { code?: number; statusCode?: number; response?: { statusCode?: number } };
  return e?.code ?? e?.statusCode ?? e?.response?.statusCode;
}

/**
 * Replaces the status of a custom object, conditional on the resourceVersion of the copy the
 * reconcile works from (CloudNativePG #11617): a JSON patch whose first operation tests it. A copy
 * that predates another write (a watch event queued before the last reconcile's write) must not
 * write an older status back, so the API server refuses it with 422 and this throws a
 * StaleCopyError. The copy takes the new resourceVersion, so the reconcile's next write applies.
 * A copy without a version (never one from the API server) is written unconditionally.
 */
export async function replaceStatus(api: CustomObjectsApi, plural: string, object: VersionedObject, status: object): Promise<void> {
  const version = object.metadata.resourceVersion;
  let written: { metadata?: { resourceVersion?: string } } | undefined;
  try {
    written = (await api.patchNamespacedCustomObjectStatus({
      group: API_GROUP,
      version: API_VERSION,
      namespace: object.metadata.namespace ?? 'default',
      plural,
      name: object.metadata.name,
      body: [
        ...(version ? [{ op: 'test' as const, path: '/metadata/resourceVersion', value: version }] : []),
        { op: 'replace' as const, path: '/status', value: status },
      ],
    })) as { metadata?: { resourceVersion?: string } } | undefined;
  } catch (err) {
    // a failed test operation: the stored object changed since this copy was read
    if (version && statusCode(err) === 422) throw new StaleCopyError(`${plural}/${object.metadata.name}`, version);
    throw err;
  }
  if (written?.metadata?.resourceVersion) object.metadata.resourceVersion = written.metadata.resourceVersion;
}

/**
 * After a write of the reconcile's own to the object (a finalizer patch that tested the copy's
 * version): the copy takes the new version, so its status writes are not refused by that write
 */
export function followOwnWrite(object: VersionedObject, written: unknown): void {
  const version = (written as { metadata?: { resourceVersion?: string } } | undefined)?.metadata?.resourceVersion;
  if (version) object.metadata.resourceVersion = version;
}
