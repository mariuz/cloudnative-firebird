import { V1EphemeralVolumeSource, V1Volume, V1VolumeMount } from '@kubernetes/client-node';
import { FirebirdCluster } from '../types';
import { parseQuantity } from './storage';

/**
 * The instances' temporary space (CloudNativePG ephemeralVolumesSizeLimit.temporaryData and
 * ephemeralVolumeSource): Firebird's sort and temporary files (TempDirectories, /tmp by default,
 * the container's writable layer) go to a volume of their own, an emptyDir with a size limit or a
 * generic ephemeral volume (a PersistentVolumeClaim of a storage class, deleted with the pod), so
 * a large sort neither fills the node's disk nor gets the pod evicted for it.
 */

export const TEMP_VOLUME = 'temp-data';
/** Where the Firebird container mounts it, and the TempDirectories setting */
export const TEMP_DIR = '/firebird-temp';

/** The temporary volume, or none */
export function tempVolume(cluster: FirebirdCluster): V1Volume | undefined {
  const { ephemeralVolumeSource, ephemeralVolumesSizeLimit } = cluster.spec;
  if (ephemeralVolumeSource) return { name: TEMP_VOLUME, ephemeral: ephemeralVolumeSource };
  const limit = ephemeralVolumesSizeLimit?.temporaryData;
  return limit ? { name: TEMP_VOLUME, emptyDir: { sizeLimit: limit } } : undefined;
}

/** Its mount in the Firebird container, or none */
export function tempVolumeMount(cluster: FirebirdCluster): V1VolumeMount | undefined {
  return tempVolume(cluster) ? { name: TEMP_VOLUME, mountPath: TEMP_DIR } : undefined;
}

/** The firebird.conf setting pointing the server at it (config.settings.TempDirectories wins) */
export function tempDirectorySettings(cluster: FirebirdCluster): Record<string, string> {
  return tempVolume(cluster) && cluster.spec.config?.settings?.TempDirectories === undefined ? { TempDirectories: TEMP_DIR } : {};
}

/** Errors of ephemeralVolumesSizeLimit and ephemeralVolumeSource */
export function tempVolumeErrors(spec: FirebirdCluster['spec']): string[] {
  const errors: string[] = [];
  const limit = spec.ephemeralVolumesSizeLimit?.temporaryData;
  if (limit !== undefined) {
    const bytes = parseQuantity(limit);
    if (bytes === null || bytes <= 0) errors.push(`ephemeralVolumesSizeLimit.temporaryData must be a positive quantity (e.g. 2Gi), got "${limit}"`);
  }
  const source: V1EphemeralVolumeSource | undefined = spec.ephemeralVolumeSource;
  if (source !== undefined) {
    if (!source.volumeClaimTemplate?.spec) errors.push('ephemeralVolumeSource needs a volumeClaimTemplate with a spec');
    if (limit !== undefined) errors.push('ephemeralVolumeSource and ephemeralVolumesSizeLimit.temporaryData are mutually exclusive (the claim template sets the size)');
  }
  return errors;
}
