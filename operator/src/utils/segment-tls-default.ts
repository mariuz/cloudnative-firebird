import { KubeConfig, VersionApi } from '@kubernetes/client-node';
import { logger } from './logger';

/**
 * The default of spec.segmentTLS.enabled for new clusters. Segment TLS runs its proxy as a native
 * sidecar (segment-tls-pods.ts), which Kubernetes enables by default from 1.29, so the default
 * follows the API server's version unless the operator is told otherwise:
 *
 * - SEGMENT_TLS_DEFAULT=auto (default): on when the API server is Kubernetes 1.29 or later
 * - SEGMENT_TLS_DEFAULT=true / false: always on / off
 *
 * The default is written into a cluster's spec when it is first reconciled (the controller's
 * defaultSegmentTls), so it never changes for an existing cluster: clusters that already have a
 * StatefulSet (created by an earlier version) are pinned to false, and changing the operator's
 * setting or upgrading Kubernetes later leaves every existing cluster as it is.
 */

/** Kubernetes version that enables native sidecar containers by default */
export const NATIVE_SIDECAR_MINOR = 29;

let serverVersion: { major: number; minor: number } | undefined;

/** Parses the API server's version ("1", "29+"); undefined when it cannot be told */
export function parseServerVersion(major: string | undefined, minor: string | undefined): { major: number; minor: number } | undefined {
  const ma = Number.parseInt((major ?? '').replace(/\D.*$/, ''), 10);
  const mi = Number.parseInt((minor ?? '').replace(/\D.*$/, ''), 10);
  return Number.isFinite(ma) && Number.isFinite(mi) ? { major: ma, minor: mi } : undefined;
}

/** Tests */
export function setServerVersion(version: { major: number; minor: number } | undefined): void {
  serverVersion = version;
}

/** Whether the API server supports native sidecars; undefined when its version is not known */
export function nativeSidecarsSupported(): boolean | undefined {
  if (!serverVersion) return undefined;
  return serverVersion.major > 1 || (serverVersion.major === 1 && serverVersion.minor >= NATIVE_SIDECAR_MINOR);
}

/** Reads the API server's version at startup (for the default and the admission warning) */
export async function discoverServerVersion(kubeConfig: KubeConfig): Promise<{ major: number; minor: number } | undefined> {
  try {
    const info = await kubeConfig.makeApiClient(VersionApi).getCode();
    serverVersion = parseServerVersion(info.major, info.minor);
  } catch (err) {
    logger.warn({ err }, 'Could not read the Kubernetes version; segment TLS is off by default for new clusters');
  }
  return serverVersion;
}

/** The default for a new cluster's spec.segmentTLS.enabled */
export function segmentTlsDefault(env = process.env): boolean {
  const setting = (env.SEGMENT_TLS_DEFAULT ?? 'auto').trim().toLowerCase();
  if (setting === 'true') return true;
  if (setting === 'false') return false;
  return nativeSidecarsSupported() === true;
}
