import { KubeConfig, VersionApi } from '@kubernetes/client-node';
import { logger } from './logger';

/**
 * The default of spec.segmentTLS.enabled for new clusters:
 *
 * - SEGMENT_TLS_DEFAULT=auto (default) or true: on
 * - SEGMENT_TLS_DEFAULT=false: off
 *
 * Until v0.171.0 auto meant on from Kubernetes 1.29 only: segment TLS ran a proxy as a native
 * sidecar. The agent does its TLS itself since, on any version (segment-tls-pods.ts).
 *
 * SEGMENT_TLS_REQUIRED=true refuses plain segment shipping altogether: admission denies a cluster
 * created with `enabled: false` or switched to it, new clusters default to true, and an existing plain cluster (pinned on upgrade, or chosen before the setting) gets
 * a SegmentTLS condition, a warning event and firebird_cluster_segment_tls 0 until it is moved
 * (SEGMENT_TLS_MIGRATE, or its owner). The operator never restarts a cluster for it.
 *
 * The default is written into a cluster's spec when it is first reconciled (the controller's
 * defaultSegmentTls), so it never changes for an existing cluster: clusters that already have a
 * StatefulSet (created by an earlier version) are pinned to false, and changing the operator's
 * setting or upgrading Kubernetes later leaves every existing cluster as it is.
 */

/** Kubernetes version that enables native sidecar containers by default (the primary Lease holder) */
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

/** Reads the API server's version at startup (for the primary Lease's default) */
export async function discoverServerVersion(kubeConfig: KubeConfig): Promise<{ major: number; minor: number } | undefined> {
  try {
    const info = await kubeConfig.makeApiClient(VersionApi).getCode();
    serverVersion = parseServerVersion(info.major, info.minor);
  } catch (err) {
    logger.warn({ err }, 'Could not read the Kubernetes version; the primary Lease is off by default for new clusters');
  }
  return serverVersion;
}

/** Whether the operator refuses plain segment shipping (SEGMENT_TLS_REQUIRED) */
export function segmentTlsRequired(env = process.env): boolean {
  return (env.SEGMENT_TLS_REQUIRED ?? '').trim().toLowerCase() === 'true';
}

/** The default for a new cluster's spec.segmentTLS.enabled */
export function segmentTlsDefault(env = process.env): boolean {
  if (segmentTlsRequired(env)) return true;
  return (env.SEGMENT_TLS_DEFAULT ?? 'auto').trim().toLowerCase() !== 'false';
}
