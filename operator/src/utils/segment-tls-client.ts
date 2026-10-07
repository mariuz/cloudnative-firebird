import { CoreV1Api, V1Pod, V1Secret } from '@kubernetes/client-node';
import { API_GROUP, FirebirdCluster, RESOURCE_KIND } from '../types';
import { certificateNamesMatch, createTlsCertificates, daysUntilExpiry } from './certificates';
import { SegmentTlsMaterial, SegmentTlsResolver } from './replication-lag';
import { SEGMENT_TLS_CONTAINER, segmentTlsDnsNames, segmentTlsSecretName } from './segment-tls-pods';

/**
 * The operator's side of segment TLS (segment-tls-pods.ts): the cluster's CA and certificate in
 * the <cluster>-segment-tls Secret, created and renewed here, and the choice per instance between
 * TLS and a plain connection. An instance is reached over TLS when its pod runs the TLS proxy:
 * while segment TLS is switched on or off, the instances restart one by one, and each one is
 * reached the way it serves.
 */

/** Certificates are renewed this many days before they expire, the CA a year before */
const RENEW_DAYS = 30;
const CA_RENEW_DAYS = 365;

function isNotFound(err: unknown): boolean {
  const e = err as { code?: number; statusCode?: number; response?: { statusCode?: number } };
  return e?.code === 404 || e?.statusCode === 404 || e?.response?.statusCode === 404;
}

/** Whether the instance pod runs the segment TLS proxy */
export function hasSegmentTlsProxy(pod: V1Pod | undefined): boolean {
  return (pod?.spec?.initContainers ?? []).some((c) => c.name === SEGMENT_TLS_CONTAINER);
}

/** The instance a segment server address names (<pod>.<cluster>-headless.<namespace>.svc) */
export function parseInstanceHost(host: string): { pod: string; cluster: string; namespace: string } | undefined {
  const m = /^([^.]+)\.(.+)-headless\.([^.]+)\.svc(?:\.cluster\.local)?$/.exec(host);
  return m ? { pod: m[1], cluster: m[2], namespace: m[3] } : undefined;
}

const decode = (secret: V1Secret | undefined, key: string) =>
  secret?.data?.[key] ? Buffer.from(secret.data[key], 'base64').toString('utf8') : '';

function material(secret: V1Secret | undefined): SegmentTlsMaterial | undefined {
  const ca = decode(secret, 'ca.crt');
  const cert = decode(secret, 'tls.crt');
  const key = decode(secret, 'tls.key');
  return ca && cert && key ? { ca, cert, key } : undefined;
}

/**
 * TLS for the instances that run the proxy, with their cluster's certificates; plain for the
 * others and for addresses that name no instance. Pods and Secrets are cached for ttlMs.
 */
export function createSegmentTlsResolver(core: CoreV1Api, ttlMs = 15_000, now = () => Date.now()): SegmentTlsResolver {
  const pods = new Map<string, { at: number; pod: V1Pod | undefined }>();
  const secrets = new Map<string, { at: number; secret: V1Secret | undefined }>();
  const cached = async <T>(cache: Map<string, { at: number } & T>, key: string, load: () => Promise<T>): Promise<T> => {
    const hit = cache.get(key);
    if (hit && now() - hit.at < ttlMs) return hit;
    const value = { ...(await load()), at: now() };
    cache.set(key, value);
    return value;
  };
  const read = async <T>(get: () => Promise<T>): Promise<T | undefined> => {
    try {
      return await get();
    } catch (err) {
      if (isNotFound(err)) return undefined;
      throw err;
    }
  };
  return async (host) => {
    const instance = parseInstanceHost(host);
    if (!instance) return undefined;
    const { pod } = await cached(pods, `${instance.namespace}/${instance.pod}`, async () => ({
      pod: await read(() => core.readNamespacedPod({ name: instance.pod, namespace: instance.namespace })),
    }));
    if (!hasSegmentTlsProxy(pod)) return undefined;
    const name = `${instance.cluster}-segment-tls`;
    const { secret } = await cached(secrets, `${instance.namespace}/${name}`, async () => ({
      secret: await read(() => core.readNamespacedSecret({ name, namespace: instance.namespace })),
    }));
    return material(secret);
  };
}

/**
 * The cluster's segment TLS Secret: created with a new CA, or renewed (the certificate 30 days
 * before it expires, the CA a year before), owned by the cluster
 */
export async function ensureSegmentTlsSecret(core: CoreV1Api, cluster: FirebirdCluster, now = new Date()): Promise<SegmentTlsMaterial> {
  const namespace = cluster.metadata.namespace ?? 'default';
  const name = segmentTlsSecretName(cluster);
  const dnsNames = segmentTlsDnsNames(cluster);
  for (let attempt = 1; ; attempt++) {
    let secret: V1Secret | undefined;
    try {
      secret = await core.readNamespacedSecret({ name, namespace });
    } catch (err) {
      if (!isNotFound(err)) throw err;
    }
    const caCert = decode(secret, 'ca.crt');
    const caKey = decode(secret, 'ca.key');
    const current = material(secret);
    const caValid = caCert !== '' && caKey !== '' && (daysUntilExpiry(caCert, now) ?? 0) > CA_RENEW_DAYS;
    if (
      current &&
      caValid &&
      (daysUntilExpiry(current.cert, now) ?? 0) > RENEW_DAYS &&
      certificateNamesMatch(current.cert, caCert, dnsNames)
    ) {
      return current;
    }
    const certs = createTlsCertificates(
      { caName: `cloudnative-firebird ${namespace}/${cluster.metadata.name} segment CA`, dnsNames, clientAuth: true },
      now,
      caValid ? { cert: caCert, key: caKey } : undefined,
    );
    const body: V1Secret = {
      metadata: {
        name,
        namespace,
        labels: { 'app.kubernetes.io/name': 'cloudnative-firebird', [`${API_GROUP}/cluster`]: cluster.metadata.name },
        ownerReferences: [
          { apiVersion: `${API_GROUP}/v1`, kind: RESOURCE_KIND, name: cluster.metadata.name, uid: cluster.metadata.uid ?? '', controller: true },
        ],
        ...(secret ? { resourceVersion: secret.metadata?.resourceVersion } : {}),
      },
      type: 'kubernetes.io/tls',
      data: {
        'ca.crt': Buffer.from(certs.caCert).toString('base64'),
        'ca.key': Buffer.from(certs.caKey).toString('base64'),
        'tls.crt': Buffer.from(certs.tlsCert).toString('base64'),
        'tls.key': Buffer.from(certs.tlsKey).toString('base64'),
      },
    };
    try {
      if (secret) await core.replaceNamespacedSecret({ name, namespace, body });
      else await core.createNamespacedSecret({ namespace, body });
      return { ca: certs.caCert, cert: certs.tlsCert, key: certs.tlsKey };
    } catch (err) {
      const code = (err as { code?: number }).code;
      // another writer (a second reconcile): read it again
      if (code !== 409 || attempt >= 3) throw err;
    }
  }
}
