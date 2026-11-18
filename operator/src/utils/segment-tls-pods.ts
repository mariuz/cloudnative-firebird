import { V1Container, V1EnvVar, V1PodSpec, V1Volume } from '@kubernetes/client-node';
import { FirebirdCluster } from '../types';
import { SEGMENT_PORT } from './replication';

/**
 * Segment TLS (spec.segmentTLS): the segment servers (replication sidecar, or the backup file
 * server without replication) and every client of theirs talk mutual TLS with the cluster's own
 * CA (the <cluster>-segment-tls Secret, managed by the operator).
 *
 * Every process of it is the agent (utils/agent.ts), which does its TLS itself, with the cluster's
 * certificates mounted into its container: the segment server accepts mutual TLS on the segment
 * port (and plain text on localhost, for the pod's own clients such as the Lease holder), and the
 * puller, the replication init and the Jobs' programs connect over TLS (SEGMENT_TLS). Since
 * v0.171.0 there is no proxy sidecar.
 */

export const SEGMENT_TLS_VOLUME = 'segment-tls';
export const SEGMENT_TLS_DIR = '/etc/segment-tls';
export const SEGMENT_TLS_PEERS_VOLUME = 'segment-tls-peers';
export const SEGMENT_TLS_PEERS_DIR = '/etc/segment-tls-peers';
/** Where the segment server listens in plain text, for the pod's own clients */
export const SEGMENT_SERVER_LOCAL_PORT = 3061;
/** Containers running a segment server */
const SEGMENT_SERVERS = ['segment-server', 'backup-files'];
/** Containers of an instance whose process is a client of the other instances' segment servers */
const CLIENTS = ['segment-puller', 'replication-init'];

export function segmentTlsEnabled(cluster: FirebirdCluster): boolean {
  return cluster.spec.segmentTLS?.enabled === true;
}

export function segmentTlsSecretName(cluster: FirebirdCluster): string {
  return `${cluster.metadata.name}-segment-tls`;
}

/** The instances still serving in plain text while segment TLS is switched (the agent's tls.rs) */
export function segmentTlsPeersName(cluster: FirebirdCluster): string {
  return `${cluster.metadata.name}-segment-tls-peers`;
}

/** Names in the certificate (informational: peers are authenticated by the cluster's CA) */
export function segmentTlsDnsNames(cluster: FirebirdCluster): string[] {
  const headless = `${cluster.metadata.name}-headless`;
  const namespace = cluster.metadata.namespace ?? 'default';
  return [`*.${headless}`, `*.${headless}.${namespace}.svc`, `*.${headless}.${namespace}.svc.cluster.local`];
}

function volumes(cluster: FirebirdCluster): V1Volume[] {
  return [
    {
      name: SEGMENT_TLS_VOLUME,
      secret: {
        secretName: segmentTlsSecretName(cluster),
        // not the CA's key, which only the operator uses (to renew the certificate)
        items: ['ca.crt', 'tls.crt', 'tls.key'].map((key) => ({ key, path: key })),
      },
    },
    // written by the operator while the instances switch modes; absent means TLS only
    { name: SEGMENT_TLS_PEERS_VOLUME, configMap: { name: segmentTlsPeersName(cluster), optional: true } },
  ];
}

const withEnv = (c: V1Container, env: V1EnvVar[]): V1Container => ({
  ...c,
  env: [...(c.env ?? []).filter((e) => !env.some((n) => n.name === e.name)), ...env],
});

/**
 * Whether the instance pod serves its segments over TLS: its certificates are mounted (with the
 * proxy sidecar of before v0.171.0, or in the agent's own containers since)
 */
export function podHasSegmentTls(spec: V1PodSpec | undefined): boolean {
  return (spec?.volumes ?? []).some((v) => v.name === SEGMENT_TLS_VOLUME);
}

/**
 * The pod spec with segment TLS: the certificates in the containers of the agent's processes
 * (SEGMENT_TLS for its clients), and in an instance pod the segment server's TLS port with a
 * plain listener on localhost
 */
export function withSegmentTls<T extends V1PodSpec>(cluster: FirebirdCluster, spec: T, instance: boolean): T {
  if (!segmentTlsEnabled(cluster)) return spec;
  const client: V1EnvVar = { name: 'SEGMENT_TLS', value: 'true' };
  const certificates = (c: V1Container, env: V1EnvVar[]): V1Container => ({
    ...withEnv(c, [
      ...env,
      { name: 'SEGMENT_TLS_DIR', value: SEGMENT_TLS_DIR },
      { name: 'SEGMENT_TLS_PEERS_DIR', value: SEGMENT_TLS_PEERS_DIR },
    ]),
    volumeMounts: [
      ...(c.volumeMounts ?? []),
      { name: SEGMENT_TLS_VOLUME, mountPath: SEGMENT_TLS_DIR, readOnly: true },
      { name: SEGMENT_TLS_PEERS_VOLUME, mountPath: SEGMENT_TLS_PEERS_DIR, readOnly: true },
    ],
  });
  const container = (c: V1Container): V1Container => {
    // a Job's programs: clients of the instances (a native sidecar of a Job is none of them)
    if (!instance) return c.restartPolicy === 'Always' ? c : certificates(c, [client]);
    if (CLIENTS.includes(c.name)) return certificates(c, [client]);
    if (!SEGMENT_SERVERS.includes(c.name)) return c;
    // mutual TLS on the segment port; plain text on localhost for the pod's own clients (the
    // Lease holder); its own requests to the other instances (the isolation check) over TLS
    return certificates(c, [
      client,
      { name: 'SEGMENT_LISTEN', value: `127.0.0.1:${SEGMENT_SERVER_LOCAL_PORT}` },
      { name: 'SEGMENT_TLS_LISTEN', value: `0.0.0.0:${SEGMENT_PORT}` },
    ]);
  };
  return {
    ...spec,
    ...(spec.initContainers ? { initContainers: spec.initContainers.map(container) } : {}),
    containers: spec.containers.map(container),
    volumes: [...(spec.volumes ?? []), ...volumes(cluster)],
  };
}
