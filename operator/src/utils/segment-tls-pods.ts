import { V1Container, V1EnvVar, V1PodSpec, V1Volume } from '@kubernetes/client-node';
import { FirebirdCluster } from '../types';
import { SEGMENT_PORT } from './replication';
import { operatorImage } from './operator-image';

/**
 * Segment TLS (spec.segmentTLS): the segment servers (replication sidecar, or the backup file
 * server without replication) and every client of theirs talk through the segment-tls proxy
 * (segment-tls.ts, from the operator image), which carries the bytes over mutual TLS with the
 * cluster's own CA (the <cluster>-segment-tls Secret, managed by the operator).
 *
 * In the instance pods the proxy runs as a native sidecar (an init container that keeps
 * running, Kubernetes 1.29+), so the replication init container's seeding can use it too: it
 * accepts TLS on the segment port and forwards to the segment server, which listens on
 * localhost only, and it relays the Perl clients' connections (SEGMENT_PROXY). Job pods get the
 * client side only.
 */

export const SEGMENT_TLS_CONTAINER = 'segment-tls';
export const SEGMENT_TLS_VOLUME = 'segment-tls';
export const SEGMENT_TLS_DIR = '/etc/segment-tls';
/** Where the segment server listens behind the proxy */
export const SEGMENT_SERVER_LOCAL_PORT = 3061;
/** The client side of the proxy, for the Perl clients of the pod */
export const SEGMENT_PROXY_PORT = 3052;
/** Containers running a segment server */
const SEGMENT_SERVERS = ['segment-server', 'backup-files'];

export function segmentTlsEnabled(cluster: FirebirdCluster): boolean {
  return cluster.spec.segmentTLS?.enabled === true;
}

export function segmentTlsSecretName(cluster: FirebirdCluster): string {
  return `${cluster.metadata.name}-segment-tls`;
}

/** Names in the certificate (informational: peers are authenticated by the cluster's CA) */
export function segmentTlsDnsNames(cluster: FirebirdCluster): string[] {
  const headless = `${cluster.metadata.name}-headless`;
  const namespace = cluster.metadata.namespace ?? 'default';
  return [`*.${headless}`, `*.${headless}.${namespace}.svc`, `*.${headless}.${namespace}.svc.cluster.local`];
}

function sidecar(server: boolean): V1Container {
  const env: V1EnvVar[] = [
    { name: 'SEGMENT_TLS_DIR', value: SEGMENT_TLS_DIR },
    { name: 'CLIENT_LISTEN', value: `127.0.0.1:${SEGMENT_PROXY_PORT}` },
    ...(server
      ? [
          { name: 'SERVER_LISTEN', value: `0.0.0.0:${SEGMENT_PORT}` },
          { name: 'SERVER_TARGET', value: `127.0.0.1:${SEGMENT_SERVER_LOCAL_PORT}` },
        ]
      : []),
  ];
  return {
    name: SEGMENT_TLS_CONTAINER,
    image: operatorImage(),
    command: ['node', 'dist/segment-tls.js'],
    // native sidecar: started before the next init container, stopped after the main containers
    restartPolicy: 'Always',
    ...(server ? { ports: [{ name: 'segments', containerPort: SEGMENT_PORT, protocol: 'TCP' }] } : {}),
    env,
    // the next init container (seeding) starts once the client side accepts connections
    startupProbe: {
      exec: {
        command: [
          'node',
          '-e',
          `require('net').connect(${SEGMENT_PROXY_PORT},'127.0.0.1').on('connect',()=>process.exit(0)).on('error',()=>process.exit(1))`,
        ],
      },
      periodSeconds: 1,
      failureThreshold: 60,
    },
    resources: { requests: { cpu: '10m', memory: '32Mi' } },
    securityContext: {
      runAsNonRoot: true,
      runAsUser: 65532,
      runAsGroup: 65532,
      allowPrivilegeEscalation: false,
      readOnlyRootFilesystem: true,
      capabilities: { drop: ['ALL'] },
    },
    volumeMounts: [{ name: SEGMENT_TLS_VOLUME, mountPath: SEGMENT_TLS_DIR, readOnly: true }],
  };
}

function volume(cluster: FirebirdCluster): V1Volume {
  return {
    name: SEGMENT_TLS_VOLUME,
    secret: {
      secretName: segmentTlsSecretName(cluster),
      // not the CA's key, which only the operator uses (to renew the certificate)
      items: ['ca.crt', 'tls.crt', 'tls.key'].map((key) => ({ key, path: key })),
    },
  };
}

const withEnv = (c: V1Container, env: V1EnvVar[]): V1Container => ({
  ...c,
  env: [...(c.env ?? []).filter((e) => !env.some((n) => n.name === e.name)), ...env],
});

/**
 * The pod spec with segment TLS: the proxy sidecar (server side too for instance pods), the
 * certificate volume, SEGMENT_PROXY for every container, and the segment server on localhost
 */
export function withSegmentTls<T extends V1PodSpec>(cluster: FirebirdCluster, spec: T, instance: boolean): T {
  if (!segmentTlsEnabled(cluster)) return spec;
  const proxy: V1EnvVar = { name: 'SEGMENT_PROXY', value: `127.0.0.1:${SEGMENT_PROXY_PORT}` };
  const container = (c: V1Container): V1Container => {
    if (!SEGMENT_SERVERS.includes(c.name)) return withEnv(c, [proxy]);
    // the proxy owns the segment port
    const { ports, ...rest } = c;
    const others = (ports ?? []).filter((p) => p.containerPort !== SEGMENT_PORT);
    return withEnv({ ...rest, ...(others.length ? { ports: others } : {}) }, [
      proxy,
      { name: 'SEGMENT_LISTEN', value: `127.0.0.1:${SEGMENT_SERVER_LOCAL_PORT}` },
    ]);
  };
  return {
    ...spec,
    initContainers: [sidecar(instance), ...(spec.initContainers ?? []).map(container)],
    containers: spec.containers.map(container),
    volumes: [...(spec.volumes ?? []), volume(cluster)],
  };
}
