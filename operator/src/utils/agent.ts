import { V1Container, V1PodSpec } from '@kubernetes/client-node';
import { API_GROUP, FirebirdCluster } from '../types';
import { operatorImage } from './operator-image';

/**
 * The Rust agent (docs/rust-agent-roadmap.md): the fb-agent binary of the operator image, copied
 * into every instance pod and every Job's pod. Since v0.170.0 it is what they run, on every
 * cluster: the segment server and puller, the backup file server, the replication init's
 * helpers, the primary Lease holder, the segment TLS proxy, the custom metrics exporter, and the
 * Jobs' programs (switchover, failover, the synchronous standby, the backup files, the journal
 * archive, point-in-time recovery). The Perl scripts they replaced are gone.
 *
 * The annotation that made a cluster opt in (v0.152.0 to v0.169.0) is accepted and ignored.
 */
export const AGENT_ANNOTATION = `${API_GROUP}/agent`;
export const AGENT_VOLUME = 'fb-agent';
export const AGENT_DIR = '/opt/fb-agent';
export const AGENT_PATH = `${AGENT_DIR}/fb-agent`;
export const AGENT_INSTALL_CONTAINER = 'agent-install';
/** The binary in the operator image (Dockerfile) */
export const AGENT_IMAGE_PATH = '/app/fb-agent';
/**
 * The containers of an instance pod that run the agent: the segment servers (with and without
 * replication), the puller, the replication init container, and the custom metrics exporter
 */
const AGENT_CONTAINERS = ['segment-server', 'backup-files', 'segment-puller', 'replication-init', 'metrics-exporter'];

/** Problems with the agent annotation, for validateClusterSpec: its values of before v0.170.0 are accepted */
export function agentErrors(cluster: FirebirdCluster): string[] {
  const value = cluster.metadata.annotations?.[AGENT_ANNOTATION];
  return value === undefined || value === 'rust' || value === 'perl'
    ? []
    : [`annotation ${AGENT_ANNOTATION} "${value}" is invalid: use "rust" or "perl"`];
}

/** The init container that copies the static binary into the pod's agent volume */
function installer(): V1Container {
  return {
    name: AGENT_INSTALL_CONTAINER,
    image: operatorImage(),
    command: ['cp', AGENT_IMAGE_PATH, `/agent/fb-agent`],
    resources: { requests: { cpu: '10m', memory: '16Mi' } },
    securityContext: {
      runAsNonRoot: true,
      runAsUser: 65532,
      runAsGroup: 65532,
      allowPrivilegeEscalation: false,
      readOnlyRootFilesystem: true,
      capabilities: { drop: ['ALL'] },
    },
    volumeMounts: [{ name: AGENT_VOLUME, mountPath: '/agent' }],
  };
}

/** The binary in the pod, where the agent's containers run it (FB_AGENT), installed after the native sidecars */
function withInstaller<T extends V1PodSpec>(spec: T, container: (c: V1Container) => V1Container): T {
  const init = spec.initContainers ?? [];
  const sidecars = init.findIndex((c) => c.restartPolicy !== 'Always');
  const at = sidecars < 0 ? init.length : sidecars;
  return {
    ...spec,
    initContainers: [...init.slice(0, at), installer(), ...init.slice(at).map(container)],
    containers: spec.containers.map(container),
    volumes: [...(spec.volumes ?? []), { name: AGENT_VOLUME, emptyDir: {} }],
  };
}

const withBinary = (c: V1Container): V1Container => ({
  ...c,
  env: [...(c.env ?? []), { name: 'FB_AGENT', value: AGENT_PATH }],
  volumeMounts: [...(c.volumeMounts ?? []), { name: AGENT_VOLUME, mountPath: AGENT_DIR, readOnly: true }],
});

/** An instance pod spec with the agent binary in the containers that run it */
export function withAgent<T extends V1PodSpec>(spec: T): T {
  return withInstaller(spec, (c) => (AGENT_CONTAINERS.includes(c.name) ? withBinary(c) : c));
}

/**
 * A Job's pod spec with the agent binary in every container (not the segment TLS proxy, a sidecar
 * from the operator image), when one of them runs it; the Jobs that only run Firebird's tools
 * (databases, users, roles) are left as they are
 */
export function withAgentInJob<T extends V1PodSpec>(spec: T): T {
  const own = [...(spec.initContainers ?? []), ...spec.containers].filter((c) => c.restartPolicy !== 'Always');
  const runs = own.some((c) => /fb-agent|\$FB_AGENT|pitr-restore\.sh/.test([...(c.command ?? []), ...(c.args ?? [])].join(' ')));
  return runs ? withInstaller(spec, (c) => (c.restartPolicy === 'Always' ? c : withBinary(c))) : spec;
}
