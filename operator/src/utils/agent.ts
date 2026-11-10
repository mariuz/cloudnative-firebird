import { V1Container, V1PodSpec } from '@kubernetes/client-node';
import { API_GROUP, FirebirdCluster } from '../types';
import { operatorImage } from './operator-image';

/**
 * The instance pods' Rust agent (docs/rust-agent-roadmap.md). Phase 1 only delivers it: a cluster
 * that opts in with the annotation below gets the fb-agent binary of the operator image copied
 * into its instance pods, next to the segment server, which keeps running the Perl scripts.
 * Nothing calls the binary yet. Setting or removing the annotation changes the pod template, so
 * the instances restart (primary last).
 */
export const AGENT_ANNOTATION = `${API_GROUP}/agent`;
export const AGENT_VOLUME = 'fb-agent';
export const AGENT_DIR = '/opt/fb-agent';
export const AGENT_PATH = `${AGENT_DIR}/fb-agent`;
export const AGENT_INSTALL_CONTAINER = 'agent-install';
/** The binary in the operator image (Dockerfile) */
const AGENT_IMAGE_PATH = '/app/fb-agent';
/** The containers that run the segment server (with and without replication) */
const AGENT_CONTAINERS = ['segment-server', 'backup-files'];

/** "rust" (the agent is delivered) or "perl" (the default) */
export function agentMode(cluster: FirebirdCluster): 'rust' | 'perl' {
  return cluster.metadata.annotations?.[AGENT_ANNOTATION] === 'rust' ? 'rust' : 'perl';
}

/** Problems with the agent annotation, for validateClusterSpec */
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

/** An instance pod spec with the agent binary, for clusters that opted in */
export function withAgent<T extends V1PodSpec>(cluster: FirebirdCluster, spec: T): T {
  if (agentMode(cluster) !== 'rust') return spec;
  const container = (c: V1Container): V1Container =>
    AGENT_CONTAINERS.includes(c.name)
      ? {
          ...c,
          env: [...(c.env ?? []), { name: 'FB_AGENT', value: AGENT_PATH }],
          volumeMounts: [...(c.volumeMounts ?? []), { name: AGENT_VOLUME, mountPath: AGENT_DIR, readOnly: true }],
        }
      : c;
  return {
    ...spec,
    initContainers: [installer(), ...(spec.initContainers ?? [])],
    containers: spec.containers.map(container),
    volumes: [...(spec.volumes ?? []), { name: AGENT_VOLUME, emptyDir: {} }],
  };
}
