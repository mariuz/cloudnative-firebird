import { describe, it, expect } from 'vitest';
import { existsSync, readFileSync } from 'fs';
import { join } from 'path';
import { AGENT_ANNOTATION, agentErrors, agentMode } from '../src/utils/agent';
import { buildStatefulSet, statefulSetNeedsUpdate } from '../src/utils/resources';
import { validateClusterSpec } from '../src/utils/validation';
import { FirebirdCluster } from '../src/types';
import { makeCluster } from './helpers/factories';

/**
 * The Rust agent's delivery (docs/rust-agent-roadmap.md, Phase 1): a cluster that opts in gets
 * the fb-agent binary of the operator image copied into its instance pods
 */
const withAgent = (cluster: FirebirdCluster, value = 'rust'): FirebirdCluster => ({
  ...cluster,
  metadata: { ...cluster.metadata, annotations: { ...cluster.metadata.annotations, [AGENT_ANNOTATION]: value } },
});
const pod = (cluster: FirebirdCluster) => buildStatefulSet(cluster).spec!.template.spec!;

describe('the Rust agent in the instance pods', () => {
  it('is off by default: the pods are as before', () => {
    const cluster = makeCluster({ replication: { enabled: true } });
    expect(agentMode(cluster)).toBe('perl');
    expect(agentMode(withAgent(cluster, 'perl'))).toBe('perl');
    expect(pod(cluster).initContainers?.map((c) => c.name)).not.toContain('agent-install');
    expect(pod(cluster).volumes?.map((v) => v.name)).not.toContain('fb-agent');
    expect(JSON.stringify(pod(withAgent(cluster, 'perl')))).toBe(JSON.stringify(pod(cluster)));
  });

  it('is copied from the operator image by the first init container, next to the segment server', () => {
    const cluster = withAgent(makeCluster({ replication: { enabled: true } }));
    expect(agentMode(cluster)).toBe('rust');
    const spec = pod(cluster);
    const install = spec.initContainers![0];
    expect(install).toMatchObject({
      name: 'agent-install',
      command: ['cp', '/app/fb-agent', '/agent/fb-agent'],
      volumeMounts: [{ name: 'fb-agent', mountPath: '/agent' }],
      securityContext: { runAsNonRoot: true, readOnlyRootFilesystem: true, allowPrivilegeEscalation: false, capabilities: { drop: ['ALL'] } },
    });
    expect(install.image).toMatch(/cloudnative-firebird/);
    expect(spec.volumes).toContainEqual({ name: 'fb-agent', emptyDir: {} });
    const server = spec.containers.find((c) => c.name === 'segment-server')!;
    expect(server.volumeMounts).toContainEqual({ name: 'fb-agent', mountPath: '/opt/fb-agent', readOnly: true });
    expect(server.env).toContainEqual({ name: 'FB_AGENT', value: '/opt/fb-agent/fb-agent' });
    // the segment server still runs the Perl script
    expect(server.command).toEqual(['perl', '/etc/firebird-operator/segment-server.pl']);
    // only there
    for (const c of spec.containers.filter((c) => c.name !== 'segment-server')) {
      expect(c.volumeMounts?.map((m) => m.name) ?? []).not.toContain('fb-agent');
    }
  });

  it('reaches the backup file server of a cluster without replication', () => {
    const spec = pod(withAgent(makeCluster()));
    expect(spec.initContainers![0].name).toBe('agent-install');
    expect(spec.containers.find((c) => c.name === 'backup-files')!.volumeMounts).toContainEqual({ name: 'fb-agent', mountPath: '/opt/fb-agent', readOnly: true });
  });

  it('rolls the instances when it is switched on or off', () => {
    const cluster = makeCluster({ replication: { enabled: true } });
    expect(statefulSetNeedsUpdate(buildStatefulSet(cluster), buildStatefulSet(withAgent(cluster)))).toBe(true);
    expect(statefulSetNeedsUpdate(buildStatefulSet(withAgent(cluster)), buildStatefulSet(cluster))).toBe(true);
    expect(statefulSetNeedsUpdate(buildStatefulSet(withAgent(cluster)), buildStatefulSet(withAgent(cluster)))).toBe(false);
  });

  it('accepts "rust" and "perl" only', () => {
    expect(agentErrors(makeCluster())).toEqual([]);
    expect(() => validateClusterSpec(withAgent(makeCluster()))).not.toThrow();
    expect(() => validateClusterSpec(withAgent(makeCluster(), 'perl'))).not.toThrow();
    expect(() => validateClusterSpec(withAgent(makeCluster(), 'go'))).toThrow(/agent "go" is invalid: use "rust" or "perl"/);
  });

  it('is built into the operator image for the target platform, without emulation', () => {
    const dockerfile = readFileSync(join(__dirname, '..', 'Dockerfile'), 'utf8');
    expect(dockerfile).toContain('FROM --platform=$BUILDPLATFORM rust:1.97-alpine AS agent');
    expect(dockerfile).toContain('COPY --from=agent /fb-agent ./fb-agent');
    // the toolchain the image builds with is the one the workspace pins
    expect(readFileSync(join(__dirname, '..', 'agent', 'rust-toolchain.toml'), 'utf8')).toContain('channel = "1.97.0"');
    // the lock file the image builds from
    expect(existsSync(join(__dirname, '..', 'agent', 'Cargo.lock'))).toBe(true);
    expect(readFileSync(join(__dirname, '..', '.dockerignore'), 'utf8')).toContain('agent/target');
  });
});
