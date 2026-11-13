import { describe, it, expect } from 'vitest';
import { existsSync, readFileSync } from 'fs';
import { join } from 'path';
import { AGENT_ANNOTATION, agentErrors, agentMode } from '../src/utils/agent';
import { V1PodSpec } from '@kubernetes/client-node';
import { buildStatefulSet, statefulSetNeedsUpdate } from '../src/utils/resources';
import { buildFailoverJob } from '../src/utils/switchover';
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

  it('is copied from the operator image by an init container, next to the segment server', () => {
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
    // the segment server is the agent's front, with the Perl script behind it
    expect(server.command).toEqual(['/opt/fb-agent/fb-agent', 'segment-server', '--', 'perl', '/etc/firebird-operator/segment-server.pl']);
    expect(server.ports).toEqual([{ name: 'segments', containerPort: 3051, protocol: 'TCP' }]);
    // and in the puller, which runs from it
    const puller = spec.containers.find((c) => c.name === 'segment-puller')!;
    expect(puller.command).toEqual(['/opt/fb-agent/fb-agent', 'puller']);
    expect(puller.volumeMounts).toContainEqual({ name: 'fb-agent', mountPath: '/opt/fb-agent', readOnly: true });
    // with the environment of the Perl puller
    const perl = pod(makeCluster({ replication: { enabled: true } })).containers.find((c) => c.name === 'segment-puller')!;
    expect(perl.command).toEqual(['perl', '/etc/firebird-operator/segment-puller.pl']);
    expect(puller.env).toEqual([...perl.env!, { name: 'FB_AGENT', value: '/opt/fb-agent/fb-agent' }]);
    // nowhere else
    for (const c of spec.containers.filter((c) => !['segment-server', 'segment-puller'].includes(c.name))) {
      expect(c.volumeMounts?.map((m) => m.name) ?? []).not.toContain('fb-agent');
    }
  });

  it('runs the puller from the agent with segment TLS and the primary Lease too', () => {
    const cluster = withAgent(makeCluster({ replication: { enabled: true, failover: { enabled: true, primaryLease: { enabled: true } } }, segmentTLS: { enabled: true } }));
    const puller = pod(cluster).containers.find((c) => c.name === 'segment-puller')!;
    expect(puller.command).toEqual(['/opt/fb-agent/fb-agent', 'puller']);
    // the installer comes after the native sidecars, which stay first, and before the replication init
    expect(pod(cluster).initContainers!.map((c) => c.name)).toEqual(['segment-tls', 'lease-holder', 'agent-install', 'security-db-init', 'replication-init']);
    // the front listens where the TLS proxy forwards to, as the Perl server did
    const server = pod(cluster).containers.find((c) => c.name === 'segment-server')!;
    expect(server.command!.slice(0, 3)).toEqual(['/opt/fb-agent/fb-agent', 'segment-server', '--']);
    expect(server.env).toContainEqual({ name: 'SEGMENT_LISTEN', value: '127.0.0.1:3061' });
    // through the TLS proxy's client side, as the Perl puller
    expect(puller.env).toContainEqual({ name: 'SEGMENT_PROXY', value: '127.0.0.1:3052' });
  });

  it('reaches the backup file server of a cluster without replication', () => {
    const spec = pod(withAgent(makeCluster()));
    expect(spec.initContainers!.map((c) => c.name)).toContain('agent-install');
    const files = spec.containers.find((c) => c.name === 'backup-files')!;
    expect(files.volumeMounts).toContainEqual({ name: 'fb-agent', mountPath: '/opt/fb-agent', readOnly: true });
    // the backup file server is the front too, in its files-only mode, with the Perl script behind it
    expect(files.command).toEqual(['/opt/fb-agent/fb-agent', 'segment-server', '--', 'perl', '/etc/firebird-operator/segment-server.pl']);
    expect(files.env).toContainEqual({ name: 'FILES_ONLY', value: 'true' });
    // without the annotation it stays the Perl script
    expect(pod(makeCluster()).containers.find((c) => c.name === 'backup-files')!.command).toEqual([
      'perl',
      '/etc/firebird-operator/segment-server.pl',
    ]);
  });

  it('runs the segment TLS proxy from the agent too, in the instance pods and in the Jobs', () => {
    const cluster = makeCluster({ instances: 3, replication: { enabled: true }, segmentTLS: { enabled: true } });
    const proxy = (spec: V1PodSpec) => spec.initContainers!.find((c) => c.name === 'segment-tls')!;
    // without the annotation: the Node proxy of the operator image
    expect(proxy(pod(cluster)).command).toEqual(['node', 'dist/segment-tls.js']);
    expect(proxy(pod(cluster)).resources).toEqual({ requests: { cpu: '10m', memory: '32Mi' } });
    // with it: fb-agent from the same image, the same environment, a probe without Node
    const agent = proxy(pod(withAgent(cluster)));
    expect(agent.command).toEqual(['/app/fb-agent', 'segment-tls']);
    expect(agent.startupProbe!.exec!.command).toEqual(['/app/fb-agent', 'tcp', '127.0.0.1', '3052']);
    expect(agent.resources).toEqual({ requests: { cpu: '10m', memory: '8Mi' } });
    expect(agent.env).toEqual(proxy(pod(cluster)).env);
    expect(agent.image).toBe(proxy(pod(cluster)).image);
    const job = (c: FirebirdCluster) => proxy(buildFailoverJob(c, ['test-cluster-1']).spec!.template.spec!);
    expect(job(cluster).command).toEqual(['node', 'dist/segment-tls.js']);
    expect(job(withAgent(cluster)).command).toEqual(['/app/fb-agent', 'segment-tls']);
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
