import { describe, it, expect } from 'vitest';
import { existsSync, readFileSync } from 'fs';
import { join } from 'path';
import { AGENT_ANNOTATION, agentErrors, agentMode } from '../src/utils/agent';
import { V1PodSpec } from '@kubernetes/client-node';
import { buildConfigMap, buildStatefulSet, statefulSetNeedsUpdate } from '../src/utils/resources';
import { AGENT_SCRIPTS, REPLICATION_SCRIPTS } from '../src/utils/replication';
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
    // the segment server is the agent's, with no Perl script behind it
    expect(server.command).toEqual(['/opt/fb-agent/fb-agent', 'segment-server']);
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
    expect(server.command).toEqual(['/opt/fb-agent/fb-agent', 'segment-server']);
    expect(server.env).toContainEqual({ name: 'SEGMENT_LISTEN', value: '127.0.0.1:3061' });
    // through the TLS proxy's client side, as the Perl puller
    expect(puller.env).toContainEqual({ name: 'SEGMENT_PROXY', value: '127.0.0.1:3052' });
  });

  it('runs the replication init with the agent\'s helpers, from a script derived from init-instance.sh', () => {
    const plain = pod(makeCluster({ replication: { enabled: true } })).initContainers!.find((c) => c.name === 'replication-init')!;
    expect(plain.command).toEqual(['sh', '/etc/firebird-operator/init-instance.sh']);
    const init = pod(withAgent(makeCluster({ replication: { enabled: true } }))).initContainers!.find((c) => c.name === 'replication-init')!;
    expect(init.command).toEqual(['sh', '/etc/firebird-operator/init-instance-agent.sh']);
    expect(init.env).toEqual([...plain.env!, { name: 'FB_AGENT', value: '/opt/fb-agent/fb-agent' }]);
    expect(init.volumeMounts).toEqual([...plain.volumeMounts!, { name: 'fb-agent', mountPath: '/opt/fb-agent', readOnly: true }]);
    // the binary is installed before it runs
    const order = pod(withAgent(makeCluster({ replication: { enabled: true } }))).initContainers!.map((c) => c.name);
    expect(order.indexOf('agent-install')).toBeLessThan(order.indexOf('replication-init'));
    // the script: init-instance.sh, with the agent's command where it ran a Perl helper
    const [original, derived] = [REPLICATION_SCRIPTS['init-instance.sh'], AGENT_SCRIPTS['init-instance-agent.sh']];
    const back = derived
      .replace('echo "replication init with the helpers of $("$FB_AGENT" version)"\n', '')
      .split('"$FB_AGENT" reposition').join('perl "$SCRIPT_DIR/pitr-plan.pl" --reposition')
      .split('"$FB_AGENT" set-repl-seq').join('perl "$SCRIPT_DIR/set-repl-seq.pl"')
      .split('"$FB_AGENT" replica-control').join('perl "$SCRIPT_DIR/replica-control.pl"')
      .split('"$FB_AGENT" request').join('perl "$SCRIPT_DIR/segment-request.pl"')
      .split('"$FB_AGENT" fetch-seed').join('perl "$SCRIPT_DIR/fetch-seed.pl"');
    expect(back).toBe(original);
    expect(derived.split('\n').filter((line) => /^[^#]*\bperl\b/.test(line))).toEqual([]);
    expect(derived.match(/"\$FB_AGENT" [a-z-]+/g)!.sort()).toEqual([
      '"$FB_AGENT" fetch-seed',
      '"$FB_AGENT" replica-control',
      '"$FB_AGENT" replica-control',
      '"$FB_AGENT" replica-control',
      '"$FB_AGENT" reposition',
      '"$FB_AGENT" request',
      '"$FB_AGENT" set-repl-seq',
      '"$FB_AGENT" set-repl-seq',
      '"$FB_AGENT" version',
    ]);
    // the ConfigMap has both, and the script is not in the hash that restarts every cluster
    expect(buildConfigMap(makeCluster({ replication: { enabled: true } }))!.data!['init-instance-agent.sh']).toBe(derived);
    const hash = (cluster: FirebirdCluster) => buildStatefulSet(cluster).spec!.template.metadata!.annotations!['firebird.cloudnative-firebird.io/replication-config-hash'];
    expect(hash(withAgent(makeCluster({ replication: { enabled: true } })))).toBe(hash(makeCluster({ replication: { enabled: true } })));
    // with segment TLS the init has the certificates: its seed is fetched over TLS made by the agent
    const tls = pod(withAgent(makeCluster({ replication: { enabled: true }, segmentTLS: { enabled: true } }))).initContainers!.find((c) => c.name === 'replication-init')!;
    expect(tls.env).toContainEqual({ name: 'SEGMENT_TLS_DIR', value: '/etc/segment-tls' });
    expect(tls.env).toContainEqual({ name: 'SEGMENT_PROXY', value: '127.0.0.1:3052' });
    expect(tls.volumeMounts).toContainEqual({ name: 'segment-tls', mountPath: '/etc/segment-tls', readOnly: true });
  });

  it('runs the primary Lease holder from the agent, in the operator image', () => {
    const spec = { replication: { enabled: true, failover: { enabled: true, primaryLease: { enabled: true } } } };
    const node = pod(makeCluster(spec)).initContainers!.find((c) => c.name === 'lease-holder')!;
    expect(node.command).toEqual(['node', 'dist/lease-holder.js']);
    expect(node.resources).toEqual({ requests: { cpu: '10m', memory: '32Mi' } });
    const holder = pod(withAgent(makeCluster(spec))).initContainers!.find((c) => c.name === 'lease-holder')!;
    // the binary of the operator image: the sidecar starts before the pod's agent volume is filled
    expect(holder.command).toEqual(['/app/fb-agent', 'lease-holder']);
    expect(holder.image).toBe(node.image);
    expect(holder.resources).toEqual({ requests: { cpu: '10m', memory: '8Mi' } });
    // everything else as the Node holder's: its environment, its token, its mounts
    expect({ ...holder, command: [], resources: {} }).toEqual({ ...node, command: [], resources: {} });
  });

  it('reaches the backup file server of a cluster without replication', () => {
    const spec = pod(withAgent(makeCluster()));
    expect(spec.initContainers!.map((c) => c.name)).toContain('agent-install');
    const files = spec.containers.find((c) => c.name === 'backup-files')!;
    expect(files.volumeMounts).toContainEqual({ name: 'fb-agent', mountPath: '/opt/fb-agent', readOnly: true });
    // the backup file server is the agent's too, in its files-only mode
    expect(files.command).toEqual(['/opt/fb-agent/fb-agent', 'segment-server']);
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
    expect(agent.image).toBe(proxy(pod(cluster)).image);
    // the agent's own processes do their TLS themselves: the front on the segment port, the puller
    // to the primary; the proxy keeps its client side for the pod's Perl clients
    const spec = pod(withAgent(cluster));
    const env = (name: string) => Object.fromEntries((spec.containers.find((c) => c.name === name)!.env ?? []).map((e) => [e.name, e.value]));
    expect(agent.env!.map((e) => e.name)).toEqual(['SEGMENT_TLS_DIR', 'SEGMENT_TLS_PEERS_DIR', 'CLIENT_LISTEN']);
    expect(agent.ports).toBeUndefined();
    const server = spec.containers.find((c) => c.name === 'segment-server')!;
    expect(server.ports).toContainEqual(expect.objectContaining({ containerPort: 3051 }));
    expect(env('segment-server')).toMatchObject({
      SEGMENT_TLS_LISTEN: '0.0.0.0:3051',
      SEGMENT_LISTEN: '127.0.0.1:3061',
      SEGMENT_TLS_DIR: '/etc/segment-tls',
      SEGMENT_TLS_PEERS_DIR: '/etc/segment-tls-peers',
      SEGMENT_PROXY: '127.0.0.1:3052',
    });
    expect(env('segment-puller')).toMatchObject({ SEGMENT_TLS_DIR: '/etc/segment-tls', SEGMENT_PROXY: '127.0.0.1:3052' });
    for (const name of ['segment-server', 'segment-puller']) {
      expect(spec.containers.find((c) => c.name === name)!.volumeMounts).toEqual(
        expect.arrayContaining([
          { name: 'segment-tls', mountPath: '/etc/segment-tls', readOnly: true },
          { name: 'segment-tls-peers', mountPath: '/etc/segment-tls-peers', readOnly: true },
        ]),
      );
    }
    // the database container and the Perl clients are as before: through the proxy
    expect(env('firebird').SEGMENT_TLS_DIR).toBeUndefined();
    expect(spec.initContainers!.find((c) => c.name === 'replication-init')!.env).toContainEqual({ name: 'SEGMENT_PROXY', value: '127.0.0.1:3052' });
    // without replication: the backup file server's front, the same way
    const single = pod(withAgent(makeCluster({ segmentTLS: { enabled: true } })));
    const files = single.containers.find((c) => c.name === 'backup-files')!;
    expect(files.env).toContainEqual({ name: 'SEGMENT_TLS_LISTEN', value: '0.0.0.0:3051' });
    expect(proxy(single).env!.map((e) => e.name)).not.toContain('SERVER_LISTEN');
    // without the annotation the proxy has both sides and the segment server no certificates
    expect(proxy(pod(cluster)).env!.map((e) => e.name)).toContain('SERVER_LISTEN');
    expect(pod(cluster).containers.find((c) => c.name === 'segment-server')!.env!.map((e) => e.name)).not.toContain('SEGMENT_TLS_DIR');
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
