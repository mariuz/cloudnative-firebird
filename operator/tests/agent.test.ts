import { describe, it, expect } from 'vitest';
import { existsSync, readFileSync } from 'fs';
import { join } from 'path';
import { AGENT_ANNOTATION, agentErrors } from '../src/utils/agent';
import { buildConfigMap, buildStatefulSet, statefulSetNeedsUpdate } from '../src/utils/resources';
import { REPLICATION_SCRIPTS } from '../src/utils/replication';
import { buildFailoverJob } from '../src/utils/switchover';
import { validateClusterSpec } from '../src/utils/validation';
import { FirebirdCluster } from '../src/types';
import { makeCluster } from './helpers/factories';

/**
 * The Rust agent in the instance pods (docs/rust-agent-roadmap.md): since v0.170.0 every cluster
 * gets the fb-agent binary of the operator image copied into its instance pods, and runs it. The
 * annotation that made a cluster opt in before is accepted and ignored.
 */
const annotated = (cluster: FirebirdCluster, value = 'rust'): FirebirdCluster => ({
  ...cluster,
  metadata: { ...cluster.metadata, annotations: { ...cluster.metadata.annotations, [AGENT_ANNOTATION]: value } },
});
const pod = (cluster: FirebirdCluster) => buildStatefulSet(cluster).spec!.template.spec!;
const AGENT_MOUNT = { name: 'fb-agent', mountPath: '/opt/fb-agent', readOnly: true };
const FB_AGENT = { name: 'FB_AGENT', value: '/opt/fb-agent/fb-agent' };

describe('the Rust agent in the instance pods', () => {
  it('is copied from the operator image by an init container, next to the segment server', () => {
    const spec = pod(makeCluster({ replication: { enabled: true } }));
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
    expect(server.volumeMounts).toContainEqual(AGENT_MOUNT);
    expect(server.env).toContainEqual(FB_AGENT);
    expect(server.command).toEqual(['/opt/fb-agent/fb-agent', 'segment-server']);
    expect(server.ports).toEqual([{ name: 'segments', containerPort: 3051, protocol: 'TCP' }]);
    // and in the puller, which runs from it
    const puller = spec.containers.find((c) => c.name === 'segment-puller')!;
    expect(puller.command).toEqual(['/opt/fb-agent/fb-agent', 'puller']);
    expect(puller.volumeMounts).toContainEqual(AGENT_MOUNT);
    expect(puller.env).toContainEqual(FB_AGENT);
    // nowhere else
    for (const c of spec.containers.filter((c) => !['segment-server', 'segment-puller'].includes(c.name))) {
      expect(c.volumeMounts?.map((m) => m.name) ?? []).not.toContain('fb-agent');
    }
  });

  it('ignores the annotation of before v0.170.0: the pods are the same, and are not rolled', () => {
    const cluster = makeCluster({ replication: { enabled: true } });
    for (const value of ['rust', 'perl']) {
      expect(JSON.stringify(pod(annotated(cluster, value))), value).toBe(JSON.stringify(pod(cluster)));
      expect(statefulSetNeedsUpdate(buildStatefulSet(cluster), buildStatefulSet(annotated(cluster, value))), value).toBe(false);
    }
  });

  it('runs the puller from the agent with segment TLS and the primary Lease too', () => {
    const cluster = makeCluster({ replication: { enabled: true, failover: { enabled: true, primaryLease: { enabled: true } } }, segmentTLS: { enabled: true } });
    const puller = pod(cluster).containers.find((c) => c.name === 'segment-puller')!;
    expect(puller.command).toEqual(['/opt/fb-agent/fb-agent', 'puller']);
    // the installer comes after the native sidecars, which stay first, and before the replication init
    expect(pod(cluster).initContainers!.map((c) => c.name)).toEqual(['lease-holder', 'agent-install', 'security-db-init', 'replication-init']);
    const server = pod(cluster).containers.find((c) => c.name === 'segment-server')!;
    expect(server.command).toEqual(['/opt/fb-agent/fb-agent', 'segment-server']);
    expect(server.env).toContainEqual({ name: 'SEGMENT_LISTEN', value: '127.0.0.1:3061' });
    expect(puller.env).toContainEqual({ name: 'SEGMENT_TLS', value: 'true' });
  });

  it('runs the replication init with the agent\'s helpers', () => {
    const init = pod(makeCluster({ replication: { enabled: true } })).initContainers!.find((c) => c.name === 'replication-init')!;
    expect(init.command).toEqual(['sh', '/etc/firebird-operator/init-instance.sh']);
    expect(init.env).toContainEqual(FB_AGENT);
    expect(init.volumeMounts).toContainEqual(AGENT_MOUNT);
    // the binary is installed before it runs
    const order = pod(makeCluster({ replication: { enabled: true } })).initContainers!.map((c) => c.name);
    expect(order.indexOf('agent-install')).toBeLessThan(order.indexOf('replication-init'));
    // the script runs the agent's commands, and no Perl
    const script = REPLICATION_SCRIPTS['init-instance.sh'];
    expect(script.split('\n').filter((line) => /^[^#]*\bperl\b/.test(line))).toEqual([]);
    expect(script.match(/"\$FB_AGENT" [a-z-]+/g)!.sort()).toEqual([
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
    // the former name of the script (pods that opted into the agent before v0.170.0) is gone (v0.172.0)
    expect(buildConfigMap(makeCluster({ replication: { enabled: true } }))!.data!['init-instance-agent.sh']).toBeUndefined();
    // with segment TLS the init has the certificates: its seed is fetched over TLS made by the agent
    const tls = pod(makeCluster({ replication: { enabled: true }, segmentTLS: { enabled: true } })).initContainers!.find((c) => c.name === 'replication-init')!;
    expect(tls.env).toContainEqual({ name: 'SEGMENT_TLS_DIR', value: '/etc/segment-tls' });
    expect(tls.env).toContainEqual({ name: 'SEGMENT_TLS', value: 'true' });
    expect(tls.volumeMounts).toContainEqual({ name: 'segment-tls', mountPath: '/etc/segment-tls', readOnly: true });
  });

  it('runs the primary Lease holder from the agent, in the operator image', () => {
    const spec = { replication: { enabled: true, failover: { enabled: true, primaryLease: { enabled: true } } } };
    const holder = pod(makeCluster(spec)).initContainers!.find((c) => c.name === 'lease-holder')!;
    // the binary of the operator image: the sidecar starts before the pod's agent volume is filled
    expect(holder.command).toEqual(['/app/fb-agent', 'lease-holder']);
    expect(holder.image).toMatch(/cloudnative-firebird/);
    expect(holder.resources).toEqual({ requests: { cpu: '10m', memory: '8Mi' } });
  });

  it('runs the custom metrics exporter from the agent: no Perl is left in an instance pod', () => {
    const spec = pod(
      makeCluster({
        replication: { enabled: true, failover: { enabled: true, primaryLease: { enabled: true } } },
        segmentTLS: { enabled: true },
        monitoring: { customQueriesConfigMap: [{ name: 'fb-queries', key: 'queries.yaml' }] },
      }),
    );
    const exporter = spec.containers.find((c) => c.name === 'metrics-exporter')!;
    expect(exporter.command).toEqual(['/opt/fb-agent/fb-agent', 'metrics-exporter']);
    expect(exporter.volumeMounts).toContainEqual(AGENT_MOUNT);
    // every container of the pod, init containers and sidecars included: none runs Perl
    for (const c of [...spec.initContainers!, ...spec.containers]) {
      expect([...(c.command ?? []), ...(c.args ?? [])].join(' '), c.name).not.toMatch(/\bperl\b|\.pl\b/);
    }
  });

  it('runs the backup file server of a cluster without replication', () => {
    const spec = pod(makeCluster());
    expect(spec.initContainers!.map((c) => c.name)).toContain('agent-install');
    const files = spec.containers.find((c) => c.name === 'backup-files')!;
    expect(files.volumeMounts).toContainEqual(AGENT_MOUNT);
    // the segment server in its files-only mode
    expect(files.command).toEqual(['/opt/fb-agent/fb-agent', 'segment-server']);
    expect(files.env).toContainEqual({ name: 'FILES_ONLY', value: 'true' });
    // its ConfigMap ships no scripts of its own
    expect(Object.keys(buildConfigMap(makeCluster())!.data!).sort()).toEqual(['pitr-restore.sh']);
  });

  it('does segment TLS in the agent\'s own processes, with no proxy sidecar (v0.171.0)', () => {
    const cluster = makeCluster({ instances: 3, replication: { enabled: true }, segmentTLS: { enabled: true } });
    const spec = pod(cluster);
    const all = [...spec.initContainers!, ...spec.containers];
    expect(all.map((c) => c.name)).not.toContain('segment-tls');
    expect(all.flatMap((c) => (c.env ?? []).map((e) => e.name))).not.toContain('SEGMENT_PROXY');
    const env = (name: string) => Object.fromEntries((all.find((c) => c.name === name)!.env ?? []).map((e) => [e.name, e.value]));
    const server = spec.containers.find((c) => c.name === 'segment-server')!;
    expect(server.ports).toContainEqual(expect.objectContaining({ containerPort: 3051 }));
    expect(env('segment-server')).toMatchObject({
      SEGMENT_TLS_LISTEN: '0.0.0.0:3051',
      SEGMENT_LISTEN: '127.0.0.1:3061',
      SEGMENT_TLS_DIR: '/etc/segment-tls',
      SEGMENT_TLS_PEERS_DIR: '/etc/segment-tls-peers',
      SEGMENT_TLS: 'true',
    });
    for (const name of ['segment-puller', 'replication-init']) {
      expect(env(name)).toMatchObject({ SEGMENT_TLS_DIR: '/etc/segment-tls', SEGMENT_TLS_PEERS_DIR: '/etc/segment-tls-peers', SEGMENT_TLS: 'true' });
    }
    for (const name of ['segment-server', 'segment-puller', 'replication-init']) {
      expect(all.find((c) => c.name === name)!.volumeMounts).toEqual(
        expect.arrayContaining([
          { name: 'segment-tls', mountPath: '/etc/segment-tls', readOnly: true },
          { name: 'segment-tls-peers', mountPath: '/etc/segment-tls-peers', readOnly: true },
        ]),
      );
    }
    // the database container is no client of the segment servers: no certificates
    expect(env('firebird').SEGMENT_TLS_DIR).toBeUndefined();
    expect(env('firebird').SEGMENT_TLS).toBeUndefined();
    // without replication: the backup file server, the same way
    const single = pod(makeCluster({ segmentTLS: { enabled: true } }));
    const files = single.containers.find((c) => c.name === 'backup-files')!;
    expect(files.env).toContainEqual({ name: 'SEGMENT_TLS_LISTEN', value: '0.0.0.0:3051' });
    expect(single.initContainers!.map((c) => c.name)).not.toContain('segment-tls');
    // a Job's programs: the certificates in its own containers, the agent installed first
    const job = buildFailoverJob(cluster, ['test-cluster-1']).spec!.template.spec!;
    expect(job.initContainers!.map((c) => c.name)).not.toContain('segment-tls');
    expect(job.initContainers![0].name).toBe('agent-install');
    const failover = job.containers.find((c) => c.command?.includes('failover'))!;
    expect(Object.fromEntries((failover.env ?? []).map((e) => [e.name, e.value]))).toMatchObject({ SEGMENT_TLS_DIR: '/etc/segment-tls', SEGMENT_TLS: 'true' });
  });

  it('accepts the annotation\'s values of before v0.170.0 only', () => {
    expect(agentErrors(makeCluster())).toEqual([]);
    expect(() => validateClusterSpec(annotated(makeCluster()))).not.toThrow();
    expect(() => validateClusterSpec(annotated(makeCluster(), 'perl'))).not.toThrow();
    expect(() => validateClusterSpec(annotated(makeCluster(), 'go'))).toThrow(/agent "go" is invalid: use "rust" or "perl"/);
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
