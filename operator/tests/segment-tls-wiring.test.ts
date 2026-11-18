import { describe, it, expect, vi, afterEach, beforeAll } from 'vitest';
import { createServer, Server } from 'net';
import { V1Container, V1PodSpec, V1Secret } from '@kubernetes/client-node';
import { makeCluster } from './helpers/factories';
import { buildStatefulSet } from '../src/utils/resources';
import { buildFailoverJob } from '../src/utils/switchover';
import { setOperatorImage } from '../src/utils/operator-image';
import { createTlsCertificates } from '../src/utils/certificates';
import { spawn } from 'child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { agentBin } from './helpers/implementations';
import { setSegmentTlsResolver } from '../src/utils/replication-lag';
import { V1ConfigMap } from '@kubernetes/client-node';
import { podHasSegmentTls } from '../src/utils/segment-tls-pods';
import { ACCEPT_PLAIN_GRACE_MS, createSegmentTlsResolver, ensureSegmentTlsSecret, parseInstanceHost, reconcileSegmentTlsPeers } from '../src/utils/segment-tls-client';

const env = (c: V1Container | undefined) => Object.fromEntries((c?.env ?? []).map((e) => [e.name, e.value]));
const podSpec = (cluster = makeCluster()) => buildStatefulSet(cluster).spec!.template.spec as V1PodSpec;
const named = (spec: V1PodSpec, name: string) => [...(spec.initContainers ?? []), ...spec.containers].find((c) => c.name === name);

describe('segment TLS in the instance pods and Jobs', () => {
  beforeAll(() => setOperatorImage('registry.example/cloudnative-firebird:1.2.3'));

  it('gives the agent\'s processes of a replicated instance the certificates: no proxy sidecar', () => {
    const spec = podSpec(makeCluster({ instances: 3, replication: { enabled: true }, segmentTLS: { enabled: true } }));
    expect(named(spec, 'segment-tls')).toBeUndefined();
    for (const name of ['replication-init', 'segment-server', 'segment-puller']) {
      const c = named(spec, name)!;
      expect(env(c)).toMatchObject({ SEGMENT_TLS: 'true', SEGMENT_TLS_DIR: '/etc/segment-tls', SEGMENT_TLS_PEERS_DIR: '/etc/segment-tls-peers' });
      expect(env(c).SEGMENT_PROXY).toBeUndefined();
      expect(c.volumeMounts).toContainEqual({ name: 'segment-tls', mountPath: '/etc/segment-tls', readOnly: true });
      expect(c.volumeMounts).toContainEqual({ name: 'segment-tls-peers', mountPath: '/etc/segment-tls-peers', readOnly: true });
    }
    // the segment server keeps the segment port and accepts mutual TLS on it; plain on localhost
    const server = named(spec, 'segment-server')!;
    expect(env(server)).toMatchObject({ SEGMENT_LISTEN: '127.0.0.1:3061', SEGMENT_TLS_LISTEN: '0.0.0.0:3051' });
    expect(server.ports).toEqual([{ name: 'segments', containerPort: 3051, protocol: 'TCP' }]);
    // the database container is no client of the segment servers
    expect(env(named(spec, 'firebird')).SEGMENT_TLS_DIR).toBeUndefined();
    // the certificates, not the CA's key
    const volume = spec.volumes!.find((v) => v.name === 'segment-tls')!;
    expect(volume.secret).toMatchObject({ secretName: 'test-cluster-segment-tls' });
    expect(volume.secret!.items!.map((i) => i.key)).toEqual(['ca.crt', 'tls.crt', 'tls.key']);
    // and the instances' modes while segment TLS is switched (optional: TLS only without it)
    expect(spec.volumes!.find((v) => v.name === 'segment-tls-peers')!.configMap).toEqual({ name: 'test-cluster-segment-tls-peers', optional: true });
    expect(podHasSegmentTls(spec)).toBe(true);
  });

  it('wraps the backup file server of an instance without replication the same way', () => {
    const spec = podSpec(makeCluster({ segmentTLS: { enabled: true } }));
    expect(named(spec, 'segment-tls')).toBeUndefined();
    expect(env(named(spec, 'backup-files'))).toMatchObject({ SEGMENT_LISTEN: '127.0.0.1:3061', SEGMENT_TLS_LISTEN: '0.0.0.0:3051', SEGMENT_TLS: 'true' });
  });

  it('changes nothing when off', () => {
    const spec = podSpec(makeCluster({ instances: 3, replication: { enabled: true } }));
    expect(named(spec, 'segment-tls')).toBeUndefined();
    expect(env(named(spec, 'segment-server')).SEGMENT_TLS).toBeUndefined();
    expect(env(named(spec, 'segment-server')).SEGMENT_LISTEN).toBeUndefined();
    expect(named(spec, 'segment-server')!.ports).toEqual([{ name: 'segments', containerPort: 3051, protocol: 'TCP' }]);
    expect(spec.volumes!.some((v) => v.name === 'segment-tls')).toBe(false);
    expect(podHasSegmentTls(spec)).toBe(false);
  });

  it('gives the Jobs\' programs the certificates', () => {
    const job = buildFailoverJob(makeCluster({ instances: 3, replication: { enabled: true }, segmentTLS: { enabled: true } }), ['test-cluster-1']);
    const spec = job.spec!.template.spec!;
    expect(named(spec, 'segment-tls')).toBeUndefined();
    for (const c of spec.containers) {
      expect(env(c)).toMatchObject({ SEGMENT_TLS: 'true', SEGMENT_TLS_DIR: '/etc/segment-tls', SEGMENT_TLS_PEERS_DIR: '/etc/segment-tls-peers' });
      expect(c.volumeMounts).toContainEqual({ name: 'segment-tls', mountPath: '/etc/segment-tls', readOnly: true });
    }
  });
});

describe('the operator side of segment TLS', () => {
  afterEach(() => setSegmentTlsResolver(undefined));

  it('names instances by their segment server address', () => {
    expect(parseInstanceHost('db-1.db-headless.prod.svc')).toEqual({ pod: 'db-1', cluster: 'db', namespace: 'prod' });
    expect(parseInstanceHost('my-db-0.my-db-headless.default.svc.cluster.local')).toEqual({ pod: 'my-db-0', cluster: 'my-db', namespace: 'default' });
    expect(parseInstanceHost('127.0.0.1')).toBeUndefined();
  });

  const secretFor = (c: ReturnType<typeof createTlsCertificates>): V1Secret => ({
    data: Object.fromEntries(
      Object.entries({ 'ca.crt': c.caCert, 'ca.key': c.caKey, 'tls.crt': c.tlsCert, 'tls.key': c.tlsKey }).map(([k, v]) => [k, Buffer.from(v).toString('base64')]),
    ),
    metadata: { resourceVersion: '7' },
  });

  it('uses TLS for an instance that serves over it, with its cluster\'s certificates', async () => {
    const certs = createTlsCertificates({ caName: 'db CA', dnsNames: ['*.db-headless'], clientAuth: true });
    const withProxy = { spec: { containers: [], volumes: [{ name: 'segment-tls' }] } };
    const core = {
      readNamespacedPod: vi.fn().mockImplementation(async ({ name }: { name: string }) => (name === 'db-1' ? withProxy : { spec: { containers: [] } })),
      readNamespacedSecret: vi.fn().mockResolvedValue(secretFor(certs)),
    };
    const resolve = createSegmentTlsResolver(core as never);
    expect(await resolve('db-1.db-headless.prod.svc')).toEqual({ ca: certs.caCert, cert: certs.tlsCert, key: certs.tlsKey });
    // not yet restarted with TLS (segment TLS just switched on): plain
    expect(await resolve('db-2.db-headless.prod.svc')).toBeUndefined();
    expect(await resolve('10.0.0.1')).toBeUndefined();
    // cached
    await resolve('db-1.db-headless.prod.svc');
    expect(core.readNamespacedPod).toHaveBeenCalledTimes(2);
    expect(core.readNamespacedSecret).toHaveBeenCalledWith({ name: 'db-segment-tls', namespace: 'prod' });
  });

  it('creates the Secret, keeps a valid one, and renews the certificate before it expires', async () => {
    const cluster = makeCluster({ segmentTLS: { enabled: true } });
    let stored: V1Secret | undefined;
    const core = {
      readNamespacedSecret: vi.fn().mockImplementation(async () => {
        if (!stored) throw Object.assign(new Error('not found'), { code: 404 });
        return stored;
      }),
      createNamespacedSecret: vi.fn().mockImplementation(async ({ body }: { body: V1Secret }) => (stored = { ...body, metadata: { ...body.metadata, resourceVersion: '1' } })),
      replaceNamespacedSecret: vi.fn().mockImplementation(async ({ body }: { body: V1Secret }) => (stored = body)),
    };
    const first = await ensureSegmentTlsSecret(core as never, cluster);
    expect(core.createNamespacedSecret).toHaveBeenCalledTimes(1);
    expect(stored!.metadata!.ownerReferences![0]).toMatchObject({ kind: 'FirebirdCluster', name: 'test-cluster', uid: 'test-uid-1234' });
    expect(Object.keys(stored!.data!).sort()).toEqual(['ca.crt', 'ca.key', 'tls.crt', 'tls.key']);
    // valid: kept as it is
    expect(await ensureSegmentTlsSecret(core as never, cluster)).toEqual(first);
    expect(core.replaceNamespacedSecret).not.toHaveBeenCalled();
    // 340 days later the certificate is renewed, signed by the same CA
    const later = await ensureSegmentTlsSecret(core as never, cluster, new Date(Date.now() + 340 * 86_400_000));
    expect(later.ca).toBe(first.ca);
    expect(later.cert).not.toBe(first.cert);
    expect(core.replaceNamespacedSecret.mock.calls[0][0].body.metadata.resourceVersion).toBe('1');
  });

  it.runIf(agentBin)('sends its requests over TLS to the agent\'s segment server', async () => {
    const certs = createTlsCertificates({ caName: 'db CA', dnsNames: ['*.db-headless'], clientAuth: true });
    const files = { ca: certs.caCert, cert: certs.tlsCert, key: certs.tlsKey };
    const dir = mkdtempSync(join(tmpdir(), 'segment-tls-wiring-'));
    writeFileSync(join(dir, 'ca.crt'), files.ca);
    writeFileSync(join(dir, 'tls.key'), files.key);
    writeFileSync(join(dir, 'tls.crt'), files.cert);
    const port = await new Promise<number>((resolve) => {
      const probe: Server = createServer();
      probe.listen(0, '127.0.0.1', () => {
        const free = (probe.address() as { port: number }).port;
        probe.close(() => resolve(free));
      });
    });
    const server = spawn(agentBin!, ['segment-server'], {
      env: {
        PATH: process.env.PATH,
        FILES_ONLY: 'true',
        DATABASE_PATH: join(dir, 'mydb.fdb'),
        ISC_PASSWORD: 'tok',
        SEGMENT_LISTEN: '127.0.0.1:0',
        SEGMENT_TLS_LISTEN: `127.0.0.1:${port}`,
        SEGMENT_TLS_DIR: dir,
      },
    });
    let out = '';
    server.stdout.on('data', (d) => (out += d));
    // the real client (tests/helpers/no-network.ts stubs it)
    const { segmentRequest, setSegmentTlsResolver: setActualResolver } = await vi.importActual<typeof import('../src/utils/replication-lag')>(
      '../src/utils/replication-lag',
    );
    setActualResolver(async () => files);
    try {
      for (let i = 0; i < 100 && !out.includes('accepts mutual TLS itself'); i++) await new Promise((r) => setTimeout(r, 50));
      // signed with the password, over TLS: verified and answered by the agent
      expect(await segmentRequest('127.0.0.1', port, 'tok PING')).toEqual(['OK']);
      expect(await segmentRequest('127.0.0.1', port, 'wrong PING')).toEqual([expect.stringMatching(/^ERR unauthorized/)]);
    } finally {
      setActualResolver(undefined);
      server.kill('SIGKILL');
      rmSync(dir, { recursive: true, force: true });
    }
  });

  describe('the instances\' modes while segment TLS is switched', () => {
    const pod = (name: string, tls: boolean) => ({
      metadata: { name },
      spec: { containers: [], volumes: tls ? [{ name: 'segment-tls' }] : [] },
    });
    function api(pods: ReturnType<typeof pod>[], initial?: V1ConfigMap) {
      let stored = initial;
      const core = {
        readNamespacedConfigMap: vi.fn().mockImplementation(async () => {
          if (!stored) throw Object.assign(new Error('not found'), { code: 404 });
          return stored;
        }),
        listNamespacedPod: vi.fn().mockImplementation(async () => ({ items: pods })),
        createNamespacedConfigMap: vi.fn().mockImplementation(async ({ body }: { body: V1ConfigMap }) => (stored = { ...body, metadata: { ...body.metadata, resourceVersion: '1' } })),
        replaceNamespacedConfigMap: vi.fn().mockImplementation(async ({ body }: { body: V1ConfigMap }) => (stored = body)),
        deleteNamespacedConfigMap: vi.fn().mockImplementation(async () => (stored = undefined)),
      };
      return { core, stored: () => stored };
    }

    it('switching on: the instances not restarted yet are plain, then TLS only after a grace period', async () => {
      const on = makeCluster({ instances: 3, replication: { enabled: true }, segmentTLS: { enabled: true } });
      const pods = [pod('test-cluster-0', false), pod('test-cluster-1', true), pod('test-cluster-2', false)];
      const { core, stored } = api(pods);
      await reconcileSegmentTlsPeers(core as never, on, 1_000);
      expect(stored()!.data).toEqual({ 'plain-peers': 'test-cluster-0\ntest-cluster-2\n', 'accept-plain-until': String(1_000 + ACCEPT_PLAIN_GRACE_MS) });
      expect(stored()!.metadata!.ownerReferences![0]).toMatchObject({ kind: 'FirebirdCluster', name: 'test-cluster' });
      // the primary restarts last; then nothing is plain, and plain connections are accepted
      // until the grace period from the last reconcile that saw a plain instance runs out
      pods[2] = pod('test-cluster-2', true);
      await reconcileSegmentTlsPeers(core as never, on, 2_000);
      pods[0] = pod('test-cluster-0', true);
      await reconcileSegmentTlsPeers(core as never, on, 3_000);
      expect(stored()!.data).toEqual({ 'plain-peers': '', 'accept-plain-until': String(2_000 + ACCEPT_PLAIN_GRACE_MS) });
      // steady: not written again
      core.replaceNamespacedConfigMap.mockClear();
      await reconcileSegmentTlsPeers(core as never, on, 9_000_000);
      expect(core.replaceNamespacedConfigMap).not.toHaveBeenCalled();
    });

    it('switching off: every instance is plain while one still serves over TLS, and the ConfigMap goes with the last one', async () => {
      const off = makeCluster({ instances: 2, replication: { enabled: true } });
      const pods = [pod('test-cluster-0', true), pod('test-cluster-1', false)];
      const { core, stored } = api(pods, { metadata: { name: 'test-cluster-segment-tls-peers', resourceVersion: '4' }, data: { 'plain-peers': '', 'accept-plain-until': '0' } });
      await reconcileSegmentTlsPeers(core as never, off, 5_000);
      expect(stored()!.data).toEqual({ 'plain-peers': 'test-cluster-0\ntest-cluster-1\n', 'accept-plain-until': String(5_000 + ACCEPT_PLAIN_GRACE_MS) });
      expect(core.replaceNamespacedConfigMap.mock.calls[0][0].body.metadata.resourceVersion).toBe('4');
      pods[0] = pod('test-cluster-0', false);
      await reconcileSegmentTlsPeers(core as never, off, 6_000);
      expect(stored()).toBeUndefined();
      // a cluster that never had segment TLS: one read, nothing else
      const never = api([]);
      await reconcileSegmentTlsPeers(never.core as never, off);
      expect(never.core.listNamespacedPod).not.toHaveBeenCalled();
      expect(never.core.createNamespacedConfigMap).not.toHaveBeenCalled();
    });
  });
});
