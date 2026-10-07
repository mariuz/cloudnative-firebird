import { describe, it, expect, vi, afterEach, beforeAll } from 'vitest';
import { createServer, Server } from 'net';
import { V1Container, V1PodSpec, V1Secret } from '@kubernetes/client-node';
import { makeCluster } from './helpers/factories';
import { buildStatefulSet } from '../src/utils/resources';
import { buildFailoverJob } from '../src/utils/switchover';
import { setOperatorImage } from '../src/utils/operator-image';
import { createTlsCertificates } from '../src/utils/certificates';
import { startServer } from '../src/segment-tls';
import { setSegmentTlsResolver } from '../src/utils/replication-lag';
import { createSegmentTlsResolver, ensureSegmentTlsSecret, parseInstanceHost } from '../src/utils/segment-tls-client';

const env = (c: V1Container | undefined) => Object.fromEntries((c?.env ?? []).map((e) => [e.name, e.value]));
const podSpec = (cluster = makeCluster()) => buildStatefulSet(cluster).spec!.template.spec as V1PodSpec;
const named = (spec: V1PodSpec, name: string) => [...(spec.initContainers ?? []), ...spec.containers].find((c) => c.name === name);

describe('segment TLS in the instance pods and Jobs', () => {
  beforeAll(() => setOperatorImage('registry.example/cloudnative-firebird:1.2.3'));

  it('runs the proxy as a native sidecar beside a replicated instance, the segment server on localhost', () => {
    const spec = podSpec(makeCluster({ instances: 3, replication: { enabled: true }, segmentTLS: { enabled: true } }));
    const proxy = spec.initContainers![0];
    expect(proxy).toMatchObject({ name: 'segment-tls', image: 'registry.example/cloudnative-firebird:1.2.3', restartPolicy: 'Always' });
    expect(proxy.ports).toEqual([{ name: 'segments', containerPort: 3051, protocol: 'TCP' }]);
    expect(env(proxy)).toMatchObject({ SERVER_LISTEN: '0.0.0.0:3051', SERVER_TARGET: '127.0.0.1:3061', CLIENT_LISTEN: '127.0.0.1:3052' });
    expect(proxy.securityContext).toMatchObject({ runAsNonRoot: true, readOnlyRootFilesystem: true });
    // the seeding init container starts after it, and every Perl client goes through it
    expect(spec.initContainers!.findIndex((c) => c.name === 'replication-init')).toBeGreaterThan(0);
    for (const name of ['replication-init', 'segment-server', 'segment-puller']) expect(env(named(spec, name)).SEGMENT_PROXY).toBe('127.0.0.1:3052');
    const server = named(spec, 'segment-server')!;
    expect(env(server).SEGMENT_LISTEN).toBe('127.0.0.1:3061');
    expect((server.ports ?? []).some((p) => p.containerPort === 3051)).toBe(false);
    // the certificates, not the CA's key
    const volume = spec.volumes!.find((v) => v.name === 'segment-tls')!;
    expect(volume.secret).toMatchObject({ secretName: 'test-cluster-segment-tls' });
    expect(volume.secret!.items!.map((i) => i.key)).toEqual(['ca.crt', 'tls.crt', 'tls.key']);
  });

  it('wraps the backup file server of an instance without replication the same way', () => {
    const spec = podSpec(makeCluster({ segmentTLS: { enabled: true } }));
    expect(spec.initContainers![0].name).toBe('segment-tls');
    expect(env(named(spec, 'backup-files')).SEGMENT_LISTEN).toBe('127.0.0.1:3061');
  });

  it('changes nothing when off', () => {
    const spec = podSpec(makeCluster({ instances: 3, replication: { enabled: true } }));
    expect(named(spec, 'segment-tls')).toBeUndefined();
    expect(env(named(spec, 'segment-server')).SEGMENT_PROXY).toBeUndefined();
    expect(named(spec, 'segment-server')!.ports).toEqual([{ name: 'segments', containerPort: 3051, protocol: 'TCP' }]);
  });

  it('gives Jobs the client side only', () => {
    const job = buildFailoverJob(makeCluster({ instances: 3, replication: { enabled: true }, segmentTLS: { enabled: true } }), ['test-cluster-1']);
    const spec = job.spec!.template.spec!;
    const proxy = spec.initContainers![0];
    expect(proxy.name).toBe('segment-tls');
    expect(env(proxy).CLIENT_LISTEN).toBe('127.0.0.1:3052');
    expect(env(proxy).SERVER_LISTEN).toBeUndefined();
    expect(proxy.ports).toBeUndefined();
    expect(env(spec.containers[0]).SEGMENT_PROXY).toBe('127.0.0.1:3052');
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

  it('uses TLS for an instance that runs the proxy, with its cluster\'s certificates', async () => {
    const certs = createTlsCertificates({ caName: 'db CA', dnsNames: ['*.db-headless'], clientAuth: true });
    const withProxy = { spec: { initContainers: [{ name: 'segment-tls' }], containers: [] } };
    const core = {
      readNamespacedPod: vi.fn().mockImplementation(async ({ name }: { name: string }) => (name === 'db-1' ? withProxy : { spec: { containers: [] } })),
      readNamespacedSecret: vi.fn().mockResolvedValue(secretFor(certs)),
    };
    const resolve = createSegmentTlsResolver(core as never);
    expect(await resolve('db-1.db-headless.prod.svc')).toEqual({ ca: certs.caCert, cert: certs.tlsCert, key: certs.tlsKey });
    // not yet restarted with the proxy (segment TLS just switched on): plain
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

  it('sends its requests over TLS to an instance behind the proxy', async () => {
    const certs = createTlsCertificates({ caName: 'db CA', dnsNames: ['*.db-headless'], clientAuth: true });
    const files = { ca: certs.caCert, cert: certs.tlsCert, key: certs.tlsKey };
    const lines: string[] = [];
    const segmentServer: Server = createServer((s) =>
      s.once('data', (d) => {
        lines.push(d.toString().trim());
        s.end(lines.length === 1 ? 'OK\n' : 'OK 5.0.4\n');
      }),
    );
    await new Promise<void>((r) => segmentServer.listen(0, '127.0.0.1', r));
    const proxy = await startServer(() => files, 0, { host: '127.0.0.1', port: (segmentServer.address() as { port: number }).port }, '127.0.0.1');
    // the real client (tests/helpers/no-network.ts stubs it)
    const { segmentRequest, setSegmentTlsResolver: setActualResolver } = await vi.importActual<typeof import('../src/utils/replication-lag')>(
      '../src/utils/replication-lag',
    );
    setActualResolver(async () => files);
    try {
      const reply = await segmentRequest('127.0.0.1', (proxy.address() as { port: number }).port, 'tok VERSION');
      expect(reply).toEqual(['OK 5.0.4']);
      // the signed PING probe, then the signed request
      expect(lines[0]).toMatch(/^SIG1 \d+ [0-9a-f]+ [0-9a-f]{64} PING$/);
      expect(lines[1]).toMatch(/ VERSION$/);
    } finally {
      setActualResolver(undefined);
      proxy.close();
      segmentServer.close();
    }
  });
});
