import { describe, it, expect, vi, Mock } from 'vitest';
import { createServer, request } from 'https';
import { connect } from 'tls';
import { X509Certificate } from 'crypto';
import { KubeConfig, V1Secret, V1ValidatingWebhookConfiguration } from '@kubernetes/client-node';
import { createWebhookCertificates, daysUntilExpiry, servingCertificateMatches } from '../src/utils/certificates';
import { review, webhookDnsNames, WebhookServer, WEBHOOK_SECRET } from '../src/utils/webhook';
import { createAdmissionValidator, AdmissionLookups, secretRefNames } from '../src/utils/admission';
import { AdmissionRequest } from '../src/utils/webhook';
import { makeCluster } from './helpers/factories';
import { FENCED_INSTANCES_ANNOTATION } from '../src/utils/fencing';

const names = webhookDnsNames('cnf-system');

describe('webhook certificates', () => {
  it('are accepted by a TLS client that trusts only the CA, for the Service names', async () => {
    const certs = createWebhookCertificates(names);
    expect(new X509Certificate(certs.caCert).ca).toBe(true);
    expect(new X509Certificate(certs.tlsCert).ca).toBe(false);
    expect(servingCertificateMatches(certs.tlsCert, certs.caCert, names)).toBe(true);
    expect(Math.round(daysUntilExpiry(certs.tlsCert)!)).toBe(365);
    const server = createServer({ cert: certs.tlsCert, key: certs.tlsKey }, (_req, res) => res.end('ok'));
    await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
    const port = (server.address() as { port: number }).port;
    const handshake = (servername: string, ca: string) =>
      new Promise<boolean>((resolve) => {
        const socket = connect({ host: '127.0.0.1', port, servername, ca }, () => {
          resolve(socket.authorized);
          socket.end();
        });
        socket.on('error', () => resolve(false));
      });
    try {
      expect(await handshake(names[0], certs.caCert)).toBe(true);
      expect(await handshake(names[1], certs.caCert)).toBe(true);
      expect(await handshake('other.cnf-system.svc', certs.caCert)).toBe(false);
      expect(await handshake(names[0], createWebhookCertificates(names).caCert)).toBe(false);
    } finally {
      server.close();
    }
  });

  it('renews the serving certificate with the same CA', () => {
    const first = createWebhookCertificates(names);
    const renewed = createWebhookCertificates(names, new Date(), { cert: first.caCert, key: first.caKey });
    expect(renewed.caCert).toBe(first.caCert);
    expect(renewed.tlsCert).not.toBe(first.tlsCert);
    expect(servingCertificateMatches(renewed.tlsCert, first.caCert, names)).toBe(true);
    expect(servingCertificateMatches(renewed.tlsCert, first.caCert, ['other.svc'])).toBe(false);
  });

  it('uses GeneralizedTime from 2050', () => {
    const certs = createWebhookCertificates(names, new Date('2049-12-01T00:00:00Z'));
    expect(new X509Certificate(certs.tlsCert).validTo).toMatch(/2050/);
  });
});

describe('webhook certificate Secret and caBundle', () => {
  function setup(secret?: V1Secret, config: V1ValidatingWebhookConfiguration | null = { webhooks: [{ name: 'w', clientConfig: {}, sideEffects: 'None', admissionReviewVersions: ['v1'] }] }) {
    const notFound = Object.assign(new Error('Not Found'), { code: 404 });
    const api: Record<string, Mock> = {
      readNamespacedSecret: secret ? vi.fn().mockResolvedValue(secret) : vi.fn().mockRejectedValue(notFound),
      createNamespacedSecret: vi.fn().mockResolvedValue({}),
      replaceNamespacedSecret: vi.fn().mockResolvedValue({}),
      readValidatingWebhookConfiguration: config ? vi.fn().mockResolvedValue(config) : vi.fn().mockRejectedValue(notFound),
      replaceValidatingWebhookConfiguration: vi.fn().mockResolvedValue({}),
    };
    const kubeConfig = new KubeConfig();
    vi.spyOn(kubeConfig, 'makeApiClient').mockReturnValue(new Proxy({}, { get: (_t, p: string) => api[p] }) as never);
    return { server: new WebhookServer(kubeConfig, 'cnf-system', async () => ({})), api };
  }
  const asSecret = (c: ReturnType<typeof createWebhookCertificates>): V1Secret => ({
    metadata: { name: WEBHOOK_SECRET, resourceVersion: '7' },
    data: {
      'ca.crt': Buffer.from(c.caCert).toString('base64'),
      'ca.key': Buffer.from(c.caKey).toString('base64'),
      'tls.crt': Buffer.from(c.tlsCert).toString('base64'),
      'tls.key': Buffer.from(c.tlsKey).toString('base64'),
    },
  });

  it('creates the Secret and fills in the caBundle', async () => {
    const s = setup();
    const certs = await s.server.ensureCertificates();
    const body = s.api.createNamespacedSecret.mock.calls[0][0].body as V1Secret;
    expect(body.metadata?.name).toBe(WEBHOOK_SECRET);
    expect(Buffer.from(body.data!['tls.crt'], 'base64').toString()).toBe(certs.tlsCert);
    const config = s.api.replaceValidatingWebhookConfiguration.mock.calls[0][0].body as V1ValidatingWebhookConfiguration;
    expect(config.webhooks![0].clientConfig.caBundle).toBe(Buffer.from(certs.caCert).toString('base64'));
  });

  it('keeps valid certificates, and renews a serving certificate close to expiry with the same CA', async () => {
    const certs = createWebhookCertificates(names);
    const bundle = Buffer.from(certs.caCert).toString('base64');
    const s = setup(asSecret(certs), { webhooks: [{ name: 'w', clientConfig: { caBundle: bundle }, sideEffects: 'None', admissionReviewVersions: ['v1'] }] });
    expect((await s.server.ensureCertificates()).tlsCert).toBe(certs.tlsCert);
    expect(s.api.replaceNamespacedSecret).not.toHaveBeenCalled();
    expect(s.api.replaceValidatingWebhookConfiguration).not.toHaveBeenCalled();

    const later = await s.server.ensureCertificates(new Date(Date.now() + 340 * 86_400_000));
    expect(later.caCert).toBe(certs.caCert);
    expect(later.tlsCert).not.toBe(certs.tlsCert);
    expect(s.api.replaceNamespacedSecret.mock.calls[0][0].body.metadata.resourceVersion).toBe('7');
  });

  it('uses the certificates another operator pod wrote at the same time', async () => {
    const theirs = createWebhookCertificates(names);
    const s = setup();
    s.api.readNamespacedSecret
      .mockRejectedValueOnce(Object.assign(new Error('Not Found'), { code: 404 }))
      .mockResolvedValueOnce(asSecret(theirs));
    s.api.createNamespacedSecret.mockRejectedValueOnce(Object.assign(new Error('AlreadyExists'), { code: 409 }));
    const certs = await s.server.ensureCertificates();
    expect(certs.tlsCert).toBe(theirs.tlsCert);
    expect(s.api.createNamespacedSecret).toHaveBeenCalledTimes(1);
    expect(s.api.replaceNamespacedSecret).not.toHaveBeenCalled();
    // other errors are not retried
    const failing = setup();
    failing.api.createNamespacedSecret.mockRejectedValue(Object.assign(new Error('Forbidden'), { code: 403 }));
    await expect(failing.server.ensureCertificates()).rejects.toThrow('Forbidden');
    expect(failing.api.createNamespacedSecret).toHaveBeenCalledTimes(1);
  });

  it('replaces certificates for other names, and works without the webhook configuration', async () => {
    const s = setup(asSecret(createWebhookCertificates(['old.svc'])), null);
    await s.server.ensureCertificates();
    expect(s.api.replaceNamespacedSecret).toHaveBeenCalled();
  });
});

const lookups = (objects: { clusters?: object[]; backups?: object[]; secrets?: Record<string, Record<string, string>> }): AdmissionLookups => ({
  cluster: async (_ns, name) => objects.clusters?.find((c) => (c as { metadata: { name: string } }).metadata.name === name) as never,
  backup: async (_ns, name) => objects.backups?.find((b) => (b as { metadata: { name: string } }).metadata.name === name) as never,
  secret: async (_ns, name) => (objects.secrets?.[name] ? { data: objects.secrets[name] } : undefined),
});

const req = (kind: string, object: object, operation: AdmissionRequest['operation'] = 'CREATE', oldObject?: object): AdmissionRequest => ({
  uid: 'u1',
  kind: { group: 'firebird.cloudnative-firebird.io', version: 'v1', kind },
  operation,
  namespace: 'default',
  object: object as Record<string, unknown>,
  oldObject: oldObject as Record<string, unknown>,
});

describe('admission checks', () => {
  const cluster = { ...makeCluster(), metadata: { name: 'db', namespace: 'default' } };
  const backup = (name: string, phase: string, type = 'logical', extra: object = {}) => ({
    metadata: { name },
    spec: { clusterName: 'db', type },
    status: { phase, backupFileName: phase === 'Completed' ? `${name}.fbk` : undefined, ...extra },
  });

  it('refuses restores the operator would fail for good', async () => {
    const validate = createAdmissionValidator(
      lookups({ clusters: [cluster], backups: [backup('ok', 'Completed'), backup('bad', 'Failed'), backup('phys', 'Completed', 'physical')] }),
    );
    const restore = (spec: object) => ({ metadata: { name: 'r', namespace: 'default' }, spec: { clusterName: 'db', ...spec } });
    expect(await validate(req('FirebirdRestore', restore({ backupName: 'ok' })))).toEqual({});
    expect((await validate(req('FirebirdRestore', restore({ backupName: 'ok', targetDatabase: 'mydb.fdb' })))).denied).toMatch(/is the cluster database/);
    expect((await validate(req('FirebirdRestore', restore({ backupName: 'missing' })))).denied).toMatch(/FirebirdBackup missing not found/);
    expect((await validate(req('FirebirdRestore', restore({ backupName: 'bad' })))).denied).toBe('FirebirdBackup bad failed');
    expect((await validate(req('FirebirdRestore', restore({ backupName: 'ok', restoreType: 'physical' })))).denied).toMatch(/does not match the logical backup/);
    expect((await validate(req('FirebirdRestore', restore({ backupName: 'phys', pointInTime: { targetTime: '2026-01-01T00:00:00Z' } })))).denied).toMatch(
      /needs a journal archive/,
    );
  });

  it('warns about what a restore waits for', async () => {
    const validate = createAdmissionValidator(lookups({ backups: [backup('running', 'Running')] }));
    const verdict = await validate(req('FirebirdRestore', { metadata: { name: 'r' }, spec: { clusterName: 'db', backupName: 'running' } }));
    expect(verdict.denied).toBeUndefined();
    expect(verdict.warnings).toEqual([
      'FirebirdCluster db does not exist (yet) in namespace default',
      'FirebirdBackup running has not completed yet: the restore waits for it',
    ]);
  });

  it('refuses specs the operator rejects, and warns about missing Secrets and clone sources', async () => {
    const validate = createAdmissionValidator(lookups({ secrets: { su: { password: 'eA==' } } }));
    expect((await validate(req('FirebirdCluster', { ...cluster, spec: { ...cluster.spec, instances: 0 } }))).denied).toMatch(/instances/i);
    const verdict = await validate(
      req('FirebirdCluster', {
        ...cluster,
        spec: {
          ...cluster.spec,
          superuserSecret: { name: 'su' },
          backup: { s3: { bucket: 'b', secretRef: { name: 's3-creds' } } },
          bootstrap: { clone: { sourceCluster: 'prod', namespace: 'other' } },
        },
      }),
    );
    expect(verdict).toEqual({
      warnings: ['Secret s3-creds does not exist (yet) in namespace default', 'clone source FirebirdCluster prod does not exist in namespace other'],
    });
  });

  it('checks users\' password Secrets and their key', async () => {
    const validate = createAdmissionValidator(lookups({ clusters: [cluster], secrets: { pw: { other: 'eA==' } } }));
    const user = (secret: string) => ({ metadata: { name: 'app' }, spec: { clusterName: 'db', passwordSecret: { name: secret } } });
    expect((await validate(req('FirebirdUser', user('pw')))).warnings).toEqual(['Secret pw has no key "password"']);
    expect((await validate(req('FirebirdUser', user('none')))).warnings).toEqual(['Secret none does not exist (yet) in namespace default']);
  });

  it('only checks updates that change the spec (the operator\'s own updates pass), and never deletions', async () => {
    const lookup = vi.fn();
    const validate = createAdmissionValidator({ cluster: lookup, backup: lookup, secret: lookup });
    const invalid = { ...cluster, spec: { ...cluster.spec, instances: 0 } };
    const withFinalizer = { ...invalid, metadata: { ...invalid.metadata, finalizers: ['x'] } };
    expect(await validate(req('FirebirdCluster', withFinalizer, 'UPDATE', invalid))).toEqual({});
    expect(await validate(req('FirebirdCluster', { ...invalid, metadata: { ...invalid.metadata, deletionTimestamp: 'now' } }, 'UPDATE', cluster))).toEqual({});
    expect(await validate(req('FirebirdCluster', invalid, 'DELETE'))).toEqual({});
    expect(lookup).not.toHaveBeenCalled();
    // the fencing annotation is checked when it changes
    const fenced = { ...cluster, metadata: { ...cluster.metadata, annotations: { [FENCED_INSTANCES_ANNOTATION]: '["other-0"]' } } };
    expect((await validate(req('FirebirdCluster', fenced, 'UPDATE', cluster))).denied).toMatch(/another cluster/);
  });

  it('finds secretRef names anywhere in a spec', () => {
    expect(secretRefNames({ a: { secretRef: { name: 'x' } }, b: [{ s3: { secretRef: { name: 'y' } } }], c: { secretRef: {} } })).toEqual(['x', 'y']);
  });
});

describe('AdmissionReview', () => {
  it('answers with the verdict, and admits the object when the check itself fails', async () => {
    const body = { apiVersion: 'admission.k8s.io/v1', kind: 'AdmissionReview', request: req('FirebirdRestore', {}) };
    expect(await review(body, async () => ({ denied: 'no', warnings: ['w'] }))).toEqual({
      apiVersion: 'admission.k8s.io/v1',
      kind: 'AdmissionReview',
      response: { uid: 'u1', allowed: false, status: { code: 422, reason: 'Invalid', message: 'no' }, warnings: ['w'] },
    });
    expect(await review(body, async () => ({}))).toMatchObject({ response: { uid: 'u1', allowed: true } });
    const failing = await review(body, async () => {
      throw new Error('API down');
    });
    expect(failing).toMatchObject({ response: { allowed: true, warnings: ['cloudnative-firebird could not check this object: API down'] } });
    await expect(review({}, async () => ({}))).rejects.toThrow();
  });

  it('is served over HTTPS at /validate', async () => {
    const certs = createWebhookCertificates(names);
    const secret: V1Secret = {
      metadata: { name: WEBHOOK_SECRET },
      data: Object.fromEntries(
        Object.entries({ 'ca.crt': certs.caCert, 'ca.key': certs.caKey, 'tls.crt': certs.tlsCert, 'tls.key': certs.tlsKey }).map(([k, v]) => [k, Buffer.from(v).toString('base64')]),
      ),
    };
    const kubeConfig = new KubeConfig();
    const api = {
      readNamespacedSecret: vi.fn().mockResolvedValue(secret),
      readValidatingWebhookConfiguration: vi.fn().mockRejectedValue(Object.assign(new Error('nf'), { code: 404 })),
    };
    vi.spyOn(kubeConfig, 'makeApiClient').mockReturnValue(api as never);
    const server = new WebhookServer(kubeConfig, 'cnf-system', async (r) => (r.kind.kind === 'FirebirdRestore' ? { denied: 'refused' } : {}), 0);
    await server.start();
    const port = ((server as unknown as { server: { address(): { port: number } } }).server.address()).port;
    const post = (path: string, payload: object) =>
      new Promise<{ status: number; body: string }>((resolve, reject) => {
        const r = request(
          { host: '127.0.0.1', port, path, method: 'POST', ca: certs.caCert, servername: names[0], headers: { 'Content-Type': 'application/json' } },
          (res) => {
            let body = '';
            res.on('data', (d) => (body += d));
            res.on('end', () => resolve({ status: res.statusCode!, body }));
          },
        );
        r.on('error', reject);
        r.end(JSON.stringify(payload));
      });
    try {
      const answer = await post('/validate', { request: req('FirebirdRestore', {}) });
      expect(answer.status).toBe(200);
      expect(JSON.parse(answer.body).response).toMatchObject({ uid: 'u1', allowed: false, status: { message: 'refused' } });
      expect((await post('/validate', { nothing: true })).status).toBe(400);
      expect((await post('/other', {})).status).toBe(404);
    } finally {
      server.stop();
    }
  });
});
