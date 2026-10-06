import { createServer, Server } from 'https';
import { IncomingMessage, ServerResponse } from 'http';
import { AdmissionregistrationV1Api, CoreV1Api, KubeConfig, V1Secret } from '@kubernetes/client-node';
import { logger } from './logger';
import { createWebhookCertificates, daysUntilExpiry, servingCertificateMatches, WebhookCertificates } from './certificates';

/**
 * Validating admission webhook (CloudNativePG runs one too). The CRDs' validation rules check
 * each object on its own; the webhook adds what needs other objects (AdmissionChecks), so
 * `kubectl apply` refuses them instead of the operator reporting them after the fact. Its
 * failurePolicy is Ignore: while the operator is down, objects are admitted and the operator
 * still validates them when it reconciles.
 *
 * The operator manages the certificates (no cert-manager needed): a CA and a serving certificate
 * in the WEBHOOK_SECRET Secret of its namespace, renewed before they expire, with the CA put
 * into the ValidatingWebhookConfiguration's caBundle.
 */

function isNotFound(err: unknown): boolean {
  const e = err as { code?: number; statusCode?: number; response?: { statusCode?: number } };
  return e?.code === 404 || e?.statusCode === 404 || e?.response?.statusCode === 404;
}

export const WEBHOOK_SECRET = 'cloudnative-firebird-webhook-cert';
export const WEBHOOK_SERVICE = 'cloudnative-firebird-webhook';
export const WEBHOOK_CONFIGURATION = 'cloudnative-firebird-validating-webhook';
export const WEBHOOK_PORT = 9443;
export const WEBHOOK_PATH = '/validate';
/** Serving certificates are renewed this many days before they expire, the CA a year before */
const RENEW_DAYS = 30;
const CA_RENEW_DAYS = 365;

export interface AdmissionRequest {
  uid: string;
  kind: { group: string; version: string; kind: string };
  operation: 'CREATE' | 'UPDATE' | 'DELETE' | 'CONNECT';
  namespace?: string;
  object?: Record<string, unknown>;
  oldObject?: Record<string, unknown>;
}

export interface AdmissionVerdict {
  /** Why the object is refused; undefined admits it */
  denied?: string;
  /** Shown by kubectl, without refusing the object */
  warnings?: string[];
}

export type AdmissionValidator = (request: AdmissionRequest) => Promise<AdmissionVerdict>;

/** DNS names the API server uses for the webhook Service */
export function webhookDnsNames(namespace: string): string[] {
  return [`${WEBHOOK_SERVICE}.${namespace}.svc`, `${WEBHOOK_SERVICE}.${namespace}.svc.cluster.local`];
}

/** Builds the AdmissionReview response for a request */
export async function review(body: unknown, validate: AdmissionValidator): Promise<Record<string, unknown>> {
  const request = (body as { request?: AdmissionRequest })?.request;
  if (!request?.uid) throw new Error('not an AdmissionReview request');
  let verdict: AdmissionVerdict;
  try {
    verdict = await validate(request);
  } catch (err) {
    // the operator validates again when it reconciles: an error here admits the object
    logger.warn({ err, kind: request.kind?.kind }, 'Admission check failed; admitting the object');
    verdict = { warnings: [`cloudnative-firebird could not check this object: ${(err as Error).message}`] };
  }
  const response: Record<string, unknown> = { uid: request.uid, allowed: verdict.denied === undefined };
  if (verdict.denied !== undefined) response.status = { code: 422, reason: 'Invalid', message: verdict.denied };
  if (verdict.warnings?.length) response.warnings = verdict.warnings;
  return { apiVersion: 'admission.k8s.io/v1', kind: 'AdmissionReview', response };
}

function readBody(req: IncomingMessage, limit = 3 * 1024 * 1024): Promise<string> {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks: Buffer[] = [];
    req.on('data', (chunk: Buffer) => {
      size += chunk.length;
      if (size > limit) {
        reject(new Error('request too large'));
        req.destroy();
      } else chunks.push(chunk);
    });
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}

export class WebhookServer {
  private server?: Server;
  private renewTimer?: NodeJS.Timeout;
  private readonly coreApi: CoreV1Api;
  private readonly admissionApi: AdmissionregistrationV1Api;

  constructor(
    kubeConfig: KubeConfig,
    private readonly namespace: string,
    private readonly validate: AdmissionValidator,
    private readonly port = WEBHOOK_PORT,
  ) {
    this.coreApi = kubeConfig.makeApiClient(CoreV1Api);
    this.admissionApi = kubeConfig.makeApiClient(AdmissionregistrationV1Api);
  }

  async start(): Promise<void> {
    const certs = await this.ensureCertificates();
    this.server = createServer({ cert: certs.tlsCert, key: certs.tlsKey }, (req, res) => void this.handle(req, res));
    await new Promise<void>((resolve) => this.server!.listen(this.port, resolve));
    logger.info({ port: this.port }, 'Admission webhook listening');
    // renewals are checked daily; the server switches to a renewed certificate in place
    this.renewTimer = setInterval(() => {
      this.ensureCertificates()
        .then((c) => this.server?.setSecureContext({ cert: c.tlsCert, key: c.tlsKey }))
        .catch((err) => logger.error({ err }, 'Webhook certificate renewal failed'));
    }, 86_400_000);
    this.renewTimer.unref();
  }

  stop(): void {
    if (this.renewTimer) clearInterval(this.renewTimer);
    this.server?.close();
  }

  private async handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
    if (req.method !== 'POST' || !req.url?.startsWith(WEBHOOK_PATH)) {
      res.writeHead(404).end();
      return;
    }
    try {
      const result = await review(JSON.parse(await readBody(req)), this.validate);
      res.writeHead(200, { 'Content-Type': 'application/json' }).end(JSON.stringify(result));
    } catch (err) {
      res.writeHead(400, { 'Content-Type': 'text/plain' }).end((err as Error).message);
    }
  }

  /**
   * Reads the certificates from the Secret, creating or renewing them as needed, and keeps the
   * webhook configuration's caBundle up to date.
   */
  async ensureCertificates(now = new Date()): Promise<WebhookCertificates> {
    // several operator pods (e.g. during a rollout) may write at the same time: a conflict means
    // another one did, so its certificates are read and used
    for (let attempt = 1; ; attempt++) {
      try {
        return await this.ensureCertificatesOnce(now);
      } catch (err) {
        const code = (err as { code?: number; statusCode?: number }).code ?? (err as { statusCode?: number }).statusCode;
        if (code !== 409 || attempt >= 5) throw err;
        logger.info({ attempt }, 'Webhook certificates changed concurrently; reading them again');
      }
    }
  }

  private async ensureCertificatesOnce(now: Date): Promise<WebhookCertificates> {
    const names = webhookDnsNames(this.namespace);
    let secret: V1Secret | undefined;
    try {
      secret = await this.coreApi.readNamespacedSecret({ name: WEBHOOK_SECRET, namespace: this.namespace });
    } catch (err) {
      if (!isNotFound(err)) throw err;
    }
    const data = secret?.data ?? {};
    const pem = (key: string) => (data[key] ? Buffer.from(data[key], 'base64').toString('utf8') : '');
    const current: WebhookCertificates = { caCert: pem('ca.crt'), caKey: pem('ca.key'), tlsCert: pem('tls.crt'), tlsKey: pem('tls.key') };
    const caDays = current.caCert && current.caKey ? daysUntilExpiry(current.caCert, now) : undefined;
    const caValid = caDays !== undefined && caDays > CA_RENEW_DAYS;
    const servingValid =
      caValid &&
      current.tlsKey !== '' &&
      (daysUntilExpiry(current.tlsCert, now) ?? 0) > RENEW_DAYS &&
      servingCertificateMatches(current.tlsCert, current.caCert, names);
    let certs = current;
    if (!servingValid) {
      certs = createWebhookCertificates(names, now, caValid ? { cert: current.caCert, key: current.caKey } : undefined);
      const body: V1Secret = {
        metadata: { name: WEBHOOK_SECRET, namespace: this.namespace, labels: { 'app.kubernetes.io/name': 'cloudnative-firebird' } },
        type: 'kubernetes.io/tls',
        data: {
          'ca.crt': Buffer.from(certs.caCert).toString('base64'),
          'ca.key': Buffer.from(certs.caKey).toString('base64'),
          'tls.crt': Buffer.from(certs.tlsCert).toString('base64'),
          'tls.key': Buffer.from(certs.tlsKey).toString('base64'),
        },
      };
      if (secret) {
        body.metadata!.resourceVersion = secret.metadata?.resourceVersion;
        await this.coreApi.replaceNamespacedSecret({ name: WEBHOOK_SECRET, namespace: this.namespace, body });
      } else {
        await this.coreApi.createNamespacedSecret({ namespace: this.namespace, body });
      }
      logger.info({ renewedCA: !caValid }, 'Webhook certificates written');
    }
    await this.updateCaBundle(certs.caCert);
    return certs;
  }

  /** Puts the CA into every webhook of the configuration (absent configuration: nothing to do) */
  private async updateCaBundle(caCert: string): Promise<void> {
    let config;
    try {
      config = await this.admissionApi.readValidatingWebhookConfiguration({ name: WEBHOOK_CONFIGURATION });
    } catch (err) {
      if (isNotFound(err)) {
        logger.warn(`ValidatingWebhookConfiguration ${WEBHOOK_CONFIGURATION} not found: admission checks are not active`);
        return;
      }
      throw err;
    }
    const bundle = Buffer.from(caCert).toString('base64');
    const webhooks = config.webhooks ?? [];
    if (webhooks.every((w) => w.clientConfig.caBundle === bundle)) return;
    for (const w of webhooks) w.clientConfig.caBundle = bundle;
    await this.admissionApi.replaceValidatingWebhookConfiguration({ name: WEBHOOK_CONFIGURATION, body: config });
    logger.info('Webhook caBundle updated');
  }
}
