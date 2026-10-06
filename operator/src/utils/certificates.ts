import { createPrivateKey, generateKeyPairSync, KeyObject, randomBytes, sign, X509Certificate } from 'crypto';

/**
 * Self-signed certificates for the operator's admission webhook (CloudNativePG generates its own
 * too, so cert-manager is not required): a CA, and a serving certificate it signs. Node can parse
 * X.509 but not create it, so the DER encoding is done here (ECDSA P-256 with SHA-256).
 */

// --- DER encoding (X.690) ---

function length(n: number): Buffer {
  if (n < 0x80) return Buffer.from([n]);
  const bytes: number[] = [];
  for (let v = n; v > 0; v = Math.floor(v / 256)) bytes.unshift(v & 0xff);
  return Buffer.from([0x80 | bytes.length, ...bytes]);
}

function tlv(tag: number, value: Buffer): Buffer {
  return Buffer.concat([Buffer.from([tag]), length(value.length), value]);
}

const sequence = (...items: Buffer[]) => tlv(0x30, Buffer.concat(items));
const set = (...items: Buffer[]) => tlv(0x31, Buffer.concat(items));
const explicit = (n: number, value: Buffer) => tlv(0xa0 + n, value);
const bitString = (value: Buffer, unused = 0) => tlv(0x03, Buffer.concat([Buffer.from([unused]), value]));
const octetString = (value: Buffer) => tlv(0x04, value);
const utf8 = (s: string) => tlv(0x0c, Buffer.from(s, 'utf8'));
const boolean = (v: boolean) => tlv(0x01, Buffer.from([v ? 0xff : 0]));

function integer(value: Buffer): Buffer {
  let v = value;
  while (v.length > 1 && v[0] === 0 && (v[1] & 0x80) === 0) v = v.subarray(1);
  if (v[0] & 0x80) v = Buffer.concat([Buffer.from([0]), v]);
  return tlv(0x02, v);
}

function oid(dotted: string): Buffer {
  const parts = dotted.split('.').map(Number);
  const bytes: number[] = [parts[0] * 40 + parts[1]];
  for (const part of parts.slice(2)) {
    const chunk: number[] = [];
    let v = part;
    do {
      chunk.unshift((v & 0x7f) | (chunk.length ? 0x80 : 0));
      v = Math.floor(v / 128);
    } while (v > 0);
    bytes.push(...chunk);
  }
  return tlv(0x06, Buffer.from(bytes));
}

/** UTCTime until 2049, GeneralizedTime from 2050 (RFC 5280 4.1.2.5) */
function time(date: Date): Buffer {
  const iso = date.toISOString().replace(/[-:T]/g, '').slice(0, 14); // YYYYMMDDHHMMSS
  return date.getUTCFullYear() < 2050 ? tlv(0x17, Buffer.from(`${iso.slice(2)}Z`)) : tlv(0x18, Buffer.from(`${iso}Z`));
}

const ECDSA_WITH_SHA256 = sequence(oid('1.2.840.10045.4.3.2'));

const name = (commonName: string) => sequence(set(sequence(oid('2.5.4.3'), utf8(commonName))));

function extension(id: string, critical: boolean, value: Buffer): Buffer {
  return sequence(oid(id), ...(critical ? [boolean(true)] : []), octetString(value));
}

interface CertificateOptions {
  subject: string;
  issuer: string;
  publicKey: KeyObject;
  signingKey: KeyObject;
  notBefore: Date;
  notAfter: Date;
  ca: boolean;
  dnsNames?: string[];
}

function certificate(o: CertificateOptions): string {
  const serial = randomBytes(16);
  serial[0] = (serial[0] & 0x7f) | 0x40; // positive, 16 bytes
  const extensions = o.ca
    ? [
        extension('2.5.29.19', true, sequence(boolean(true))), // basicConstraints: CA
        extension('2.5.29.15', true, bitString(Buffer.from([0x06]), 1)), // keyUsage: keyCertSign, cRLSign
      ]
    : [
        extension('2.5.29.19', true, sequence()),
        extension('2.5.29.15', true, bitString(Buffer.from([0x80]), 7)), // keyUsage: digitalSignature
        extension('2.5.29.37', false, sequence(oid('1.3.6.1.5.5.7.3.1'))), // extKeyUsage: serverAuth
        // subjectAltName: dNSName entries ([2] IMPLICIT IA5String)
        extension('2.5.29.17', false, sequence(...(o.dnsNames ?? []).map((d) => tlv(0x82, Buffer.from(d, 'ascii'))))),
      ];
  const tbs = sequence(
    explicit(0, integer(Buffer.from([2]))), // v3
    integer(serial),
    ECDSA_WITH_SHA256,
    name(o.issuer),
    sequence(time(o.notBefore), time(o.notAfter)),
    name(o.subject),
    o.publicKey.export({ type: 'spki', format: 'der' }),
    explicit(3, sequence(...extensions)),
  );
  const signature = sign('sha256', tbs, { key: o.signingKey, dsaEncoding: 'der' });
  const der = sequence(tbs, ECDSA_WITH_SHA256, bitString(signature));
  return `-----BEGIN CERTIFICATE-----\n${der.toString('base64').replace(/(.{64})/g, '$1\n').replace(/\n?$/, '\n')}-----END CERTIFICATE-----\n`;
}

export interface WebhookCertificates {
  /** PEM: the CA certificate (the webhook configuration's caBundle) */
  caCert: string;
  caKey: string;
  /** PEM: the serving certificate and its key */
  tlsCert: string;
  tlsKey: string;
}

const DAY_MS = 86_400_000;
export const CA_VALIDITY_DAYS = 3650;
export const SERVING_VALIDITY_DAYS = 365;

/**
 * Creates a CA and a serving certificate for the given DNS names. With an existing CA (still
 * valid), only the serving certificate is renewed, so the caBundle stays the same.
 */
export function createWebhookCertificates(dnsNames: string[], now = new Date(), ca?: { cert: string; key: string }): WebhookCertificates {
  const notBefore = new Date(now.getTime() - 5 * 60_000); // clock skew
  let caCert = ca?.cert;
  let caKeyPem = ca?.key;
  if (!caCert || !caKeyPem) {
    const pair = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
    caKeyPem = pair.privateKey.export({ type: 'pkcs8', format: 'pem' }).toString();
    caCert = certificate({
      subject: 'cloudnative-firebird webhook CA',
      issuer: 'cloudnative-firebird webhook CA',
      publicKey: pair.publicKey,
      signingKey: pair.privateKey,
      notBefore,
      notAfter: new Date(now.getTime() + CA_VALIDITY_DAYS * DAY_MS),
      ca: true,
    });
  }
  const leaf = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  const tlsCert = certificate({
    subject: dnsNames[0],
    issuer: 'cloudnative-firebird webhook CA',
    publicKey: leaf.publicKey,
    signingKey: createPrivateKey(caKeyPem),
    notBefore,
    notAfter: new Date(now.getTime() + SERVING_VALIDITY_DAYS * DAY_MS),
    ca: false,
    dnsNames,
  });
  return {
    caCert,
    caKey: caKeyPem,
    tlsCert,
    tlsKey: leaf.privateKey.export({ type: 'pkcs8', format: 'pem' }).toString(),
  };
}

/** Days until a PEM certificate expires (negative when expired), or undefined when it cannot be parsed */
export function daysUntilExpiry(pem: string, now = new Date()): number | undefined {
  try {
    return (Date.parse(new X509Certificate(pem).validTo) - now.getTime()) / DAY_MS;
  } catch {
    return undefined;
  }
}

/** Whether the serving certificate was issued by this CA and names every DNS name */
export function servingCertificateMatches(tlsCert: string, caCert: string, dnsNames: string[]): boolean {
  try {
    const leaf = new X509Certificate(tlsCert);
    const ca = new X509Certificate(caCert);
    return leaf.verify(ca.publicKey) && dnsNames.every((d) => leaf.checkHost(d) === d);
  } catch {
    return false;
  }
}
