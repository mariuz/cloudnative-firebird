import { randomInt } from 'crypto';
import { V1Secret } from '@kubernetes/client-node';
import { API_GROUP, FirebirdUser, FirebirdUserPasswordSpec } from '../types';
import { ValidationError } from './validation';

/**
 * Generated passwords for FirebirdUser (spec.password, CloudNativePG 1.31's DatabaseRole password
 * stanza): the operator generates the password into a kubernetes.io/basic-auth Secret it owns,
 * "<name>-password" by default, and applies it like a password from passwordSecret. The password
 * is generated once and kept, unless duration gives it a lifetime: a new one is generated
 * renewBefore ahead of the expiration. A Secret the operator does not own is never overwritten.
 */

/** When the Secret's password was generated (RFC 3339); the expiration is counted from it */
export const PASSWORD_GENERATED_ANNOTATION = `${API_GROUP}/password-generated`;
export const DEFAULT_PASSWORD_LENGTH = 32;
export const MIN_PASSWORD_LENGTH = 16;
export const MAX_PASSWORD_LENGTH = 128;
// letters and digits: nothing a client, a connection string or a shell needs to quote
const ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
const DURATION = /^(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$/;
const DNS_NAME = /^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$/;

/** Milliseconds of a duration in hours, minutes and seconds ("2160h", "1h30m", "45s"); undefined if malformed or zero */
export function parseDuration(value: string): number | undefined {
  const m = DURATION.exec(value);
  if (!m || value === '') return undefined;
  const ms = ((Number(m[1] ?? 0) * 60 + Number(m[2] ?? 0)) * 60 + Number(m[3] ?? 0)) * 1000;
  return ms > 0 ? ms : undefined;
}

/** The Secret a generated password is kept in */
export function generatedSecretName(user: FirebirdUser): string {
  return user.spec.password?.secretName ?? `${user.metadata.name}-password`;
}

/** The Secret and key the user's password comes from: passwordSecret, or the generated Secret */
export function passwordSecretRef(user: FirebirdUser): { name: string; key: string } {
  if (user.spec.password) return { name: generatedSecretName(user), key: 'password' };
  return { name: user.spec.passwordSecret?.name ?? '', key: user.spec.passwordSecret?.key ?? 'password' };
}

/** spec.password: exactly one of it and passwordSecret, and settings a password can satisfy */
export function validatePasswordSpec(user: FirebirdUser): void {
  const { password, passwordSecret } = user.spec;
  if (!password && !passwordSecret?.name) throw new ValidationError('FirebirdUser needs passwordSecret.name or password');
  if (password && passwordSecret) throw new ValidationError('FirebirdUser passwordSecret and password are mutually exclusive');
  if (!password) return;
  if (password.mode !== 'generate') throw new ValidationError(`Invalid password.mode "${password.mode}": must be "generate"`);
  const secretName = generatedSecretName(user);
  if (secretName.length > 253 || !DNS_NAME.test(secretName)) {
    throw new ValidationError(`Invalid password Secret name "${secretName}": a DNS subdomain name (set password.secretName)`);
  }
  const length = password.length ?? DEFAULT_PASSWORD_LENGTH;
  if (!Number.isInteger(length) || length < MIN_PASSWORD_LENGTH || length > MAX_PASSWORD_LENGTH) {
    throw new ValidationError(`Invalid password.length ${length}: from ${MIN_PASSWORD_LENGTH} to ${MAX_PASSWORD_LENGTH}`);
  }
  if (password.renewBefore !== undefined && password.duration === undefined) {
    throw new ValidationError('password.renewBefore needs password.duration');
  }
  if (password.duration === undefined) return;
  const duration = parseDuration(password.duration);
  if (duration === undefined) throw new ValidationError(`Invalid password.duration "${password.duration}": hours, minutes and seconds, e.g. "2160h"`);
  if (password.renewBefore === undefined) return;
  const renewBefore = parseDuration(password.renewBefore);
  if (renewBefore === undefined) throw new ValidationError(`Invalid password.renewBefore "${password.renewBefore}": hours, minutes and seconds, e.g. "168h"`);
  if (renewBefore >= duration) throw new ValidationError('password.renewBefore must be shorter than password.duration');
}

/** A random password of letters and digits, from the system's cryptographic generator */
export function generatePassword(length: number, random: (max: number) => number = randomInt): string {
  let out = '';
  for (let i = 0; i < length; i++) out += ALPHABET[random(ALPHABET.length)];
  return out;
}

/** When a generated password expires, and when it is renewed; undefined without a duration */
export function passwordTimes(spec: FirebirdUserPasswordSpec, generated: Date): { expiration: Date; renewAt: Date } | undefined {
  const duration = spec.duration === undefined ? undefined : parseDuration(spec.duration);
  if (duration === undefined) return undefined;
  const renewBefore = (spec.renewBefore === undefined ? undefined : parseDuration(spec.renewBefore)) ?? Math.floor(duration / 3);
  const expiration = new Date(generated.getTime() + duration);
  return { expiration, renewAt: new Date(expiration.getTime() - renewBefore) };
}

/** Whether the FirebirdUser is the Secret's controlling owner */
export function ownsSecret(user: FirebirdUser, secret: V1Secret): boolean {
  return (secret.metadata?.ownerReferences ?? []).some((r) => r.controller && r.kind === 'FirebirdUser' && r.uid === user.metadata.uid);
}

/** When the Secret's password was generated: its annotation, else the Secret's creation */
export function generatedAt(secret: V1Secret): Date | undefined {
  const value = secret.metadata?.annotations?.[PASSWORD_GENERATED_ANNOTATION];
  const t = value ? Date.parse(value) : NaN;
  if (!Number.isNaN(t)) return new Date(t);
  const created = secret.metadata?.creationTimestamp;
  return created ? new Date(created) : undefined;
}

export type PasswordPlan =
  | { kind: 'create' }
  | { kind: 'renew'; reason: string }
  | { kind: 'keep' }
  | { kind: 'conflict'; message: string };

/** What to do with the generated password's Secret (undefined: it does not exist) at `now` */
export function planPassword(user: FirebirdUser, secret: V1Secret | undefined, now: Date): PasswordPlan {
  if (!secret) return { kind: 'create' };
  const name = generatedSecretName(user);
  if (!ownsSecret(user, secret)) {
    return { kind: 'conflict', message: `Secret ${name} exists and is not owned by this FirebirdUser: it is never overwritten (set password.secretName)` };
  }
  if (!secret.data?.password) return { kind: 'renew', reason: `Secret ${name} has no password` };
  const generated = generatedAt(secret);
  const times = generated && passwordTimes(user.spec.password!, generated);
  if (times && now.getTime() >= times.renewAt.getTime()) return { kind: 'renew', reason: `the password expires at ${times.expiration.toISOString()}` };
  return { kind: 'keep' };
}

/** The Secret of a generated password: basic-auth (username, password), owned by the FirebirdUser */
export function buildPasswordSecret(user: FirebirdUser, username: string, password: string, now: Date): V1Secret {
  return {
    apiVersion: 'v1',
    kind: 'Secret',
    type: 'kubernetes.io/basic-auth',
    metadata: {
      name: generatedSecretName(user),
      namespace: user.metadata.namespace ?? 'default',
      labels: { 'app.kubernetes.io/managed-by': 'cloudnative-firebird', [`${API_GROUP}/user`]: user.metadata.name },
      annotations: { [PASSWORD_GENERATED_ANNOTATION]: now.toISOString() },
      ownerReferences: [
        {
          apiVersion: `${API_GROUP}/v1`,
          kind: 'FirebirdUser',
          name: user.metadata.name,
          uid: user.metadata.uid ?? '',
          controller: true,
          blockOwnerDeletion: false,
        },
      ],
    },
    data: {
      username: Buffer.from(username).toString('base64'),
      password: Buffer.from(password).toString('base64'),
    },
  };
}

/** status.password of a generated password's Secret */
export function passwordStatus(user: FirebirdUser, secret: V1Secret): { secretName: string; generatedTime?: string; expiration?: string } {
  const generated = generatedAt(secret);
  const times = generated && passwordTimes(user.spec.password!, generated);
  return {
    secretName: generatedSecretName(user),
    ...(generated ? { generatedTime: generated.toISOString() } : {}),
    ...(times ? { expiration: times.expiration.toISOString() } : {}),
  };
}
