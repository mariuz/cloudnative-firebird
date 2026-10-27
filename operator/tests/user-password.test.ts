import { describe, it, expect } from 'vitest';
import { V1Secret } from '@kubernetes/client-node';
import { FirebirdUser } from '../src/types';
import { validateUserSpec } from '../src/utils/users';
import { ValidationError } from '../src/utils/validation';
import {
  PASSWORD_GENERATED_ANNOTATION,
  buildPasswordSecret,
  generatePassword,
  generatedSecretName,
  parseDuration,
  passwordSecretRef,
  passwordStatus,
  passwordTimes,
  planPassword,
} from '../src/utils/user-password';

const makeUser = (password: FirebirdUser['spec']['password'] | undefined, extra: Partial<FirebirdUser['spec']> = {}): FirebirdUser => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdUser',
  metadata: { name: 'app-user', namespace: 'apps', uid: 'u-uid', resourceVersion: '1' },
  spec: { clusterName: 'db', ...(password ? { password } : {}), ...extra },
});
const generate = (extra: Partial<NonNullable<FirebirdUser['spec']['password']>> = {}) => makeUser({ mode: 'generate', ...extra });
const owned = (user: FirebirdUser, generated: string, password = 'cGFzcw=='): V1Secret => ({
  metadata: {
    name: generatedSecretName(user),
    annotations: { [PASSWORD_GENERATED_ANNOTATION]: generated },
    ownerReferences: [{ apiVersion: 'firebird.cloudnative-firebird.io/v1', kind: 'FirebirdUser', name: 'app-user', uid: 'u-uid', controller: true }],
  },
  data: { username: 'QVBQX1VTRVI=', password },
});

describe('generated passwords (FirebirdUser spec.password)', () => {
  it('reads durations in hours, minutes and seconds', () => {
    expect(parseDuration('2160h')).toBe(2160 * 3600_000);
    expect(parseDuration('1h30m')).toBe(5400_000);
    expect(parseDuration('45s')).toBe(45_000);
    for (const bad of ['', '0h', '1d', '1.5h', 'h', '30m1h']) expect(parseDuration(bad)).toBeUndefined();
  });

  it('takes the password from passwordSecret, or from the generated Secret', () => {
    expect(passwordSecretRef(makeUser(undefined, { passwordSecret: { name: 'pw', key: 'secret' } }))).toEqual({ name: 'pw', key: 'secret' });
    expect(passwordSecretRef(generate())).toEqual({ name: 'app-user-password', key: 'password' });
    expect(passwordSecretRef(generate({ secretName: 'mine' }))).toEqual({ name: 'mine', key: 'password' });
  });

  it('is validated: exactly one source, a length and durations a password can have', () => {
    expect(() => validateUserSpec(generate({ length: 24, duration: '2160h', renewBefore: '168h' }))).not.toThrow();
    const invalid: Array<[FirebirdUser, RegExp]> = [
      [makeUser(undefined), /passwordSecret.name or password/],
      [makeUser({ mode: 'generate' }, { passwordSecret: { name: 'pw' } }), /mutually exclusive/],
      [generate({ mode: 'random' as 'generate' }), /must be "generate"/],
      [generate({ length: 8 }), /from 16 to 128/],
      [generate({ length: 20.5 }), /from 16 to 128/],
      [generate({ secretName: 'Not_A_Name' }), /DNS subdomain/],
      [generate({ renewBefore: '1h' }), /needs password.duration/],
      [generate({ duration: '90d' }), /Invalid password.duration/],
      [generate({ duration: '1h', renewBefore: '1h' }), /shorter than/],
    ];
    for (const [user, message] of invalid) expect(() => validateUserSpec(user), JSON.stringify(user.spec)).toThrow(message);
    expect(() => validateUserSpec(makeUser(undefined))).toThrow(ValidationError);
  });

  it('generates letters and digits of the asked length', () => {
    const pw = generatePassword(64);
    expect(pw).toMatch(/^[A-Za-z0-9]{64}$/);
    expect(generatePassword(64)).not.toBe(pw);
    // every character comes from the generator: index 0 and the last of the alphabet
    expect(generatePassword(3, (max) => max - 1)).toBe('999');
    expect(generatePassword(2, () => 0)).toBe('AA');
  });

  it('expires duration after generation and renews renewBefore (default a third) ahead', () => {
    const t0 = new Date('2026-10-01T00:00:00Z');
    expect(passwordTimes({ mode: 'generate' }, t0)).toBeUndefined();
    expect(passwordTimes({ mode: 'generate', duration: '90h', renewBefore: '10h' }, t0)).toEqual({
      expiration: new Date('2026-10-04T18:00:00Z'),
      renewAt: new Date('2026-10-04T08:00:00Z'),
    });
    expect(passwordTimes({ mode: 'generate', duration: '3h' }, t0)!.renewAt).toEqual(new Date('2026-10-01T02:00:00Z'));
  });

  it('creates the Secret, keeps it, renews it when due, and never takes over a Secret it does not own', () => {
    const user = generate({ duration: '90h', renewBefore: '10h' });
    const now = new Date('2026-10-02T00:00:00Z');
    expect(planPassword(user, undefined, now)).toEqual({ kind: 'create' });
    expect(planPassword(user, owned(user, '2026-10-01T00:00:00Z'), now)).toEqual({ kind: 'keep' });
    expect(planPassword(user, owned(user, '2026-09-28T00:00:00Z'), now)).toEqual({ kind: 'renew', reason: 'the password expires at 2026-10-01T18:00:00.000Z' });
    expect(planPassword(user, owned(user, '2026-10-01T00:00:00Z', ''), now)).toMatchObject({ kind: 'renew', reason: 'Secret app-user-password has no password' });
    // without a duration: generated once and kept, however old
    expect(planPassword(generate(), owned(generate(), '2020-01-01T00:00:00Z'), now)).toEqual({ kind: 'keep' });
    // a Secret of the name someone else made, or another FirebirdUser's
    const foreign: V1Secret = { metadata: { name: 'app-user-password' }, data: { password: 'eA==' } };
    expect(planPassword(user, foreign, now)).toMatchObject({ kind: 'conflict', message: expect.stringContaining('is not owned by this FirebirdUser') });
    const other = owned(user, '2026-10-01T00:00:00Z');
    other.metadata!.ownerReferences![0].uid = 'another';
    expect(planPassword(user, other, now).kind).toBe('conflict');
  });

  it('writes a basic-auth Secret owned by the FirebirdUser, and reports it in the status', () => {
    const user = generate({ duration: '90h' });
    const now = new Date('2026-10-01T00:00:00Z');
    const secret = buildPasswordSecret(user, 'APP_USER', 'Abc123', now);
    expect(secret.type).toBe('kubernetes.io/basic-auth');
    expect(secret.metadata).toMatchObject({
      name: 'app-user-password',
      namespace: 'apps',
      annotations: { [PASSWORD_GENERATED_ANNOTATION]: '2026-10-01T00:00:00.000Z' },
      ownerReferences: [{ kind: 'FirebirdUser', name: 'app-user', uid: 'u-uid', controller: true, blockOwnerDeletion: false }],
    });
    expect(Buffer.from(secret.data!.username, 'base64').toString()).toBe('APP_USER');
    expect(Buffer.from(secret.data!.password, 'base64').toString()).toBe('Abc123');
    expect(passwordStatus(user, secret)).toEqual({
      secretName: 'app-user-password',
      generatedTime: '2026-10-01T00:00:00.000Z',
      expiration: '2026-10-04T18:00:00.000Z',
    });
    expect(passwordStatus(generate(), secret)).toEqual({ secretName: 'app-user-password', generatedTime: '2026-10-01T00:00:00.000Z' });
  });
});
