import { describe, it, expect } from 'vitest';
import { V1StatefulSet } from '@kubernetes/client-node';
import { FirebirdCluster } from '../src/types';
import { extraConfigErrors } from '../src/utils/pod-settings';
import { buildStatefulSet, setFieldsMatch, statefulSetNeedsUpdate } from '../src/utils/resources';
import { validateClusterSpec } from '../src/utils/validation';
import { makeCluster } from './helpers/factories';

const envFrom: FirebirdCluster['spec']['envFrom'] = [{ configMapRef: { name: 'app-settings' } }, { secretRef: { name: 'app-keys', optional: true }, prefix: 'APP_' }];
const projectedVolumeTemplate: FirebirdCluster['spec']['projectedVolumeTemplate'] = {
  sources: [
    { secret: { name: 'client-certs', items: [{ key: 'tls.crt', path: 'certs/client.crt' }] } },
    { serviceAccountToken: { audience: 'vault', path: 'token' } },
    { downwardAPI: { items: [{ path: 'labels', fieldRef: { fieldPath: 'metadata.labels' } }] } },
  ],
};
const firebird = (sts: V1StatefulSet) => sts.spec!.template.spec!.containers.find((c) => c.name === 'firebird')!;

describe('envFrom and projected volumes', () => {
  it('are unset by default', () => {
    const sts = buildStatefulSet(makeCluster());
    expect(firebird(sts).envFrom).toBeUndefined();
    expect(sts.spec!.template.spec!.volumes!.some((v) => v.name === 'projected')).toBe(false);
    expect(firebird(sts).volumeMounts!.some((m) => m.name === 'projected')).toBe(false);
  });

  it('go on the Firebird container: envFrom, and the projected volume read-only at /projected', () => {
    const sts = buildStatefulSet(makeCluster({ envFrom, projectedVolumeTemplate }));
    expect(firebird(sts).envFrom).toEqual(envFrom);
    expect(firebird(sts).volumeMounts).toContainEqual({ name: 'projected', mountPath: '/projected', readOnly: true });
    expect(sts.spec!.template.spec!.volumes).toContainEqual({ name: 'projected', projected: projectedVolumeTemplate });
    // the sidecars get neither
    for (const c of sts.spec!.template.spec!.containers.filter((c) => c.name !== 'firebird')) {
      expect(c.envFrom).toBeUndefined();
      expect((c.volumeMounts ?? []).some((m) => m.name === 'projected')).toBe(false);
    }
    // a template without sources is no volume
    expect(buildStatefulSet(makeCluster({ projectedVolumeTemplate: { sources: [] } })).spec!.template.spec!.volumes!.some((v) => v.name === 'projected')).toBe(false);
  });
});

describe('StatefulSet updates for envFrom and the projected volume', () => {
  // as the API server stores it: the mode, a token's expiration and a field reference's version defaulted
  const asStored = (sts: V1StatefulSet): V1StatefulSet => {
    const copy = JSON.parse(JSON.stringify(sts)) as V1StatefulSet;
    for (const v of copy.spec!.template.spec!.volumes ?? []) {
      if (!v.projected) continue;
      v.projected.defaultMode ??= 420;
      for (const s of v.projected.sources ?? []) {
        if (s.serviceAccountToken) s.serviceAccountToken.expirationSeconds ??= 3600;
        for (const item of s.downwardAPI?.items ?? []) if (item.fieldRef) item.fieldRef.apiVersion ??= 'v1';
      }
    }
    return copy;
  };
  const full = makeCluster({ envFrom, projectedVolumeTemplate });

  it('are not needed for an unchanged cluster, whatever the server defaulted', () => {
    expect(statefulSetNeedsUpdate(asStored(buildStatefulSet(full)), buildStatefulSet(full))).toBe(false);
    expect(statefulSetNeedsUpdate(asStored(buildStatefulSet(makeCluster())), buildStatefulSet(makeCluster()))).toBe(false);
  });

  it.each<[string, Partial<FirebirdCluster['spec']>]>([
    ['envFrom added', { envFrom: [{ configMapRef: { name: 'other' } }] }],
    ['envFrom prefix', { envFrom: [envFrom![0], { ...envFrom![1], prefix: 'X_' }] }],
    ['projected source', { projectedVolumeTemplate: { sources: [projectedVolumeTemplate!.sources![0]] } }],
    ['projected mode', { projectedVolumeTemplate: { ...projectedVolumeTemplate, defaultMode: 0o400 } }],
    ['projected removed', { projectedVolumeTemplate: undefined }],
  ])('are needed when %s changes', (_what, extra) => {
    expect(statefulSetNeedsUpdate(asStored(buildStatefulSet(full)), buildStatefulSet(makeCluster({ envFrom, projectedVolumeTemplate, ...extra })))).toBe(true);
  });

  it('compares the fields a spec sets', () => {
    expect(setFieldsMatch({ a: 1, b: [{ c: 2 }] }, { a: 1, z: 9, b: [{ c: 2, d: 3 }] })).toBe(true);
    expect(setFieldsMatch({ a: 1 }, { a: 2 })).toBe(false);
    expect(setFieldsMatch([{ a: 1 }], [{ a: 1 }, { a: 2 }])).toBe(false);
    expect(setFieldsMatch({ a: { b: 1 } }, { a: 'x' })).toBe(false);
    expect(setFieldsMatch({ a: undefined }, {})).toBe(true);
  });
});

describe('envFrom and projected volume validation', () => {
  it('accepts valid settings', () => {
    expect(extraConfigErrors(makeCluster({ envFrom, projectedVolumeTemplate }).spec)).toEqual([]);
    expect(() => validateClusterSpec(makeCluster({ envFrom, projectedVolumeTemplate }))).not.toThrow();
  });

  it.each<[Partial<FirebirdCluster['spec']>, string]>([
    [{ envFrom: [{}] }, 'envFrom[0] needs exactly one of configMapRef and secretRef'],
    [{ envFrom: [{ configMapRef: { name: 'a' }, secretRef: { name: 'b' } }] }, 'envFrom[0] needs exactly one'],
    [{ envFrom: [{ secretRef: { name: 'Bad_Name' } }] }, 'envFrom[0]: the ConfigMap or Secret name'],
    [{ envFrom: [{ secretRef: { name: 'ok' }, prefix: '1BAD' }] }, 'envFrom[0].prefix'],
    [{ projectedVolumeTemplate: { sources: 'x' as never } }, 'projectedVolumeTemplate.sources must be a list'],
    [{ projectedVolumeTemplate: { sources: [{}] } }, 'projectedVolumeTemplate.sources[0] needs exactly one of'],
    [{ projectedVolumeTemplate: { sources: [{ secret: { name: 'a' }, configMap: { name: 'b' } }] } }, 'sources[0] needs exactly one'],
    [{ projectedVolumeTemplate: { sources: [{ secret: { name: 'a' } }], defaultMode: 0o1000 } }, 'defaultMode must be a file mode'],
  ])('rejects %j', (extra, message) => {
    expect(() => validateClusterSpec(makeCluster(extra))).toThrow(message);
  });
});
