import { describe, it, expect, vi, afterEach } from 'vitest';

describe('default Firebird image', () => {
  afterEach(() => {
    vi.unstubAllEnvs();
    vi.resetModules();
  });

  it('is firebirdsql/firebird:latest without FIREBIRD_DEFAULT_IMAGE', async () => {
    vi.stubEnv('FIREBIRD_DEFAULT_IMAGE', '');
    vi.resetModules();
    const { DEFAULT_FIREBIRD_IMAGE } = await import('../src/types');
    expect(DEFAULT_FIREBIRD_IMAGE).toBe('firebirdsql/firebird:latest');
  });

  it('follows the operator\'s FIREBIRD_DEFAULT_IMAGE for clusters without spec.imageName', async () => {
    vi.stubEnv('FIREBIRD_DEFAULT_IMAGE', ' registry.example.com/firebird:6-snapshot ');
    vi.resetModules();
    const { DEFAULT_FIREBIRD_IMAGE } = await import('../src/types');
    const { buildStatefulSet } = await import('../src/utils/resources');
    expect(DEFAULT_FIREBIRD_IMAGE).toBe('registry.example.com/firebird:6-snapshot');
    const cluster = {
      apiVersion: 'firebird.cloudnative-firebird.io/v1' as const,
      kind: 'FirebirdCluster' as const,
      metadata: { name: 'db', namespace: 'default' },
      spec: { instances: 1, storage: { size: '1Gi' } },
    };
    const images = (s: ReturnType<typeof buildStatefulSet>) =>
      [...(s.spec!.template.spec!.initContainers ?? []), ...s.spec!.template.spec!.containers].map((c) => c.image);
    expect(new Set(images(buildStatefulSet(cluster)))).toEqual(new Set(['registry.example.com/firebird:6-snapshot']));
    // spec.imageName still wins
    const pinned = buildStatefulSet({ ...cluster, spec: { ...cluster.spec, imageName: 'firebirdsql/firebird:4' } });
    expect(new Set(images(pinned))).toEqual(new Set(['firebirdsql/firebird:4']));
  });
});
