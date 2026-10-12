import { describe, it, expect, vi, type Mock } from 'vitest';
import { KubeConfig } from '@kubernetes/client-node';
import { FirebirdClusterController } from '../src/controllers/firebirdcluster.controller';
import { DEFAULT_FIREBIRD_IMAGE, FirebirdImageCatalog } from '../src/types';
import {
  clusterImage,
  imageCatalogErrors,
  imageCatalogRefErrors,
  resolveCatalogImage,
} from '../src/utils/image-catalog';
import { buildBackupJob } from '../src/utils/backup';
import { validateClusterSpec } from '../src/utils/validation';
import { makeCluster, notFoundError } from './helpers/factories';

const catalog = (images: Array<{ major: number; image: string }>, kind: FirebirdImageCatalog['kind'] = 'FirebirdImageCatalog'): FirebirdImageCatalog => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind,
  metadata: { name: 'firebird' },
  spec: { images },
});

describe('image catalogs', () => {
  it('validates references and catalogs', () => {
    expect(imageCatalogRefErrors(makeCluster({ imageCatalogRef: { name: 'firebird', major: 5 } }))).toEqual([]);
    expect(imageCatalogRefErrors(makeCluster({ imageName: 'x', imageCatalogRef: { name: 'firebird', major: 5 } }))).toEqual([
      'spec.imageName and spec.imageCatalogRef are mutually exclusive',
    ]);
    expect(imageCatalogRefErrors(makeCluster({ imageCatalogRef: { name: 'Bad Name', major: 0 } }))).toHaveLength(2);
    expect(() => validateClusterSpec(makeCluster({ imageName: 'x', imageCatalogRef: { name: 'firebird', major: 5 } }))).toThrow(/mutually exclusive/);

    expect(imageCatalogErrors(catalog([{ major: 4, image: 'fb:4' }, { major: 5, image: 'fb:5' }]))).toEqual([]);
    expect(imageCatalogErrors(catalog([{ major: 5, image: 'a' }, { major: 5, image: 'b' }]))).toEqual(['spec.images: major 5 is listed twice']);
    expect(imageCatalogErrors(catalog([]))).toEqual(['spec.images must list at least one image']);
    expect(imageCatalogErrors(catalog([{ major: 5, image: '' }]))).toEqual(['spec.images: major 5 needs an image']);
  });

  it('resolves the image of the major version, or says why it cannot', () => {
    const ref = { name: 'firebird', major: 5 };
    expect(resolveCatalogImage(ref, catalog([{ major: 4, image: 'fb:4' }, { major: 5, image: 'fb:5' }]))).toEqual({ image: 'fb:5' });
    expect(resolveCatalogImage(ref, undefined)).toEqual({ error: 'FirebirdImageCatalog firebird not found' });
    expect(resolveCatalogImage({ ...ref, kind: 'FirebirdClusterImageCatalog' }, catalog([{ major: 4, image: 'fb:4' }]))).toEqual({
      error: 'FirebirdClusterImageCatalog firebird has no image for Firebird 5',
    });
  });

  it('gives Jobs the image the cluster reconcile recorded for a catalog cluster', () => {
    expect(clusterImage(makeCluster())).toBe(DEFAULT_FIREBIRD_IMAGE);
    expect(clusterImage(makeCluster({ imageName: 'fb:4' }))).toBe('fb:4');
    const catalogCluster = { ...makeCluster({ imageCatalogRef: { name: 'firebird', major: 5 } }), status: { image: 'fb:5.0.3' } };
    expect(clusterImage(catalogCluster)).toBe('fb:5.0.3');
    // status.image of a cluster without a catalog is not used: the operator default applies as before
    expect(clusterImage({ ...makeCluster(), status: { image: 'stale' } })).toBe(DEFAULT_FIREBIRD_IMAGE);
    const backup = {
      apiVersion: 'firebird.cloudnative-firebird.io/v1' as const,
      kind: 'FirebirdBackup' as const,
      metadata: { name: 'b' },
      spec: { clusterName: 'test-cluster' },
    };
    const job = buildBackupJob(backup, catalogCluster);
    expect(job.spec!.template.spec!.containers.map((c) => c.image)).toContain('fb:5.0.3');
  });
});

/** API clients whose calls default to: read → 404, list → empty, others → {} */
function mockApi(overrides: Record<string, Mock> = {}) {
  const calls: Record<string, Mock> = {};
  const fn = (method: string): Mock =>
    (calls[method] ??=
      overrides[method] ??
      (method.startsWith('read') || method.startsWith('get')
        ? vi.fn().mockRejectedValue(notFoundError)
        : method.startsWith('list')
          ? vi.fn().mockResolvedValue({ items: [] })
          : vi.fn().mockResolvedValue({})));
  const kubeConfig = new KubeConfig();
  vi.spyOn(kubeConfig, 'makeApiClient').mockReturnValue(new Proxy({}, { get: (_t, p: string) => fn(p) }) as never);
  return { kubeConfig, api: fn };
}

describe('cluster reconcile with an image catalog', () => {
  const images = (body: { spec: { template: { spec: { containers: Array<{ name: string; image: string }> } } } }) =>
    body.spec.template.spec.containers.find((c) => c.name === 'firebird')?.image;
  const lastStatus = (api: (m: string) => Mock) => api('patchNamespacedCustomObjectStatus').mock.calls.at(-1)?.[0].body[0].value;
  const reasons = (api: (m: string) => Mock) => api('createNamespacedEvent').mock.calls.map((c) => c[0].body.reason);

  it('runs the catalog image of the major version and records it in status.image', async () => {
    const { kubeConfig, api } = mockApi({
      getNamespacedCustomObject: vi.fn().mockImplementation(({ plural, name }) =>
        plural === 'firebirdimagecatalogs' && name === 'firebird'
          ? Promise.resolve(catalog([{ major: 4, image: 'fb:4' }, { major: 5, image: 'fb:5.0.3' }]))
          : Promise.reject(notFoundError),
      ),
    });
    const cluster = makeCluster({ imageCatalogRef: { name: 'firebird', major: 5 } });
    await new FirebirdClusterController(kubeConfig).reconcile(cluster);
    const sts = api('createNamespacedStatefulSet').mock.calls[0][0].body;
    expect(images(sts)).toBe('fb:5.0.3');
    expect(lastStatus(api).image).toBe('fb:5.0.3');
    // the spec is never written with the resolved image
    expect(cluster.spec.imageName).toBeUndefined();
  });

  it('reads a cluster-wide catalog, and reports a changed entry', async () => {
    const cluster = {
      ...makeCluster({ imageCatalogRef: { kind: 'FirebirdClusterImageCatalog' as const, name: 'firebird', major: 5 } }),
      status: { phase: 'Running' as const, image: 'fb:5.0.3' },
    };
    const { kubeConfig, api } = mockApi({
      getClusterCustomObject: vi.fn().mockResolvedValue(catalog([{ major: 5, image: 'fb:5.0.4' }], 'FirebirdClusterImageCatalog')),
      // the cluster exists (a reconcile of a running cluster checks it)
      getNamespacedCustomObject: vi.fn().mockResolvedValue(cluster),
    });
    await new FirebirdClusterController(kubeConfig).reconcile(cluster);
    expect(api('getClusterCustomObject')).toHaveBeenCalledWith(expect.objectContaining({ plural: 'firebirdclusterimagecatalogs', name: 'firebird' }));
    expect(reasons(api)).toContain('ImageCatalogChanged');
    expect(lastStatus(api).image).toBe('fb:5.0.4');
  });

  it('keeps a running cluster on its last image when the catalog is gone, and holds a new one', async () => {
    const cluster = {
      ...makeCluster({ imageCatalogRef: { name: 'firebird', major: 5 } }),
      status: { phase: 'Running' as const, image: 'fb:5.0.3' },
    };
    const running = mockApi({
      getNamespacedCustomObject: vi.fn().mockImplementation(({ plural }) => (plural === 'firebirdclusters' ? Promise.resolve(cluster) : Promise.reject(notFoundError))),
    });
    await new FirebirdClusterController(running.kubeConfig).reconcile(cluster);
    expect(reasons(running.api)).toContain('ImageCatalogUnresolved');
    expect(images(running.api('createNamespacedStatefulSet').mock.calls[0][0].body)).toBe('fb:5.0.3');

    const fresh = mockApi();
    await new FirebirdClusterController(fresh.kubeConfig).reconcile(makeCluster({ imageCatalogRef: { name: 'firebird', major: 5 } }));
    expect(fresh.api('createNamespacedStatefulSet')).not.toHaveBeenCalled();
    expect(lastStatus(fresh.api).conditions[0]).toMatchObject({ type: 'Ready', status: 'False', reason: 'ImageCatalogUnresolved' });
    expect(lastStatus(fresh.api).phaseReason).toBe('FirebirdImageCatalog firebird not found: the cluster waits for it');
  });

  it('records the image of a cluster without a catalog too', async () => {
    const { kubeConfig, api } = mockApi();
    await new FirebirdClusterController(kubeConfig).reconcile(makeCluster({ imageName: 'fb:4' }));
    expect(lastStatus(api).image).toBe('fb:4');
  });
});
