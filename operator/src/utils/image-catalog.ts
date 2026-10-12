import { DEFAULT_FIREBIRD_IMAGE, FirebirdCluster, FirebirdImageCatalog, ImageCatalogRef } from '../types';

/**
 * Image catalogs (CloudNativePG's ImageCatalog / ClusterImageCatalog and imageCatalogRef).
 *
 * A cluster with spec.imageCatalogRef runs the catalog's image for its major version. The cluster
 * reconcile resolves it first and continues with the image as if it were spec.imageName (in
 * memory, never written to the spec), so a changed catalog entry is rolled out like a new
 * imageName: the image check, then a rolling update or a major upgrade. The resolved image is
 * recorded in status.image, where the other controllers' Jobs read it (clusterImage).
 *
 * A catalog or entry that cannot be found leaves a running cluster on the image it resolved last
 * (status.image) and keeps a new cluster from starting; both report it.
 */

export const IMAGE_CATALOG_KIND = 'FirebirdImageCatalog';
export const CLUSTER_IMAGE_CATALOG_KIND = 'FirebirdClusterImageCatalog';

/** The catalog kind a reference names */
export function catalogKind(ref: ImageCatalogRef): 'FirebirdImageCatalog' | 'FirebirdClusterImageCatalog' {
  return ref.kind ?? IMAGE_CATALOG_KIND;
}

/** The plural of a catalog kind */
export function catalogPlural(ref: ImageCatalogRef): string {
  return catalogKind(ref) === CLUSTER_IMAGE_CATALOG_KIND ? 'firebirdclusterimagecatalogs' : 'firebirdimagecatalogs';
}

/** Errors of spec.imageCatalogRef */
export function imageCatalogRefErrors(cluster: FirebirdCluster): string[] {
  const ref = cluster.spec.imageCatalogRef;
  if (!ref) return [];
  const errors: string[] = [];
  if (cluster.spec.imageName) errors.push('spec.imageName and spec.imageCatalogRef are mutually exclusive');
  if (!ref.name || !/^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$/.test(ref.name)) errors.push('spec.imageCatalogRef.name must be the name of an image catalog');
  if (ref.kind !== undefined && ![IMAGE_CATALOG_KIND, CLUSTER_IMAGE_CATALOG_KIND].includes(ref.kind)) {
    errors.push(`spec.imageCatalogRef.kind must be ${IMAGE_CATALOG_KIND} or ${CLUSTER_IMAGE_CATALOG_KIND}`);
  }
  if (!Number.isInteger(ref.major) || ref.major < 1 || ref.major > 99) errors.push('spec.imageCatalogRef.major must be a Firebird major version (1-99)');
  return errors;
}

/** Errors of a catalog's spec: an image per entry, one entry per major version */
export function imageCatalogErrors(catalog: FirebirdImageCatalog): string[] {
  const errors: string[] = [];
  const images = catalog.spec?.images ?? [];
  if (images.length === 0) errors.push('spec.images must list at least one image');
  const seen = new Set<number>();
  for (const entry of images) {
    if (!Number.isInteger(entry.major) || entry.major < 1 || entry.major > 99) errors.push(`spec.images: invalid major ${entry.major}`);
    if (!entry.image || /\s/.test(entry.image)) errors.push(`spec.images: major ${entry.major} needs an image`);
    if (seen.has(entry.major)) errors.push(`spec.images: major ${entry.major} is listed twice`);
    seen.add(entry.major);
  }
  return errors;
}

/** The image a catalog gives the cluster's major version, or why there is none */
export function resolveCatalogImage(
  ref: ImageCatalogRef,
  catalog: FirebirdImageCatalog | undefined,
): { image: string; error?: undefined } | { image?: undefined; error: string } {
  const what = `${catalogKind(ref)} ${ref.name}`;
  if (!catalog) return { error: `${what} not found` };
  const errors = imageCatalogErrors(catalog);
  if (errors.length > 0) return { error: `${what} is invalid: ${errors.join('; ')}` };
  const entry = catalog.spec.images.find((i) => i.major === ref.major);
  if (!entry) return { error: `${what} has no image for Firebird ${ref.major}` };
  return { image: entry.image };
}

/** The cluster as the reconcile uses it: the catalog's image as spec.imageName (in memory only) */
export function withCatalogImage(cluster: FirebirdCluster, image: string): FirebirdCluster {
  return { ...cluster, spec: { ...cluster.spec, imageName: image } };
}

/**
 * The Firebird image of a cluster: spec.imageName (resolved from the catalog during the cluster
 * reconcile), else the image the cluster reconcile recorded (status.image: a catalog's for
 * resources reading the cluster from the API), else the operator default
 */
export function clusterImage(cluster: FirebirdCluster): string {
  if (cluster.spec.imageName) return cluster.spec.imageName;
  if (cluster.spec.imageCatalogRef && cluster.status?.image) return cluster.status.image;
  return DEFAULT_FIREBIRD_IMAGE;
}

/**
 * The key of a catalog (kind, namespace for FirebirdImageCatalog, name), as the operator's catalog
 * watches record catalogs and clusters reference them
 */
export function catalogKey(kind: string, namespace: string | undefined, name: string): string {
  return kind === CLUSTER_IMAGE_CATALOG_KIND ? `${kind}//${name}` : `${kind}/${namespace ?? 'default'}/${name}`;
}

/** The key of the catalog a cluster references, if it references one */
export function referencedCatalogKey(cluster: FirebirdCluster): string | undefined {
  const ref = cluster.spec?.imageCatalogRef;
  return ref ? catalogKey(catalogKind(ref), cluster.metadata.namespace, ref.name) : undefined;
}
