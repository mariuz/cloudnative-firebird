/** An image of a Firebird major version */
export interface CatalogImage {
  /** Firebird major version (e.g. 5) */
  major: number;
  /** Image of that major version (e.g. firebirdsql/firebird:5.0.3) */
  image: string;
}

/**
 * FirebirdImageCatalog (namespaced) and FirebirdClusterImageCatalog (cluster-wide), after
 * CloudNativePG's ImageCatalog and ClusterImageCatalog: the image of each Firebird major version,
 * for clusters that reference the catalog (spec.imageCatalogRef) instead of naming an image.
 */
export interface FirebirdImageCatalog {
  apiVersion: 'firebird.cloudnative-firebird.io/v1';
  kind: 'FirebirdImageCatalog' | 'FirebirdClusterImageCatalog';
  metadata: {
    name: string;
    namespace?: string;
    uid?: string;
    resourceVersion?: string;
    labels?: Record<string, string>;
    annotations?: Record<string, string>;
  };
  spec: {
    /** One image per major version */
    images: CatalogImage[];
  };
}
