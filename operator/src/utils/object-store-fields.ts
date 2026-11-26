import { AzureBlobConfiguration, FirebirdCluster, GCSConfiguration, PointInTimeRecovery, S3BackupConfiguration } from '../types';

/**
 * Where a resource configures its object store, resolved into the shape the call sites take
 * (docs/object-stores.md, phases 2 and 3). Types only are imported here: utils/replication uses
 * these, and must not load utils/backup (which loads utils/replication: a module of the cycle
 * loaded first would see the other's constants undefined, as a Job's config mount once did).
 */

/**
 * A store of any kind, as the call sites take it (phase 2): an S3 configuration as it is, or an
 * Azure Blob or GCS one in its shape, with `store` set and the agent's client (the bucket is the
 * container; region, encryption and clientImage are S3's only and never set on them)
 */
export interface ObjectStoreConfiguration extends S3BackupConfiguration {
  /** The kind of store: s3 when unset */
  store?: 's3' | 'azure' | 'gcs';
  /** Azure: the storage account */
  storageAccount?: string;
}

/** Where a resource configures its store: s3, or one of its siblings */
export interface ObjectStoreFields {
  s3?: S3BackupConfiguration;
  azureBlob?: AzureBlobConfiguration;
  gcs?: GCSConfiguration;
}

/** The fields of a store's resource that are set (more than one is refused by the webhook) */
export function objectStoreFieldsSet(fields: ObjectStoreFields | undefined): ('s3' | 'azureBlob' | 'gcs')[] {
  return (['s3', 'azureBlob', 'gcs'] as const).filter((f) => fields?.[f] !== undefined && fields[f] !== null);
}

export function azureBlobStore(azure: AzureBlobConfiguration): ObjectStoreConfiguration {
  return {
    store: 'azure',
    client: 'agent',
    storageAccount: azure.storageAccount,
    bucket: azure.container,
    ...(azure.prefix !== undefined ? { prefix: azure.prefix } : {}),
    ...(azure.endpoint !== undefined ? { endpoint: azure.endpoint } : {}),
    ...(azure.secretRef ? { secretRef: azure.secretRef } : {}),
  };
}

export function gcsStore(gcs: GCSConfiguration): ObjectStoreConfiguration {
  return {
    store: 'gcs',
    client: 'agent',
    bucket: gcs.bucket,
    ...(gcs.prefix !== undefined ? { prefix: gcs.prefix } : {}),
    ...(gcs.endpoint !== undefined ? { endpoint: gcs.endpoint } : {}),
    ...(gcs.secretRef ? { secretRef: gcs.secretRef } : {}),
  };
}

/** The store a resource configures (s3 first, as the webhook allows one only), or none */
export function objectStoreOf(fields: ObjectStoreFields | undefined): ObjectStoreConfiguration | undefined {
  if (fields?.s3) return fields.s3;
  if (fields?.azureBlob) return azureBlobStore(fields.azureBlob);
  if (fields?.gcs) return gcsStore(fields.gcs);
  return undefined;
}

/** The cluster's journal archive (phase 3): journalArchiveS3, journalArchiveAzureBlob or journalArchiveGCS */
export function journalArchiveOf(cluster: FirebirdCluster): ObjectStoreConfiguration | undefined {
  const r = cluster.spec.replication;
  return r ? objectStoreOf({ s3: r.journalArchiveS3, azureBlob: r.journalArchiveAzureBlob, gcs: r.journalArchiveGCS }) : undefined;
}

/** The source's journal archive a replica cluster follows (phase 4): journalS3, journalAzureBlob or journalGCS */
export function replicaJournalOf(cluster: FirebirdCluster): ObjectStoreConfiguration | undefined {
  const r = cluster.spec.replica;
  return r ? objectStoreOf({ s3: r.journalS3, azureBlob: r.journalAzureBlob, gcs: r.journalGCS }) : undefined;
}

/** The journal archive a point-in-time recovery names: journalS3, journalAzureBlob or journalGCS */
export function pointInTimeJournalOf(pitr: PointInTimeRecovery | undefined): ObjectStoreConfiguration | undefined {
  return pitr ? objectStoreOf({ s3: pitr.journalS3, azureBlob: pitr.journalAzureBlob, gcs: pitr.journalGCS }) : undefined;
}

/** Whether two stores are the same place: kind, account, bucket or container, and prefix */
export function sameObjectStoreLocation(a: S3BackupConfiguration, b: S3BackupConfiguration): boolean {
  return (
    storeKind(a) === storeKind(b) &&
    (a as ObjectStoreConfiguration).storageAccount === (b as ObjectStoreConfiguration).storageAccount &&
    a.bucket === b.bucket &&
    objectStorePrefix(a) === objectStorePrefix(b)
  );
}

/** The kind of a store */
export function storeKind(s3: S3BackupConfiguration): 's3' | 'azure' | 'gcs' {
  return (s3 as ObjectStoreConfiguration).store ?? 's3';
}

/** The prefix without its slashes, as the agent joins keys to it */
export function objectStorePrefix(s3: S3BackupConfiguration): string {
  return (s3.prefix ?? '').replace(/^\/+|\/+$/g, '');
}
