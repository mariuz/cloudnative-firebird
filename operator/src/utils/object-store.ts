import { V1Container, V1EnvVar, V1PodSpec } from '@kubernetes/client-node';
import { FirebirdCluster, S3BackupConfiguration } from '../types';
import { ObjectStoreConfiguration, objectStorePrefix as prefix, storeKind } from './object-store-fields';
import { AGENT_PATH } from './agent';
import { clusterImage } from './image-catalog';
import { shellQuote, sseCustomerKeyFile } from './backup';

/**
 * The object store clients (docs/object-stores.md, phase 1b): "aws", the aws CLI of
 * s3.clientImage (the default, its command lines as before), or "agent", `fb-agent objects` in
 * the instance image, over the store's own API (agent/fb-agent/src/objects.rs).
 *
 * The call sites keep their aws command lines as they were and take the agent's from here: keys
 * are relative to the configuration's prefix (OBJECT_PREFIX), and a listing the scripts parse is
 * `list --long`, laid out as `aws s3 ls` prints it.
 */

export * from './object-store-fields';

/** Whether the configuration's transfers run through `fb-agent objects` (always, off S3) */
export function agentClient(s3: S3BackupConfiguration): boolean {
  return s3.client === 'agent' || storeKind(s3) !== 's3';
}

/** `s3://bucket/prefix/key`, `azure://account/container/prefix/key` or `gs://bucket/prefix/key` */
export function objectUri(s3: S3BackupConfiguration, key: string): string {
  const path = `${s3.bucket}/${prefix(s3) ? `${prefix(s3)}/` : ''}${key}`;
  switch (storeKind(s3)) {
    case 'azure':
      return `azure://${(s3 as ObjectStoreConfiguration).storageAccount}/${path}`;
    case 'gcs':
      return `gs://${path}`;
    default:
      return `s3://${path}`;
  }
}

const GCS_KEY_DIR = '/var/run/gcs';

/** The pod volume holding a GCS service account key */
export function gcsKeyVolumeName(id: string): string {
  return `${id}-gcs-key`;
}

/** The file of the GCS service account key in the agent's containers */
export function gcsKeyFile(id = 's3'): string {
  return `${GCS_KEY_DIR}/${id}/key.json`;
}

/** The mount directory of the GCS key */
export function gcsKeyDir(id: string): string {
  return `${GCS_KEY_DIR}/${id}`;
}

const OBJECTS = `${AGENT_PATH} objects`;


/**
 * The agent's environment for the configuration: the store, its credentials (or none, for the
 * pod's identity), endpoint, region and encryption. `sseCustomerKey: false` for a container that
 * only lists (it has no key mounted, and a listing needs none).
 */
export function objectStoreEnv(s3: S3BackupConfiguration, id = 's3', options: { sseCustomerKey?: boolean } = {}): V1EnvVar[] {
  const kind = storeKind(s3);
  if (kind === 'azure') return azureEnv(s3 as ObjectStoreConfiguration);
  if (kind === 'gcs') return gcsEnv(s3, id);
  const keys: V1EnvVar[] = s3.secretRef
    ? [
        { name: 'AWS_ACCESS_KEY_ID', valueFrom: { secretKeyRef: { name: s3.secretRef.name, key: 'AWS_ACCESS_KEY_ID' } } },
        { name: 'AWS_SECRET_ACCESS_KEY', valueFrom: { secretKeyRef: { name: s3.secretRef.name, key: 'AWS_SECRET_ACCESS_KEY' } } },
      ]
    : [];
  return [
    { name: 'OBJECT_STORE', value: 's3' },
    { name: 'AWS_BUCKET', value: s3.bucket },
    ...(prefix(s3) ? [{ name: 'OBJECT_PREFIX', value: prefix(s3) }] : []),
    ...keys,
    { name: 'AWS_DEFAULT_REGION', value: s3.region ?? 'us-east-1' },
    // an endpoint of its own (MinIO, Ceph) is addressed by path, AWS's by the bucket's host
    // name, as the aws CLI does
    ...(s3.endpoint
      ? [{ name: 'AWS_ENDPOINT', value: s3.endpoint }]
      : [{ name: 'AWS_VIRTUAL_HOSTED_STYLE_REQUEST', value: 'true' }]),
    ...(s3.encryption ? [{ name: 'AWS_SERVER_SIDE_ENCRYPTION', value: s3.encryption }] : []),
    ...(s3.encryption === 'aws:kms' && s3.kmsKeyId ? [{ name: 'AWS_SSE_KMS_KEY_ID', value: s3.kmsKeyId }] : []),
    ...(s3.sseCustomerKey && options.sseCustomerKey !== false ? [{ name: 'OBJECT_SSE_C_KEY_FILE', value: sseCustomerKeyFile(id) }] : []),
  ];
}

/** The agent's environment for an Azure Blob container: the crate's AZURE_* variables */
function azureEnv(azure: ObjectStoreConfiguration): V1EnvVar[] {
  // either key of the Secret: an account key or a SAS token (the one missing is left unset)
  const optionalKey = (key: string): V1EnvVar => ({
    name: key,
    valueFrom: { secretKeyRef: { name: azure.secretRef!.name, key, optional: true } },
  });
  return [
    { name: 'OBJECT_STORE', value: 'azure' },
    { name: 'AZURE_STORAGE_ACCOUNT_NAME', value: azure.storageAccount ?? '' },
    { name: 'AZURE_CONTAINER_NAME', value: azure.bucket },
    ...(prefix(azure) ? [{ name: 'OBJECT_PREFIX', value: prefix(azure) }] : []),
    ...(azure.secretRef ? [optionalKey('AZURE_STORAGE_ACCOUNT_KEY'), optionalKey('AZURE_STORAGE_SAS_KEY')] : []),
    ...(azure.endpoint ? [{ name: 'AZURE_STORAGE_ENDPOINT', value: azure.endpoint }] : []),
  ];
}

/** The agent's environment for a GCS bucket: the crate's GOOGLE_* variables, the key as a file */
function gcsEnv(gcs: S3BackupConfiguration, id: string): V1EnvVar[] {
  return [
    { name: 'OBJECT_STORE', value: 'gcs' },
    { name: 'GOOGLE_BUCKET', value: gcs.bucket },
    ...(prefix(gcs) ? [{ name: 'OBJECT_PREFIX', value: prefix(gcs) }] : []),
    ...(gcs.secretRef ? [{ name: 'GOOGLE_SERVICE_ACCOUNT', value: gcsKeyFile(id) }] : []),
    ...(gcs.endpoint ? [{ name: 'GOOGLE_BASE_URL', value: gcs.endpoint }] : []),
  ];
}

/** The image of a container that runs the agent's client: the cluster's (the agent is mounted into it) */
export function objectStoreImage(cluster: FirebirdCluster): string {
  return clusterImage(cluster);
}

/**
 * The agent's command lines; `key` and `dir` are shell words (quoted literals, or with "$f" and
 * the like), a key relative to the configuration's prefix
 */
export const agentObjects = {
  put: (file: string, key: string) => `${OBJECTS} put ${file} ${key}`,
  get: (key: string, file: string) => `${OBJECTS} get ${key} ${file}`,
  /** the names under the prefix, one per line */
  list: (key: string) => `${OBJECTS} list ${key}`,
  /** the objects under the prefix as `aws s3 ls` lists them (date, time, size, name) */
  listLong: (key: string) => `${OBJECTS} list --long ${key}`,
  sync: (dir: string, key: string) => `${OBJECTS} sync ${dir} ${key}`,
  rm: (key: string) => `${OBJECTS} rm ${key}`,
  exists: (key: string) => `${OBJECTS} exists ${key}`,
  /** a literal key, quoted */
  key: (key: string) => shellQuote(key),
};

/** A container of the agent's client: the cluster's image, the configuration's environment */
export function agentClientContainer(
  cluster: FirebirdCluster,
  s3: S3BackupConfiguration,
  container: Omit<V1Container, 'image'> & { env?: V1EnvVar[] },
  id = 's3',
  options: { sseCustomerKey?: boolean } = {},
): V1Container {
  return { ...container, image: objectStoreImage(cluster), env: [...objectStoreEnv(s3, id, options), ...(container.env ?? [])] };
}

/**
 * A pod template's labels with AKS workload identity's (`azure.workload.identity/use`) when one
 * of its containers reaches an Azure container without a Secret: its webhook then injects the
 * identity's variables and token file (CloudNativePG's inheritFromAzureAD)
 */
export function withObjectStoreLabels(labels: Record<string, string>, podSpec: V1PodSpec): Record<string, string> {
  const azureIdentity = [...(podSpec.initContainers ?? []), ...podSpec.containers].some((c) => {
    const env = c.env ?? [];
    return (
      env.some((e) => e.name === 'OBJECT_STORE' && e.value === 'azure') &&
      !env.some((e) => e.name === 'AZURE_STORAGE_ACCOUNT_KEY' || e.name === 'AZURE_STORAGE_SAS_KEY')
    );
  });
  return azureIdentity ? { ...labels, 'azure.workload.identity/use': 'true' } : labels;
}
