import { V1Container, V1EnvVar } from '@kubernetes/client-node';
import { FirebirdCluster, S3BackupConfiguration } from '../types';
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

/** Whether the configuration's transfers run through `fb-agent objects` */
export function agentClient(s3: S3BackupConfiguration): boolean {
  return s3.client === 'agent';
}

const OBJECTS = `${AGENT_PATH} objects`;

/** The prefix without its slashes, as the agent joins keys to it */
function prefix(s3: S3BackupConfiguration): string {
  return (s3.prefix ?? '').replace(/^\/+|\/+$/g, '');
}

/**
 * The agent's environment for the configuration: the store, its credentials (or none, for the
 * pod's identity), endpoint, region and encryption. `sseCustomerKey: false` for a container that
 * only lists (it has no key mounted, and a listing needs none).
 */
export function objectStoreEnv(s3: S3BackupConfiguration, id = 's3', options: { sseCustomerKey?: boolean } = {}): V1EnvVar[] {
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
