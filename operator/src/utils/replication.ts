import { readFileSync } from 'fs';
import { join } from 'path';
import { V1Container, V1EnvVar, V1VolumeMount } from '@kubernetes/client-node';
import { FirebirdCluster } from '../types';

/**
 * Journal-based asynchronous replication for Firebird 4+.
 *
 * Every instance gets the same replication.conf; its role follows from database state:
 * - the bootstrap primary creates the database with publication enabled, journals changes
 *   and archives full segments;
 * - replicas are seeded with a physical copy of the primary (seed-replica.sh), run in
 *   read-only replica mode and apply segments from journal_source_directory.
 * Segments are shipped by two sidecars running the Firebird image's perl: segment-server
 * serves the local archive (and seed copies) and segment-puller fetches new segments from
 * the current primary, whose address the operator publishes in the cluster ConfigMap.
 */

/** Port of the segment server sidecar */
export const SEGMENT_PORT = 3051;

/** Mount path of the cluster ConfigMap (scripts and the current primary address) */
export const OPERATOR_CONFIG_DIR = '/etc/firebird-operator';

/** ConfigMap key holding the address of the current primary instance */
export const PRIMARY_KEY = 'primary';

const SCRIPT_DIR = join(__dirname, '..', 'replication');

/** Replication helper scripts shipped in the cluster ConfigMap, keyed by file name */
export const REPLICATION_SCRIPTS: Readonly<Record<string, string>> = Object.fromEntries(
  ['segment-server.pl', 'segment-puller.pl', 'fetch-seed.pl', 'seed-replica.sh', 'enable-publication.sql'].map(
    (name) => [name, readFileSync(join(SCRIPT_DIR, name), 'utf8')],
  ),
);

/** Returns true when journal replication is configured for the cluster */
export function replicationEnabled(cluster: FirebirdCluster): boolean {
  return Boolean(cluster.spec.replication?.enabled);
}

/** Directories used by replication, derived from spec.replication.journalDirectory */
export function replicationDirectories(cluster: FirebirdCluster, dataDir: string) {
  const base = cluster.spec.replication?.journalDirectory ?? `${dataDir}/replication`;
  return {
    base,
    journal: `${base}/journal`,
    archive: `${base}/archive`,
    source: `${base}/source`,
    state: `${base}/.last-pulled`,
  };
}

/** Stable DNS name of an instance through the headless Service */
export function instanceHost(cluster: FirebirdCluster, podName: string): string {
  return `${podName}.${cluster.metadata.name}-headless`;
}

/**
 * Builds replication.conf. The same file is used on every instance: primary settings
 * (journal and archive) only take effect while publication is enabled, and replica settings
 * (journal_source_directory) only while the database is in replica mode.
 */
export function buildReplicationConf(cluster: FirebirdCluster, databasePath: string, dataDir: string): string {
  const dirs = replicationDirectories(cluster, dataDir);
  const archiveTimeout = cluster.spec.replication?.archiveTimeoutSeconds ?? 10;
  return [
    'database',
    '{',
    '}',
    '',
    `database = ${databasePath}`,
    '{',
    `    journal_directory = ${dirs.journal}`,
    `    journal_archive_directory = ${dirs.archive}`,
    '    journal_archive_command = "test ! -f $(archivepathname) && cp $(pathname) $(archivepathname)"',
    `    journal_archive_timeout = ${archiveTimeout}`,
    `    journal_source_directory = ${dirs.source}`,
    '    apply_idle_timeout = 5',
    '}',
    '',
  ].join('\n');
}

/** Environment shared by the replication init container and sidecars */
function replicationEnv(
  cluster: FirebirdCluster,
  databasePath: string,
  dataDir: string,
  credentials: V1EnvVar[],
): V1EnvVar[] {
  const dirs = replicationDirectories(cluster, dataDir);
  return [
    ...credentials,
    { name: 'POD_NAME', valueFrom: { fieldRef: { fieldPath: 'metadata.name' } } },
    { name: 'DATA_DIR', value: dataDir },
    { name: 'DATABASE_PATH', value: databasePath },
    { name: 'JOURNAL_DIR', value: dirs.journal },
    { name: 'ARCHIVE_DIR', value: dirs.archive },
    { name: 'SOURCE_DIR', value: dirs.source },
    { name: 'STATE_FILE', value: dirs.state },
    { name: 'PRIMARY_FILE', value: `${OPERATOR_CONFIG_DIR}/${PRIMARY_KEY}` },
    { name: 'SCRIPT_DIR', value: OPERATOR_CONFIG_DIR },
    { name: 'SEGMENT_PORT', value: String(SEGMENT_PORT) },
    {
      name: 'SEGMENT_RETENTION_SECONDS',
      value: String((cluster.spec.replication?.segmentRetentionHours ?? 24) * 3600),
    },
  ];
}

/**
 * Builds the replication init container, sidecars and mounts for the instance pod.
 * The data volume and cluster ConfigMap volume are provided by the StatefulSet.
 */
export function buildReplicationContainers(
  cluster: FirebirdCluster,
  options: { image: string; databasePath: string; dataDir: string; credentials: V1EnvVar[] },
): { initContainer: V1Container; sidecars: V1Container[]; mainMounts: V1VolumeMount[] } {
  const env = replicationEnv(cluster, options.databasePath, options.dataDir, options.credentials);
  const mounts: V1VolumeMount[] = [
    { name: 'firebird-data', mountPath: options.dataDir },
    { name: 'cluster-config', mountPath: OPERATOR_CONFIG_DIR, readOnly: true },
  ];
  const confMount: V1VolumeMount = {
    name: 'cluster-config',
    mountPath: '/opt/firebird/replication.conf',
    subPath: 'replication.conf',
  };

  return {
    initContainer: {
      name: 'replication-init',
      image: options.image,
      command: ['sh', `${OPERATOR_CONFIG_DIR}/seed-replica.sh`],
      env,
      volumeMounts: [...mounts, confMount],
    },
    sidecars: [
      {
        name: 'segment-server',
        image: options.image,
        command: ['perl', `${OPERATOR_CONFIG_DIR}/segment-server.pl`],
        ports: [{ name: 'segments', containerPort: SEGMENT_PORT, protocol: 'TCP' }],
        env,
        volumeMounts: [...mounts, confMount],
      },
      {
        name: 'segment-puller',
        image: options.image,
        command: ['perl', `${OPERATOR_CONFIG_DIR}/segment-puller.pl`],
        env,
        volumeMounts: mounts,
      },
    ],
    mainMounts: [
      confMount,
      {
        name: 'cluster-config',
        mountPath: '/docker-entrypoint-initdb.d/00-enable-publication.sql',
        subPath: 'enable-publication.sql',
      },
    ],
  };
}
