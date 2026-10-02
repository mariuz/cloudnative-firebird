import { readFileSync } from 'fs';
import { join } from 'path';
import { V1Container, V1EnvVar, V1VolumeMount } from '@kubernetes/client-node';
import { FirebirdCluster } from '../types';

/**
 * Journal-based asynchronous replication for Firebird 4+.
 *
 * Every instance gets the same replication.conf; its role follows from database state:
 * - the bootstrap primary creates its database offline in the init container with
 *   publication enabled, keeps an offline bootstrap seed, journals changes and archives
 *   full segments;
 * - replicas are seeded with a physical copy (init-instance.sh), preferably of a ready
 *   replica, run in read-only replica mode and apply segments from journal_source_directory.
 * Segments are shipped by two sidecars running the Firebird image's perl: segment-server
 * serves the local archive (and seed copies) and segment-puller fetches new segments from
 * the current primary. The operator publishes the primary and the ready replicas (seed
 * sources) in the cluster ConfigMap. Seeds avoid locking the primary: see ISSUES.md, issue 2.
 */

/** Port of the segment server sidecar */
export const SEGMENT_PORT = 3051;

/** Mount path of the cluster ConfigMap (scripts and the current primary address) */
export const OPERATOR_CONFIG_DIR = '/etc/firebird-operator';

/** ConfigMap key holding the address of the current primary instance */
export const PRIMARY_KEY = 'primary';

/** ConfigMap key listing ready replicas that can serve seed copies, one host per line */
export const SEED_SOURCES_KEY = 'seed-sources';
/** ConfigMap key listing replicas to re-seed: "<pod> <token>" per line (token: the pod UID at request time) */
export const RESEED_KEY = 'reseed';
/** ConfigMap keys with planned switchover directives: "<pod> <token>" (see init-instance.sh) */
export const PROMOTE_KEY = 'promote';
export const DEMOTE_KEY = 'demote';
/** Pod annotation requesting that a replica discards its database and is seeded again */
export const RESEED_ANNOTATION = 'firebird.cloudnative-firebird.io/reseed';

const SCRIPT_DIR = join(__dirname, '..', 'replication');

/** Replication helper scripts shipped in the cluster ConfigMap, keyed by file name */
export const REPLICATION_SCRIPTS: Readonly<Record<string, string>> = Object.fromEntries(
  [
    'segment-server.pl',
    'segment-puller.pl',
    'fetch-seed.pl',
    'init-instance.sh',
    'replica-control.pl',
    'enable-publication.sql',
    'backup-file.pl',
    'set-repl-seq.pl',
    'switchover.pl',
    'failover.pl',
  ].map(
    (name) => [name, readFileSync(join(SCRIPT_DIR, name), 'utf8')],
  ),
);

/**
 * Scripts only Jobs run (journal archive, point-in-time recovery), shipped in every cluster's
 * ConfigMap. Not part of any pod template hash: changing them restarts no instance.
 */
export const JOB_SCRIPTS: Readonly<Record<string, string>> = Object.fromEntries(
  ['fetch-segments.pl', 'pitr-plan.pl', 'pitr-restore.sh'].map((name) => [
    name,
    readFileSync(join(SCRIPT_DIR, name), 'utf8'),
  ]),
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

/** Hours unapplied segments are kept at most (maxSegmentRetentionHours, never below segmentRetentionHours) */
export function maxSegmentRetentionHours(cluster: FirebirdCluster): number {
  const replication = cluster.spec.replication;
  return Math.max(replication?.maxSegmentRetentionHours ?? 168, replication?.segmentRetentionHours ?? 24);
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
    { name: 'REPLICATION_DIR', value: dirs.base },
    { name: 'DATABASE_PATH', value: databasePath },
    { name: 'JOURNAL_DIR', value: dirs.journal },
    { name: 'ARCHIVE_DIR', value: dirs.archive },
    { name: 'SOURCE_DIR', value: dirs.source },
    { name: 'STATE_FILE', value: dirs.state },
    { name: 'PRIMARY_FILE', value: `${OPERATOR_CONFIG_DIR}/${PRIMARY_KEY}` },
    { name: 'SEED_SOURCES_FILE', value: `${OPERATOR_CONFIG_DIR}/${SEED_SOURCES_KEY}` },
    { name: 'RESEED_FILE', value: `${OPERATOR_CONFIG_DIR}/${RESEED_KEY}` },
    { name: 'PROMOTE_FILE', value: `${OPERATOR_CONFIG_DIR}/${PROMOTE_KEY}` },
    { name: 'DEMOTE_FILE', value: `${OPERATOR_CONFIG_DIR}/${DEMOTE_KEY}` },
    { name: 'ALLOW_LIVE_SEED', value: String(cluster.spec.replication?.allowLiveSeedFromPrimary !== false) },
    { name: 'SCRIPT_DIR', value: OPERATOR_CONFIG_DIR },
    { name: 'SEGMENT_PORT', value: String(SEGMENT_PORT) },
    {
      name: 'SEGMENT_RETENTION_SECONDS',
      value: String((cluster.spec.replication?.segmentRetentionHours ?? 24) * 3600),
    },
    {
      name: 'SEGMENT_MAX_RETENTION_SECONDS',
      value: String(maxSegmentRetentionHours(cluster) * 3600),
    },
    // only when enabled, so the instance pods of other clusters do not change
    ...(cluster.spec.replication?.pruneAppliedSegments
      ? [
          { name: 'PRUNE_APPLIED', value: 'true' },
          ...(cluster.spec.replication.journalArchiveS3 ? [{ name: 'ARCHIVE_UPLOAD', value: 'true' }] : []),
        ]
      : []),
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
      command: ['sh', `${OPERATOR_CONFIG_DIR}/init-instance.sh`],
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
    // the database is created by the init container, so the entrypoint never runs initdb scripts
    mainMounts: [confMount],
  };
}
