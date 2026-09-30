import {
  V1Container,
  V1CronJob,
  V1EnvVar,
  V1Job,
  V1OwnerReference,
  V1PodSpec,
  V1Volume,
  V1VolumeMount,
} from '@kubernetes/client-node';
import {
  API_GROUP,
  DEFAULT_FIREBIRD_IMAGE,
  FirebirdBackup,
  FirebirdCluster,
  FirebirdRestore,
  FirebirdScheduledBackup,
  RESOURCE_KIND,
  S3BackupConfiguration,
} from '../types';
import {
  clusterLabels,
  databaseName,
  FIREBIRD_DATA_DIR,
  jobPodSpec,
  superuserClientEnv,
  withTemplateHash,
} from './resources';
import {
  instanceHost,
  OPERATOR_CONFIG_DIR,
  PRIMARY_KEY,
  replicationEnabled,
  SEGMENT_PORT,
} from './replication';

/**
 * Backups and restores run as Jobs that reach the primary over the network; nothing mounts an
 * instance volume (it is ReadWriteOnce and belongs to the running instance).
 *
 * - Without S3, the primary's server writes the backup through the service manager
 *   (`gbak -se` / `nbackup` service actions) into its own data directory. The service manager
 *   cannot create directories, so backup files sit next to the database file.
 * - With S3, a logical backup is streamed to the Job pod by `gbak`; a physical backup is written
 *   by the primary's server into its data directory as usual, then copied to the Job pod through
 *   the primary's segment server (so it needs replication) and removed from the volume. A separate
 *   S3 client container uploads it. The Firebird image ships no S3 client.
 */

/** Image providing the `aws` CLI for S3 uploads and downloads */
export const DEFAULT_S3_CLIENT_IMAGE = 'amazon/aws-cli:2.37.4';

/** Scratch directory shared by the containers of a backup or restore pod */
const WORK_DIR = '/work';

/** Shell expression for a UTC timestamp used in scheduled backup file names */
const TIMESTAMP = '$(date -u +%Y%m%dT%H%M%SZ)';

export type BackupType = 'logical' | 'physical';

/** A backup source resolved for a restore */
export interface BackupSource {
  type: BackupType;
  /** Backup file: relative to the data directory (or absolute within it), or the S3 object key relative to s3.prefix */
  path: string;
  /** Further nbackup files (levels 1, 2) applied on top of `path` by a physical restore */
  incrementalPaths?: string[];
  s3?: S3BackupConfiguration;
}

/** Prefix for S3 object keys, with a trailing slash when set */
export function s3KeyPrefix(s3: S3BackupConfiguration): string {
  return s3.prefix ? `${s3.prefix.replace(/^\/+|\/+$/g, '')}/` : '';
}

/** `s3://bucket/prefix/key` */
export function s3Uri(s3: S3BackupConfiguration, key: string): string {
  return `s3://${s3.bucket}/${s3KeyPrefix(s3)}${key}`;
}

/** Accepted retentionPolicy values (CloudNativePG's format): days, weeks or months (30 days) */
export const RETENTION_POLICY_PATTERN = /^([1-9][0-9]*)([dwm])$/;

/** Seconds covered by a retentionPolicy such as "30d", "4w" or "6m" */
export function retentionSeconds(policy: string): number {
  const m = RETENTION_POLICY_PATTERN.exec(policy.trim());
  if (!m) throw new Error(`invalid retentionPolicy "${policy}": use <n>d, <n>w or <n>m`);
  return Number(m[1]) * { d: 1, w: 7, m: 30 }[m[2] as 'd' | 'w' | 'm'] * 86400;
}

/**
 * Shell writing the primary's nbackup history ("<level> <file name>" per backup, oldest first) to
 * a file. A level N backup is based on the latest earlier level N-1 backup of the database, of
 * any schedule (nbackup looks it up there), so the history gives every backup's chain.
 */
export function nbackupHistoryScript(out: string): string {
  return (
    `echo 'set list on; select rdb$backup_level, rdb$file_name from rdb$backup_history order by rdb$backup_id;' | ` +
    `isql -q "$FIREBIRD_HOST:$DATABASE_PATH" | ` +
    `awk '/^RDB[$]BACKUP_LEVEL/ { l = $2 } /^RDB[$]FILE_NAME/ { n = $2; sub(/^.*[/]/, "", n); print l, n }' > ${out}`
  );
}

/**
 * awk program printing the expired nbackup files that no kept backup depends on. Input files: the
 * history (see nbackupHistoryScript), every file name in the location, and the series; variables
 * c (cutoff), n (newest) and f (the new backup).
 *
 * Every backup in the history that is not expired is kept, with its whole chain, unless it is gone:
 * missing from the location while its schedule has files there (a schedule always keeps its newest
 * backup, so that is where it stores them). Backups of other locations, taken on demand or by hand
 * count as kept. Files missing from the history are never deleted (their dependants are unknown).
 */
export const NBACKUP_EXPIRED_AWK =
  'function series(x) { if (sub(/^nbackup-l[0-2]-/, "", x) && sub(/-[0-9]+T[0-9]+Z[.]nbk$/, "", x)) return x; return "" } ' +
  'FILENAME == ARGV[1] { h++; lvl[h] = $1; name[h] = $2; next } ' +
  'FILENAME == ARGV[2] { here[$0] = 1; s = series($0); if (s != "") live[s] = 1; next } ' +
  '$0 != n && $0 != f { ts = $0; sub(/^.*-/, "", ts); sub(/[.]nbk$/, "", ts); if (ts < c) old[$0] = 1 } ' +
  'END { for (i = 1; i <= h; i++) { l = lvl[i]; par[i] = (l > 0 && (l - 1) in last) ? last[l - 1] : 0; last[l] = i; known[name[i]] = 1 } ' +
  'for (i = 1; i <= h; i++) { if (name[i] in old) continue; s = series(name[i]); if (!(name[i] in here) && s != "" && (s in live)) continue; ' +
  'for (p = par[i]; p; p = par[p]) needed[name[p]] = 1 } ' +
  'for (k in old) if ((k in known) && !(k in needed)) print k }';

/**
 * Shell that deletes the backups of a scheduled series older than the retention window, after a
 * new backup ($f): "backup-<series>-<timestamp>.fbk", or with `history` (a file written by
 * nbackupHistoryScript) "nbackup-l<level>-<series>-<timestamp>.nbk", keeping every file a kept
 * backup's chain needs. The newest backup of the series and $f are always kept, so a stopped
 * schedule never loses its last backup. Timestamps are compared as strings (fixed-width UTC);
 * other series and other files are never touched. `list` prints the file names, `remove` deletes
 * "$k", `tmp` is a writable directory.
 */
function retentionScript(o: {
  list: string;
  remove: string;
  tmp: string;
  series: string;
  seconds: number;
  history?: string;
}): string {
  const series = o.series.replace(/[.]/g, '[.]');
  const re = o.history
    ? `^nbackup-l[0-2]-${series}-[0-9]{8}T[0-9]{6}Z[.]nbk$`
    : `^backup-${series}-[0-9]{8}T[0-9]{6}Z[.]fbk$`;
  const expired = o.history
    ? `awk -v c="$cutoff" -v n="$newest" -v f="$f" '${NBACKUP_EXPIRED_AWK}' ${o.history} ${o.tmp}/all ${o.tmp}/series`
    : `awk -v c="$cutoff" -v n="$newest" -v f="$f" '$0 != n && $0 != f { ts = $0; sub(/^.*-/, "", ts); sub(/[.]fbk$/, "", ts); if (ts < c) print }' ${o.tmp}/series`;
  return (
    `cutoff=$(date -u -d "@$(( $(date +%s) - ${o.seconds} ))" +%Y%m%dT%H%M%SZ); ` +
    // a failed listing deletes nothing (and does not fail the backup)
    `{ ${o.list}; } > ${o.tmp}/all || true; grep -E ${shellQuote(re)} ${o.tmp}/all | sort > ${o.tmp}/series || true; ` +
    `newest=$(tail -n 1 ${o.tmp}/series); ` +
    `${expired} > ${o.tmp}/expired; ` +
    `while read -r k; do ${o.remove}; echo "retention: deleted $k"; done < ${o.tmp}/expired; ` +
    `echo "retention: kept $(( $(wc -l < ${o.tmp}/series) - $(wc -l < ${o.tmp}/expired) )) backup(s) of ${o.series} newer than $cutoff, newest${o.history ? ' or needed by a kept chain' : ''}"`
  );
}

/**
 * Retention of a backup series in S3 (after the upload); `history` for nbackup series
 */
export function s3RetentionScript(s3: S3BackupConfiguration, series: string, seconds: number, history?: string): string {
  const dir = `s3://${s3.bucket}/${s3KeyPrefix(s3)}`;
  return retentionScript({
    list: `${awsCommand(s3)} s3 ls ${shellQuote(dir)} | awk '{ print $4 }'`,
    remove: `${awsCommand(s3)} s3 rm ${shellQuote(dir)}"$k"`,
    tmp: WORK_DIR,
    series,
    seconds,
    history,
  });
}

/**
 * Retention of a backup series in the primary's data directory, listed and deleted through its
 * segment server (clusters with replication); nbackup series read the chains from the history
 */
export function serverSideRetentionScript(series: string, seconds: number, physical = false): string {
  return (
    'rt=$(mktemp -d); ' +
    (physical ? `${nbackupHistoryScript('$rt/history')}; ` : '') +
    retentionScript({
      list: `${BACKUP_FILE} list`,
      remove: `${BACKUP_FILE} remove "$k" >/dev/null`,
      tmp: '$rt',
      series,
      seconds,
      history: physical ? '$rt/history' : undefined,
    }) +
    '; rm -rf "$rt"'
  );
}

/** Shell-quotes a value for /bin/sh */
export function shellQuote(value: string): string {
  return `'${value.replace(/'/g, `'\\''`)}'`;
}

function awsCommand(s3: S3BackupConfiguration): string {
  return s3.endpoint ? `aws --endpoint-url ${shellQuote(s3.endpoint)}` : 'aws';
}

/**
 * Environment for the S3 client container: static keys from secretRef, or none, so that the aws
 * CLI uses the pod's own credentials (workload identity via the service account, instance profile)
 */
export function s3ClientEnv(s3: S3BackupConfiguration): V1EnvVar[] {
  const keys: V1EnvVar[] = s3.secretRef
    ? [
        {
          name: 'AWS_ACCESS_KEY_ID',
          valueFrom: { secretKeyRef: { name: s3.secretRef.name, key: 'AWS_ACCESS_KEY_ID' } },
        },
        {
          name: 'AWS_SECRET_ACCESS_KEY',
          valueFrom: { secretKeyRef: { name: s3.secretRef.name, key: 'AWS_SECRET_ACCESS_KEY' } },
        },
      ]
    : [];
  return [...keys, { name: 'AWS_DEFAULT_REGION', value: s3.region ?? 'us-east-1' }];
}

function s3ClientImage(s3: S3BackupConfiguration): string {
  return s3.clientImage ?? DEFAULT_S3_CLIENT_IMAGE;
}

/** Resolves a server-side backup path against the data directory */
export function serverPath(path: string): string {
  return path.startsWith('/') ? path : `${FIREBIRD_DATA_DIR}/${path}`;
}

function ownerReference(kind: string, name: string, uid: string | undefined): V1OwnerReference {
  return {
    apiVersion: `${API_GROUP}/v1`,
    kind,
    name,
    uid: uid ?? '',
    controller: true,
    blockOwnerDeletion: true,
  };
}

const workVolume: V1Volume = { name: 'work', emptyDir: {} };
const workMount: V1VolumeMount = { name: 'work', mountPath: WORK_DIR };

/** The cluster ConfigMap, which ships backup-file.pl (replication clusters only) */
function configVolume(cluster: FirebirdCluster): V1Volume {
  return { name: 'cluster-config', configMap: { name: `${cluster.metadata.name}-config` } };
}
const configMount: V1VolumeMount = { name: 'cluster-config', mountPath: OPERATOR_CONFIG_DIR, readOnly: true };

/** Shell command moving nbackup files through the primary's segment server (see backup-file.pl) */
const BACKUP_FILE = `perl ${OPERATOR_CONFIG_DIR}/backup-file.pl`;

/** Physical backups and restores with S3 copy nbackup files through the segment server */
export function physicalS3NeedsReplication(cluster: FirebirdCluster, what: string): string | undefined {
  return replicationEnabled(cluster)
    ? undefined
    : `${what}: physical backups to and restores from S3 copy the nbackup file through the primary's ` +
        `segment server; enable spec.replication on cluster ${cluster.metadata.name}`;
}

/**
 * Pod spec that takes one backup of the primary. `fileName` may contain shell expressions
 * (a timestamp for scheduled backups); it is evaluated once, when the backup starts.
 */
export function buildBackupPodSpec(
  cluster: FirebirdCluster,
  options: {
    primaryHost: string;
    type: BackupType;
    level?: number;
    fileName: string;
    s3?: S3BackupConfiguration;
    /** Scheduled series and retentionPolicy: expired S3 objects of the series are deleted after the upload */
    retention?: { series: string; policy?: string };
    /** Restore the backup into a scratch database and validate it (logical backups) */
    verify?: boolean;
  },
): V1PodSpec {
  const verify = Boolean(options.verify) && options.type !== 'physical';
  const image = cluster.spec.imageName ?? DEFAULT_FIREBIRD_IMAGE;
  const env: V1EnvVar[] = [
    ...superuserClientEnv(cluster),
    { name: 'FIREBIRD_HOST', value: options.primaryHost },
    { name: 'DATABASE_PATH', value: `${FIREBIRD_DATA_DIR}/${databaseName(cluster)}` },
  ];

  if (!options.s3) {
    const action =
      options.type === 'physical'
        ? `action_nbak dbname "$DATABASE_PATH" nbk_file "${FIREBIRD_DATA_DIR}/$f" nbk_level ${options.level ?? 0}`
        : `action_backup dbname "$DATABASE_PATH" bkp_file "${FIREBIRD_DATA_DIR}/$f"`;
    // expired backups are deleted through the primary's segment server (replication only)
    const retention =
      options.retention?.policy && replicationEnabled(cluster)
        ? { series: options.retention.series, seconds: retentionSeconds(options.retention.policy) }
        : undefined;
    return jobPodSpec(cluster, {
      restartPolicy: 'Never',
      containers: [
        {
          name: 'firebird-backup',
          image,
          command: ['/bin/sh', '-c'],
          args: [
            `set -eu; f="${options.fileName}"; ` +
              `fbsvcmgr "$FIREBIRD_HOST:service_mgr" ${action}; ` +
              `echo "backup written to ${FIREBIRD_DATA_DIR}/$f on $FIREBIRD_HOST"` +
              (verify ? `; ${serverSideVerifyScript()}` : '') +
              (retention ? `; ${serverSideRetentionScript(retention.series, retention.seconds, options.type === 'physical')}` : ''),
          ],
          env: retention ? [...env, { name: 'SEGMENT_PORT', value: String(SEGMENT_PORT) }] : env,
          ...(retention ? { volumeMounts: [configMount] } : {}),
        },
      ],
      ...(retention ? { volumes: [configVolume(cluster)] } : {}),
    });
  }

  const s3 = options.s3;
  const physical = options.type === 'physical';
  return jobPodSpec(cluster, {
    restartPolicy: 'Never',
    initContainers: [
      physical
        ? {
            // written next to the database by the primary's server, copied here, then removed
            // from the volume whether or not the copy succeeded
            name: 'firebird-backup',
            image,
            command: ['/bin/sh', '-c'],
            args: [
              `set -eu; f="${options.fileName}"; trap '${BACKUP_FILE} remove "$f" || true' EXIT; ` +
                `fbsvcmgr "$FIREBIRD_HOST:service_mgr" action_nbak dbname "$DATABASE_PATH" nbk_file "${FIREBIRD_DATA_DIR}/$f" nbk_level ${options.level ?? 0}; ` +
                `${BACKUP_FILE} get "$f" "${WORK_DIR}/$f"; echo "$f" > ${WORK_DIR}/.name` +
                // the chains, for retention in the upload container (no Firebird client there)
                (options.retention?.policy ? `; ${nbackupHistoryScript(`${WORK_DIR}/history`)}` : ''),
            ],
            env: [...env, { name: 'SEGMENT_PORT', value: String(SEGMENT_PORT) }],
            volumeMounts: [workMount, configMount],
          }
        : {
            // logical backup streamed to this pod
            name: 'firebird-backup',
            image,
            command: ['/bin/sh', '-c'],
            args: [
              `set -eu; f="${options.fileName}"; ` +
                `gbak -b "$FIREBIRD_HOST:$DATABASE_PATH" "${WORK_DIR}/$f"; echo "$f" > ${WORK_DIR}/.name`,
            ],
            env,
            volumeMounts: [workMount],
          },
      // a backup that does not restore and validate is never uploaded
      ...(verify
        ? [
            {
              name: 'verify',
              image,
              command: ['/bin/sh', '-c'],
              args: [LOCAL_VERIFY_SCRIPT],
              env,
              volumeMounts: [workMount],
            },
          ]
        : []),
    ],
    containers: [
      {
        name: 'upload',
        image: s3ClientImage(s3),
        command: ['/bin/sh', '-c'],
        args: [
          `set -eu; f=$(cat ${WORK_DIR}/.name); ` +
            `${awsCommand(s3)} s3 cp "${WORK_DIR}/$f" "s3://${s3.bucket}/${s3KeyPrefix(s3)}$f"` +
            (options.retention?.policy
              ? `; ${s3RetentionScript(
                  s3,
                  options.retention.series,
                  retentionSeconds(options.retention.policy),
                  physical ? `${WORK_DIR}/history` : undefined,
                )}`
              : ''),
        ],
        env: s3ClientEnv(s3),
        volumeMounts: [workMount],
      },
    ],
    volumes: physical ? [workVolume, configVolume(cluster)] : [workVolume],
  });
}

/**
 * Restores the backup in the Job's work volume into a scratch database with the embedded engine and
 * validates it (gfix -v -full prints nothing for a sound database)
 */
const LOCAL_VERIFY_SCRIPT =
  `set -eu; f=$(cat ${WORK_DIR}/.name); v=${WORK_DIR}/verify.fdb; ` +
  `gbak -c "${WORK_DIR}/$f" "$v"; ` +
  `out=$(gfix -v -full "$v" 2>&1) || { echo "$out"; echo "backup $f does not validate"; exit 1; }; ` +
  `if [ -n "$out" ]; then echo "$out"; echo "backup $f does not validate"; exit 1; fi; ` +
  `rm -f "$v"; echo "backup $f restored and validated"`;

/**
 * Restores a backup kept on the server into a scratch database next to it, validates it online and
 * drops it again ($f is the backup file name)
 */
function serverSideVerifyScript(): string {
  return (
    `v="${FIREBIRD_DATA_DIR}/.verify-\${f%.*}.fdb"; set +e; ` +
    `fbsvcmgr "$FIREBIRD_HOST:service_mgr" action_restore bkp_file "${FIREBIRD_DATA_DIR}/$f" dbname "$v" res_replace; restored=$?; ` +
    `if [ $restored -eq 0 ]; then out=$(fbsvcmgr "$FIREBIRD_HOST:service_mgr" action_validate dbname "$v" 2>&1); valid=$?; echo "$out"; fi; ` +
    `echo "drop database;" | isql -q "$FIREBIRD_HOST:$v" >/dev/null 2>&1 || echo "note: could not drop the scratch database $v"; ` +
    `if [ $restored -ne 0 ]; then echo "backup $f does not restore"; exit 1; fi; ` +
    `if [ $valid -ne 0 ] || echo "$out" | grep -qi 'errors found'; then echo "backup $f does not validate"; exit 1; fi; ` +
    `echo "backup $f restored and validated"`
  );
}

/** Where a backup with a fixed file name ends up */
export function backupLocation(fileName: string, s3?: S3BackupConfiguration): string {
  return s3 ? s3Uri(s3, fileName) : `${FIREBIRD_DATA_DIR}/${fileName}`;
}

/** File name of the backup taken for a FirebirdBackup */
export function onDemandBackupFileName(backup: FirebirdBackup): string {
  return backup.spec.type === 'physical'
    ? `nbackup-l${backup.spec.level ?? 0}-${backup.metadata.name}.nbk`
    : `backup-${backup.metadata.name}.fbk`;
}

function scheduledFileName(prefix: string, type: BackupType, level?: number): string {
  return type === 'physical'
    ? `nbackup-l${level ?? 0}-${prefix}-${TIMESTAMP}.nbk`
    : `backup-${prefix}-${TIMESTAMP}.fbk`;
}

function cronJob(
  metadata: V1CronJob['metadata'],
  schedule: string,
  labels: Record<string, string>,
  podSpec: V1PodSpec,
  suspend?: boolean,
): V1CronJob {
  return withTemplateHash({
    apiVersion: 'batch/v1',
    kind: 'CronJob',
    metadata,
    spec: {
      schedule,
      ...(suspend !== undefined ? { suspend } : {}),
      concurrencyPolicy: 'Forbid',
      successfulJobsHistoryLimit: 3,
      failedJobsHistoryLimit: 1,
      jobTemplate: {
        spec: {
          backoffLimit: 2,
          template: { metadata: { labels }, spec: podSpec },
        },
      },
    },
  });
}

/**
 * Builds the CronJob for the cluster's `spec.backup` schedule.
 */
export function buildBackupCronJob(cluster: FirebirdCluster, primaryPod?: string): V1CronJob {
  const { name, namespace = 'default' } = cluster.metadata;
  const backup = cluster.spec.backup;
  const type = backup?.type ?? 'logical';
  const labels = { ...clusterLabels(name), 'app.kubernetes.io/component': 'backup' };
  return cronJob(
    {
      name: `${name}-backup`,
      namespace,
      labels,
      ownerReferences: [ownerReference(RESOURCE_KIND, name, cluster.metadata.uid)],
    },
    backup?.schedule ?? '0 2 * * *',
    labels,
    buildBackupPodSpec(cluster, {
      primaryHost: instanceHost(cluster, primaryPod ?? `${name}-0`),
      type,
      level: backup?.level,
      fileName: scheduledFileName(name, type, backup?.level),
      s3: backup?.s3,
      retention: { series: name, policy: backup?.retentionPolicy },
      verify: backup?.verify,
    }),
  );
}

/**
 * Builds the CronJob for a FirebirdScheduledBackup.
 */
export function buildScheduledBackupCronJob(
  scheduledBackup: FirebirdScheduledBackup,
  cluster: FirebirdCluster,
  primaryPod?: string,
): V1CronJob {
  const { name: sbName, namespace = 'default', uid } = scheduledBackup.metadata;
  const spec = scheduledBackup.spec;
  const type = spec.type ?? 'logical';
  const labels = { ...clusterLabels(spec.clusterName), 'app.kubernetes.io/component': 'scheduled-backup' };
  return cronJob(
    {
      name: `sched-backup-${sbName}`,
      namespace,
      labels,
      ownerReferences: [ownerReference('FirebirdScheduledBackup', sbName, uid)],
    },
    spec.schedule,
    labels,
    buildBackupPodSpec(cluster, {
      primaryHost: instanceHost(cluster, primaryPod ?? `${spec.clusterName}-0`),
      type,
      level: spec.level,
      fileName: scheduledFileName(sbName, type, spec.level),
      s3: spec.s3,
      retention: { series: sbName, policy: spec.retentionPolicy },
      verify: spec.verify,
    }),
    (spec.suspend ?? false) || Boolean(cluster.spec.hibernated),
  );
}

/**
 * Builds the Job for an on-demand FirebirdBackup.
 */
export function buildBackupJob(backup: FirebirdBackup, cluster: FirebirdCluster, primaryPod?: string): V1Job {
  const { name: backupName, namespace = 'default', uid } = backup.metadata;
  const clusterName = backup.spec.clusterName;
  const labels = { ...clusterLabels(clusterName), 'app.kubernetes.io/component': 'on-demand-backup' };
  return {
    apiVersion: 'batch/v1',
    kind: 'Job',
    metadata: {
      name: `backup-${backupName}`,
      namespace,
      labels,
      ownerReferences: [ownerReference('FirebirdBackup', backupName, uid)],
    },
    spec: {
      backoffLimit: 2,
      template: {
        metadata: { labels },
        spec: buildBackupPodSpec(cluster, {
          primaryHost: instanceHost(cluster, primaryPod ?? `${clusterName}-0`),
          type: backup.spec.type ?? 'logical',
          level: backup.spec.level,
          fileName: onDemandBackupFileName(backup),
          s3: backup.spec.s3,
          verify: backup.spec.verify,
        }),
      },
    },
  };
}

/** Target database file of a FirebirdRestore */
export function restoreTargetDatabase(restore: FirebirdRestore): string {
  return restore.spec.targetDatabase ?? `restore-${restore.metadata.name}.fdb`;
}

/**
 * Builds the Job for a FirebirdRestore. The backup is restored into a new database file
 * (`targetDatabase`) on the primary; the cluster database is never overwritten.
 */
export function buildRestoreJob(
  restore: FirebirdRestore,
  cluster: FirebirdCluster,
  source: BackupSource,
  primaryPod?: string,
): V1Job {
  const { name: restoreName, namespace = 'default', uid } = restore.metadata;
  const clusterName = restore.spec.clusterName;
  const image = cluster.spec.imageName ?? DEFAULT_FIREBIRD_IMAGE;
  const labels = { ...clusterLabels(clusterName), 'app.kubernetes.io/component': 'restore' };
  const env: V1EnvVar[] = [
    ...superuserClientEnv(cluster),
    { name: 'FIREBIRD_HOST', value: instanceHost(cluster, primaryPod ?? `${clusterName}-0`) },
    { name: 'TARGET_PATH', value: `${FIREBIRD_DATA_DIR}/${restoreTargetDatabase(restore)}` },
  ];

  let podSpec: V1PodSpec;
  if (source.s3 && source.type === 'physical') {
    // nbackup files are downloaded here, copied next to the database through the primary's segment
    // server for the server to restore them, then removed from the volume again
    const s3 = source.s3;
    const keys = [source.path, ...(source.incrementalPaths ?? [])];
    const serverNames = keys.map((_, i) => `restore-${restoreName}-${i}.nbk`);
    podSpec = jobPodSpec(cluster, {
      restartPolicy: 'Never',
      initContainers: [
        {
          name: 'download',
          image: s3ClientImage(s3),
          command: ['/bin/sh', '-c'],
          args: [
            'set -eu; ' +
              keys.map((k, i) => `${awsCommand(s3)} s3 cp ${shellQuote(s3Uri(s3, k))} ${WORK_DIR}/${serverNames[i]}`).join('; '),
          ],
          env: s3ClientEnv(s3),
          volumeMounts: [workMount],
        },
      ],
      containers: [
        {
          name: 'firebird-restore',
          image,
          command: ['/bin/sh', '-c'],
          args: [
            `set -eu; trap '${BACKUP_FILE} remove ${serverNames.join(' ')} || true' EXIT; ` +
              serverNames.map((n) => `${BACKUP_FILE} put ${WORK_DIR}/${n} ${n}; `).join('') +
              physicalRestoreScript(serverNames.map((n) => `"${FIREBIRD_DATA_DIR}/${n}"`)),
          ],
          env: [...env, { name: 'SEGMENT_PORT', value: String(SEGMENT_PORT) }],
          volumeMounts: [workMount, configMount],
        },
      ],
      volumes: [workVolume, configVolume(cluster)],
    });
  } else if (source.s3) {
    const s3 = source.s3;
    podSpec = jobPodSpec(cluster, {
      restartPolicy: 'Never',
      initContainers: [
        {
          name: 'download',
          image: s3ClientImage(s3),
          command: ['/bin/sh', '-c'],
          args: [`set -eu; ${awsCommand(s3)} s3 cp ${shellQuote(s3Uri(s3, source.path))} ${WORK_DIR}/backup.fbk`],
          env: s3ClientEnv(s3),
          volumeMounts: [workMount],
        },
      ],
      containers: [
        {
          name: 'firebird-restore',
          image,
          command: ['/bin/sh', '-c'],
          args: [`set -eu; gbak -c ${WORK_DIR}/backup.fbk "$FIREBIRD_HOST:$TARGET_PATH"; echo "restored into $TARGET_PATH"`],
          env,
          volumeMounts: [workMount],
        },
      ],
      volumes: [workVolume],
    });
  } else {
    const script =
      source.type === 'physical'
        ? physicalRestoreScript([source.path, ...(source.incrementalPaths ?? [])].map((p) => shellQuote(serverPath(p))))
        : `fbsvcmgr "$FIREBIRD_HOST:service_mgr" action_restore bkp_file ${shellQuote(serverPath(source.path))} dbname "$TARGET_PATH"; ` +
          'echo "restored into $TARGET_PATH"';
    podSpec = jobPodSpec(cluster, {
      restartPolicy: 'Never',
      containers: [
        {
          name: 'firebird-restore',
          image,
          command: ['/bin/sh', '-c'],
          args: [`set -eu; ${script}`],
          env,
        },
      ],
    });
  }

  return {
    apiVersion: 'batch/v1',
    kind: 'Job',
    metadata: {
      name: `restore-${restoreName}`,
      namespace,
      labels,
      ownerReferences: [ownerReference('FirebirdRestore', restoreName, uid)],
    },
    spec: { backoffLimit: 2, template: { metadata: { labels }, spec: podSpec } },
  };
}

/**
 * Shell restoring an nbackup chain (server-side paths, already quoted) into $TARGET_PATH. A failed
 * restore leaves a partial database locked for backup merging (it expects a .delta file), which
 * blocks every retry with "File exists": it is fixed up (action_nfix) and dropped. A target that
 * existed before is never touched: nrest refuses it with "File exists" before writing anything.
 */
export function physicalRestoreScript(nbkFiles: string[]): string {
  return (
    `out=$(fbsvcmgr "$FIREBIRD_HOST:service_mgr" action_nrest dbname "$TARGET_PATH" ` +
    nbkFiles.map((f) => `nbk_file ${f}`).join(' ') +
    ' 2>&1) && rc=0 || rc=$?; [ -z "$out" ] || echo "$out"; ' +
    'if [ $rc -ne 0 ]; then ' +
    `if echo "$out" | grep -q 'File exists'; then echo "$TARGET_PATH already exists; left as is"; ` +
    'else fbsvcmgr "$FIREBIRD_HOST:service_mgr" action_nfix dbname "$TARGET_PATH" >/dev/null 2>&1 || true; ' +
    `if echo 'drop database;' | isql -q "$FIREBIRD_HOST:$TARGET_PATH" >/dev/null 2>&1; ` +
    'then echo "removed the partial restore $TARGET_PATH"; else echo "note: could not remove a partial restore at $TARGET_PATH"; fi; fi; ' +
    'exit $rc; fi; echo "restored into $TARGET_PATH"'
  );
}

/** Outcome of a Job from its status conditions */
export function jobOutcome(job: V1Job): 'Running' | 'Completed' | 'Failed' {
  const conditions = job.status?.conditions ?? [];
  if (conditions.some((c) => c.type === 'Complete' && c.status === 'True')) return 'Completed';
  if (conditions.some((c) => c.type === 'Failed' && c.status === 'True')) return 'Failed';
  return 'Running';
}

/**
 * Builds the CronJob that ships the primary's archived journal segments to S3 for PITR.
 * Segments are fetched from the primary's segment server (the archive lives on its volume);
 * only segments not yet in the bucket are fetched and uploaded.
 */
export function buildJournalArchiveCronJob(cluster: FirebirdCluster, primaryPod?: string): V1CronJob | null {
  const { name, namespace = 'default' } = cluster.metadata;
  const s3 = cluster.spec.replication?.journalArchiveS3;
  if (!replicationEnabled(cluster) || !s3) return null;

  const labels = { ...clusterLabels(name), 'app.kubernetes.io/component': 'journal-archive' };
  const journals = s3Uri(s3, 'journals/');
  const podSpec: V1PodSpec = jobPodSpec(cluster, {
    restartPolicy: 'Never',
    initContainers: [
      {
        name: 'list-uploaded',
        image: s3ClientImage(s3),
        command: ['/bin/sh', '-c'],
        // an empty or missing prefix lists nothing (and exits non-zero)
        args: [
          `${awsCommand(s3)} s3 ls ${shellQuote(journals)} > ${WORK_DIR}/listing || true; ` +
            `awk '{ print $4 }' ${WORK_DIR}/listing > ${WORK_DIR}/uploaded`,
        ],
        env: s3ClientEnv(s3),
        volumeMounts: [workMount],
      },
      {
        name: 'fetch-segments',
        image: cluster.spec.imageName ?? DEFAULT_FIREBIRD_IMAGE,
        command: ['perl', `${OPERATOR_CONFIG_DIR}/fetch-segments.pl`],
        env: [
          ...superuserClientEnv(cluster),
          { name: 'FIREBIRD_HOST', value: instanceHost(cluster, primaryPod ?? `${name}-0`) },
          { name: 'SEGMENT_PORT', value: String(SEGMENT_PORT) },
          { name: 'OUT_DIR', value: `${WORK_DIR}/segments` },
          { name: 'SKIP_FILE', value: `${WORK_DIR}/uploaded` },
        ],
        volumeMounts: [workMount, { name: 'cluster-config', mountPath: OPERATOR_CONFIG_DIR, readOnly: true }],
      },
    ],
    containers: [
      {
        name: 'upload',
        image: s3ClientImage(s3),
        command: ['/bin/sh', '-c'],
        // no --delete: the primary prunes segments locally after segmentRetentionHours, while
        // the object store keeps the full history for point-in-time recovery
        args: [`set -eu; ${awsCommand(s3)} s3 sync ${WORK_DIR}/segments/ ${shellQuote(journals)}`],
        env: s3ClientEnv(s3),
        volumeMounts: [workMount],
      },
    ],
    volumes: [workVolume, { name: 'cluster-config', configMap: { name: `${name}-config` } }],
  });

  return cronJob(
    {
      name: `${name}-journal-archive`,
      namespace,
      labels,
      ownerReferences: [ownerReference(RESOURCE_KIND, name, cluster.metadata.uid)],
    },
    cluster.spec.replication?.archiveSchedule ?? '*/15 * * * *',
    labels,
    podSpec,
  );
}

/**
 * Init containers that bootstrap a new cluster's database from a backup or another cluster.
 *
 * The database is restored to a temporary file and moved into place only when complete. With
 * replication, only the primary bootstraps: the restored file is left at `.bootstrap.fdb` for
 * the replication init container, which enables publication and keeps the offline seed; replicas
 * are then seeded by replication.
 */
export function buildBootstrapInitContainers(cluster: FirebirdCluster): V1Container[] {
  const bootstrap = cluster.spec.bootstrap;
  if (!bootstrap?.recovery && !bootstrap?.clone) return [];

  const image = cluster.spec.imageName ?? DEFAULT_FIREBIRD_IMAGE;
  const replicated = replicationEnabled(cluster);
  const target = replicated
    ? `${FIREBIRD_DATA_DIR}/.bootstrap.fdb`
    : `${FIREBIRD_DATA_DIR}/${databaseName(cluster)}`;
  const env: V1EnvVar[] = [
    { name: 'POD_NAME', valueFrom: { fieldRef: { fieldPath: 'metadata.name' } } },
    { name: 'DATABASE_PATH', value: `${FIREBIRD_DATA_DIR}/${databaseName(cluster)}` },
    { name: 'TARGET_PATH', value: target },
    ...(replicated ? [{ name: 'PRIMARY_FILE', value: `${OPERATOR_CONFIG_DIR}/${PRIMARY_KEY}` }] : []),
  ];
  const mounts: V1VolumeMount[] = [
    { name: 'firebird-data', mountPath: FIREBIRD_DATA_DIR },
    ...(replicated ? [{ name: 'cluster-config', mountPath: OPERATOR_CONFIG_DIR, readOnly: true }] : []),
  ];
  // skip when initialised already, or on a replica (replication seeds it)
  const guard =
    'set -eu; ' +
    'if [ -f "$DATABASE_PATH" ] || [ -f "$TARGET_PATH" ]; then echo "database exists, skipping bootstrap"; exit 0; fi; ' +
    'if [ -n "${PRIMARY_FILE:-}" ]; then p=$(cat "$PRIMARY_FILE" 2>/dev/null || true); ' +
    'case "$p" in ""|"$POD_NAME"|"$POD_NAME".*) ;; *) echo "replica: seeded by replication, skipping bootstrap"; exit 0 ;; esac; fi; ';
  const finish =
    'chown firebird:firebird "$TARGET_PATH.tmp"; mv "$TARGET_PATH.tmp" "$TARGET_PATH"; ';

  if (!bootstrap.recovery) {
    const clone = bootstrap.clone!;
    const sourceHost = clone.namespace ? `${clone.sourceCluster}.${clone.namespace}` : clone.sourceCluster;
    const secret = clone.superuserSecret ?? cluster.spec.superuserSecret;
    return [
      {
        name: 'bootstrap-clone',
        image,
        command: ['/bin/sh', '-c'],
        // gbak streams a logical backup of the source straight into a local restore
        args: [
          guard +
            'rm -f "$TARGET_PATH.tmp"; ' +
            'gbak -b "$SOURCE_HOST:$SOURCE_DATABASE" stdout | gbak -c stdin "$TARGET_PATH.tmp"; ' +
            finish +
            'echo "cloned $SOURCE_DATABASE from $SOURCE_HOST"',
        ],
        env: [
          ...env,
          { name: 'ISC_USER', value: 'SYSDBA' },
          secret
            ? { name: 'ISC_PASSWORD', valueFrom: { secretKeyRef: { name: secret.name, key: 'password' } } }
            : { name: 'ISC_PASSWORD', value: 'masterkey' },
          { name: 'SOURCE_HOST', value: sourceHost },
          {
            name: 'SOURCE_DATABASE',
            value: `${FIREBIRD_DATA_DIR}/${clone.databaseName ?? databaseName(cluster)}`,
          },
        ],
        volumeMounts: mounts,
      },
    ];
  }

  const recovery = bootstrap.recovery;
  const restore = (backupFile: string, extraMounts: V1VolumeMount[]): V1Container => ({
    name: 'bootstrap-restore',
    image,
    command: ['/bin/sh', '-c'],
    args: [
      guard +
        'rm -f "$TARGET_PATH.tmp"; ' +
        `gbak -c ${shellQuote(backupFile)} "$TARGET_PATH.tmp"; ` +
        finish +
        `echo "restored database from ${backupFile.replace(/"/g, '')}"`,
    ],
    env: [...env, ...superuserClientEnv(cluster)],
    volumeMounts: [...mounts, ...extraMounts],
  });

  if (recovery.s3) {
    const s3 = recovery.s3;
    const key = recovery.sourcePath ?? 'backup.fbk';
    return [
      {
        name: 'bootstrap-download',
        image: s3ClientImage(s3),
        command: ['/bin/sh', '-c'],
        args: [guard + `${awsCommand(s3)} s3 cp ${shellQuote(s3Uri(s3, key))} ${WORK_DIR}/backup.fbk`],
        env: [...env, ...s3ClientEnv(s3)],
        volumeMounts: [...mounts, workMount],
      },
      restore(`${WORK_DIR}/backup.fbk`, [workMount]),
    ];
  }
  return [restore(recovery.sourcePath ?? '', [])];
}

/** Volumes needed by the bootstrap init containers, beyond the data and config volumes */
export function bootstrapVolumes(cluster: FirebirdCluster): V1Volume[] {
  return cluster.spec.bootstrap?.recovery?.s3 ? [workVolume] : [];
}
