import { clusterImage } from './image-catalog';
import { V1Job } from '@kubernetes/client-node';
import { API_GROUP, FirebirdCluster, FirebirdDatabase } from '../types';
import { clusterLabels, databaseName, FIREBIRD_DATA_DIR, jobPodSpec, superuserClientEnv } from './resources';
import { instanceHost } from './replication';
import { ValidationError } from './validation';
import { boundedJobName } from './volume-snapshot';

/**
 * Declarative databases (FirebirdDatabase, CloudNativePG's Database): more database files in a
 * cluster without replication. A Job creates the database on every ready instance over the
 * network (CREATE DATABASE as SYSDBA, then initSql), adopting a file that exists already; the
 * status records the instances and volumes it is on, so a replaced volume gets it again. With
 * reclaimPolicy "delete" a finalizer drops it from every instance when the resource is deleted.
 *
 * Replicated clusters are refused: replication, seeding, switchover, failover, backups and
 * point-in-time recovery cover the one cluster database only, so another database would exist on
 * the primary alone and be lost at the first failover.
 */

/** Holds a FirebirdDatabase with reclaimPolicy "delete" until it is dropped */
export const DATABASE_FINALIZER = `${API_GROUP}/drop-database`;

export const DATABASE_JOB_ACTION_LABEL = `${API_GROUP}/database-action`;
export const DATABASE_JOB_TARGETS_ANNOTATION = `${API_GROUP}/database-targets`;

const FILE_NAME = /^[A-Za-z0-9][A-Za-z0-9._-]*$/;
const CHARACTER_SET = /^[A-Za-z][A-Za-z0-9_]*$/;
export const PAGE_SIZES = [4096, 8192, 16384, 32768];

/** The database file a FirebirdDatabase manages */
export function managedDatabaseName(database: FirebirdDatabase): string {
  return database.spec.databaseName ?? `${database.metadata.name}.fdb`;
}

export function managedDatabasePath(database: FirebirdDatabase): string {
  return `${FIREBIRD_DATA_DIR}/${managedDatabaseName(database)}`;
}

export function databaseJobName(database: FirebirdDatabase): string {
  return boundedJobName('database', database.metadata.name);
}

/** Checks a FirebirdDatabase on its own (the cluster is checked by databaseClusterError) */
export function validateDatabaseSpec(database: FirebirdDatabase): void {
  const spec = database.spec;
  if (!spec?.clusterName || spec.clusterName.trim() === '') throw new ValidationError('FirebirdDatabase clusterName is required');
  const file = managedDatabaseName(database);
  if (!FILE_NAME.test(file) || file.length > 253) {
    throw new ValidationError(`Invalid databaseName "${file}": a file name (letters, digits, ".", "_", "-"; set spec.databaseName)`);
  }
  if (file.toLowerCase() === 'security.fdb' || /^security\d*\.fdb$/i.test(file)) {
    throw new ValidationError(`databaseName ${file} is reserved for the security database`);
  }
  if (spec.pageSize !== undefined && !PAGE_SIZES.includes(spec.pageSize)) {
    throw new ValidationError(`Invalid pageSize ${spec.pageSize}: one of ${PAGE_SIZES.join(', ')}`);
  }
  if (spec.characterSet !== undefined && !CHARACTER_SET.test(spec.characterSet)) {
    throw new ValidationError(`Invalid characterSet "${spec.characterSet}": a character set name such as UTF8 or WIN1252`);
  }
  if (spec.reclaimPolicy && !['retain', 'delete'].includes(spec.reclaimPolicy)) {
    throw new ValidationError(`Invalid reclaimPolicy "${spec.reclaimPolicy}": must be "retain" or "delete"`);
  }
}

/**
 * Why the database cannot be managed on this cluster: replication, the cluster's own database, or
 * a file another (older) FirebirdDatabase of the cluster manages
 */
export function databaseClusterError(database: FirebirdDatabase, cluster: FirebirdCluster, others: FirebirdDatabase[]): string | undefined {
  if (cluster.spec.replication?.enabled) {
    return (
      `cluster ${cluster.metadata.name} uses replication, which covers its database ${databaseName(cluster)} only: ` +
      'another database would exist on the primary alone and be lost at a failover'
    );
  }
  const file = managedDatabaseName(database);
  if (file === databaseName(cluster)) return `${file} is the cluster database (spec.databaseName)`;
  const created = (d: FirebirdDatabase) => new Date(d.metadata.creationTimestamp ?? 0).getTime();
  const owner = others
    .filter((o) => o.metadata.name !== database.metadata.name && o.spec.clusterName === database.spec.clusterName)
    .filter((o) => managedDatabaseName(o) === file && !o.metadata.deletionTimestamp)
    .find((o) => created(o) < created(database) || (created(o) === created(database) && o.metadata.name < database.metadata.name));
  return owner ? `${file} is managed by FirebirdDatabase ${owner.metadata.name}` : undefined;
}

/**
 * Shell creating the database on each host of $HOSTS (adopting an existing one: no initSql; a
 * failed initSql drops the new database, so the retry runs it again), or
 * dropping it from each (a host without it is skipped). Values come from the environment, never
 * from the shell text: initSql is piped to isql as it is.
 */
export function databaseJobScript(action: 'create' | 'drop'): string {
  const exists = `exists() { echo 'SELECT 1 FROM RDB$DATABASE;' | isql -q "$1:$DATABASE_PATH" >/dev/null 2>&1; }`;
  return action === 'create'
    ? [
        'set -eu',
        exists,
        'for h in $HOSTS; do',
        '  if exists "$h"; then echo "$DATABASE_PATH exists on $h: adopted"; continue; fi',
        `  printf '%s\\n' "CREATE DATABASE '$h:$DATABASE_PATH'\${PAGE_SIZE:+ PAGE_SIZE $PAGE_SIZE} DEFAULT CHARACTER SET $CHARACTER_SET;" 'COMMIT;' | isql -q -b`,
        // a failed initSql drops the new database: the retry creates it again (an existing file is adopted)
        '  if [ -n "${INIT_SQL:-}" ] && ! printf \'%s\\n\' "$INIT_SQL" | isql -q -b "$h:$DATABASE_PATH"; then',
        `    echo 'DROP DATABASE;' | isql -q "$h:$DATABASE_PATH" >/dev/null 2>&1 || true`,
        '    echo "initSql failed on $h: the new database was dropped" >&2; exit 1',
        '  fi',
        '  echo "$DATABASE_PATH created on $h"',
        'done',
      ].join('\n')
    : [
        'set -eu',
        exists,
        'for h in $HOSTS; do',
        '  if ! exists "$h"; then echo "$DATABASE_PATH not on $h"; continue; fi',
        `  echo 'DROP DATABASE;' | isql -q -b "$h:$DATABASE_PATH"`,
        '  echo "$DATABASE_PATH dropped on $h"',
        'done',
      ].join('\n');
}

/** Job creating or dropping the database on these instances */
export function buildDatabaseJob(
  cluster: FirebirdCluster,
  database: FirebirdDatabase,
  action: 'create' | 'drop',
  targets: Array<{ name: string; volume: string }>,
): V1Job {
  const { namespace = 'default' } = database.metadata;
  const labels = { ...clusterLabels(cluster.metadata.name), 'app.kubernetes.io/component': 'database-resource', [DATABASE_JOB_ACTION_LABEL]: action };
  const spec = database.spec;
  return {
    apiVersion: 'batch/v1',
    kind: 'Job',
    metadata: {
      name: databaseJobName(database),
      namespace,
      labels,
      annotations: { [DATABASE_JOB_TARGETS_ANNOTATION]: JSON.stringify(targets) },
      ownerReferences: [
        {
          apiVersion: `${API_GROUP}/v1`,
          kind: 'FirebirdDatabase',
          name: database.metadata.name,
          uid: database.metadata.uid ?? '',
          controller: true,
          // the drop Job must survive while the FirebirdDatabase is being deleted
          blockOwnerDeletion: false,
        },
      ],
    },
    spec: {
      backoffLimit: 1,
      ttlSecondsAfterFinished: 3600,
      template: {
        metadata: { labels },
        spec: jobPodSpec(cluster, {
          restartPolicy: 'Never',
          containers: [
            {
              name: 'database',
              image: clusterImage(cluster),
              command: ['/bin/sh', '-c'],
              args: [databaseJobScript(action)],
              env: [
                ...superuserClientEnv(cluster),
                { name: 'DATABASE_PATH', value: managedDatabasePath(database) },
                { name: 'HOSTS', value: targets.map((t) => instanceHost(cluster, t.name)).join(' ') },
                ...(action === 'create'
                  ? [
                      ...(spec.pageSize ? [{ name: 'PAGE_SIZE', value: String(spec.pageSize) }] : []),
                      { name: 'CHARACTER_SET', value: (spec.characterSet ?? 'UTF8').toUpperCase() },
                      ...(spec.initSql ? [{ name: 'INIT_SQL', value: spec.initSql }] : []),
                    ]
                  : []),
              ],
            },
          ],
        }),
      },
    },
  };
}
