import { V1ConfigMap, V1Job } from '@kubernetes/client-node';
import { API_GROUP, DEFAULT_FIREBIRD_IMAGE, FirebirdCluster, RESOURCE_KIND } from '../types';
import { clusterLabels, databaseName, FIREBIRD_DATA_DIR, jobPodSpec, superuserClientEnv } from './resources';
import { instanceHost } from './replication';

/**
 * Users to drop from instances that were not ready when their FirebirdUser was deleted
 * (reclaimPolicy "delete"), so they would otherwise keep the user in their security database.
 *
 * The ConfigMap "<cluster>-pending-user-drops" holds one key per instance pod, with one line per
 * user: "<USERNAME> <ISO time recorded>". The instance's security-db-init container drops them
 * from the security database on its next start, before the server accepts connections; once the
 * instance is ready the operator drops them again through a Job (the instance may have become
 * ready without a restart) and removes the entries.
 */

/** Where the security-db-init container finds the pending drops (optional ConfigMap volume) */
export const PENDING_DROPS_DIR = '/etc/firebird-pending-drops';

/** Job label naming the instance a pending-drop Job works on */
export const PENDING_DROP_POD_LABEL = `${API_GROUP}/pending-drop-pod`;

/** Job annotation listing the users a pending-drop Job drops */
export const PENDING_DROP_USERS_ANNOTATION = `${API_GROUP}/pending-drop-users`;

/** Regular Firebird user name (the only kind FirebirdUser creates) */
const USERNAME = /^[A-Z][A-Z0-9_$]{0,62}$/;

export interface PendingDrop {
  username: string;
  /** When the drop was recorded */
  since: string;
}

export function pendingDropsConfigMapName(clusterName: string): string {
  return `${clusterName}-pending-user-drops`;
}

/** Parses one instance's entry; lines that are not valid entries are ignored */
export function parsePendingDrops(value: string | undefined): PendingDrop[] {
  const drops: PendingDrop[] = [];
  for (const line of (value ?? '').split('\n')) {
    const [username, since] = line.trim().split(/\s+/);
    if (username && USERNAME.test(username) && !drops.some((d) => d.username === username)) {
      drops.push({ username, since: since ?? '' });
    }
  }
  return drops;
}

export function formatPendingDrops(drops: PendingDrop[]): string {
  return drops
    .slice()
    .sort((a, b) => a.username.localeCompare(b.username))
    .map((d) => `${d.username} ${d.since}\n`)
    .join('');
}

/** Adds users to instances' entries (existing entries keep their time) */
export function addPendingDrops(
  data: Record<string, string>,
  pods: string[],
  username: string,
  now: string,
): Record<string, string> {
  const next = { ...data };
  for (const pod of pods) {
    const drops = parsePendingDrops(next[pod]);
    if (!drops.some((d) => d.username === username)) drops.push({ username, since: now });
    next[pod] = formatPendingDrops(drops);
  }
  return next;
}

/** The ConfigMap holding the pending drops, owned by the cluster */
export function buildPendingDropsConfigMap(cluster: FirebirdCluster, data: Record<string, string>): V1ConfigMap {
  const { name, namespace = 'default', uid } = cluster.metadata;
  return {
    apiVersion: 'v1',
    kind: 'ConfigMap',
    metadata: {
      name: pendingDropsConfigMapName(name),
      namespace,
      labels: clusterLabels(name),
      ownerReferences: [
        { apiVersion: `${API_GROUP}/v1`, kind: RESOURCE_KIND, name, uid: uid ?? '', controller: true, blockOwnerDeletion: true },
      ],
    },
    data,
  };
}

/** SQL dropping a user if it exists (usernames are validated regular identifiers) */
export function dropIfExistsSql(username: string): string {
  return (
    `EXECUTE BLOCK AS BEGIN IF (EXISTS(SELECT 1 FROM SEC$USERS WHERE SEC$USER_NAME = '${username}')) THEN ` +
    `EXECUTE STATEMENT 'DROP USER ${username}'; END^`
  );
}

/**
 * Shell run by the security-db-init container (after the security database exists): drops the
 * instance's pending users, embedded, before the server starts. The security database is reached
 * through an alias of the container's own databases.conf that makes it its own security database.
 */
export function pendingDropsInitScript(securityDbPath: string): string {
  return [
    `f="${PENDING_DROPS_DIR}/$POD_NAME"`,
    'if [ -s "$f" ]; then',
    `  printf '%s\\n' 'pending_drops_security = ${securityDbPath}' '{' '    SecurityDatabase = ${securityDbPath}' '}' >> /opt/firebird/databases.conf`,
    '  while read -r u _; do',
    // only regular user names, as recorded by the operator
    '    case "$u" in ""|*[!A-Z0-9_$]*) continue ;; esac',
    `    if printf '%s\\n' 'SET TERM ^;' "EXECUTE BLOCK AS BEGIN IF (EXISTS(SELECT 1 FROM SEC\\$USERS WHERE SEC\\$USER_NAME = '$u')) THEN EXECUTE STATEMENT 'DROP USER $u'; END^" 'SET TERM ;^' 'COMMIT;' | isql -q -b -user SYSDBA pending_drops_security; then`,
    '      echo "pending drop of user $u applied to the security database"',
    '    else',
    '      echo "pending drop: could not drop user $u (retried once the instance is ready)"',
    '    fi',
    '  done < "$f"',
    'fi',
  ].join('\n');
}

/** Name of the Job dropping an instance's pending users */
export function pendingDropJobName(pod: string): string {
  return `drop-users-${pod}`.slice(0, 52);
}

/**
 * Job dropping an instance's pending users through its server, once it is ready. Without
 * replication their privileges in the instance's own database are revoked too (a replica's
 * database is read-only and receives the primary's revokes).
 */
export function buildPendingDropJob(cluster: FirebirdCluster, pod: string, usernames: string[], revoke: boolean): V1Job {
  const { name, namespace = 'default', uid } = cluster.metadata;
  const labels = { ...clusterLabels(name), 'app.kubernetes.io/component': 'pending-user-drops', [PENDING_DROP_POD_LABEL]: pod };
  const sql = [
    ...(revoke ? usernames.map((u) => `REVOKE ALL ON ALL FROM USER ${u};`) : []),
    ...(revoke ? ['COMMIT;'] : []),
    'SET TERM ^;',
    ...usernames.map(dropIfExistsSql),
    'SET TERM ;^',
    'COMMIT;',
  ].join('\n');
  return {
    apiVersion: 'batch/v1',
    kind: 'Job',
    metadata: {
      name: pendingDropJobName(pod),
      namespace,
      labels,
      annotations: { [PENDING_DROP_USERS_ANNOTATION]: usernames.join(' ') },
      ownerReferences: [
        { apiVersion: `${API_GROUP}/v1`, kind: RESOURCE_KIND, name, uid: uid ?? '', controller: true, blockOwnerDeletion: true },
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
              name: 'drop-users',
              image: cluster.spec.imageName ?? DEFAULT_FIREBIRD_IMAGE,
              command: ['/bin/sh', '-c'],
              args: [
                'set -eu; printf \'%s\\n\' "$DROP_SQL" | isql -q -b "$FIREBIRD_HOST:$DATABASE_PATH"; ' +
                  'echo "pending users dropped on $FIREBIRD_HOST: $USERS"',
              ],
              env: [
                ...superuserClientEnv(cluster),
                { name: 'FIREBIRD_HOST', value: instanceHost(cluster, pod) },
                { name: 'DATABASE_PATH', value: `${FIREBIRD_DATA_DIR}/${databaseName(cluster)}` },
                { name: 'USERS', value: usernames.join(' ') },
                { name: 'DROP_SQL', value: sql },
              ],
            },
          ],
        }),
      },
    },
  };
}
