import { createHash } from 'crypto';
import { V1Job } from '@kubernetes/client-node';
import { API_GROUP, DEFAULT_FIREBIRD_IMAGE, FirebirdCluster, FirebirdUser } from '../types';
import { clusterLabels, databaseName, FIREBIRD_DATA_DIR, superuserClientEnv, jobPodSpec } from './resources';
import { instanceHost } from './replication';
import { ValidationError } from './validation';

/**
 * Declarative Firebird users (FirebirdUser, after CloudNativePG's DatabaseRole).
 *
 * Firebird keeps users in each instance's security database, which journal replication does not
 * ship, so users are applied to every instance (CREATE OR ALTER USER through that instance's
 * server; this works on read-only replicas too). Role grants live in the cluster database: with
 * replication they are applied on the primary and replicated, otherwise on every instance (each
 * holds an independent database). The password is read from a Secret inside the Job and only
 * appears in the SQL text sent to the server.
 */

/** Finalizer that drops the Firebird user when a FirebirdUser with reclaimPolicy "delete" is deleted */
export const USER_FINALIZER = `${API_GROUP}/drop-user`;

/** Job annotations recording what a user Job applies */
export const USER_JOB_HASH_ANNOTATION = `${API_GROUP}/user-hash`;
export const USER_JOB_TARGETS_ANNOTATION = `${API_GROUP}/user-targets`;
export const USER_JOB_ACTION_LABEL = `${API_GROUP}/user-action`;

/** Regular (unquoted) Firebird identifier */
const IDENTIFIER = /^[A-Za-z][A-Za-z0-9_$]{0,62}$/;

/** Firebird user name: spec.username or the resource name, upper-cased as Firebird stores it */
export function firebirdUsername(user: FirebirdUser): string {
  return (user.spec.username ?? user.metadata.name.replace(/-/g, '_')).toUpperCase();
}

/** Granted roles, upper-cased, sorted and de-duplicated */
export function desiredRoles(user: FirebirdUser): string[] {
  return [...new Set((user.spec.roles ?? []).map((r) => r.toUpperCase()))].sort();
}

export function validateUserSpec(user: FirebirdUser): void {
  const spec = user.spec;
  if (!spec?.clusterName || spec.clusterName.trim() === '') {
    throw new ValidationError('FirebirdUser clusterName is required');
  }
  if (!spec.passwordSecret?.name) {
    throw new ValidationError('FirebirdUser passwordSecret.name is required');
  }
  const username = spec.username ?? user.metadata.name.replace(/-/g, '_');
  if (!IDENTIFIER.test(username)) {
    throw new ValidationError(
      `Invalid Firebird user name "${username}": letters, digits, "_" and "$", starting with a letter (set spec.username)`,
    );
  }
  if (username.toUpperCase() === 'SYSDBA') {
    throw new ValidationError('SYSDBA is managed through the cluster superuserSecret');
  }
  for (const role of spec.roles ?? []) {
    if (!IDENTIFIER.test(role)) throw new ValidationError(`Invalid role name "${role}"`);
    if (role.toUpperCase() === 'RDB$ADMIN') throw new ValidationError('Use spec.admin instead of the RDB$ADMIN role');
  }
  if (spec.reclaimPolicy && !['retain', 'delete'].includes(spec.reclaimPolicy)) {
    throw new ValidationError(`Invalid reclaimPolicy "${spec.reclaimPolicy}": must be "retain" or "delete"`);
  }
}

/**
 * Hash of everything a user Job applies. The password enters through the Secret's UID and
 * resourceVersion, so no password-derived value ends up in the status.
 */
export function userSpecHash(user: FirebirdUser, secret: { uid?: string; resourceVersion?: string }): string {
  const canonical = JSON.stringify({
    username: firebirdUsername(user),
    active: user.spec.active ?? true,
    admin: user.spec.admin ?? false,
    roles: desiredRoles(user),
    secret: `${secret.uid ?? ''}/${secret.resourceVersion ?? ''}/${user.spec.passwordSecret.key ?? 'password'}`,
  });
  return createHash('sha256').update(canonical).digest('hex').slice(0, 16);
}

/** Job name for a FirebirdUser (at most 52 characters, so pod names stay valid) */
export function userJobName(user: FirebirdUser): string {
  const name = `fbuser-${user.metadata.name}`;
  if (name.length <= 52) return name;
  const suffix = createHash('sha256').update(user.metadata.name).digest('hex').slice(0, 6);
  return `${name.slice(0, 45)}-${suffix}`;
}

/** SQL granting exactly the desired roles (revoking other role memberships) */
export function grantSql(username: string, roles: string[]): string {
  const keep = roles.length ? `r NOT IN (${roles.map((r) => `'${r}'`).join(', ')})` : '1 = 1';
  return [
    'SET TERM ^;',
    'EXECUTE BLOCK AS DECLARE r VARCHAR(63); BEGIN',
    '  FOR SELECT TRIM(RDB$RELATION_NAME) FROM RDB$USER_PRIVILEGES',
    `    WHERE RDB$USER = '${username}' AND RDB$PRIVILEGE = 'M' AND RDB$USER_TYPE = 8 INTO :r DO`,
    `    IF (${keep}) THEN EXECUTE STATEMENT 'REVOKE "' || r || '" FROM USER ${username}';`,
    'END^',
    'SET TERM ;^',
    ...roles.map((r) => `GRANT ${r} TO USER ${username};`),
    'COMMIT;',
  ].join('\n');
}

/** SQL dropping the user from an instance's security database if it exists */
export function dropUserSql(username: string): string {
  return [
    'SET TERM ^;',
    `EXECUTE BLOCK AS BEGIN IF (EXISTS(SELECT 1 FROM SEC$USERS WHERE SEC$USER_NAME = '${username}')) THEN`,
    `  EXECUTE STATEMENT 'DROP USER ${username}'; END^`,
    'SET TERM ;^',
    'COMMIT;',
  ].join('\n');
}

/**
 * Builds the Job that applies (or drops) a user on the given instances. `hosts` receive the user
 * in their security database; `grantHosts` receive the role grants in the cluster database.
 */
export function buildUserJob(
  cluster: FirebirdCluster,
  user: FirebirdUser,
  options: { action: 'apply' | 'drop'; instances: string[]; grantInstances: string[]; hash: string; targets: string },
): V1Job {
  const { namespace = 'default' } = user.metadata;
  const username = firebirdUsername(user);
  const labels = {
    ...clusterLabels(cluster.metadata.name),
    'app.kubernetes.io/component': 'user',
    [USER_JOB_ACTION_LABEL]: options.action,
  };
  const hosts = options.instances.map((pod) => instanceHost(cluster, pod)).join(' ');
  const grantHosts = options.grantInstances.map((pod) => instanceHost(cluster, pod)).join(' ');
  const userClause = `${user.spec.active === false ? 'INACTIVE' : 'ACTIVE'} ${user.spec.admin ? 'GRANT' : 'REVOKE'} ADMIN ROLE`;

  const script =
    options.action === 'apply'
      ? [
          'set -eu',
          // SQL string literal: double single quotes
          `pw=$(printf '%s' "$PASSWORD" | sed "s/'/''/g")`,
          'for h in $HOSTS; do',
          `  printf '%s\\n' "CREATE OR ALTER USER $FB_USER PASSWORD '$pw' $USER_CLAUSE;" 'COMMIT;' | isql -q -b "$h:$DATABASE_PATH"`,
          '  echo "user $FB_USER applied on $h"',
          'done',
          'for h in $GRANT_HOSTS; do',
          `  printf '%s\\n' "$GRANT_SQL" | isql -q -b "$h:$DATABASE_PATH"`,
          '  echo "roles of $FB_USER applied on $h"',
          'done',
        ].join('\n')
      : [
          'set -eu',
          'for h in $GRANT_HOSTS; do',
          `  printf '%s\\n' "REVOKE ALL ON ALL FROM USER $FB_USER;" 'COMMIT;' | isql -q -b "$h:$DATABASE_PATH"`,
          '  echo "privileges of $FB_USER revoked on $h"',
          'done',
          'for h in $HOSTS; do',
          `  printf '%s\\n' "$DROP_SQL" | isql -q -b "$h:$DATABASE_PATH"`,
          '  echo "user $FB_USER dropped on $h"',
          'done',
        ].join('\n');

  return {
    apiVersion: 'batch/v1',
    kind: 'Job',
    metadata: {
      name: userJobName(user),
      namespace,
      labels,
      annotations: {
        [USER_JOB_HASH_ANNOTATION]: options.hash,
        [USER_JOB_TARGETS_ANNOTATION]: options.targets,
      },
      ownerReferences: [
        {
          apiVersion: `${API_GROUP}/v1`,
          kind: 'FirebirdUser',
          name: user.metadata.name,
          uid: user.metadata.uid ?? '',
          controller: true,
          // the drop Job must survive while the FirebirdUser is being deleted
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
              name: 'user',
              image: cluster.spec.imageName ?? DEFAULT_FIREBIRD_IMAGE,
              command: ['/bin/sh', '-c'],
              args: [script],
              env: [
                ...superuserClientEnv(cluster),
                { name: 'DATABASE_PATH', value: `${FIREBIRD_DATA_DIR}/${databaseName(cluster)}` },
                { name: 'FB_USER', value: username },
                { name: 'HOSTS', value: hosts },
                { name: 'GRANT_HOSTS', value: grantHosts },
                ...(options.action === 'apply'
                  ? [
                      {
                        name: 'PASSWORD',
                        valueFrom: {
                          secretKeyRef: {
                            name: user.spec.passwordSecret.name,
                            key: user.spec.passwordSecret.key ?? 'password',
                          },
                        },
                      },
                      { name: 'USER_CLAUSE', value: userClause },
                      { name: 'GRANT_SQL', value: grantSql(username, desiredRoles(user)) },
                    ]
                  : [{ name: 'DROP_SQL', value: dropUserSql(username) }]),
              ],
            },
          ],
        }),
      },
    },
  };
}
