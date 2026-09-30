import { createHash } from 'crypto';
import { V1Job } from '@kubernetes/client-node';
import { API_GROUP, DEFAULT_FIREBIRD_IMAGE, FirebirdCluster, FirebirdRole, FirebirdRoleObjectKind } from '../types';
import { clusterLabels, databaseName, FIREBIRD_DATA_DIR, jobPodSpec, superuserClientEnv } from './resources';
import { instanceHost } from './replication';
import { ValidationError } from './validation';

/**
 * Declarative roles (FirebirdRole). Roles and privileges live in the cluster database: with
 * replication they are applied on the primary and replicate, otherwise on every instance (each
 * holds an independent database).
 *
 * Applying creates the role when it does not exist, then revokes everything the role holds and
 * grants exactly the listed privileges in one transaction, so privileges removed from the spec
 * (or granted by hand) go away and nothing is ever missing in between. Memberships (users granted
 * the role) are not privileges of the role and are kept.
 */

/** Finalizer that drops the role when a FirebirdRole with reclaimPolicy "delete" is deleted */
export const ROLE_FINALIZER = `${API_GROUP}/drop-role`;

export const ROLE_JOB_HASH_ANNOTATION = `${API_GROUP}/role-hash`;
export const ROLE_JOB_TARGETS_ANNOTATION = `${API_GROUP}/role-targets`;
export const ROLE_JOB_ACTION_LABEL = `${API_GROUP}/role-action`;

/** Regular (unquoted) Firebird identifier */
const IDENTIFIER = /^[A-Za-z][A-Za-z0-9_$]{0,62}$/;

/** Privileges allowed per object kind, and the keyword naming the kind in GRANT */
const KINDS: Record<FirebirdRoleObjectKind, { keyword: string; privileges: string[] }> = {
  table: { keyword: 'TABLE', privileges: ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'REFERENCES', 'ALL'] },
  view: { keyword: 'TABLE', privileges: ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'REFERENCES', 'ALL'] },
  procedure: { keyword: 'PROCEDURE', privileges: ['EXECUTE'] },
  function: { keyword: 'FUNCTION', privileges: ['EXECUTE'] },
  package: { keyword: 'PACKAGE', privileges: ['EXECUTE'] },
  sequence: { keyword: 'SEQUENCE', privileges: ['USAGE'] },
  exception: { keyword: 'EXCEPTION', privileges: ['USAGE'] },
};

/** Roles managed elsewhere */
const RESERVED_ROLES = ['RDB$ADMIN', 'PUBLIC'];

/** Firebird role name: spec.roleName or the resource name, upper-cased as Firebird stores it */
export function firebirdRoleName(role: FirebirdRole): string {
  return (role.spec.roleName ?? role.metadata.name.replace(/-/g, '_')).toUpperCase();
}

/** The GRANT statements for the listed privileges, normalized and sorted */
export function grantStatements(role: FirebirdRole): string[] {
  const name = firebirdRoleName(role);
  const statements = (role.spec.privileges ?? []).map((p) => {
    const privileges = [...new Set(p.privileges.map((x) => x.toUpperCase()))].sort();
    const list = privileges.includes('ALL') ? 'ALL' : privileges.join(', ');
    return `GRANT ${list} ON ${KINDS[p.object.kind].keyword} ${p.object.name.toUpperCase()} TO ROLE ${name};`;
  });
  return [...new Set(statements)].sort();
}

export function validateRoleSpec(role: FirebirdRole): void {
  const spec = role.spec;
  if (!spec?.clusterName || spec.clusterName.trim() === '') {
    throw new ValidationError('FirebirdRole clusterName is required');
  }
  const roleName = spec.roleName ?? role.metadata.name.replace(/-/g, '_');
  if (!IDENTIFIER.test(roleName)) {
    throw new ValidationError(
      `Invalid Firebird role name "${roleName}": letters, digits, "_" and "$", starting with a letter (set spec.roleName)`,
    );
  }
  if (RESERVED_ROLES.includes(roleName.toUpperCase())) {
    throw new ValidationError(`${roleName.toUpperCase()} is a system role and cannot be managed`);
  }
  for (const p of spec.privileges ?? []) {
    const kind = KINDS[p.object?.kind];
    if (!kind) throw new ValidationError(`Invalid object kind "${p.object?.kind}": use ${Object.keys(KINDS).join(', ')}`);
    if (!IDENTIFIER.test(p.object.name ?? '')) {
      throw new ValidationError(`Invalid ${p.object.kind} name "${p.object.name}": a regular identifier is required`);
    }
    if (!p.privileges?.length) throw new ValidationError(`No privileges listed on ${p.object.kind} ${p.object.name}`);
    for (const privilege of p.privileges) {
      if (!kind.privileges.includes(privilege.toUpperCase())) {
        throw new ValidationError(
          `Privilege ${privilege} does not apply to a ${p.object.kind} (use ${kind.privileges.join(', ')})`,
        );
      }
    }
  }
  if (spec.reclaimPolicy && !['retain', 'delete'].includes(spec.reclaimPolicy)) {
    throw new ValidationError(`Invalid reclaimPolicy "${spec.reclaimPolicy}": must be "retain" or "delete"`);
  }
}

/** Hash of what a role Job applies */
export function roleSpecHash(role: FirebirdRole): string {
  const canonical = JSON.stringify({ role: firebirdRoleName(role), grants: grantStatements(role) });
  return createHash('sha256').update(canonical).digest('hex').slice(0, 16);
}

/** Job name for a FirebirdRole (at most 52 characters, so pod names stay valid) */
export function roleJobName(role: FirebirdRole): string {
  const name = `fbrole-${role.metadata.name}`;
  if (name.length <= 52) return name;
  const suffix = createHash('sha256').update(role.metadata.name).digest('hex').slice(0, 6);
  return `${name.slice(0, 45)}-${suffix}`;
}

/** SQL creating the role if needed and making its privileges exactly the listed ones */
export function applyRoleSql(role: FirebirdRole): string {
  const name = firebirdRoleName(role);
  return [
    'SET TERM ^;',
    'EXECUTE BLOCK AS BEGIN',
    `  IF (NOT EXISTS(SELECT 1 FROM RDB$ROLES WHERE RDB$ROLE_NAME = '${name}')) THEN EXECUTE STATEMENT 'CREATE ROLE ${name}';`,
    'END^',
    'SET TERM ;^',
    'COMMIT;',
    // one transaction: the role never lacks a privilege it keeps
    `REVOKE ALL ON ALL FROM ROLE ${name};`,
    ...grantStatements(role),
    'COMMIT;',
  ].join('\n');
}

/** SQL dropping the role if it exists (its privileges and memberships go with it) */
export function dropRoleSql(role: FirebirdRole): string {
  const name = firebirdRoleName(role);
  return [
    'SET TERM ^;',
    'EXECUTE BLOCK AS BEGIN',
    `  IF (EXISTS(SELECT 1 FROM RDB$ROLES WHERE RDB$ROLE_NAME = '${name}')) THEN EXECUTE STATEMENT 'DROP ROLE ${name}';`,
    'END^',
    'SET TERM ;^',
    'COMMIT;',
  ].join('\n');
}

/** Builds the Job that applies (or drops) a role in the databases of the given instances */
export function buildRoleJob(
  cluster: FirebirdCluster,
  role: FirebirdRole,
  options: { action: 'apply' | 'drop'; instances: string[]; hash: string; targets: string },
): V1Job {
  const { namespace = 'default' } = role.metadata;
  const labels = {
    ...clusterLabels(cluster.metadata.name),
    'app.kubernetes.io/component': 'role',
    [ROLE_JOB_ACTION_LABEL]: options.action,
  };
  const script = [
    'set -eu',
    'for h in $HOSTS; do',
    `  printf '%s\\n' "$ROLE_SQL" | isql -q -b "$h:$DATABASE_PATH"`,
    `  echo "role $FB_ROLE ${options.action === 'apply' ? 'applied' : 'dropped'} on $h"`,
    'done',
  ].join('\n');
  return {
    apiVersion: 'batch/v1',
    kind: 'Job',
    metadata: {
      name: roleJobName(role),
      namespace,
      labels,
      annotations: {
        [ROLE_JOB_HASH_ANNOTATION]: options.hash,
        [ROLE_JOB_TARGETS_ANNOTATION]: options.targets,
      },
      ownerReferences: [
        {
          apiVersion: `${API_GROUP}/v1`,
          kind: 'FirebirdRole',
          name: role.metadata.name,
          uid: role.metadata.uid ?? '',
          controller: true,
          // the drop Job must survive while the FirebirdRole is being deleted
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
              name: 'role',
              image: cluster.spec.imageName ?? DEFAULT_FIREBIRD_IMAGE,
              command: ['/bin/sh', '-c'],
              args: [script],
              env: [
                ...superuserClientEnv(cluster),
                { name: 'DATABASE_PATH', value: `${FIREBIRD_DATA_DIR}/${databaseName(cluster)}` },
                { name: 'FB_ROLE', value: firebirdRoleName(role) },
                { name: 'HOSTS', value: options.instances.map((pod) => instanceHost(cluster, pod)).join(' ') },
                { name: 'ROLE_SQL', value: options.action === 'apply' ? applyRoleSql(role) : dropRoleSql(role) },
              ],
            },
          ],
        }),
      },
    },
  };
}
