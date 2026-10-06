import { CoreV1Api, CustomObjectsApi, KubeConfig, V1Secret } from '@kubernetes/client-node';
import { API_GROUP, API_VERSION, FirebirdBackup, FirebirdCluster, FirebirdRestore, FirebirdRole, FirebirdUser, RESOURCE_PLURAL } from '../types';
import { validateBackupSpec, validateClusterSpec, validateRestoreSpec, validateScheduledBackupSpec } from './validation';
import { validateUserSpec } from './users';
import { validateRoleSpec } from './roles';
import { desiredFencedInstances } from './fencing';
import { pointInTimeSourceError, restoreTargetDatabase } from './backup';
import { databaseName } from './resources';
import { AdmissionRequest, AdmissionVerdict, AdmissionValidator } from './webhook';

/**
 * What the admission webhook checks (webhook.ts):
 *
 * - every object: the operator's own validation of its spec (mostly mirrored by the CRDs'
 *   rules; a few checks exist only here, e.g. WireCrypt settings with tls.enabled);
 * - FirebirdRestore: what the operator refuses for good (status Failed) and that depends on other
 *   objects: a target that is the cluster database, a FirebirdBackup that does not exist or
 *   failed, a restoreType that does not match it, and a point-in-time source it cannot use;
 * - as warnings only (the objects may be created in any order, e.g. by one `kubectl apply` of a
 *   directory): a FirebirdCluster that does not exist (yet) or is hibernated, Secrets that do not
 *   exist (superuser, S3 credentials, a user's password and its key), a clone source that does
 *   not exist, a FirebirdBackup a restore waits for.
 *
 * Updates are only checked when the spec changes: the operator's own updates (finalizers,
 * annotations) and deletions of objects that no longer validate are never held up.
 */

export interface AdmissionLookups {
  /** The object, or undefined when it does not exist */
  cluster(namespace: string, name: string): Promise<FirebirdCluster | undefined>;
  backup(namespace: string, name: string): Promise<FirebirdBackup | undefined>;
  secret(namespace: string, name: string): Promise<V1Secret | undefined>;
}

const notFound = (err: unknown) => {
  const e = err as { code?: number; statusCode?: number; response?: { statusCode?: number } };
  return e?.code === 404 || e?.statusCode === 404 || e?.response?.statusCode === 404;
};

/** Lookups through the API server */
export function apiLookups(kubeConfig: KubeConfig): AdmissionLookups {
  const custom = kubeConfig.makeApiClient(CustomObjectsApi);
  const core = kubeConfig.makeApiClient(CoreV1Api);
  const get = async <T>(read: () => Promise<unknown>): Promise<T | undefined> => {
    try {
      return (await read()) as T;
    } catch (err) {
      if (notFound(err)) return undefined;
      throw err;
    }
  };
  return {
    cluster: (namespace, name) =>
      get(() => custom.getNamespacedCustomObject({ group: API_GROUP, version: API_VERSION, namespace, plural: RESOURCE_PLURAL, name })),
    backup: (namespace, name) =>
      get(() => custom.getNamespacedCustomObject({ group: API_GROUP, version: API_VERSION, namespace, plural: 'firebirdbackups', name })),
    secret: (namespace, name) => get(() => core.readNamespacedSecret({ namespace, name })),
  };
}

/** Names of the Secrets an object's spec refers to with secretRef (S3 credentials), anywhere in it */
export function secretRefNames(spec: unknown): string[] {
  const names = new Set<string>();
  const walk = (value: unknown) => {
    if (Array.isArray(value)) value.forEach(walk);
    else if (value && typeof value === 'object') {
      for (const [key, child] of Object.entries(value)) {
        const ref = child as { name?: unknown };
        if (key === 'secretRef' && typeof ref?.name === 'string' && ref.name !== '') names.add(ref.name);
        else walk(child);
      }
    }
  };
  walk(spec);
  return [...names].sort();
}

function message(err: unknown): string {
  return (err as Error).message ?? String(err);
}

export function createAdmissionValidator(lookups: AdmissionLookups): AdmissionValidator {
  return async (request: AdmissionRequest): Promise<AdmissionVerdict> => {
    const object = request.object as { metadata?: { name?: string; namespace?: string; deletionTimestamp?: string }; spec?: unknown } | undefined;
    if (!object || (request.operation !== 'CREATE' && request.operation !== 'UPDATE')) return {};
    if (object.metadata?.deletionTimestamp) return {};
    const old = request.oldObject as { spec?: unknown; metadata?: { annotations?: Record<string, string> } } | undefined;
    const kind = request.kind.kind;
    const specChanged = request.operation === 'CREATE' || JSON.stringify(old?.spec) !== JSON.stringify(object.spec);
    const namespace = object.metadata?.namespace ?? request.namespace ?? 'default';
    const warnings: string[] = [];

    // the fenced-instances annotation is checked whenever it changes too
    if (kind === 'FirebirdCluster' && !specChanged) {
      const annotations = (object.metadata as { annotations?: Record<string, string> })?.annotations ?? {};
      if (JSON.stringify(annotations) === JSON.stringify(old?.metadata?.annotations ?? {})) return {};
      try {
        desiredFencedInstances(object as FirebirdCluster);
      } catch (err) {
        return { denied: message(err) };
      }
      return {};
    }
    if (!specChanged) return {};

    const secretWarnings = async (names: string[]) => {
      for (const name of names) {
        if (!(await lookups.secret(namespace, name))) warnings.push(`Secret ${name} does not exist (yet) in namespace ${namespace}`);
      }
    };
    const clusterOf = async (clusterName: string): Promise<FirebirdCluster | undefined> => {
      const cluster = await lookups.cluster(namespace, clusterName);
      if (!cluster) warnings.push(`FirebirdCluster ${clusterName} does not exist (yet) in namespace ${namespace}`);
      else if (cluster.spec?.hibernated) warnings.push(`FirebirdCluster ${clusterName} is hibernated: this waits until it resumes`);
      return cluster;
    };

    try {
      switch (kind) {
        case 'FirebirdCluster': {
          const cluster = object as FirebirdCluster;
          validateClusterSpec(cluster);
          desiredFencedInstances(cluster);
          const superuser = cluster.spec.superuserSecret?.name;
          const clone = cluster.spec.bootstrap?.clone;
          await secretWarnings(
            [...new Set([...(superuser ? [superuser] : []), ...(clone?.superuserSecret?.name ? [clone.superuserSecret.name] : []), ...secretRefNames(cluster.spec)])].sort(),
          );
          if (clone && request.operation === 'CREATE') {
            const sourceNamespace = clone.namespace ?? namespace;
            if (!(await lookups.cluster(sourceNamespace, clone.sourceCluster))) {
              warnings.push(`clone source FirebirdCluster ${clone.sourceCluster} does not exist in namespace ${sourceNamespace}`);
            }
          }
          break;
        }
        case 'FirebirdBackup': {
          const backup = object as FirebirdBackup;
          validateBackupSpec(backup);
          await clusterOf(backup.spec.clusterName);
          await secretWarnings(secretRefNames(backup.spec));
          break;
        }
        case 'FirebirdScheduledBackup': {
          const scheduled = object as { spec: { clusterName: string } } & Parameters<typeof validateScheduledBackupSpec>[0];
          validateScheduledBackupSpec(scheduled);
          await clusterOf(scheduled.spec.clusterName);
          await secretWarnings(secretRefNames(scheduled.spec));
          break;
        }
        case 'FirebirdRestore': {
          const restore = object as FirebirdRestore;
          validateRestoreSpec(restore);
          await secretWarnings(secretRefNames(restore.spec));
          const denied = await restoreRefusal(restore, namespace, lookups, clusterOf, warnings);
          if (denied) return { denied, warnings };
          break;
        }
        case 'FirebirdUser': {
          const user = object as FirebirdUser;
          validateUserSpec(user);
          await clusterOf(user.spec.clusterName);
          const ref = user.spec.passwordSecret;
          const secret = await lookups.secret(namespace, ref.name);
          const key = ref.key ?? 'password';
          if (!secret) warnings.push(`Secret ${ref.name} does not exist (yet) in namespace ${namespace}`);
          else if (!secret.data?.[key] && !secret.stringData?.[key]) warnings.push(`Secret ${ref.name} has no key "${key}"`);
          break;
        }
        case 'FirebirdRole': {
          const role = object as FirebirdRole;
          validateRoleSpec(role);
          await clusterOf(role.spec.clusterName);
          break;
        }
        default:
          return {};
      }
    } catch (err) {
      if ((err as Error).name === 'ValidationError') return { denied: message(err), warnings };
      throw err;
    }
    return warnings.length ? { warnings } : {};
  };
}

/** The reason the operator would refuse a restore for good, from the objects it refers to */
async function restoreRefusal(
  restore: FirebirdRestore,
  namespace: string,
  lookups: AdmissionLookups,
  clusterOf: (name: string) => Promise<FirebirdCluster | undefined>,
  warnings: string[],
): Promise<string | undefined> {
  const spec = restore.spec;
  const cluster = await clusterOf(spec.clusterName);
  const target = restoreTargetDatabase(restore);
  if (cluster && target === databaseName(cluster)) {
    return `targetDatabase ${target} is the cluster database; restores create a new database file (bootstrap a new cluster from the backup to replace a database)`;
  }
  let source: Parameters<typeof pointInTimeSourceError>[2] | undefined;
  if (spec.backupName) {
    const backup = await lookups.backup(namespace, spec.backupName);
    if (!backup) return `FirebirdBackup ${spec.backupName} not found in namespace ${namespace}`;
    if (backup.status?.phase === 'Failed') return `FirebirdBackup ${spec.backupName} failed`;
    const type = backup.spec.type ?? 'logical';
    if (spec.restoreType && spec.restoreType !== type) {
      return `restoreType ${spec.restoreType} does not match the ${type} backup ${spec.backupName}`;
    }
    if (backup.status?.phase !== 'Completed' || !backup.status.backupFileName) {
      warnings.push(`FirebirdBackup ${spec.backupName} has not completed yet: the restore waits for it`);
      // its file name is known once it completes: a placeholder, so only the type is checked
      source = { type, path: 'pending.nbk', s3: backup.spec.s3 };
    } else {
      source = { type, path: backup.status.backupFileName, s3: backup.spec.s3 };
    }
  } else {
    source = { type: spec.restoreType ?? 'logical', path: spec.backupPath ?? '', incrementalPaths: spec.incrementalBackupPaths, s3: spec.s3 };
  }
  return cluster ? pointInTimeSourceError(restore, cluster, source) : undefined;
}
