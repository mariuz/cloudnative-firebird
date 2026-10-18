import { V1Volume, V1VolumeProjection } from '@kubernetes/client-node';
import { FirebirdCluster } from '../types';

/**
 * The SQL run once, when the cluster database is created (CloudNativePG postInitSQL and
 * postInitSQLRefs): bootstrap.initSql (kept in the cluster ConfigMap), then the keys of
 * bootstrap.initSqlRefs.secretRefs, then those of configMapRefs, each list in its order. One
 * projected volume holds them as numbered files, so a directory listing gives the order. Without
 * replication the image's entrypoint runs them from /docker-entrypoint-initdb.d when it creates the
 * database; with replication the replication init container does (init-instance.sh, INIT_SQL_DIR).
 * The references are optional in the volume, so a Secret deleted after the bootstrap does not keep
 * the pods from starting; the operator creates the StatefulSet only once every key exists.
 */

export const INIT_SQL_VOLUME = 'init-sql';
/** Where the replication init container finds the scripts */
export const INIT_SQL_DIR = '/firebird-init-sql';
/** Where the official image's entrypoint runs them when it creates the database */
export const ENTRYPOINT_INIT_DIR = '/docker-entrypoint-initdb.d';
/** At most this many references per list */
export const MAX_INIT_SQL_REFS = 50;

const pad = (i: number) => String(i).padStart(2, '0');

/** The projected sources, each with the file name that sets its order */
export function initSqlSources(cluster: FirebirdCluster): V1VolumeProjection[] {
  const bootstrap = cluster.spec.bootstrap;
  const name = cluster.metadata.name;
  const sources: V1VolumeProjection[] = [];
  if (bootstrap?.initSql) sources.push({ configMap: { name: `${name}-config`, items: [{ key: 'init.sql', path: '00-init.sql' }] } });
  (bootstrap?.initSqlRefs?.secretRefs ?? []).forEach((ref, i) =>
    sources.push({ secret: { name: ref.name, items: [{ key: ref.key, path: `10-secret-${pad(i)}.sql` }], optional: true } }),
  );
  (bootstrap?.initSqlRefs?.configMapRefs ?? []).forEach((ref, i) =>
    sources.push({ configMap: { name: ref.name, items: [{ key: ref.key, path: `20-configmap-${pad(i)}.sql` }], optional: true } }),
  );
  return sources;
}

/** The file names in the order they run */
export function initSqlFiles(cluster: FirebirdCluster): string[] {
  return initSqlSources(cluster).map((s) => (s.secret ?? s.configMap)!.items![0].path);
}

/** The projected volume, or none without init SQL */
export function initSqlVolume(cluster: FirebirdCluster): V1Volume | undefined {
  const sources = initSqlSources(cluster);
  // readable by the instance's user and group only (fsGroup), the scripts may hold passwords
  return sources.length > 0 ? { name: INIT_SQL_VOLUME, projected: { sources, defaultMode: 0o440 } } : undefined;
}

/** The references of bootstrap.initSqlRefs, Secrets first */
export function initSqlRefs(cluster: FirebirdCluster): Array<{ kind: 'Secret' | 'ConfigMap'; name: string; key: string }> {
  const refs = cluster.spec.bootstrap?.initSqlRefs;
  return [
    ...(refs?.secretRefs ?? []).map((r) => ({ kind: 'Secret' as const, ...r })),
    ...(refs?.configMapRefs ?? []).map((r) => ({ kind: 'ConfigMap' as const, ...r })),
  ];
}

/** Errors of bootstrap.initSqlRefs */
export function initSqlRefsErrors(cluster: FirebirdCluster): string[] {
  const refs = cluster.spec.bootstrap?.initSqlRefs;
  if (!refs) return [];
  const errors: string[] = [];
  for (const [field, list] of [['secretRefs', refs.secretRefs], ['configMapRefs', refs.configMapRefs]] as const) {
    if (list === undefined) continue;
    if (!Array.isArray(list)) {
      errors.push(`bootstrap.initSqlRefs.${field} must be a list`);
      continue;
    }
    if (list.length > MAX_INIT_SQL_REFS) errors.push(`bootstrap.initSqlRefs.${field}: at most ${MAX_INIT_SQL_REFS} references`);
    list.forEach((ref, i) => {
      if (!ref?.name || ref.name.length > 253 || !/^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$/.test(ref.name)) {
        errors.push(`bootstrap.initSqlRefs.${field}[${i}].name must be a DNS subdomain name`);
      }
      if (!ref?.key || ref.key.length > 253 || !/^[-._a-zA-Z0-9]+$/.test(ref.key)) {
        errors.push(`bootstrap.initSqlRefs.${field}[${i}].key must be a ${field === 'secretRefs' ? 'Secret' : 'ConfigMap'} key`);
      }
    });
  }
  return errors;
}
