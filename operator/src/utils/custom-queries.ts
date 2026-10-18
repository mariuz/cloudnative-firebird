import { loadYaml, V1Container, V1Secret, V1Volume } from '@kubernetes/client-node';
import { API_GROUP, FirebirdCluster, RESOURCE_KIND } from '../types';
import { UPGRADE_SCRIPTS } from './replication';

/**
 * Custom metrics (CloudNativePG monitoring.customQueriesConfigMap and customQueriesSecret): SQL
 * queries in CloudNativePG's format, from ConfigMap and Secret keys, which the metrics-exporter
 * sidecar of every instance (metrics-exporter.pl) runs on each scrape and serves as Prometheus
 * metrics. The operator merges and validates them into one rendered Secret, mounted into the
 * sidecar: changed queries apply at the next scrape, without restarting the instances.
 *
 *   attachments:
 *     query: "SELECT MON$USER AS USERNAME, COUNT(*) AS TOTAL FROM MON$ATTACHMENTS GROUP BY 1"
 *     primary: false            # true: only on the primary (not a replica database)
 *     metrics:
 *       - username: { usage: LABEL, description: "User" }
 *       - total: { usage: GAUGE, description: "Attachments per user" }
 *
 * gives firebird_attachments_total{username="SYSDBA"} 2. Usages: GAUGE, COUNTER, LABEL, DISCARD.
 */

export const METRICS_EXPORTER_CONTAINER = 'metrics-exporter';
export const METRICS_EXPORTER_PORT = 9187;
export const METRICS_EXPORTER_PORT_NAME = 'custom-metrics';
export const CUSTOM_QUERIES_VOLUME = 'custom-queries';
export const CUSTOM_QUERIES_DIR = '/custom-queries';
/** The rendered queries' key in the Secret */
export const CUSTOM_QUERIES_KEY = 'queries';
/** Annotation on the rendered Secret: the problems found in the sources (an event when it changes) */
export const CUSTOM_QUERIES_ERRORS_ANNOTATION = `${API_GROUP}/custom-queries-errors`;

const USAGES = ['GAUGE', 'COUNTER', 'LABEL', 'DISCARD'] as const;
export type MetricUsage = (typeof USAGES)[number];

export interface CustomQuery {
  name: string;
  query: string;
  primary: boolean;
  metrics: Array<{ column: string; usage: MetricUsage; description: string }>;
}

export interface CustomQueryRef {
  kind: 'ConfigMap' | 'Secret';
  name: string;
  key: string;
}

/** The rendered queries' Secret of a cluster */
export function customQueriesSecretName(cluster: FirebirdCluster): string {
  return `${cluster.metadata.name}-custom-queries`;
}

/** The query sources, ConfigMaps first, each in spec order (a later query of the same name wins) */
export function customQueryRefs(cluster: FirebirdCluster): CustomQueryRef[] {
  const monitoring = cluster.spec.monitoring;
  return [
    ...(monitoring?.customQueriesConfigMap ?? []).map((r) => ({ kind: 'ConfigMap' as const, name: r.name, key: r.key })),
    ...(monitoring?.customQueriesSecret ?? []).map((r) => ({ kind: 'Secret' as const, name: r.name, key: r.key })),
  ];
}

/** Whether the instances run the metrics exporter sidecar */
export function customQueriesEnabled(cluster: FirebirdCluster): boolean {
  return customQueryRefs(cluster).length > 0;
}

const QUERY_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;
const COLUMN_NAME = /^[A-Za-z_][A-Za-z0-9_$]*$/;

/** The queries of one source document, and what is wrong with the rest (each skipped) */
export function parseCustomQueries(text: string, source: string): { queries: CustomQuery[]; errors: string[] } {
  const queries: CustomQuery[] = [];
  const errors: string[] = [];
  let doc: unknown;
  try {
    doc = loadYaml<unknown>(text);
  } catch (err) {
    // loadYaml refuses an empty document (no queries), syntax errors keep js-yaml's message
    if ((err as Error).message === 'Failed to load YAML') return { queries, errors };
    return { queries, errors: [`${source}: not valid YAML (${(err as Error).message.split('\n')[0]})`] };
  }
  if (doc === null || doc === undefined) return { queries, errors };
  if (typeof doc !== 'object' || Array.isArray(doc)) return { queries, errors: [`${source}: expected a map of queries by name`] };
  for (const [name, value] of Object.entries(doc as Record<string, unknown>)) {
    const where = `${source}: query ${name}`;
    if (!QUERY_NAME.test(name)) {
      errors.push(`${where}: the name must be letters, digits and underscores`);
      continue;
    }
    const spec = value as Record<string, unknown> | null;
    if (!spec || typeof spec !== 'object' || Array.isArray(spec)) {
      errors.push(`${where}: expected query and metrics`);
      continue;
    }
    if (typeof spec.query !== 'string' || spec.query.trim() === '') {
      errors.push(`${where}: query must be an SQL statement`);
      continue;
    }
    if (spec.primary !== undefined && typeof spec.primary !== 'boolean') {
      errors.push(`${where}: primary must be true or false`);
      continue;
    }
    if (!Array.isArray(spec.metrics) || spec.metrics.length === 0) {
      errors.push(`${where}: metrics must list the query's columns`);
      continue;
    }
    const metrics: CustomQuery['metrics'] = [];
    let bad = false;
    for (const entry of spec.metrics as unknown[]) {
      const pairs = entry && typeof entry === 'object' && !Array.isArray(entry) ? Object.entries(entry as Record<string, unknown>) : [];
      if (pairs.length !== 1) {
        errors.push(`${where}: each metrics entry maps one column to its usage and description`);
        bad = true;
        break;
      }
      const [column, def] = pairs[0];
      const d = (def ?? {}) as Record<string, unknown>;
      const usage = typeof d.usage === 'string' ? d.usage.toUpperCase() : '';
      if (!COLUMN_NAME.test(column)) {
        errors.push(`${where}: column ${column} must be a plain column name (an alias of the query)`);
        bad = true;
        break;
      }
      if (!(USAGES as readonly string[]).includes(usage)) {
        errors.push(`${where}: column ${column}: usage must be one of ${USAGES.join(', ')}`);
        bad = true;
        break;
      }
      if (d.description !== undefined && typeof d.description !== 'string') {
        errors.push(`${where}: column ${column}: description must be text`);
        bad = true;
        break;
      }
      metrics.push({ column, usage: usage as MetricUsage, description: ((d.description as string | undefined) ?? '').replace(/\s+/g, ' ').trim() });
    }
    if (bad) continue;
    if (!metrics.some((m) => m.usage === 'GAUGE' || m.usage === 'COUNTER')) {
      errors.push(`${where}: no GAUGE or COUNTER column, so it gives no metric`);
      continue;
    }
    queries.push({ name, query: spec.query.trim(), primary: spec.primary === true, metrics });
  }
  return { queries, errors };
}

/** The queries of all sources in order, a later one replacing an earlier one of the same name */
export function mergeCustomQueries(lists: CustomQuery[][]): CustomQuery[] {
  const byName = new Map<string, CustomQuery>();
  for (const list of lists) for (const q of list) byName.set(q.name, q);
  return [...byName.values()];
}

/** The queries in the exporter's line format (metrics-exporter.pl) */
export function renderCustomQueries(queries: CustomQuery[]): string {
  return queries
    .map((q) =>
      [
        `query ${q.name}`,
        `primary ${q.primary ? 1 : 0}`,
        `sql ${q.query.replace(/\\/g, '\\\\').replace(/\r?\n/g, '\\n')}`,
        ...q.metrics.map((m) => `metric ${m.column} ${m.usage}${m.description ? ` ${m.description}` : ''}`),
        'end',
      ].join('\n') + '\n',
    )
    .join('');
}

/** The rendered queries' Secret, with the problems found in the sources as an annotation */
export function buildCustomQueriesSecret(cluster: FirebirdCluster, queries: CustomQuery[], errors: string[], labels: Record<string, string>): V1Secret {
  return {
    apiVersion: 'v1',
    kind: 'Secret',
    metadata: {
      name: customQueriesSecretName(cluster),
      namespace: cluster.metadata.namespace ?? 'default',
      labels,
      annotations: { [CUSTOM_QUERIES_ERRORS_ANNOTATION]: errors.join('; ') },
      ownerReferences: [
        {
          apiVersion: `${API_GROUP}/v1`,
          kind: RESOURCE_KIND,
          name: cluster.metadata.name!,
          uid: cluster.metadata.uid ?? '',
          controller: true,
          blockOwnerDeletion: true,
        },
      ],
    },
    type: 'Opaque',
    stringData: { [CUSTOM_QUERIES_KEY]: renderCustomQueries(queries) },
  };
}

/** The Secret volume of the rendered queries (optional: the pod starts before it exists) */
export function customQueriesVolume(cluster: FirebirdCluster): V1Volume | undefined {
  if (!customQueriesEnabled(cluster)) return undefined;
  return { name: CUSTOM_QUERIES_VOLUME, secret: { secretName: customQueriesSecretName(cluster), optional: true, defaultMode: 0o440 } };
}

/** The metrics exporter sidecar (Firebird image: isql against the local server), or none */
export function buildMetricsExporterContainer(
  cluster: FirebirdCluster,
  options: { image: string; databasePath: string; credentials: V1Container['env'] },
): V1Container | undefined {
  if (!customQueriesEnabled(cluster)) return undefined;
  return {
    name: METRICS_EXPORTER_CONTAINER,
    image: options.image,
    command: ['perl', '-e', UPGRADE_SCRIPTS['metrics-exporter.pl']],
    ports: [{ name: METRICS_EXPORTER_PORT_NAME, containerPort: METRICS_EXPORTER_PORT, protocol: 'TCP' }],
    env: [
      ...(options.credentials ?? []),
      { name: 'DATABASE_PATH', value: options.databasePath },
      { name: 'QUERIES_FILE', value: `${CUSTOM_QUERIES_DIR}/${CUSTOM_QUERIES_KEY}` },
      { name: 'METRICS_PORT', value: String(METRICS_EXPORTER_PORT) },
    ],
    volumeMounts: [{ name: CUSTOM_QUERIES_VOLUME, mountPath: CUSTOM_QUERIES_DIR, readOnly: true }],
  };
}

const DNS_SUBDOMAIN = /^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$/;
const DATA_KEY = /^[-._a-zA-Z0-9]+$/;

/** Errors of monitoring.customQueriesConfigMap and customQueriesSecret */
export function customQueriesErrors(spec: FirebirdCluster['spec']): string[] {
  const errors: string[] = [];
  for (const field of ['customQueriesConfigMap', 'customQueriesSecret'] as const) {
    const refs = spec.monitoring?.[field];
    if (refs === undefined) continue;
    if (!Array.isArray(refs)) {
      errors.push(`monitoring.${field} must be a list of {name, key}`);
      continue;
    }
    refs.forEach((ref, i) => {
      if (typeof ref?.name !== 'string' || ref.name.length > 253 || !DNS_SUBDOMAIN.test(ref.name)) {
        errors.push(`monitoring.${field}[${i}].name must be a DNS subdomain name`);
      }
      if (typeof ref?.key !== 'string' || ref.key.length > 253 || !DATA_KEY.test(ref.key)) {
        errors.push(`monitoring.${field}[${i}].key must be a ConfigMap or Secret key`);
      }
    });
  }
  const refs = (spec.monitoring?.customQueriesConfigMap?.length ?? 0) + (spec.monitoring?.customQueriesSecret?.length ?? 0);
  if (refs > 0 && spec.monitoring?.exporter?.enabled && (spec.monitoring.exporter.port ?? 9108) === METRICS_EXPORTER_PORT) {
    errors.push(`monitoring.exporter.port ${METRICS_EXPORTER_PORT} is the custom queries' metrics exporter port`);
  }
  return errors;
}
