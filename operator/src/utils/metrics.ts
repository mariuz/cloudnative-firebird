/**
 * Prometheus metrics of the operator, served on /metrics of the health port (8080) in the text
 * exposition format. The gauges describe each cluster as of its last reconcile; replication lag is
 * the operator's own measurement from the segment servers (utils/replication-lag.ts), so it needs
 * no exporter sidecar.
 */
import { FirebirdCluster, FirebirdClusterStatus } from '../types';

type Labels = Record<string, string>;

interface MetricFamily {
  help: string;
  type: 'gauge' | 'counter';
  /** series keyed by their rendered labels */
  series: Map<string, { labels: Labels; value: number }>;
}

const escape = (v: string) => v.replace(/\\/g, '\\\\').replace(/\n/g, '\\n').replace(/"/g, '\\"');
const render = (labels: Labels) =>
  Object.keys(labels).length
    ? `{${Object.entries(labels)
        .map(([k, v]) => `${k}="${escape(v)}"`)
        .join(',')}}`
    : '';

export class MetricsRegistry {
  private readonly families = new Map<string, MetricFamily>();

  private family(name: string, type: MetricFamily['type'], help: string): MetricFamily {
    let f = this.families.get(name);
    if (!f) {
      f = { help, type, series: new Map() };
      this.families.set(name, f);
    }
    return f;
  }

  set(name: string, help: string, labels: Labels, value: number): void {
    this.family(name, 'gauge', help).series.set(render(labels), { labels, value });
  }

  inc(name: string, help: string, labels: Labels, by = 1): void {
    const f = this.family(name, 'counter', help);
    const key = render(labels);
    f.series.set(key, { labels, value: (f.series.get(key)?.value ?? 0) + by });
  }

  /** Removes every series whose labels include all of `match` (e.g. a deleted cluster) */
  remove(match: Labels): void {
    for (const f of this.families.values()) {
      for (const [key, s] of f.series) {
        if (Object.entries(match).every(([k, v]) => s.labels[k] === v)) f.series.delete(key);
      }
    }
  }

  /** Removes the series of the given families whose labels include all of `match` */
  removeFamilies(names: string[], match: Labels): void {
    for (const name of names) {
      const f = this.families.get(name);
      if (!f) continue;
      for (const [key, s] of f.series) {
        if (Object.entries(match).every(([k, v]) => s.labels[k] === v)) f.series.delete(key);
      }
    }
  }

  /** Prometheus text exposition format */
  render(): string {
    const out: string[] = [];
    for (const [name, f] of [...this.families].sort(([a], [b]) => a.localeCompare(b))) {
      if (f.series.size === 0) continue;
      out.push(`# HELP ${name} ${f.help}`, `# TYPE ${name} ${f.type}`);
      for (const [key, s] of f.series) out.push(`${name}${key} ${Number.isFinite(s.value) ? s.value : 'NaN'}`);
    }
    return out.join('\n') + '\n';
  }
}

/** The operator's registry */
export const metrics = new MetricsRegistry();

/** Records a cluster's state after a reconcile; replica series not reported any more disappear */
export function recordClusterMetrics(cluster: FirebirdCluster, status: Partial<FirebirdClusterStatus>): void {
  const base = { namespace: cluster.metadata.namespace ?? 'default', cluster: cluster.metadata.name };
  // lag series of replicas that are gone (scaled away, promoted, unmeasured) must not linger
  metrics.removeFamilies(
    ['firebird_replication_lag_seconds', 'firebird_replication_lag_segments', 'firebird_replication_pending_segments', 'firebird_replication_applied_sequence', 'firebird_replication_last_archived_sequence'],
    base,
  );
  metrics.set('firebird_cluster_instances', 'Instances requested by spec.instances', base, cluster.spec.instances);
  metrics.set('firebird_cluster_ready_instances', 'Instances whose pod is ready', base, status.readyInstances ?? 0);
  metrics.set('firebird_cluster_fenced_instances', 'Instances fenced by the fencedInstances annotation', base, status.fencedInstances?.length ?? 0);
  metrics.set('firebird_cluster_ready', '1 when the cluster phase is Running', base, status.phase === 'Running' ? 1 : 0);
  const repl = status.replicationStatus;
  if (repl?.lastArchivedSequence !== undefined) {
    metrics.set('firebird_replication_last_archived_sequence', 'Last journal segment archived on the primary', base, repl.lastArchivedSequence);
  }
  for (const r of repl?.replicas ?? []) {
    if (r.lagSeconds === undefined) continue;
    const labels = { ...base, pod: r.name };
    metrics.set('firebird_replication_lag_seconds', 'Age of the oldest archived journal segment the replica has not applied', labels, r.lagSeconds);
    metrics.set('firebird_replication_lag_segments', 'Archived journal segments the replica has not applied', labels, r.lagSegments ?? 0);
    metrics.set('firebird_replication_pending_segments', 'Segments received by the replica but not applied yet', labels, r.pendingSegments ?? 0);
    metrics.set('firebird_replication_applied_sequence', 'Journal segment applied by the replica', labels, r.appliedSequence ?? 0);
  }
}

/** Counts a reconcile of the cluster by result (success or error) */
export function recordReconcile(cluster: FirebirdCluster, result: 'success' | 'error'): void {
  metrics.inc(
    'firebird_operator_reconciles_total',
    'Cluster reconciles by result',
    { namespace: cluster.metadata.namespace ?? 'default', cluster: cluster.metadata.name, result },
  );
}
