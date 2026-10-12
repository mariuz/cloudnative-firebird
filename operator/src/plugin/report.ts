import { dumpYaml, CoreV1Event, V1Pod } from '@kubernetes/client-node';

/**
 * kubectl firebird report: a support bundle of one cluster (CloudNativePG's kubectl cnpg report
 * cluster) written as a directory of plain files. Secrets are never read; credentials written as
 * literal environment values (the default "masterkey" of clusters without a superuser Secret) are
 * redacted, and managedFields are left out.
 */

/** Environment variables whose literal values are redacted */
const SENSITIVE_ENV = /PASSWORD|SECRET|TOKEN|KEY/i;

/** A copy of a Kubernetes object without managedFields and with sensitive literal env values redacted */
export function sanitize<T>(obj: T): T {
  const walk = (value: unknown): unknown => {
    if (Array.isArray(value)) return value.map(walk);
    // the client's typed objects hold timestamps as Dates
    if (value instanceof Date) return Number.isNaN(value.getTime()) ? undefined : value.toISOString();
    if (!value || typeof value !== 'object') return value;
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
      if (k === 'managedFields') continue;
      out[k] = walk(v);
    }
    if (typeof out.name === 'string' && typeof out.value === 'string' && SENSITIVE_ENV.test(out.name)) out.value = '<redacted>';
    return out;
  };
  return walk(obj) as T;
}

/** YAML of a list of objects (a kind: List, as kubectl get -o yaml writes it) */
export function listYaml(items: unknown[]): string {
  return dumpYaml({ apiVersion: 'v1', kind: 'List', items: items.map(sanitize) });
}

/** The events of the cluster and its resources (named after it), oldest first */
export function formatEvents(events: CoreV1Event[], cluster: string): string {
  const own = events
    .filter((e) => e.involvedObject?.name === cluster || e.involvedObject?.name?.startsWith(`${cluster}-`))
    .map((e) => ({ e, time: new Date((e.lastTimestamp ?? e.eventTime ?? e.firstTimestamp ?? e.metadata?.creationTimestamp ?? 0) as string | Date) }))
    .sort((a, b) => a.time.getTime() - b.time.getTime());
  if (own.length === 0) return '(no events)\n';
  return (
    own
      .map(({ e, time }) =>
        [
          Number.isNaN(time.getTime()) ? '-' : time.toISOString(),
          e.type ?? '',
          e.reason ?? '',
          `${e.involvedObject?.kind ?? ''}/${e.involvedObject?.name ?? ''}`,
          `${(e.count ?? 1) > 1 ? `(x${e.count}) ` : ''}${e.message ?? ''}`,
        ].join('  '),
      )
      .join('\n') + '\n'
  );
}

/** The operator's log lines about this cluster (its JSON logs carry the cluster and namespace) */
export function operatorLogLines(log: string, cluster: string, namespace: string): string {
  const lines = log.split('\n').filter((line) => {
    if (!line.startsWith('{')) return false;
    try {
      const entry = JSON.parse(line) as { cluster?: unknown; namespace?: unknown };
      return entry.cluster === cluster && (entry.namespace === undefined || entry.namespace === namespace);
    } catch {
      return false;
    }
  });
  return lines.length > 0 ? `${lines.join('\n')}\n` : '';
}

/** The containers of a pod whose logs the report collects: init containers (the sidecars) and the others */
export function podContainers(pod: V1Pod): string[] {
  return [...(pod.spec?.initContainers ?? []), ...(pod.spec?.containers ?? [])].map((c) => c.name);
}
