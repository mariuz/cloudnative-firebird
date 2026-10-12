import { V1Pod } from '@kubernetes/client-node';
import { API_GROUP, API_VERSION, FirebirdBackup, FirebirdCluster, FirebirdDatabase, FirebirdScheduledBackup } from '../types';
import { isPodReady, ROLE_LABEL } from '../utils/routing';

/**
 * kubectl-firebird, the kubectl plugin (CloudNativePG's kubectl cnpg): status of a cluster and the
 * day-2 actions the operator exposes through annotations and fields, without writing them by hand.
 * The plugin only edits the resources; the operator does the work. Its imports stay light (types
 * and routing): the operator's constants it writes are repeated here and checked by the tests.
 */

export const PLUGIN_VERSION = '0.97.0';

export const TARGET_PRIMARY_ANNOTATION = `${API_GROUP}/targetPrimary`;
export const FENCED_INSTANCES_ANNOTATION = `${API_GROUP}/fencedInstances`;
export const RESEED_ANNOTATION = `${API_GROUP}/reseed`;
export const RESTARTED_AT_ANNOTATION = 'kubectl.kubernetes.io/restartedAt';
const CLUSTER_LABEL = `${API_GROUP}/cluster`;

/** The Kubernetes calls the plugin makes */
export interface PluginApi {
  getCluster(namespace: string, name: string): Promise<FirebirdCluster>;
  /** JSON merge patch (null removes a field) */
  patchCluster(namespace: string, name: string, patch: object): Promise<void>;
  listPods(namespace: string, labelSelector: string): Promise<V1Pod[]>;
  patchPod(namespace: string, name: string, patch: object): Promise<void>;
  deletePod(namespace: string, name: string): Promise<void>;
  patchStatefulSet(namespace: string, name: string, patch: object): Promise<void>;
  listBackups(namespace: string): Promise<FirebirdBackup[]>;
  listScheduledBackups(namespace: string): Promise<FirebirdScheduledBackup[]>;
  listDatabases(namespace: string): Promise<FirebirdDatabase[]>;
  createBackup(namespace: string, body: FirebirdBackup): Promise<void>;
}

export interface PluginIo {
  out(text: string): void;
  err(text: string): void;
  /** Runs kubectl with these arguments, attached to the terminal; resolves to its exit code */
  kubectl(args: string[]): Promise<number>;
  /** Whether stdin is a terminal (isql: -t for kubectl exec) */
  interactive: boolean;
  now(): Date;
}

export class UsageError extends Error {}

export const USAGE = `kubectl firebird: manage cloudnative-firebird clusters

Usage: kubectl firebird <command> [arguments] [-n namespace] [--context context]

Commands:
  status <cluster>                          the cluster, its instances, replication, databases and backups
  promote <cluster> <instance>              switchover: promote a replica (targetPrimary annotation)
  restart <cluster> [instance]              rolling restart of the cluster (primary last), or restart one instance
  fence on|off <cluster> <instance|all>     fence (database full shutdown) or unfence instances
  hibernate on|off <cluster>                stop every instance keeping the volumes, or resume
  reseed <cluster> <instance>               discard a replica's database and seed it again
  backup <cluster> [--type logical|physical|volumeSnapshot] [--level 0|1|2]
                   [--snapshot-class name] [--name backup-name]
                                            take an on-demand backup (FirebirdBackup)
  isql <cluster> [--replica | --instance i] [-- isql arguments]
                                            isql on the primary (or a replica), in the firebird container
  version                                   the plugin's version

An instance is its pod name (my-cluster-1) or its ordinal (1).`;

interface Parsed {
  positional: string[];
  flags: Map<string, string | true>;
  /** Arguments after "--" */
  rest: string[];
}

const VALUE_FLAGS = new Set(['namespace', 'context', 'type', 'level', 'name', 'snapshot-class', 'instance']);
const SHORT: Record<string, string> = { n: 'namespace' };

export function parseArgs(argv: string[]): Parsed {
  const positional: string[] = [];
  const flags = new Map<string, string | true>();
  let rest: string[] = [];
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === '--') {
      rest = argv.slice(i + 1);
      break;
    }
    const long = /^--([a-z-]+)(?:=(.*))?$/.exec(arg);
    const short = /^-([a-z])$/.exec(arg);
    if (long || short) {
      const name = long ? long[1] : SHORT[short![1]];
      if (!name) throw new UsageError(`unknown flag ${arg}`);
      if (VALUE_FLAGS.has(name)) {
        const value = long?.[2] ?? argv[++i];
        if (value === undefined) throw new UsageError(`${arg} needs a value`);
        flags.set(name, value);
      } else if (['replica', 'help'].includes(name)) {
        flags.set(name, true);
      } else {
        throw new UsageError(`unknown flag ${arg}`);
      }
    } else {
      positional.push(arg);
    }
  }
  return { positional, flags, rest };
}

/** An instance argument: the pod name, or its ordinal */
export function instanceName(cluster: FirebirdCluster, arg: string): string {
  const name = cluster.metadata.name;
  const pod = /^\d+$/.test(arg) ? `${name}-${arg}` : arg;
  const ordinal = pod.startsWith(`${name}-`) ? pod.slice(name.length + 1) : '';
  if (!/^\d+$/.test(ordinal) || Number(ordinal) >= cluster.spec.instances) {
    throw new UsageError(`${arg} is not an instance of cluster ${name} (${name}-0 to ${name}-${cluster.spec.instances - 1})`);
  }
  return pod;
}

/** The primary: the operator's record, else the pod labelled primary, else ordinal 0 */
export function primaryOf(cluster: FirebirdCluster, pods: V1Pod[]): string {
  return (
    cluster.status?.replicationStatus?.primaryPod ??
    pods.find((p) => p.metadata?.labels?.[ROLE_LABEL] === 'primary')?.metadata?.name ??
    `${cluster.metadata.name}-0`
  );
}

/** The fenced instances the annotation lists ("*" for all) */
export function fencedList(cluster: FirebirdCluster): string[] {
  const raw = cluster.metadata.annotations?.[FENCED_INSTANCES_ANNOTATION];
  if (!raw || raw.trim() === '') return [];
  try {
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed.filter((v): v is string => typeof v === 'string') : [];
  } catch {
    throw new UsageError(`the ${FENCED_INSTANCES_ANNOTATION} annotation is not a JSON list: ${raw}`);
  }
}

/** The annotation value after fencing or unfencing an instance (or every instance) */
export function nextFencedList(cluster: FirebirdCluster, on: boolean, target: string | '*'): string[] {
  const current = fencedList(cluster);
  const all = Array.from({ length: cluster.spec.instances }, (_, i) => `${cluster.metadata.name}-${i}`);
  if (target === '*') return on ? ['*'] : [];
  const expanded = current.includes('*') ? all : current;
  if (on) return expanded.includes(target) ? expanded : [...expanded, target].sort();
  return expanded.filter((p) => p !== target);
}

function table(rows: string[][]): string {
  const widths = rows[0].map((_, c) => Math.max(...rows.map((r) => (r[c] ?? '').length)));
  return rows.map((r) => r.map((cell, c) => (c === r.length - 1 ? cell : cell.padEnd(widths[c]))).join('  ')).join('\n');
}

function age(from: string | Date | undefined, now: Date): string {
  if (!from) return '-';
  const s = Math.max(0, Math.round((now.getTime() - new Date(from).getTime()) / 1000));
  if (s < 120) return `${s}s`;
  if (s < 3600) return `${Math.round(s / 60)}m`;
  if (s < 172800) return `${Math.round(s / 3600)}h`;
  return `${Math.round(s / 86400)}d`;
}

/** The status report of a cluster */
export function formatStatus(
  cluster: FirebirdCluster,
  pods: V1Pod[],
  backups: FirebirdBackup[],
  scheduled: FirebirdScheduledBackup[],
  now: Date,
  databases: FirebirdDatabase[] = [],
): string {
  const { name, namespace = 'default' } = cluster.metadata;
  const status = cluster.status ?? {};
  const replication = cluster.spec.replication?.enabled === true;
  const primary = primaryOf(cluster, pods);
  const fenced = new Set(status.fencedInstances ?? []);
  const lag = new Map((status.replicationStatus?.replicas ?? []).map((r) => [r.name, r]));
  const lines: string[] = [];
  const field = (label: string, value: string | undefined) => {
    if (value !== undefined && value !== '') lines.push(`${`${label}:`.padEnd(14)}${value}`);
  };

  field('Cluster', `${namespace}/${name}`);
  field('Phase', `${status.phase ?? 'Unknown'}${status.phaseReason ? ` (${status.phaseReason})` : ''}`);
  field('Instances', `${status.readyInstances ?? 0}/${cluster.spec.instances} ready`);
  const catalog = cluster.spec.imageCatalogRef;
  field(
    'Image',
    cluster.spec.imageName ??
      (catalog
        ? `${status.image ?? '(not resolved)'} (${catalog.kind ?? 'FirebirdImageCatalog'} ${catalog.name}, Firebird ${catalog.major})`
        : status.image ?? 'operator default'),
  );
  field('Database', cluster.spec.databaseName ?? 'mydb.fdb');
  field('Primary', primary);
  if (replication) {
    const sync = cluster.spec.replication?.synchronous;
    const parts = [
      sync ? `synchronous (${sync.dataDurability ?? 'required'})` : 'asynchronous',
      status.replicationStatus?.lastArchivedSequence !== undefined
        ? `last archived segment ${status.replicationStatus.lastArchivedSequence}`
        : undefined,
      cluster.spec.replication?.failover?.enabled ? 'automatic failover' : undefined,
    ].filter(Boolean);
    field('Replication', parts.join(', '));
  } else {
    field('Replication', 'off');
  }
  if (status.switchover) {
    const s = status.switchover;
    field(s.kind === 'failover' ? 'Failover' : 'Switchover', `${s.from} -> ${s.target || '(electing)'}: ${s.phase}${s.message ? ` (${s.message})` : ''}`);
  }
  if (status.replicaCluster) {
    const r = status.replicaCluster;
    field('Replica of', `${r.phase}${r.appliedSegment !== undefined ? `, applied segment ${r.appliedSegment}` : ''}${r.archivedSegment !== undefined ? ` of ${r.archivedSegment}` : ''}`);
  }
  if (status.majorUpgrade) field('Upgrade', `${status.majorUpgrade.phase ?? ''} ${status.majorUpgrade.from ?? ''} -> ${status.majorUpgrade.to ?? ''}`.trim());
  if (cluster.spec.hibernated) field('Hibernated', 'yes');
  if (fenced.size > 0) field('Fenced', [...fenced].join(', '));
  if (status.reseedingInstances?.length) field('Re-seeding', status.reseedingInstances.join(', '));

  lines.push('', 'Instances:');
  const rows = [['NAME', 'ROLE', 'READY', 'STATUS', 'NODE', 'APPLIED', 'LAG', 'AGE']];
  const byOrdinal = [...pods].sort((a, b) => (a.metadata?.name ?? '').localeCompare(b.metadata?.name ?? '', undefined, { numeric: true }));
  for (const pod of byOrdinal) {
    const pod_name = pod.metadata?.name ?? '';
    const replica = lag.get(pod_name);
    const role = pod_name === primary ? 'primary' : replication ? 'replica' : 'instance';
    rows.push([
      pod_name,
      fenced.has(pod_name) ? `${role} (fenced)` : role,
      isPodReady(pod) ? 'true' : 'false',
      pod.metadata?.deletionTimestamp ? 'Terminating' : pod.status?.phase ?? 'Unknown',
      pod.spec?.nodeName ?? '-',
      replica?.appliedSequence !== undefined ? String(replica.appliedSequence) : '-',
      replica
        ? replica.error
          ? `error: ${replica.error}`
          : `${replica.lagSegments ?? 0} seg / ${replica.lagSeconds ?? 0}s`
        : '-',
      age(pod.metadata?.creationTimestamp, now),
    ]);
  }
  lines.push(rows.length > 1 ? table(rows) : '(no instance pods)');

  const ownDatabases = databases.filter((d) => d.spec.clusterName === name);
  if (ownDatabases.length > 0) {
    lines.push('', 'Databases:');
    const databaseRows = [['NAME', 'PATH', 'PHASE', 'RECLAIM']];
    for (const d of ownDatabases) {
      databaseRows.push([
        d.metadata.name,
        d.status?.path ?? `/var/lib/firebird/data/${d.spec.databaseName ?? `${d.metadata.name}.fdb`}`,
        d.status?.phase ?? 'Pending',
        d.spec.reclaimPolicy ?? 'retain',
      ]);
    }
    lines.push(table(databaseRows));
  }

  const own = backups
    .filter((b) => b.spec.clusterName === name)
    .sort((a, b) => new Date(b.status?.startTime ?? b.metadata.creationTimestamp ?? 0).getTime() - new Date(a.status?.startTime ?? a.metadata.creationTimestamp ?? 0).getTime())
    .slice(0, 5);
  lines.push('', 'Backups (latest 5):');
  if (own.length === 0) {
    lines.push('(none)');
  } else {
    const backupRows = [['NAME', 'TYPE', 'PHASE', 'STARTED', 'LOCATION']];
    for (const b of own) {
      backupRows.push([
        b.metadata.name,
        b.spec.type ?? 'logical',
        b.status?.phase ?? 'Pending',
        b.status?.startTime ? `${age(b.status.startTime, now)} ago` : '-',
        b.status?.location ?? b.status?.error ?? '-',
      ]);
    }
    lines.push(table(backupRows));
  }
  const schedules = scheduled.filter((s) => s.spec.clusterName === name);
  if (schedules.length > 0) {
    lines.push('', 'Scheduled backups:');
    const scheduleRows = [['NAME', 'SCHEDULE', 'TYPE', 'SUSPENDED', 'LAST SUCCESS']];
    for (const s of schedules) {
      scheduleRows.push([
        s.metadata.name,
        s.spec.schedule,
        s.spec.type ?? 'logical',
        s.spec.suspend ? 'yes' : 'no',
        s.status?.lastSuccessfulTime ? `${age(s.status.lastSuccessfulTime, now)} ago` : '-',
      ]);
    }
    lines.push(table(scheduleRows));
  }
  const conditions = (status.conditions ?? []).filter((c) => c.status !== 'True' || c.type === 'Ready');
  if (conditions.length > 0) {
    lines.push('', 'Conditions:');
    for (const c of conditions) lines.push(`  ${c.type}=${c.status}${c.reason ? ` ${c.reason}` : ''}${c.message ? `: ${c.message}` : ''}`);
  }
  return lines.join('\n');
}

function need(parsed: Parsed, count: number, usage: string): string[] {
  if (parsed.positional.length < count + 1) throw new UsageError(`usage: kubectl firebird ${usage}`);
  if (parsed.positional.length > count + 1) throw new UsageError(`unexpected argument ${parsed.positional[count + 1]}; usage: kubectl firebird ${usage}`);
  return parsed.positional.slice(1);
}

function onOff(value: string, usage: string): boolean {
  if (value === 'on') return true;
  if (value === 'off') return false;
  throw new UsageError(`expected on or off, got ${value}; usage: kubectl firebird ${usage}`);
}

/** The API server's message of a failed request (its Status body), or the error's own */
export function apiErrorMessage(err: unknown): string {
  const e = err as { code?: number; statusCode?: number; body?: unknown; message?: string };
  let body = e.body;
  if (typeof body === 'string') {
    try {
      body = JSON.parse(body);
    } catch {
      // not a Status
    }
  }
  const message = (body as { message?: string } | undefined)?.message;
  if (message) return message;
  if ((e.code ?? e.statusCode) === 404) return 'not found';
  return e.message ?? String(err);
}

/** Runs one plugin command; resolves to the exit code */
export async function run(argv: string[], api: PluginApi, io: PluginIo, defaultNamespace: string): Promise<number> {
  try {
    const parsed = parseArgs(argv);
    const command = parsed.positional[0];
    if (!command || parsed.flags.has('help') || command === 'help') {
      io.out(USAGE);
      return command || parsed.flags.has('help') ? 0 : 1;
    }
    const namespace = (parsed.flags.get('namespace') as string | undefined) ?? defaultNamespace;
    const context = parsed.flags.get('context') as string | undefined;

    switch (command) {
      case 'version':
        io.out(`kubectl-firebird ${PLUGIN_VERSION}`);
        return 0;

      case 'status': {
        const [name] = need(parsed, 1, 'status <cluster>');
        const cluster = await api.getCluster(namespace, name);
        const [pods, backups, scheduled, databases] = await Promise.all([
          api.listPods(namespace, `${CLUSTER_LABEL}=${name},app.kubernetes.io/component=database`),
          api.listBackups(namespace),
          api.listScheduledBackups(namespace),
          // an operator without the FirebirdDatabase CRD: no databases
          api.listDatabases(namespace).catch(() => [] as FirebirdDatabase[]),
        ]);
        io.out(formatStatus(cluster, pods, backups, scheduled, io.now(), databases));
        return 0;
      }

      case 'promote': {
        const [name, arg] = need(parsed, 2, 'promote <cluster> <instance>');
        const cluster = await api.getCluster(namespace, name);
        if (cluster.spec.replication?.enabled !== true) throw new UsageError(`cluster ${name} has no replication: there is no replica to promote`);
        const target = instanceName(cluster, arg);
        const pods = await api.listPods(namespace, `${CLUSTER_LABEL}=${name},app.kubernetes.io/component=database`);
        const primary = primaryOf(cluster, pods);
        if (target === primary) throw new UsageError(`${target} is the primary already`);
        const pod = pods.find((p) => p.metadata?.name === target);
        if (!pod || !isPodReady(pod)) io.err(`warning: ${target} is not ready; the switchover waits for it`);
        await api.patchCluster(namespace, name, { metadata: { annotations: { [TARGET_PRIMARY_ANNOTATION]: target } } });
        io.out(`switchover of cluster ${name} requested: ${primary} -> ${target} (kubectl firebird status ${name} follows it)`);
        return 0;
      }

      case 'restart': {
        const args = parsed.positional.slice(1);
        if (args.length < 1 || args.length > 2) throw new UsageError('usage: kubectl firebird restart <cluster> [instance]');
        const cluster = await api.getCluster(namespace, args[0]);
        if (args.length === 2) {
          const pod = instanceName(cluster, args[1]);
          await api.deletePod(namespace, pod);
          io.out(`instance ${pod} restarting`);
        } else {
          // the StatefulSet's pod template changes: the operator's rolling update restarts the
          // replicas, then the primary (kubectl rollout restart's annotation)
          await api.patchStatefulSet(namespace, args[0], {
            spec: { template: { metadata: { annotations: { [RESTARTED_AT_ANNOTATION]: io.now().toISOString() } } } },
          });
          io.out(`rolling restart of cluster ${args[0]} requested`);
        }
        return 0;
      }

      case 'fence': {
        const [mode, name, arg] = need(parsed, 3, 'fence on|off <cluster> <instance|all>');
        const on = onOff(mode, 'fence on|off <cluster> <instance|all>');
        const cluster = await api.getCluster(namespace, name);
        const target = arg === 'all' || arg === '*' ? '*' : instanceName(cluster, arg);
        const next = nextFencedList(cluster, on, target);
        await api.patchCluster(namespace, name, {
          metadata: { annotations: { [FENCED_INSTANCES_ANNOTATION]: next.length > 0 ? JSON.stringify(next) : null } },
        });
        io.out(next.length > 0 ? `fenced instances of ${name}: ${next.join(', ')}` : `no instance of ${name} is fenced`);
        return 0;
      }

      case 'hibernate': {
        const [mode, name] = need(parsed, 2, 'hibernate on|off <cluster>');
        const on = onOff(mode, 'hibernate on|off <cluster>');
        await api.getCluster(namespace, name);
        await api.patchCluster(namespace, name, { spec: { hibernated: on } });
        io.out(on ? `cluster ${name} hibernating (its volumes are kept)` : `cluster ${name} resuming`);
        return 0;
      }

      case 'reseed': {
        const [name, arg] = need(parsed, 2, 'reseed <cluster> <instance>');
        const cluster = await api.getCluster(namespace, name);
        if (cluster.spec.replication?.enabled !== true) throw new UsageError(`cluster ${name} has no replication: only replicas are re-seeded`);
        const target = instanceName(cluster, arg);
        const pods = await api.listPods(namespace, `${CLUSTER_LABEL}=${name},app.kubernetes.io/component=database`);
        if (target === primaryOf(cluster, pods)) throw new UsageError(`${target} is the primary: only replicas are re-seeded`);
        await api.patchPod(namespace, target, { metadata: { annotations: { [RESEED_ANNOTATION]: 'true' } } });
        io.out(`re-seed of ${target} requested: its database is discarded and seeded again`);
        return 0;
      }

      case 'backup': {
        const [name] = need(parsed, 1, 'backup <cluster> [--type logical|physical|volumeSnapshot] [--level n] [--snapshot-class c] [--name n]');
        const cluster = await api.getCluster(namespace, name);
        const type = (parsed.flags.get('type') as string | undefined) ?? 'logical';
        if (!['logical', 'physical', 'volumeSnapshot'].includes(type)) throw new UsageError(`--type must be logical, physical or volumeSnapshot, got ${type}`);
        const levelFlag = parsed.flags.get('level') as string | undefined;
        if (levelFlag !== undefined && (type !== 'physical' || !['0', '1', '2'].includes(levelFlag))) {
          throw new UsageError('--level is 0, 1 or 2, for physical backups');
        }
        const snapshotClass = parsed.flags.get('snapshot-class') as string | undefined;
        if (snapshotClass && type !== 'volumeSnapshot') throw new UsageError('--snapshot-class is for volumeSnapshot backups');
        const stamp = io.now().toISOString().replace(/[-:T]/g, '').slice(0, 14);
        const backupName = (parsed.flags.get('name') as string | undefined) ?? `${cluster.metadata.name}-${stamp}`;
        // the backup's own S3 destination, as the cluster's scheduled backups have it
        const s3 = type !== 'volumeSnapshot' ? cluster.spec.backup?.s3 : undefined;
        const body: FirebirdBackup = {
          apiVersion: `${API_GROUP}/${API_VERSION}` as FirebirdBackup['apiVersion'],
          kind: 'FirebirdBackup',
          metadata: { name: backupName, namespace },
          spec: {
            clusterName: name,
            type: type as FirebirdBackup['spec']['type'],
            ...(levelFlag !== undefined ? { level: Number(levelFlag) as 0 | 1 | 2 } : {}),
            ...(s3 ? { s3 } : {}),
            ...(snapshotClass ? { volumeSnapshot: { className: snapshotClass } } : {}),
          },
        };
        await api.createBackup(namespace, body);
        io.out(`FirebirdBackup ${backupName} created (${type}${s3 ? `, to s3://${s3.bucket}` : ''}); kubectl get firebirdbackup ${backupName} -n ${namespace} -w`);
        return 0;
      }

      case 'isql': {
        const [name] = need(parsed, 1, 'isql <cluster> [--replica | --instance i] [-- isql arguments]');
        const cluster = await api.getCluster(namespace, name);
        const pods = await api.listPods(namespace, `${CLUSTER_LABEL}=${name},app.kubernetes.io/component=database`);
        const primary = primaryOf(cluster, pods);
        let target = primary;
        const instance = parsed.flags.get('instance') as string | undefined;
        if (instance) {
          target = instanceName(cluster, instance);
        } else if (parsed.flags.has('replica')) {
          const replica = pods.find((p) => p.metadata?.name !== primary && isPodReady(p));
          if (!replica) throw new UsageError(`cluster ${name} has no ready replica`);
          target = replica.metadata!.name!;
        }
        const database = `/var/lib/firebird/data/${cluster.spec.databaseName ?? 'mydb.fdb'}`;
        // the container's ISC_USER / ISC_PASSWORD (SYSDBA) authenticate the local connection
        return io.kubectl([
          ...(context ? ['--context', context] : []),
          '-n',
          namespace,
          'exec',
          io.interactive ? '-it' : '-i',
          target,
          '-c',
          'firebird',
          '--',
          'isql',
          ...parsed.rest,
          `localhost:${database}`,
        ]);
      }

      default:
        throw new UsageError(`unknown command ${command}\n\n${USAGE}`);
    }
  } catch (err) {
    if (err instanceof UsageError) {
      io.err(`error: ${err.message}`);
      return 2;
    }
    io.err(`error: ${apiErrorMessage(err)}`);
    return 1;
  }
}
