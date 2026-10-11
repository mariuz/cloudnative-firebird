import { BatchV1Api, CoreV1Api, CustomObjectsApi, KubeConfig, V1Job } from '@kubernetes/client-node';
import { clusterStoppedReason } from '../utils/major-upgrade';
import { EventReason, EventRecorder, EventType } from '../utils/events';
import { logger } from '../utils/logger';
import { CLUSTER_LABEL, instancePodSelector } from '../utils/resources';
import { isPodReady } from '../utils/routing';
import { ValidationError } from '../utils/validation';
import {
  buildDatabaseJob,
  DATABASE_FINALIZER,
  DATABASE_JOB_ACTION_LABEL,
  DATABASE_JOB_TARGETS_ANNOTATION,
  databaseClusterError,
  databaseJobName,
  managedDatabasePath,
  validateDatabaseSpec,
} from '../utils/databases';
import {
  API_GROUP,
  API_VERSION,
  FirebirdCluster,
  FirebirdDatabase,
  FirebirdDatabaseInstanceStatus,
  FirebirdDatabaseStatus,
  reconciliationDisabled,
} from '../types';

/** A failed Job is retried after this delay */
export const DATABASE_RETRY_DELAY_MS = 5 * 60 * 1000;

/**
 * How long the deletion of a FirebirdDatabase (reclaimPolicy "delete") waits for instances that
 * hold the database but are not ready, before releasing the finalizer and keeping the file there
 */
export const DATABASE_DROP_WAIT_MS = 15 * 60 * 1000;

function isNotFound(err: unknown): boolean {
  const e = err as { code?: number; statusCode?: number };
  return e?.code === 404 || e?.statusCode === 404;
}

function jobState(job: V1Job): 'running' | 'complete' | 'failed' {
  const conditions = job.status?.conditions ?? [];
  if (conditions.some((c) => c.type === 'Complete' && c.status === 'True')) return 'complete';
  if (conditions.some((c) => c.type === 'Failed' && c.status === 'True')) return 'failed';
  return 'running';
}

interface Instance {
  name: string;
  ready: boolean;
  volume?: string;
}

/**
 * Reconciles FirebirdDatabase resources (utils/databases.ts): one Job at a time per database
 * creates it on the instances whose volume does not have it yet, or drops it while the resource is
 * deleted with reclaimPolicy "delete".
 */
export class FirebirdDatabaseController {
  private readonly batchApi: BatchV1Api;
  private readonly coreApi: CoreV1Api;
  private readonly customApi: CustomObjectsApi;
  private readonly events: EventRecorder;

  constructor(kubeConfig: KubeConfig, private readonly now: () => number = Date.now) {
    this.batchApi = kubeConfig.makeApiClient(BatchV1Api);
    this.coreApi = kubeConfig.makeApiClient(CoreV1Api);
    this.customApi = kubeConfig.makeApiClient(CustomObjectsApi);
    this.events = new EventRecorder(this.coreApi, now);
  }

  private event(database: FirebirdDatabase, type: EventType, reason: string, message: string): Promise<void> {
    return this.events.record(
      { apiVersion: `${API_GROUP}/${API_VERSION}`, kind: 'FirebirdDatabase', metadata: database.metadata },
      type,
      reason,
      message,
    );
  }

  async reconcileDatabase(database: FirebirdDatabase): Promise<void> {
    const { name, namespace = 'default' } = database.metadata;
    const log = logger.child({ database: name, namespace });

    if (reconciliationDisabled(database)) {
      // a paused resource never blocks its own deletion: the files are kept (as with "retain")
      if (database.metadata.deletionTimestamp) await this.ensureFinalizer(database, false);
      log.debug('Reconciliation disabled by annotation');
      return;
    }
    if (database.metadata.deletionTimestamp) {
      await this.reconcileDeletion(database, log);
      return;
    }

    try {
      validateDatabaseSpec(database);
    } catch (err) {
      if (!(err instanceof ValidationError)) throw err;
      await this.updateStatus(database, { phase: 'Failed', message: err.message });
      throw err;
    }
    await this.ensureFinalizer(database, database.spec.reclaimPolicy === 'delete');

    const status: FirebirdDatabaseStatus = { ...(database.status ?? {}), path: managedDatabasePath(database) };
    const cluster = await this.getCluster(namespace, database.spec.clusterName);
    if (!cluster) {
      await this.updateStatus(database, { ...status, phase: 'Pending', message: `FirebirdCluster ${database.spec.clusterName} not found` });
      return;
    }
    const conflict = databaseClusterError(database, cluster, await this.listDatabases(namespace));
    if (conflict) {
      if (database.status?.message !== conflict) await this.event(database, 'Warning', EventReason.DatabaseFailed, conflict);
      await this.updateStatus(database, { ...status, phase: 'Failed', message: conflict });
      return;
    }
    if (clusterStoppedReason(cluster)) {
      await this.updateStatus(database, { ...status, phase: 'Pending', message: clusterStoppedReason(cluster) });
      return;
    }

    // at most one Job per database: record a finished one before planning the next
    const jobName = databaseJobName(database);
    const job = await this.readJob(jobName, namespace);
    let created = [...(status.instances ?? [])];
    if (job) {
      const state = jobState(job);
      if (state === 'running') {
        await this.updateStatus(database, { ...status, phase: 'Creating', jobName, message: 'creating' });
        return;
      }
      await this.batchApi.deleteNamespacedJob({ name: jobName, namespace, propagationPolicy: 'Background' });
      if (job.metadata?.labels?.[DATABASE_JOB_ACTION_LABEL] === 'create') {
        if (state === 'complete') {
          const targets = JSON.parse(job.metadata?.annotations?.[DATABASE_JOB_TARGETS_ANNOTATION] ?? '[]') as FirebirdDatabaseInstanceStatus[];
          created = [...created.filter((c) => !targets.some((t) => t.name === c.name)), ...targets].sort((a, b) => a.name.localeCompare(b.name));
          status.lastFailureTime = undefined;
          log.info({ instances: targets.map((t) => t.name) }, 'Database created');
          await this.event(database, 'Normal', EventReason.DatabaseCreated, `${status.path} on ${targets.map((t) => t.name).join(', ')}`);
        } else {
          status.lastFailureTime = new Date(this.now()).toISOString();
          log.warn({ jobName }, 'Database Job failed');
          await this.event(database, 'Warning', EventReason.DatabaseFailed, `Job ${jobName} failed; see its pod logs (e.g. initSql)`);
        }
      }
    }

    const instances = await this.instances(cluster);
    status.instances = created.filter((c) => instances.some((i) => i.name === c.name));
    const has = (i: Instance) => created.some((c) => c.name === i.name && c.volume === i.volume);
    const missing = instances.filter((i) => !has(i));
    const targets = missing.filter((i) => i.ready && i.volume);
    const waiting = missing.filter((i) => !targets.includes(i)).map((i) => i.name);

    if (targets.length === 0) {
      await this.updateStatus(database, {
        ...status,
        jobName: undefined,
        ...(waiting.length
          ? { phase: 'Pending', message: `waiting for instance(s) to be ready: ${waiting.join(', ')}` }
          : { phase: 'Ready', message: `on ${instances.length} instance(s)` }),
      });
      return;
    }
    if (status.lastFailureTime && this.now() - Date.parse(status.lastFailureTime) < DATABASE_RETRY_DELAY_MS) {
      await this.updateStatus(database, {
        ...status,
        jobName: undefined,
        phase: 'Failed',
        message: `Job ${jobName} failed (see its pod logs); retrying after ${DATABASE_RETRY_DELAY_MS / 60000} minutes`,
      });
      return;
    }

    const body = buildDatabaseJob(cluster, database, 'create', targets.map((t) => ({ name: t.name, volume: t.volume! })));
    try {
      await this.batchApi.createNamespacedJob({ namespace, body });
      log.info({ instances: targets.map((t) => t.name) }, 'Creating database');
    } catch (err) {
      // the previous Job is still being deleted; retried on the next reconcile
      if ((err as { code?: number })?.code !== 409) throw err;
    }
    await this.updateStatus(database, { ...status, phase: 'Creating', jobName, message: `creating on ${targets.map((t) => t.name).join(', ')}` });
  }

  /**
   * Drops the database (reclaimPolicy "delete") from every instance it is on before releasing the
   * finalizer; instances that stay unready longer than DATABASE_DROP_WAIT_MS keep their file
   */
  private async reconcileDeletion(database: FirebirdDatabase, log: typeof logger): Promise<void> {
    const { namespace = 'default' } = database.metadata;
    if (!database.metadata.finalizers?.includes(DATABASE_FINALIZER)) return;
    const cluster = await this.getCluster(namespace, database.spec.clusterName);
    if (!cluster || cluster.spec.hibernated || database.spec.reclaimPolicy !== 'delete') {
      if (cluster?.spec.hibernated) log.warn('Cluster is hibernated; the database files are kept');
      await this.ensureFinalizer(database, false);
      return;
    }

    const status: FirebirdDatabaseStatus = { ...(database.status ?? {}) };
    let droppedFrom = [...(status.droppedFrom ?? [])];
    const jobName = databaseJobName(database);
    const job = await this.readJob(jobName, namespace);
    if (job) {
      const drop = job.metadata?.labels?.[DATABASE_JOB_ACTION_LABEL] === 'drop';
      const state = jobState(job);
      if (drop && state === 'running') return;
      // a create Job is removed first, even while running
      await this.batchApi.deleteNamespacedJob({ name: jobName, namespace, propagationPolicy: 'Background' });
      if (!drop) return;
      if (state !== 'complete') {
        await this.event(database, 'Warning', EventReason.DatabaseFailed, `Job ${jobName} failed to drop the database (still attached?); retrying`);
        return;
      }
      const targets = JSON.parse(job.metadata?.annotations?.[DATABASE_JOB_TARGETS_ANNOTATION] ?? '[]') as FirebirdDatabaseInstanceStatus[];
      droppedFrom = [...droppedFrom.filter((d) => !targets.some((t) => t.name === d.name)), ...targets].sort((a, b) => a.name.localeCompare(b.name));
      log.info({ instances: targets.map((t) => t.name) }, 'Database dropped');
    }

    const instances = await this.instances(cluster);
    const dropped = (i: Instance) => droppedFrom.some((d) => d.name === i.name && d.volume === i.volume);
    // every ready instance: a database created there after the status was written is dropped too
    const targets = instances.filter((i) => i.ready && i.volume && !dropped(i));
    const waiting = instances.filter((i) => !i.ready && !dropped(i) && status.instances?.some((c) => c.name === i.name && c.volume === i.volume));

    if (targets.length > 0) {
      const body = buildDatabaseJob(cluster, database, 'drop', targets.map((t) => ({ name: t.name, volume: t.volume! })));
      try {
        await this.batchApi.createNamespacedJob({ namespace, body });
        log.info({ instances: targets.map((i) => i.name) }, 'Dropping database');
      } catch (err) {
        if ((err as { code?: number })?.code !== 409) throw err;
      }
      await this.updateStatus(database, { ...status, droppedFrom, jobName, phase: 'Dropping', message: `dropping from ${targets.map((i) => i.name).join(', ')}` });
      return;
    }
    if (waiting.length > 0) {
      const since = new Date(database.metadata.deletionTimestamp ?? NaN).getTime();
      if (!Number.isFinite(since) || this.now() - since < DATABASE_DROP_WAIT_MS) {
        await this.updateStatus(database, {
          ...status,
          droppedFrom,
          jobName: undefined,
          phase: 'Dropping',
          message: `waiting for instance(s) to be ready to drop the database: ${waiting.map((i) => i.name).join(', ')}`,
        });
        return;
      }
      await this.event(
        database,
        'Warning',
        EventReason.DatabaseFailed,
        `${managedDatabasePath(database)} kept on ${waiting.map((i) => i.name).join(', ')} (not ready for ${DATABASE_DROP_WAIT_MS / 60000} minutes)`,
      );
    }
    if (droppedFrom.length > 0) {
      await this.event(database, 'Normal', EventReason.DatabaseDropped, `${managedDatabasePath(database)} dropped from ${droppedFrom.map((d) => d.name).join(', ')}`);
    }
    await this.ensureFinalizer(database, false);
  }

  private async instances(cluster: FirebirdCluster): Promise<Instance[]> {
    const { name, namespace = 'default' } = cluster.metadata;
    const [pods, pvcs] = await Promise.all([
      this.coreApi.listNamespacedPod({ namespace, labelSelector: instancePodSelector(name) }),
      this.coreApi.listNamespacedPersistentVolumeClaim({ namespace, labelSelector: `${CLUSTER_LABEL}=${name}` }),
    ]);
    return Array.from({ length: cluster.spec.instances }, (_, i) => {
      const pod = `${name}-${i}`;
      const podObj = pods.items.find((p) => p.metadata?.name === pod);
      const pvc = pvcs.items.find((p) => p.metadata?.name === `firebird-data-${pod}`);
      return { name: pod, ready: Boolean(podObj && isPodReady(podObj)), volume: pvc?.metadata?.uid };
    });
  }

  private async listDatabases(namespace: string): Promise<FirebirdDatabase[]> {
    const list = (await this.customApi.listNamespacedCustomObject({ group: API_GROUP, version: API_VERSION, namespace, plural: 'firebirddatabases' })) as {
      items?: FirebirdDatabase[];
    };
    return list.items ?? [];
  }

  private async getCluster(namespace: string, name: string): Promise<FirebirdCluster | undefined> {
    try {
      return (await this.customApi.getNamespacedCustomObject({ group: API_GROUP, version: API_VERSION, namespace, plural: 'firebirdclusters', name })) as FirebirdCluster;
    } catch (err) {
      if (isNotFound(err)) return undefined;
      throw err;
    }
  }

  private async readJob(name: string, namespace: string): Promise<V1Job | undefined> {
    try {
      return await this.batchApi.readNamespacedJob({ name, namespace });
    } catch (err) {
      if (isNotFound(err)) return undefined;
      throw err;
    }
  }

  /** Adds or removes the drop-database finalizer */
  private async ensureFinalizer(database: FirebirdDatabase, wanted: boolean): Promise<void> {
    const finalizers = database.metadata.finalizers ?? [];
    if (finalizers.includes(DATABASE_FINALIZER) === wanted) return;
    const next = wanted ? [...finalizers, DATABASE_FINALIZER] : finalizers.filter((f) => f !== DATABASE_FINALIZER);
    await this.customApi.patchNamespacedCustomObject({
      group: API_GROUP,
      version: API_VERSION,
      namespace: database.metadata.namespace ?? 'default',
      plural: 'firebirddatabases',
      name: database.metadata.name,
      body: [
        // guards against overwriting finalizers added concurrently by someone else
        { op: 'test', path: '/metadata/resourceVersion', value: database.metadata.resourceVersion },
        { op: 'add', path: '/metadata/finalizers', value: next },
      ],
    });
    database.metadata.finalizers = next;
  }

  /** Replaces the status when it changed; undefined fields are dropped */
  private async updateStatus(database: FirebirdDatabase, update: FirebirdDatabaseStatus): Promise<void> {
    const merged = Object.fromEntries(Object.entries({ ...(database.status ?? {}), ...update }).filter(([, v]) => v !== undefined));
    const canonical = (o: object) => JSON.stringify(Object.entries(o).sort(([a], [b]) => a.localeCompare(b)));
    if (database.status && canonical(database.status) === canonical(merged)) return;
    try {
      await this.customApi.patchNamespacedCustomObjectStatus({
        group: API_GROUP,
        version: API_VERSION,
        namespace: database.metadata.namespace ?? 'default',
        plural: 'firebirddatabases',
        name: database.metadata.name,
        body: [{ op: 'replace', path: '/status', value: merged }],
      });
    } catch (err) {
      // deleted meanwhile (its finalizer released)
      if (!isNotFound(err)) throw err;
    }
    database.status = merged as FirebirdDatabaseStatus;
  }
}
