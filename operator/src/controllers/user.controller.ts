import {
  BatchV1Api,
  CoordinationV1Api,
  CoreV1Api,
  CustomObjectsApi,
  KubeConfig,
  V1Job,
} from '@kubernetes/client-node';
import { EventReason, EventRecorder, EventType } from '../utils/events';
import { logger } from '../utils/logger';
import { CLUSTER_LABEL, instancePodSelector } from '../utils/resources';
import { replicationEnabled } from '../utils/replication';
import { isPodReady } from '../utils/routing';
import {
  USER_FINALIZER,
  USER_JOB_ACTION_LABEL,
  USER_JOB_HASH_ANNOTATION,
  USER_JOB_TARGETS_ANNOTATION,
  buildUserJob,
  firebirdUsername,
  userJobName,
  userSpecHash,
  validateUserSpec,
} from '../utils/users';
import { ValidationError } from '../utils/validation';
import {
  API_GROUP,
  API_VERSION,
  FirebirdCluster,
  FirebirdUser,
  FirebirdUserInstanceStatus,
  FirebirdUserStatus,
  reconciliationDisabled,
} from '../types';

/** A failed Job for an unchanged spec is retried after this delay */
export const USER_RETRY_DELAY_MS = 5 * 60 * 1000;

/**
 * How long the deletion of a FirebirdUser (reclaimPolicy "delete") waits for instances that hold
 * the user but are not ready, before releasing the finalizer and keeping the user on them
 */
export const USER_DROP_WAIT_MS = 15 * 60 * 1000;

function isNotFound(err: unknown): boolean {
  const e = err as { code?: number; statusCode?: number; response?: { statusCode?: number } };
  return e?.code === 404 || e?.statusCode === 404 || e?.response?.statusCode === 404;
}

function jobState(job: V1Job): 'running' | 'complete' | 'failed' {
  const conditions = job.status?.conditions ?? [];
  if (conditions.some((c) => c.type === 'Complete' && c.status === 'True')) return 'complete';
  if (conditions.some((c) => c.type === 'Failed' && c.status === 'True')) return 'failed';
  return 'running';
}

/** An instance of the cluster, with its readiness and volume */
interface Instance {
  name: string;
  ready: boolean;
  volume?: string;
}

/**
 * Reconciles FirebirdUser resources: applies each user to every instance whose applied state
 * (spec hash and volume UID) is out of date, through one Job at a time per user.
 */
export class FirebirdUserController {
  private readonly batchApi: BatchV1Api;
  private readonly coreApi: CoreV1Api;
  private readonly customApi: CustomObjectsApi;
  private readonly coordinationApi: CoordinationV1Api;
  private readonly events: EventRecorder;

  constructor(kubeConfig: KubeConfig, private readonly now: () => number = Date.now) {
    this.batchApi = kubeConfig.makeApiClient(BatchV1Api);
    this.coreApi = kubeConfig.makeApiClient(CoreV1Api);
    this.customApi = kubeConfig.makeApiClient(CustomObjectsApi);
    this.coordinationApi = kubeConfig.makeApiClient(CoordinationV1Api);
    this.events = new EventRecorder(this.coreApi, now);
  }

  /** Records a Kubernetes event on a FirebirdUser */
  private event(user: FirebirdUser, type: EventType, reason: string, message: string): Promise<void> {
    return this.events.record({ apiVersion: `${API_GROUP}/${API_VERSION}`, kind: 'FirebirdUser', metadata: user.metadata }, type, reason, message);
  }

  async reconcileUser(user: FirebirdUser): Promise<void> {
    const { name, namespace = 'default' } = user.metadata;
    const log = logger.child({ user: name, namespace });

    if (reconciliationDisabled(user)) {
      // a paused user never blocks its own deletion: the Firebird user is kept (as with "retain")
      if (user.metadata.deletionTimestamp) await this.ensureFinalizer(user, false);
      log.debug('Reconciliation disabled by annotation');
      return;
    }
    if (user.metadata.deletionTimestamp) {
      await this.reconcileDeletion(user, log);
      return;
    }

    try {
      validateUserSpec(user);
    } catch (err) {
      if (!(err instanceof ValidationError)) throw err;
      await this.updateStatus(user, { phase: 'Failed', message: err.message });
      throw err;
    }
    await this.ensureFinalizer(user, user.spec.reclaimPolicy === 'delete');

    const cluster = await this.getCluster(namespace, user.spec.clusterName);
    if (!cluster) {
      await this.updateStatus(user, { phase: 'Pending', message: `FirebirdCluster ${user.spec.clusterName} not found` });
      return;
    }
    if (cluster.spec.hibernated) {
      await this.updateStatus(user, { phase: 'Pending', message: 'cluster is hibernated' });
      return;
    }

    let secret;
    try {
      secret = await this.coreApi.readNamespacedSecret({ name: user.spec.passwordSecret.name, namespace });
    } catch (err) {
      if (!isNotFound(err)) throw err;
      await this.updateStatus(user, { phase: 'Pending', message: `Secret ${user.spec.passwordSecret.name} not found` });
      return;
    }
    const key = user.spec.passwordSecret.key ?? 'password';
    if (!secret.data?.[key] && !secret.stringData?.[key]) {
      await this.updateStatus(user, {
        phase: 'Failed',
        message: `Secret ${user.spec.passwordSecret.name} has no key "${key}"`,
      });
      return;
    }
    const hash = userSpecHash(user, secret.metadata ?? {});

    const status: FirebirdUserStatus = { ...(user.status ?? {}), username: firebirdUsername(user) };
    let applied = [...(status.instances ?? [])];

    // at most one Job per user: record a finished one before planning the next
    const jobName = userJobName(user);
    const job = await this.readJob(jobName, namespace);
    if (job) {
      const state = jobState(job);
      if (state === 'running') {
        await this.updateStatus(user, { ...status, phase: 'Applying', jobName, message: 'applying' });
        return;
      }
      await this.batchApi.deleteNamespacedJob({ name: jobName, namespace, propagationPolicy: 'Background' });
      if (job.metadata?.labels?.[USER_JOB_ACTION_LABEL] === 'apply') {
        const jobHash = job.metadata?.annotations?.[USER_JOB_HASH_ANNOTATION] ?? '';
        if (state === 'complete') {
          const targets = JSON.parse(job.metadata?.annotations?.[USER_JOB_TARGETS_ANNOTATION] ?? '[]') as Array<{
            name: string;
            volume: string;
          }>;
          applied = [
            ...applied.filter((a) => !targets.some((t) => t.name === a.name)),
            ...targets.map((t) => ({ name: t.name, volume: t.volume, hash: jobHash })),
          ].sort((a, b) => a.name.localeCompare(b.name));
          status.instances = applied;
          status.failedHash = undefined;
          status.lastFailureTime = undefined;
          log.info({ instances: targets.map((t) => t.name) }, 'Firebird user applied');
          await this.event(user, 'Normal', EventReason.UserApplied, `user ${firebirdUsername(user)} applied to ${targets.map((t) => t.name).join(', ')}`);
        } else {
          status.failedHash = jobHash;
          status.lastFailureTime = new Date(this.now()).toISOString();
          log.warn({ jobName }, 'Firebird user Job failed');
          await this.event(user, 'Warning', EventReason.UserFailed, `Job ${jobName} failed (see its pod logs; e.g. a role that does not exist)`);
        }
      }
    }

    const instances = await this.instances(cluster);
    const current = (i: Instance, a?: FirebirdUserInstanceStatus) => a && a.hash === hash && a.volume === i.volume;
    status.instances = applied.filter((a) => instances.some((i) => i.name === a.name));
    const stale = instances.filter((i) => !current(i, applied.find((a) => a.name === i.name)));
    const targets = stale.filter((i) => i.ready && i.volume);
    const waiting = stale.filter((i) => !targets.includes(i)).map((i) => i.name);

    if (targets.length === 0) {
      await this.updateStatus(user, {
        ...status,
        jobName: undefined,
        ...(waiting.length
          ? { phase: 'Pending', message: `waiting for instance(s) to be ready: ${waiting.join(', ')}` }
          : { phase: 'Applied', message: `applied to ${instances.length} instance(s)` }),
      });
      return;
    }

    if (
      status.failedHash === hash &&
      status.lastFailureTime &&
      this.now() - Date.parse(status.lastFailureTime) < USER_RETRY_DELAY_MS
    ) {
      await this.updateStatus(user, {
        ...status,
        jobName: undefined,
        phase: 'Failed',
        message: `Job ${jobName} failed (see its pod logs; e.g. a role that does not exist); retrying after ${USER_RETRY_DELAY_MS / 60000} minutes`,
      });
      return;
    }

    // role grants live in the database: on the primary with replication, else on every instance
    const primary = await this.primaryPod(cluster);
    const grantInstances = replicationEnabled(cluster)
      ? targets.filter((t) => t.name === primary).map((t) => t.name)
      : targets.map((t) => t.name);
    const body = buildUserJob(cluster, user, {
      action: 'apply',
      instances: targets.map((t) => t.name),
      grantInstances,
      hash,
      targets: JSON.stringify(targets.map((t) => ({ name: t.name, volume: t.volume }))),
    });
    try {
      await this.batchApi.createNamespacedJob({ namespace, body });
      log.info({ instances: targets.map((t) => t.name) }, 'Applying Firebird user');
    } catch (err) {
      // the previous Job is still being deleted; retried on the next reconcile
      if ((err as { code?: number })?.code !== 409) throw err;
    }
    await this.updateStatus(user, { ...status, phase: 'Applying', jobName, message: `applying to ${targets.map((t) => t.name).join(', ')}` });
  }

  /**
   * Drops the user (reclaimPolicy "delete") before releasing the finalizer: from every ready
   * instance, then from each instance that holds it (status.instances) once it is ready again.
   * Instances that stay unready longer than USER_DROP_WAIT_MS keep the user (Warning event).
   */
  private async reconcileDeletion(user: FirebirdUser, log: typeof logger): Promise<void> {
    const { namespace = 'default' } = user.metadata;
    if (!user.metadata.finalizers?.includes(USER_FINALIZER)) return;

    const cluster = await this.getCluster(namespace, user.spec.clusterName);
    if (!cluster || cluster.spec.hibernated || user.spec.reclaimPolicy !== 'delete') {
      if (cluster?.spec.hibernated) log.warn('Cluster is hibernated; the Firebird user is kept');
      await this.ensureFinalizer(user, false);
      return;
    }

    const status: FirebirdUserStatus = { ...(user.status ?? {}), username: firebirdUsername(user) };
    let droppedFrom = [...(status.droppedFrom ?? [])];
    const jobName = userJobName(user);
    const job = await this.readJob(jobName, namespace);
    if (job) {
      const drop = job.metadata?.labels?.[USER_JOB_ACTION_LABEL] === 'drop';
      const state = jobState(job);
      if (drop && state === 'running') return;
      // an apply Job is removed first, even while running
      await this.batchApi.deleteNamespacedJob({ name: jobName, namespace, propagationPolicy: 'Background' });
      if (!drop || state !== 'complete') return; // a failed drop is retried on the next reconcile
      const targets = JSON.parse(job.metadata?.annotations?.[USER_JOB_TARGETS_ANNOTATION] ?? '[]') as Array<{
        name: string;
        volume: string;
      }>;
      droppedFrom = [
        ...droppedFrom.filter((d) => !targets.some((t) => t.name === d.name)),
        ...targets,
      ].sort((a, b) => a.name.localeCompare(b.name));
      log.info({ instances: targets.map((t) => t.name) }, 'Firebird user dropped');
    }

    const instances = await this.instances(cluster);
    const dropped = (i: Instance) => droppedFrom.some((d) => d.name === i.name && d.volume === i.volume);
    const targets = instances.filter((i) => i.ready && i.volume && !dropped(i));
    // the user is known to be on these volumes, whose instances are not ready
    const waiting = instances.filter(
      (i) => !i.ready && !dropped(i) && status.instances?.some((a) => a.name === i.name && a.volume === i.volume),
    );

    if (targets.length > 0) {
      const primary = await this.primaryPod(cluster);
      const body = buildUserJob(cluster, user, {
        action: 'drop',
        instances: targets.map((i) => i.name),
        grantInstances: replicationEnabled(cluster)
          ? targets.filter((i) => i.name === primary).map((i) => i.name)
          : targets.map((i) => i.name),
        hash: '',
        targets: JSON.stringify(targets.map((t) => ({ name: t.name, volume: t.volume }))),
      });
      try {
        await this.batchApi.createNamespacedJob({ namespace, body });
        log.info({ instances: targets.map((i) => i.name) }, 'Dropping Firebird user');
      } catch (err) {
        if ((err as { code?: number })?.code !== 409) throw err;
      }
      await this.updateStatus(user, {
        ...status,
        droppedFrom,
        jobName,
        phase: 'Dropping',
        message: `dropping from ${targets.map((i) => i.name).join(', ')}`,
      });
      return;
    }

    const names = (list: Instance[]) => list.map((i) => i.name).join(', ');
    if (waiting.length > 0) {
      const since = new Date(user.metadata.deletionTimestamp ?? NaN).getTime();
      if (!Number.isFinite(since) || this.now() - since < USER_DROP_WAIT_MS) {
        await this.updateStatus(user, {
          ...status,
          droppedFrom,
          jobName: undefined,
          phase: 'Dropping',
          message: `waiting for instance(s) to be ready to drop the user: ${names(waiting)}`,
        });
        return;
      }
      log.warn({ instances: waiting.map((i) => i.name) }, 'Instances not ready; the Firebird user is kept on them');
      await this.event(
        user,
        'Warning',
        EventReason.UserFailed,
        `user ${firebirdUsername(user)} kept on ${names(waiting)}: not ready for ${USER_DROP_WAIT_MS / 60000} minutes`,
      );
    }
    if (droppedFrom.length > 0) {
      await this.event(
        user,
        'Normal',
        EventReason.UserDropped,
        `user ${firebirdUsername(user)} dropped from ${droppedFrom.map((d) => d.name).join(', ')}`,
      );
    }
    await this.ensureFinalizer(user, false);
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

  private async primaryPod(cluster: FirebirdCluster): Promise<string> {
    const { name, namespace = 'default' } = cluster.metadata;
    try {
      const lease = await this.coordinationApi.readNamespacedLease({ name: `${name}-lease`, namespace });
      if (lease.spec?.holderIdentity) return lease.spec.holderIdentity;
    } catch {
      // no Lease yet
    }
    return `${name}-0`;
  }

  private async getCluster(namespace: string, name: string): Promise<FirebirdCluster | undefined> {
    try {
      return (await this.customApi.getNamespacedCustomObject({
        group: API_GROUP,
        version: API_VERSION,
        namespace,
        plural: 'firebirdclusters',
        name,
      })) as FirebirdCluster;
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

  /** Adds or removes the drop-user finalizer */
  private async ensureFinalizer(user: FirebirdUser, wanted: boolean): Promise<void> {
    const finalizers = user.metadata.finalizers ?? [];
    const has = finalizers.includes(USER_FINALIZER);
    if (has === wanted) return;
    const next = wanted ? [...finalizers, USER_FINALIZER] : finalizers.filter((f) => f !== USER_FINALIZER);
    await this.customApi.patchNamespacedCustomObject({
      group: API_GROUP,
      version: API_VERSION,
      namespace: user.metadata.namespace ?? 'default',
      plural: 'firebirdusers',
      name: user.metadata.name,
      body: [
        // guards against overwriting finalizers added concurrently by someone else
        { op: 'test', path: '/metadata/resourceVersion', value: user.metadata.resourceVersion },
        { op: 'add', path: '/metadata/finalizers', value: next },
      ],
    });
    user.metadata.finalizers = next;
  }

  /** Replaces the status when it changed; undefined fields are dropped */
  private async updateStatus(user: FirebirdUser, update: FirebirdUserStatus): Promise<void> {
    const merged = Object.fromEntries(
      Object.entries({ ...(user.status ?? {}), ...update }).filter(([, v]) => v !== undefined),
    );
    const canonical = (o: object) => JSON.stringify(Object.entries(o).sort(([a], [b]) => a.localeCompare(b)));
    if (user.status && canonical(user.status) === canonical(merged)) return;
    await this.customApi.patchNamespacedCustomObjectStatus({
      group: API_GROUP,
      version: API_VERSION,
      namespace: user.metadata.namespace ?? 'default',
      plural: 'firebirdusers',
      name: user.metadata.name,
      body: [{ op: 'replace' as const, path: '/status', value: merged }],
    });
  }
}
