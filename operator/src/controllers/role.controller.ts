import { BatchV1Api, CoordinationV1Api, CoreV1Api, CustomObjectsApi, KubeConfig, V1Job } from '@kubernetes/client-node';
import { EventReason, EventRecorder, EventType } from '../utils/events';
import { logger } from '../utils/logger';
import { CLUSTER_LABEL, instancePodSelector } from '../utils/resources';
import { replicationEnabled } from '../utils/replication';
import { isPodReady } from '../utils/routing';
import {
  ROLE_FINALIZER,
  ROLE_JOB_ACTION_LABEL,
  ROLE_JOB_HASH_ANNOTATION,
  ROLE_JOB_TARGETS_ANNOTATION,
  buildRoleJob,
  firebirdRoleName,
  roleJobName,
  roleSpecHash,
  validateRoleSpec,
} from '../utils/roles';
import { ValidationError } from '../utils/validation';
import {
  API_GROUP,
  API_VERSION,
  FirebirdCluster,
  FirebirdRole,
  FirebirdRoleInstanceStatus,
  FirebirdRoleStatus,
  reconciliationDisabled,
} from '../types';

/** A failed Job for an unchanged spec is retried after this delay */
export const ROLE_RETRY_DELAY_MS = 5 * 60 * 1000;

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

interface Instance {
  name: string;
  ready: boolean;
  volume?: string;
}

/**
 * Reconciles FirebirdRole resources: a role of the cluster database and exactly the privileges
 * it holds (see utils/roles.ts). With replication the role is applied on the primary and
 * replicates; without it, on every instance's database, tracked per instance and volume.
 */
export class FirebirdRoleController {
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

  private event(role: FirebirdRole, type: EventType, reason: string, message: string): Promise<void> {
    return this.events.record({ apiVersion: `${API_GROUP}/${API_VERSION}`, kind: 'FirebirdRole', metadata: role.metadata }, type, reason, message);
  }

  async reconcileRole(role: FirebirdRole): Promise<void> {
    const { name, namespace = 'default' } = role.metadata;
    const log = logger.child({ role: name, namespace });

    if (reconciliationDisabled(role)) {
      // a paused role never blocks its own deletion: the Firebird role is kept (as with "retain")
      if (role.metadata.deletionTimestamp) await this.ensureFinalizer(role, false);
      return;
    }
    if (role.metadata.deletionTimestamp) {
      await this.reconcileDeletion(role, log);
      return;
    }

    try {
      validateRoleSpec(role);
    } catch (err) {
      if (!(err instanceof ValidationError)) throw err;
      await this.updateStatus(role, { phase: 'Failed', message: err.message });
      throw err;
    }
    await this.ensureFinalizer(role, role.spec.reclaimPolicy === 'delete');

    const cluster = await this.getCluster(namespace, role.spec.clusterName);
    if (!cluster) {
      await this.updateStatus(role, { phase: 'Pending', message: `FirebirdCluster ${role.spec.clusterName} not found` });
      return;
    }
    if (cluster.spec.hibernated) {
      await this.updateStatus(role, { phase: 'Pending', message: 'cluster is hibernated' });
      return;
    }

    const hash = roleSpecHash(role);
    const replication = replicationEnabled(cluster);
    const status: FirebirdRoleStatus = { ...(role.status ?? {}), roleName: firebirdRoleName(role) };

    // at most one Job per role: record a finished one before planning the next
    const jobName = roleJobName(role);
    const job = await this.readJob(jobName, namespace);
    if (job) {
      const state = jobState(job);
      if (state === 'running') {
        await this.updateStatus(role, { ...status, phase: 'Applying', jobName, message: 'applying' });
        return;
      }
      await this.batchApi.deleteNamespacedJob({ name: jobName, namespace, propagationPolicy: 'Background' });
      if (job.metadata?.labels?.[ROLE_JOB_ACTION_LABEL] === 'apply') {
        const jobHash = job.metadata?.annotations?.[ROLE_JOB_HASH_ANNOTATION] ?? '';
        const targets = JSON.parse(job.metadata?.annotations?.[ROLE_JOB_TARGETS_ANNOTATION] ?? '[]') as Array<{
          name: string;
          volume: string;
        }>;
        if (state === 'complete') {
          if (replication) {
            status.appliedHash = jobHash;
          } else {
            status.instances = [
              ...(status.instances ?? []).filter((a) => !targets.some((t) => t.name === a.name)),
              ...targets.map((t) => ({ name: t.name, volume: t.volume, hash: jobHash })),
            ].sort((a, b) => a.name.localeCompare(b.name));
          }
          status.failedHash = undefined;
          status.lastFailureTime = undefined;
          log.info({ instances: targets.map((t) => t.name) }, 'Firebird role applied');
          await this.event(role, 'Normal', EventReason.RoleApplied, `role ${firebirdRoleName(role)} applied on ${targets.map((t) => t.name).join(', ')}`);
        } else {
          status.failedHash = jobHash;
          status.lastFailureTime = new Date(this.now()).toISOString();
          log.warn({ jobName }, 'Firebird role Job failed');
          await this.event(role, 'Warning', EventReason.RoleFailed, `Job ${jobName} failed (see its pod logs; e.g. an object that does not exist)`);
        }
      }
    }

    const instances = await this.instances(cluster);
    let targets: Instance[];
    let waiting: string[] = [];
    if (replication) {
      // the database replicates, roles included: applied once, on the primary
      const primary = await this.primaryPod(cluster);
      const primaryInstance = instances.find((i) => i.name === primary);
      if (status.appliedHash === hash) {
        targets = [];
      } else if (primaryInstance?.ready) {
        targets = [primaryInstance];
      } else {
        targets = [];
        waiting = [primary];
      }
      status.instances = undefined;
    } else {
      const current = (i: Instance, a?: FirebirdRoleInstanceStatus) => a && a.hash === hash && a.volume === i.volume;
      status.instances = (status.instances ?? []).filter((a) => instances.some((i) => i.name === a.name));
      const stale = instances.filter((i) => !current(i, status.instances?.find((a) => a.name === i.name)));
      targets = stale.filter((i) => i.ready && i.volume);
      waiting = stale.filter((i) => !targets.includes(i)).map((i) => i.name);
      status.appliedHash = undefined;
    }

    if (targets.length === 0) {
      await this.updateStatus(role, {
        ...status,
        jobName: undefined,
        ...(waiting.length
          ? { phase: 'Pending', message: `waiting for instance(s) to be ready: ${waiting.join(', ')}` }
          : { phase: 'Applied', message: replication ? 'applied on the primary (replicated)' : `applied to ${instances.length} instance(s)` }),
      });
      return;
    }

    if (
      status.failedHash === hash &&
      status.lastFailureTime &&
      this.now() - Date.parse(status.lastFailureTime) < ROLE_RETRY_DELAY_MS
    ) {
      await this.updateStatus(role, {
        ...status,
        jobName: undefined,
        phase: 'Failed',
        message: `Job ${jobName} failed (see its pod logs; e.g. an object that does not exist); retrying after ${ROLE_RETRY_DELAY_MS / 60000} minutes`,
      });
      return;
    }

    const body = buildRoleJob(cluster, role, {
      action: 'apply',
      instances: targets.map((t) => t.name),
      hash,
      targets: JSON.stringify(targets.map((t) => ({ name: t.name, volume: t.volume ?? '' }))),
    });
    try {
      await this.batchApi.createNamespacedJob({ namespace, body });
      log.info({ instances: targets.map((t) => t.name) }, 'Applying Firebird role');
    } catch (err) {
      // the previous Job is still being deleted; retried on the next reconcile
      if ((err as { code?: number })?.code !== 409) throw err;
    }
    await this.updateStatus(role, { ...status, phase: 'Applying', jobName, message: `applying on ${targets.map((t) => t.name).join(', ')}` });
  }

  /** Drops the role (reclaimPolicy "delete") before releasing the finalizer */
  private async reconcileDeletion(role: FirebirdRole, log: typeof logger): Promise<void> {
    const { namespace = 'default' } = role.metadata;
    if (!role.metadata.finalizers?.includes(ROLE_FINALIZER)) return;

    const cluster = await this.getCluster(namespace, role.spec.clusterName);
    if (!cluster || cluster.spec.hibernated || role.spec.reclaimPolicy !== 'delete') {
      if (cluster?.spec.hibernated) log.warn('Cluster is hibernated; the Firebird role is kept');
      await this.ensureFinalizer(role, false);
      return;
    }

    const jobName = roleJobName(role);
    const job = await this.readJob(jobName, namespace);
    if (job) {
      const drop = job.metadata?.labels?.[ROLE_JOB_ACTION_LABEL] === 'drop';
      const state = jobState(job);
      if (drop && state === 'running') return;
      // an apply Job is removed first, even while running
      await this.batchApi.deleteNamespacedJob({ name: jobName, namespace, propagationPolicy: 'Background' });
      if (drop && state === 'complete') {
        log.info('Firebird role dropped');
        await this.event(role, 'Normal', EventReason.RoleDropped, `role ${firebirdRoleName(role)} dropped`);
        await this.ensureFinalizer(role, false);
      }
      return; // a failed drop is retried on the next reconcile
    }

    const instances = (await this.instances(cluster)).filter((i) => i.ready);
    const primary = await this.primaryPod(cluster);
    const targets = replicationEnabled(cluster) ? instances.filter((i) => i.name === primary) : instances;
    if (targets.length === 0) {
      await this.updateStatus(role, { phase: 'Dropping', message: 'waiting for a ready instance to drop the role' });
      return;
    }
    const body = buildRoleJob(cluster, role, {
      action: 'drop',
      instances: targets.map((i) => i.name),
      hash: '',
      targets: '[]',
    });
    try {
      await this.batchApi.createNamespacedJob({ namespace, body });
      log.info({ instances: targets.map((i) => i.name) }, 'Dropping Firebird role');
    } catch (err) {
      if ((err as { code?: number })?.code !== 409) throw err;
    }
    await this.updateStatus(role, { phase: 'Dropping', jobName, message: `dropping on ${targets.map((i) => i.name).join(', ')}` });
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

  /** Adds or removes the drop-role finalizer */
  private async ensureFinalizer(role: FirebirdRole, wanted: boolean): Promise<void> {
    const finalizers = role.metadata.finalizers ?? [];
    if (finalizers.includes(ROLE_FINALIZER) === wanted) return;
    const next = wanted ? [...finalizers, ROLE_FINALIZER] : finalizers.filter((f) => f !== ROLE_FINALIZER);
    await this.customApi.patchNamespacedCustomObject({
      group: API_GROUP,
      version: API_VERSION,
      namespace: role.metadata.namespace ?? 'default',
      plural: 'firebirdroles',
      name: role.metadata.name,
      body: [
        // guards against overwriting finalizers added concurrently by someone else
        { op: 'test', path: '/metadata/resourceVersion', value: role.metadata.resourceVersion },
        { op: 'add', path: '/metadata/finalizers', value: next },
      ],
    });
    role.metadata.finalizers = next;
  }

  /** Replaces the status when it changed; undefined fields are dropped */
  private async updateStatus(role: FirebirdRole, update: FirebirdRoleStatus): Promise<void> {
    const merged = Object.fromEntries(
      Object.entries({ ...(role.status ?? {}), ...update }).filter(([, v]) => v !== undefined),
    );
    const canonical = (o: object) => JSON.stringify(Object.entries(o).sort(([a], [b]) => a.localeCompare(b)));
    if (role.status && canonical(role.status) === canonical(merged)) return;
    await this.customApi.patchNamespacedCustomObjectStatus({
      group: API_GROUP,
      version: API_VERSION,
      namespace: role.metadata.namespace ?? 'default',
      plural: 'firebirdroles',
      name: role.metadata.name,
      body: [{ op: 'replace' as const, path: '/status', value: merged }],
    });
    role.status = merged as FirebirdRoleStatus;
  }
}
