import crypto from 'crypto';
import {
  AppsV1Api,
  BatchV1Api,
  CoordinationV1Api,
  CoreV1Api,
  CustomObjectsApi,
  KubeConfig,
  NetworkingV1Api,
  PatchStrategy,
  PolicyV1Api,
  setHeaderOptions,
  V1MicroTime,
  V1StatefulSet,
} from '@kubernetes/client-node';
import { Logger } from 'pino';
import { buildBackupCronJob, buildJournalArchiveCronJob } from '../utils/backup';
import { recordClusterMetrics, recordReconcile } from '../utils/metrics';
import { EventReason, EventRecorder, EventType } from '../utils/events';
import { PRIMARY_RESTART_GRACE_SECONDS, operatorRollsPods, planRollingUpdate } from '../utils/rolling-update';
import { chooseBackupInstance } from '../utils/backup-target';
import {
  DEFAULT_FAILOVER_DELAY_SECONDS,
  TARGET_PRIMARY_ANNOTATION,
  buildFailoverJob,
  buildSwitchoverJob,
  failoverJobName,
  parseElection,
  switchoverJobName,
} from '../utils/switchover';
import {
  buildFencingJob,
  desiredFencedInstances,
  fencingJobName,
  planFencing,
} from '../utils/fencing';
import { logger } from '../utils/logger';
import {
  buildAutoSweepCronJob,
  buildCertificate,
  buildConfigMap,
  buildDiagnosticsCronJob,
  buildGrafanaDashboardConfigMap,
  buildHeadlessService,
  buildLease,
  buildNetworkPolicy,
  buildPodDisruptionBudget,
  buildPodMonitor,
  buildReplicaService,
  buildService,
  buildStatefulSet,
  autoSweepCronJobNeedsUpdate,
  configMapNeedsUpdate,
  cronJobNeedsUpdate,
  diagnosticsCronJobNeedsUpdate,
  networkPolicyNeedsUpdate,
  networkPolicyWireFormat,
  podDisruptionBudgetNeedsUpdate,
  primaryServiceSelector,
  readOnlyRoutingEnabled,
  replicaServiceSelector,
  statefulSetNeedsUpdate,
  withHibernation,
  CLUSTER_LABEL,
  clusterLabels,
  instancePodSelector,
  instancePods,
} from '../utils/resources';
import { REPLICATION_LAG_ANNOTATION, computeReadRouting, isPodReady, podRoutingLabelPatch } from '../utils/routing';
import { SegmentClient, computeLag, parseArchived, parsePosition, segmentRequest } from '../utils/replication-lag';
import { RESEED_ANNOTATION, RESEED_KEY, SEGMENT_PORT, instanceHost, replicationEnabled } from '../utils/replication';
import { planVolumeExpansion } from '../utils/storage';
import { validateClusterSpec } from '../utils/validation';
import {
  API_GROUP,
  API_VERSION,
  FirebirdCluster,
  FirebirdClusterCondition,
  FirebirdClusterStatus,
  ReplicaLagStatus,
  RollingUpdateStatus,
  SwitchoverStatus,
  RESOURCE_KIND,
  RESOURCE_PLURAL,
  VolumeStatus,
} from '../types';

/**
 * Request options for patches that send a whole desired object. The client defaults
 * to JSON Patch (an array of operations), which the API server rejects for object bodies.
 */
const MERGE_PATCH = setHeaderOptions('Content-Type', PatchStrategy.MergePatch);

/** Outcome of the switchover reconciliation */
interface SwitchoverResult {
  primaryPod: string;
  promote: Record<string, string>;
  demote: Record<string, string>;
  restart: string[];
  reseed: Record<string, string>;
  status?: SwitchoverStatus;
  primaryNotReadySince?: string;
}

/** Whether a switchover or failover is between its start and its completion */
function switchoverInFlight(state?: SwitchoverStatus): boolean {
  return state?.phase === 'Electing' || state?.phase === 'Stopping' || state?.phase === 'Promoting';
}

/** Event type and reason of a switchover / failover phase */
function switchoverEvent(status: SwitchoverStatus): [EventType, string] {
  const failover = status.kind === 'failover';
  switch (status.phase) {
    case 'Electing':
      return ['Warning', EventReason.FailoverStarted];
    case 'Stopping':
      return ['Normal', EventReason.SwitchoverStarted];
    case 'Promoting':
      return failover ? ['Warning', EventReason.FailingOver] : ['Normal', EventReason.SwitchoverPromoting];
    case 'Completed':
      return ['Normal', failover ? EventReason.FailoverCompleted : EventReason.SwitchoverCompleted];
    default:
      return ['Warning', failover ? EventReason.FailoverFailed : EventReason.SwitchoverFailed];
  }
}

/** Outcome of the fencing reconciliation */
interface FencingResult {
  /** Instances whose database is fenced (full shutdown applied) */
  fenced: string[];
  /** Instances with a fencing change in progress */
  pending: string[];
  /** Instances whose last fencing Job failed (retried on the next reconcile) */
  failed: string[];
}

/** Label selector string of the instance pods (status.selector, scale subresource) */
function podSelector(cluster: FirebirdCluster): string {
  return Object.entries(clusterLabels(cluster.metadata.name))
    .map(([k, v]) => `${k}=${v}`)
    .join(',');
}

/** Returns true for a Kubernetes API "not found" error */
function isNotFound(err: unknown): boolean {
  const e = err as { code?: number; statusCode?: number; response?: { statusCode?: number } };
  return e?.code === 404 || e?.statusCode === 404 || e?.response?.statusCode === 404;
}

/** Outcome of the read-only routing reconciliation */
interface ReadRoutingResult {
  primaryPod: string;
  readRoutablePods: string[];
  laggingReplicas: string[];
}

/**
 * FirebirdClusterController reconciles FirebirdCluster resources
 * to the desired state by managing StatefulSets, Services, PVCs, CronJobs, and PodMonitors.
 */
export class FirebirdClusterController {
  private readonly appsApi: AppsV1Api;
  private readonly batchApi: BatchV1Api;
  private readonly coordinationApi: CoordinationV1Api;
  private readonly coreApi: CoreV1Api;
  private readonly customApi: CustomObjectsApi;
  private readonly networkingApi: NetworkingV1Api;
  private readonly policyApi: PolicyV1Api;
  private readonly events: EventRecorder;

  constructor(
    kubeConfig: KubeConfig,
    private readonly segmentClient: SegmentClient = segmentRequest,
  ) {
    this.appsApi = kubeConfig.makeApiClient(AppsV1Api);
    this.batchApi = kubeConfig.makeApiClient(BatchV1Api);
    this.coordinationApi = kubeConfig.makeApiClient(CoordinationV1Api);
    this.coreApi = kubeConfig.makeApiClient(CoreV1Api);
    this.customApi = kubeConfig.makeApiClient(CustomObjectsApi);
    this.networkingApi = kubeConfig.makeApiClient(NetworkingV1Api);
    this.policyApi = kubeConfig.makeApiClient(PolicyV1Api);
    this.events = new EventRecorder(this.coreApi);
  }

  /**
   * Reconcile a FirebirdCluster resource.
   * This is the main entry point for processing cluster events.
   */
  async reconcile(cluster: FirebirdCluster): Promise<void> {
    const { name, namespace = 'default' } = cluster.metadata;
    const log = logger.child({ cluster: name, namespace });

    log.info('Reconciling FirebirdCluster');

    try {
      validateClusterSpec(cluster);
      desiredFencedInstances(cluster); // rejects a malformed fencedInstances annotation

      if (cluster.spec.suspended) {
        log.info('Cluster reconciliation is suspended');
        await this.updateStatus(cluster, {
          phase: 'Paused',
          phaseReason: 'Reconciliation suspended by spec.suspended',
          conditions: [
            this.makeCondition(
              'Paused',
              'True',
              'ClusterSuspended',
              'Reconciliation is suspended',
            ),
          ],
        });
        return;
      }

      await this.updateStatus(cluster, {
        phase: 'Creating',
        phaseReason: 'Reconciliation started',
      });

      const switchover = await this.reconcileSwitchover(cluster, await this.resolvePrimaryPod(cluster, log), log);
      const primaryPod = switchover.primaryPod;
      const reseed = await this.resolveReseeds(cluster, primaryPod, log);
      Object.assign(reseed.requests, switchover.reseed);
      const seedSourcePods = (await this.resolveSeedSources(cluster, primaryPod)).filter(
        (pod) => !(pod in reseed.requests) && !(pod in switchover.promote) && !(pod in switchover.demote),
      );
      await this.reconcileConfigMap(cluster, primaryPod, seedSourcePods, reseed.requests, log, switchover);
      // the ConfigMap lists the requests before the pods restart into their init containers
      for (const pod of [...new Set([...reseed.restart, ...switchover.restart])]) {
        log.info({ pod }, 'Restarting instance into its init container');
        try {
          await this.coreApi.deleteNamespacedPod({ name: pod, namespace });
        } catch (err) {
          if (!isNotFound(err)) throw err; // already gone
        }
      }
      // lag is published as pod annotations before read-only routing evaluates them
      const lag = await this.reconcileReplicationLag(cluster, primaryPod, switchover.status, log);
      // Label pods before (re)pointing service selectors at the routing labels
      const readRouting = await this.reconcileReadRouting(cluster, primaryPod, log);
      await this.reconcileHeadlessService(cluster, log);
      await this.reconcileService(cluster, log);
      const { readyInstances, superuserSecretHash, statefulSetExisted, statefulSet } =
        await this.reconcileStatefulSet(cluster, log);
      const volumes = statefulSetExisted
        ? await this.reconcileVolumeExpansion(cluster, log)
        : undefined;
      const fencing = await this.reconcileFencing(cluster, log);
      const busy = switchoverInFlight(switchover.status)
        ? `${switchover.status?.kind ?? 'switchover'} in progress`
        : Object.keys(reseed.requests).length > 0 || reseed.restart.length > 0 || switchover.restart.length > 0
          ? 'instances are being re-seeded or restarted'
          : undefined;
      const rollingUpdate = statefulSetExisted
        ? await this.reconcileRollingUpdate(cluster, statefulSet, primaryPod, fencing.fenced, busy, switchover.status, log)
        : undefined;

      if (cluster.spec.replication?.enabled) {
        await this.reconcileReplicaService(cluster, log);
      }

      await this.reconcileJournalArchiveCronJob(cluster, primaryPod, log);
      await this.reconcileLease(cluster, log);
      await this.reconcileCertificate(cluster, log);
      await this.reconcilePodDisruptionBudget(cluster, log);
      await this.reconcileNetworkPolicy(cluster, log);
      await this.reconcileBackupCronJob(cluster, primaryPod, log);
      await this.reconcileAutoSweepCronJob(cluster, primaryPod, log);
      await this.reconcileDiagnosticsCronJob(cluster, primaryPod, log);
      await this.reconcilePodMonitor(cluster, log);
      await this.reconcileGrafanaDashboard(cluster, log);

      if (cluster.spec.hibernated) {
        const hibernated: Partial<FirebirdClusterStatus> = {
          phase: 'Hibernated',
          phaseReason: readyInstances > 0
            ? `Hibernating: waiting for ${readyInstances} pod(s) to terminate`
            : 'Cluster is hibernated; PVCs are retained',
          instances: cluster.spec.instances,
          readyInstances,
          replicationStatus: undefined,
          superuserSecretHash,
          fencedInstances: fencing.fenced,
          selector: podSelector(cluster),
          ...(volumes ? { volumes } : {}),
          conditions: [
            this.makeCondition('Hibernated', 'True', 'HibernationRequested', 'spec.hibernated is true'),
            this.makeCondition('Ready', 'False', 'Hibernated', 'Cluster is hibernated'),
          ],
        };
        await this.updateStatus(cluster, hibernated);
        recordClusterMetrics(cluster, hibernated);
        recordReconcile(cluster, 'success');
        log.info('Cluster is hibernated');
        return;
      }

      const targetInstances = cluster.spec.instances;
      // fenced instances are expected to be not ready
      const fencedCount = fencing.fenced.filter((pod) => Number(pod.slice(name.length + 1)) < targetInstances).length;
      const expectedReady = targetInstances - fencedCount;
      const isReady = readyInstances >= expectedReady;

      const replicationStatus = cluster.spec.replication?.enabled
        ? {
            primaryPod,
            activeReplicas: Math.max(0, readyInstances - 1),
            ...(readRouting
              ? {
                  readRoutablePods: readRouting.readRoutablePods,
                  laggingReplicas: readRouting.laggingReplicas,
                }
              : {}),
            ...(lag ?? {}),
          }
        : undefined;

      const status: Partial<FirebirdClusterStatus> = {
        phase: isReady ? (rollingUpdate ? 'Updating' : 'Running') : 'Creating',
        phaseReason: isReady
          ? rollingUpdate
            ? `Rolling update: ${rollingUpdate.message}`
            : fencedCount > 0
            ? `All resources reconciled; ${fencedCount} instance(s) fenced`
            : 'All resources reconciled successfully'
          : `Waiting for pods: ${readyInstances}/${expectedReady} ready`,
        instances: targetInstances,
        readyInstances,
        replicationStatus,
        superuserSecretHash,
        fencedInstances: fencing.fenced,
        selector: podSelector(cluster),
        reseedingInstances: Object.keys(reseed.requests).sort(),
        rollingUpdate,
        ...(switchover.status ? { switchover: switchover.status } : {}),
        primaryNotReadySince: switchover.primaryNotReadySince,
        ...(volumes ? { volumes } : {}),
        conditions: [
          this.makeCondition(
            'Ready',
            isReady ? 'True' : 'False',
            isReady ? 'ClusterReady' : 'PodsNotReady',
            isReady ? 'Cluster is ready' : `${readyInstances}/${expectedReady} ready`,
          ),
          this.fencingCondition(fencing),
          this.makeCondition(
            'Progressing',
            isReady ? 'False' : 'True',
            isReady ? 'ReconciliationComplete' : 'PodsStarting',
            isReady ? 'Reconciliation completed' : 'Waiting for StatefulSet pods',
          ),
        ],
      };
      await this.updateStatus(cluster, status);
      recordClusterMetrics(cluster, status);
      recordReconcile(cluster, 'success');

      log.info('Reconciliation complete');
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      log.error({ err }, 'Reconciliation failed');
      recordReconcile(cluster, 'error');
      await this.event(cluster, 'Warning', EventReason.ReconcileFailed, message);

      await this.updateStatus(cluster, {
        phase: 'Degraded',
        phaseReason: message,
        conditions: [
          this.makeCondition('Ready', 'False', 'ReconciliationFailed', message),
          this.makeCondition('Degraded', 'True', 'ReconciliationFailed', message),
        ],
      }).catch((statusErr) => {
        log.error({ err: statusErr }, 'Failed to update status after error');
      });

      throw err;
    }
  }

  /** Reconcile the headless service used by the StatefulSet */
  private async reconcileHeadlessService(
    cluster: FirebirdCluster,
    log: Logger,
  ): Promise<void> {
    const { name, namespace = 'default' } = cluster.metadata;
    const headlessName = `${name}-headless`;
    const desired = buildHeadlessService(cluster);

    try {
      await this.coreApi.readNamespacedService({ name: headlessName, namespace });
      log.debug('Headless service already exists, skipping');
    } catch {
      log.info('Creating headless service');
      await this.coreApi.createNamespacedService({
        namespace,
        body: desired,
      });
    }
  }

  /** Reconcile the primary service for the cluster */
  private async reconcileService(
    cluster: FirebirdCluster,
    log: Logger,
  ): Promise<void> {
    const { name, namespace = 'default' } = cluster.metadata;
    const desired = buildService(cluster);

    let existing;
    try {
      existing = await this.coreApi.readNamespacedService({ name, namespace });
    } catch {
      log.info('Creating cluster service');
      await this.coreApi.createNamespacedService({
        namespace,
        body: desired,
      });
      return;
    }
    await this.reconcileServiceSelector(name, namespace, existing?.spec?.selector, primaryServiceSelector(cluster), log);
  }

  /** Reconcile the read-replica service for replication-enabled clusters */
  private async reconcileReplicaService(
    cluster: FirebirdCluster,
    log: Logger,
  ): Promise<void> {
    const { name, namespace = 'default' } = cluster.metadata;
    const replicaName = `${name}-replica`;
    const desired = buildReplicaService(cluster);

    let existing;
    try {
      existing = await this.coreApi.readNamespacedService({ name: replicaName, namespace });
    } catch {
      log.info('Creating replica service');
      await this.coreApi.createNamespacedService({
        namespace,
        body: desired,
      });
      return;
    }
    await this.reconcileServiceSelector(replicaName, namespace, existing?.spec?.selector, replicaServiceSelector(cluster), log);
  }

  /** Patch a Service selector when it drifts from the desired routing selector */
  private async reconcileServiceSelector(
    serviceName: string,
    namespace: string,
    current: Record<string, string> | undefined,
    desired: Record<string, string>,
    log: Logger,
  ): Promise<void> {
    if (!current || JSON.stringify(sortKeys(current)) === JSON.stringify(sortKeys(desired))) {
      log.debug({ service: serviceName }, 'Service already exists, skipping');
      return;
    }
    log.info({ service: serviceName }, 'Updating service selector');
    await this.coreApi.patchNamespacedService({
      name: serviceName,
      namespace,
      body: [{ op: 'replace', path: '/spec/selector', value: desired }],
    });
  }

  /** The current primary instance: the leader Lease holder, or ordinal 0 before one exists */
  private async resolvePrimaryPod(cluster: FirebirdCluster, log: Logger): Promise<string> {
    const { name, namespace = 'default' } = cluster.metadata;
    try {
      const lease = await this.coordinationApi.readNamespacedLease({ name: `${name}-lease`, namespace });
      if (lease.spec?.holderIdentity) return lease.spec.holderIdentity;
    } catch {
      log.debug('Leader lease not found, assuming ordinal 0 is primary');
    }
    return `${name}-0`;
  }

  /**
   * Planned switchover (targetPrimary annotation): see utils/switchover.ts. Returns the primary to
   * use for this reconcile, the promote / demote directives, pods to restart and replicas to
   * re-seed.
   */
  private async reconcileSwitchover(cluster: FirebirdCluster, primaryPod: string, log: Logger): Promise<SwitchoverResult> {
    const { name, namespace = 'default' } = cluster.metadata;
    const result: SwitchoverResult = { primaryPod, promote: {}, demote: {}, restart: [], reseed: {} };
    result.status = cluster.status?.switchover;
    if (!replicationEnabled(cluster) || cluster.spec.hibernated) return result;

    // the switchover state machine acts on the latest stored state, never on a stale watch copy
    let current: FirebirdCluster = cluster;
    try {
      const fresh = (await this.customApi.getNamespacedCustomObject({
        group: API_GROUP,
        version: API_VERSION,
        namespace,
        plural: RESOURCE_PLURAL,
        name,
      })) as FirebirdCluster;
      if (fresh?.kind === 'FirebirdCluster' && fresh.metadata?.name === name) current = fresh;
    } catch {
      // not readable: use the object this reconcile started with
    }
    const state = current.status?.switchover;
    const desired = current.metadata.annotations?.[TARGET_PRIMARY_ANNOTATION]?.trim();
    const inFlight = state && (state.phase === 'Electing' || state.phase === 'Stopping' || state.phase === 'Promoting');
    const failover = cluster.spec.replication?.failover;
    result.status = state;
    // the unavailability timer only runs while automatic failover is enabled
    result.primaryNotReadySince = failover?.enabled ? current.status?.primaryNotReadySince : undefined;
    const wantsSwitchover =
      desired !== undefined &&
      desired !== '' &&
      desired !== primaryPod &&
      !(state?.phase === 'Failed' && state.kind !== 'failover' && state.target === desired && state.from === primaryPod);
    if (!inFlight && !wantsSwitchover && !failover?.enabled) return result;

    const pods = {
      items: instancePods(
        (await this.coreApi.listNamespacedPod({ namespace, labelSelector: instancePodSelector(name) })).items,
        name,
      ),
    };
    const podOf = (pod: string) => pods.items.find((p) => p.metadata?.name === pod);
    const jobName = switchoverJobName(cluster);
    const electionJob = failoverJobName(cluster);
    const now = new Date().toISOString();
    const fenced = current.status?.fencedInstances ?? [];
    const primaryReady = Boolean(podOf(primaryPod) && isPodReady(podOf(primaryPod)!));
    // each phase change is stored before it is acted upon, so a failed or concurrent reconcile
    // resumes the phase instead of repeating the previous one
    const persist = async (status: SwitchoverStatus, reason?: string) => {
      await this.customApi.patchNamespacedCustomObjectStatus({
        group: API_GROUP,
        version: API_VERSION,
        namespace,
        plural: RESOURCE_PLURAL,
        name,
        body: [{ op: 'add', path: '/status/switchover', value: status }],
      });
      result.status = status;
      const [type, defaultReason] = switchoverEvent(status);
      await this.event(cluster, type, reason ?? defaultReason, status.message ?? `${status.phase}`);
    };

    // Automatic failover: the primary has not been ready for failover.delaySeconds
    if (!inFlight && failover?.enabled) {
      const restart = current.status?.rollingUpdate?.primaryRestart;
      const graceMs = Math.max(PRIMARY_RESTART_GRACE_SECONDS, failover.delaySeconds ?? DEFAULT_FAILOVER_DELAY_SECONDS) * 1000;
      const plannedRestart = restart?.pod === primaryPod && Date.now() - Date.parse(restart.time) < graceMs;
      if (primaryReady || fenced.includes(primaryPod) || plannedRestart) {
        // a fenced primary is never failed over, a primary restarted by a rolling update not yet
        result.primaryNotReadySince = undefined;
      } else {
        const since = current.status?.primaryNotReadySince ?? now;
        result.primaryNotReadySince = since;
        if (!current.status?.primaryNotReadySince) {
          const delay = failover.delaySeconds ?? DEFAULT_FAILOVER_DELAY_SECONDS;
          await this.event(cluster, 'Warning', EventReason.PrimaryNotReady, `primary ${primaryPod} is not ready; failover in ${delay}s unless it recovers`);
        }
        const delayMs = (failover.delaySeconds ?? DEFAULT_FAILOVER_DELAY_SECONDS) * 1000;
        const recentFailure =
          state?.kind === 'failover' && state.phase === 'Failed' && state.from === primaryPod &&
          Date.now() - Date.parse(state.completionTime ?? now) < delayMs;
        if (Date.now() - Date.parse(since) >= delayMs && !recentFailure) {
          const candidates = pods.items
            .filter((p) => p.metadata?.name !== primaryPod && isPodReady(p) && !fenced.includes(p.metadata?.name ?? ''))
            .map((p) => p.metadata!.name!)
            .sort();
          if (candidates.length === 0) {
            log.warn({ primary: primaryPod }, 'Primary unavailable and no ready replica to promote');
            return result;
          }
          log.warn({ primary: primaryPod, since, candidates }, 'Primary unavailable: starting automatic failover');
          await persist({
            kind: 'failover',
            target: '',
            from: primaryPod,
            phase: 'Electing',
            message: `primary unavailable since ${since}: electing the most advanced replica`,
            startTime: now,
          });
          try {
            await this.batchApi.createNamespacedJob({ namespace, body: buildFailoverJob(cluster, candidates) });
          } catch (err) {
            if ((err as { code?: number })?.code !== 409) throw err;
          }
          return result;
        }
      }
    }

    if (!inFlight && !wantsSwitchover) return result;

    if (!inFlight) {
      const target = desired!;
      const index = Number(target.startsWith(`${name}-`) ? target.slice(name.length + 1) : NaN);
      const fenced = current.status?.fencedInstances ?? [];
      const problem = !Number.isInteger(index) || index < 0 || index >= cluster.spec.instances
        ? `${target} is not an instance of this cluster`
        : !podOf(target) || !isPodReady(podOf(target)!)
          ? `${target} is not ready`
          : !podOf(primaryPod) || !isPodReady(podOf(primaryPod)!)
            ? `the current primary ${primaryPod} is not ready`
            : fenced.includes(target) || fenced.includes(primaryPod)
              ? 'the target or the current primary is fenced'
              : undefined;
      if (problem) {
        log.warn({ target, problem }, 'Switchover refused');
        await persist({ target, from: primaryPod, phase: 'Failed', message: problem, startTime: now, completionTime: now });
        return result;
      }
      const replicas = pods.items
        .filter((p) => p.metadata?.name && p.metadata.name !== primaryPod && p.metadata.name !== target && isPodReady(p))
        .map((p) => p.metadata!.name!)
        .sort();
      log.info({ from: primaryPod, target, replicas }, 'Starting planned switchover');
      await persist({ target, from: primaryPod, phase: 'Stopping', message: 'stopping writes on the primary', startTime: now });
      try {
        await this.batchApi.createNamespacedJob({
          namespace,
          body: buildSwitchoverJob(cluster, { from: primaryPod, target, replicas }),
        });
      } catch (err) {
        if ((err as { code?: number })?.code !== 409) throw err;
      }
      return result;
    }

    let phase = state!;
    if (phase.phase === 'Electing') {
      if (primaryReady) {
        // nothing was changed yet: the primary came back, discard the election
        log.info({ primary: phase.from }, 'Primary recovered; automatic failover cancelled');
        await persist(
          { ...phase, phase: 'Failed', message: `${phase.from} recovered before a replica was promoted`, completionTime: now },
          EventReason.FailoverCancelled,
        );
        await this.batchApi.deleteNamespacedJob({ name: electionJob, namespace, propagationPolicy: 'Background' }).catch(() => undefined);
        result.primaryNotReadySince = undefined;
        return result;
      }
      let job;
      try {
        job = await this.batchApi.readNamespacedJob({ name: electionJob, namespace });
      } catch (err) {
        if (!isNotFound(err)) throw err;
      }
      const candidates = pods.items
        .filter((p) => p.metadata?.name !== phase.from && isPodReady(p) && !fenced.includes(p.metadata?.name ?? ''))
        .map((p) => p.metadata!.name!)
        .sort();
      if (!job) {
        if (candidates.length) {
          await this.batchApi.createNamespacedJob({ namespace, body: buildFailoverJob(cluster, candidates) });
        }
        return result;
      }
      const conditions = job.status?.conditions ?? [];
      const failed = conditions.some((c) => c.type === 'Failed' && c.status === 'True');
      const complete = conditions.some((c) => c.type === 'Complete' && c.status === 'True');
      if (!failed && !complete) return result;
      let election;
      if (complete) {
        const jobPods = await this.coreApi.listNamespacedPod({ namespace, labelSelector: `job-name=${electionJob}` });
        const message = jobPods.items
          .map((p) => p.status?.containerStatuses?.[0]?.state?.terminated?.message ?? '')
          .find((m) => m.includes('target='));
        election = message ? parseElection(message) : undefined;
      }
      const target = election ? election.target.split('.')[0] : undefined;
      if (!election || !target || !podOf(target)) {
        log.warn({ primary: phase.from }, 'Failover election failed');
        await persist({ ...phase, phase: 'Failed', message: `election Job ${electionJob} failed (see its logs)`, completionTime: now });
        await this.batchApi.deleteNamespacedJob({ name: electionJob, namespace, propagationPolicy: 'Background' });
        return result;
      }

      // replicas behind the elected one, unready ones and the old primary are re-seeded
      const reseed: Record<string, string> = {};
      for (const pod of pods.items) {
        const podName = pod.metadata!.name!;
        if (podName === target || podName === phase.from) continue;
        const seq = election.positions[instanceHost(cluster, podName)];
        if (seq === undefined || seq < election.sequence) reseed[podName] = pod.metadata?.uid ?? '';
      }
      reseed[phase.from] = podOf(phase.from)?.metadata?.uid ?? `failover-${Date.parse(now)}`;
      phase = {
        ...phase,
        target,
        phase: 'Promoting',
        message: `promoting ${target} (applied up to segment ${election.sequence}); ${phase.from} will be re-seeded`,
        targetToken: podOf(target)?.metadata?.uid ?? '',
        reseed,
      };
      await persist(phase);
      await this.batchApi.deleteNamespacedJob({ name: electionJob, namespace, propagationPolicy: 'Background' });
      // a stale targetPrimary annotation must not switch back to the failed primary
      await this.customApi.patchNamespacedCustomObject(
        {
          group: API_GROUP,
          version: API_VERSION,
          namespace,
          plural: RESOURCE_PLURAL,
          name,
          body: { metadata: { annotations: { [TARGET_PRIMARY_ANNOTATION]: target } } },
        },
        MERGE_PATCH,
      );
      result.primaryNotReadySince = undefined;
      log.warn({ primary: target, failed: phase.from, sequence: election.sequence, reseed: Object.keys(reseed) }, 'Failing over');
    }

    if (phase.phase === 'Stopping') {
      let job;
      try {
        job = await this.batchApi.readNamespacedJob({ name: jobName, namespace });
      } catch (err) {
        if (!isNotFound(err)) throw err;
      }
      if (!job) {
        // lost (e.g. deleted by hand): the Job is idempotent
        await this.batchApi.createNamespacedJob({
          namespace,
          body: buildSwitchoverJob(cluster, { from: phase.from, target: phase.target, replicas: [] }),
        });
        return result;
      }
      const conditions = job.status?.conditions ?? [];
      if (conditions.some((c) => c.type === 'Failed' && c.status === 'True')) {
        log.warn({ target: phase.target }, 'Switchover failed; bringing the primary back online');
        await persist({
          ...phase,
          phase: 'Failed',
          message: `switchover Job ${jobName} failed (see its logs); ${phase.from} stays primary`,
          completionTime: now,
        });
        await this.batchApi.deleteNamespacedJob({ name: jobName, namespace, propagationPolicy: 'Background' });
        if (!(current.status?.fencedInstances ?? []).includes(phase.from)) {
          try {
            await this.batchApi.createNamespacedJob({ namespace, body: buildFencingJob(cluster, phase.from, 'unfence') });
          } catch (err) {
            if ((err as { code?: number })?.code !== 409) throw err;
          }
        }
        return result;
      }
      if (!conditions.some((c) => c.type === 'Complete' && c.status === 'True')) return result;

      // every ready replica has applied the old primary's last segment: move the primary
      const lagging: Record<string, string> = {};
      for (const pod of pods.items) {
        const podName = pod.metadata?.name;
        if (!podName || podName === phase.target || podName === phase.from || isPodReady(pod)) continue;
        lagging[podName] = pod.metadata?.uid ?? ''; // may have missed segments: re-seeded
      }
      phase = {
        ...phase,
        phase: 'Promoting',
        message: 'promoting the target and demoting the old primary',
        targetToken: podOf(phase.target)?.metadata?.uid ?? '',
        fromToken: podOf(phase.from)?.metadata?.uid ?? '',
        ...(Object.keys(lagging).length ? { reseed: lagging } : {}),
      };
      await persist(phase);
      await this.batchApi.deleteNamespacedJob({ name: jobName, namespace, propagationPolicy: 'Background' });
      log.info({ primary: phase.target, demoted: phase.from }, 'Promoting the switchover target');
    }

    // Promoting (idempotent): Lease, directives and restarts of pods still running as before
    result.primaryPod = phase.target;
    const lease = await this.coordinationApi.readNamespacedLease({ name: `${name}-lease`, namespace }).catch(() => undefined);
    if (lease?.spec?.holderIdentity !== phase.target) {
      await this.coordinationApi.patchNamespacedLease({
        name: `${name}-lease`,
        namespace,
        body: [
          { op: 'replace', path: '/spec/holderIdentity', value: phase.target },
          { op: 'replace', path: '/spec/renewTime', value: new V1MicroTime() },
        ],
      });
    }
    const restarted = (pod: string, token?: string) => {
      const p = podOf(pod);
      return Boolean(p && p.metadata?.uid !== token && isPodReady(p));
    };
    const isFailover = phase.kind === 'failover';
    if (restarted(phase.target, phase.targetToken) && (isFailover || restarted(phase.from, phase.fromToken))) {
      log.info({ primary: phase.target }, 'Switchover completed');
      await persist({ ...phase, phase: 'Completed', message: `${phase.target} is the primary`, completionTime: now });
      return result;
    }
    result.promote = { [phase.target]: phase.targetToken ?? '' };
    // after a failover the old primary diverged (its unshipped transactions): it is re-seeded
    result.demote = isFailover ? {} : { [phase.from]: phase.fromToken ?? '' };
    // pods still running with the UID they had when the primary moved (not yet restarted)
    const stillOld = (pod: string, token?: string) => {
      const p = podOf(pod);
      return Boolean(p && p.metadata?.uid === token && !p.metadata?.deletionTimestamp);
    };
    const restartable: Array<[string, string | undefined]> = isFailover
      ? [[phase.target, phase.targetToken]]
      : [[phase.target, phase.targetToken], [phase.from, phase.fromToken]];
    for (const [pod, token] of restartable) {
      if (stillOld(pod, token)) result.restart.push(pod);
    }
    for (const [pod, token] of Object.entries(phase.reseed ?? {})) {
      result.reseed[pod] = token;
      if (stillOld(pod, token)) result.restart.push(pod);
    }
    result.status = phase;
    return result;
  }

  /**
   * Re-seed requests: replicas annotated with RESEED_ANNOTATION get a token (their pod UID) in the
   * ConfigMap and are restarted; replication-init then discards the database and seeds again.
   * A request stays listed until the instance is ready again as a new pod.
   */
  private async resolveReseeds(
    cluster: FirebirdCluster,
    primaryPod: string,
    log: Logger,
  ): Promise<{ requests: Record<string, string>; restart: string[] }> {
    const result = { requests: {} as Record<string, string>, restart: [] as string[] };
    if (!replicationEnabled(cluster) || cluster.spec.hibernated) return result;
    const { name, namespace = 'default' } = cluster.metadata;

    let current = '';
    try {
      const cm = await this.coreApi.readNamespacedConfigMap({ name: `${name}-config`, namespace });
      current = cm.data?.[RESEED_KEY] ?? '';
    } catch {
      // not created yet
    }
    const pods = { items: instancePods((await this.coreApi.listNamespacedPod({ namespace, labelSelector: instancePodSelector(name) })).items, name) };
    const byName = new Map(pods.items.map((p) => [p.metadata?.name ?? '', p]));

    for (const line of current.split('\n')) {
      const [pod, token] = line.trim().split(/\s+/);
      if (!pod || !token) continue;
      const podObj = byName.get(pod);
      // done once a new pod (another UID) is ready: its init container re-seeded it
      if (podObj && podObj.metadata?.uid !== token && isPodReady(podObj)) {
        log.info({ pod }, 'Replica re-seeded');
        await this.event(cluster, 'Normal', EventReason.ReseedCompleted, `${pod} re-seeded and ready`);
        continue;
      }
      if (Number(pod.slice(name.length + 1)) >= cluster.spec.instances) continue; // scaled away
      result.requests[pod] = token;
    }

    for (const pod of pods.items) {
      const podName = pod.metadata?.name;
      if (!podName || pod.metadata?.annotations?.[RESEED_ANNOTATION] !== 'true' || pod.metadata.deletionTimestamp) continue;
      if (podName === primaryPod) {
        log.warn({ pod: podName }, 'Ignoring the re-seed annotation on the primary');
        continue;
      }
      if (result.requests[podName] === pod.metadata.uid) continue; // already requested, restart pending
      result.requests[podName] = pod.metadata.uid ?? '';
      result.restart.push(podName);
      await this.event(cluster, 'Normal', EventReason.ReseedStarted, `re-seeding ${podName} (reseed annotation)`);
    }
    return result;
  }

  /**
   * Ready replicas that can serve seed copies to new replicas, so seeding does not lock or
   * load the primary (see ISSUES.md, issue 2).
   */
  private async resolveSeedSources(cluster: FirebirdCluster, primaryPod: string): Promise<string[]> {
    if (!replicationEnabled(cluster) || cluster.spec.hibernated) return [];
    const { name, namespace = 'default' } = cluster.metadata;
    const pods = {
      items: instancePods(
        (await this.coreApi.listNamespacedPod({ namespace, labelSelector: instancePodSelector(name) })).items,
        name,
      ),
    };
    return pods.items
      .filter((pod) => pod.metadata?.name && pod.metadata.name !== primaryPod && isPodReady(pod))
      .map((pod) => pod.metadata!.name!)
      .sort();
  }

  /**
   * Label cluster pods with their role and read-routability so that the
   * primary and `-replica` Services route traffic based on readiness and replication lag.
   */
  private async reconcileReadRouting(
    cluster: FirebirdCluster,
    primaryPod: string,
    log: Logger,
  ): Promise<ReadRoutingResult | undefined> {
    if (!readOnlyRoutingEnabled(cluster) || cluster.spec.hibernated) return undefined;
    const { name, namespace = 'default' } = cluster.metadata;

    const pods = {
      items: instancePods(
        (await this.coreApi.listNamespacedPod({ namespace, labelSelector: instancePodSelector(name) })).items,
        name,
      ),
    };
    const plan = computeReadRouting(pods.items, primaryPod, cluster.spec.replication!.readOnlyRouting!);

    for (const decision of plan.decisions) {
      const pod = pods.items.find((p) => p.metadata?.name === decision.name)!;
      const patch = podRoutingLabelPatch(pod, decision);
      if (patch.length === 0) continue;
      log.info({ pod: decision.name, role: decision.role, readRoutable: decision.readRoutable }, 'Updating pod routing labels');
      await this.coreApi.patchNamespacedPod({ name: decision.name, namespace, body: patch });
    }

    if (plan.laggingReplicas.length > 0) {
      log.warn({ laggingReplicas: plan.laggingReplicas }, 'Replicas excluded from read-only routing due to replication lag');
    }
    const wasLagging = cluster.status?.replicationStatus?.laggingReplicas ?? [];
    for (const pod of plan.laggingReplicas.filter((p) => !wasLagging.includes(p))) {
      await this.event(cluster, 'Warning', EventReason.ReplicaLagging, `${pod} excluded from read-only routing: replication lag`);
    }

    return { primaryPod, readRoutablePods: plan.readRoutablePods, laggingReplicas: plan.laggingReplicas };
  }

  /**
   * Expand instance PVCs when spec.storage.size grows. StatefulSet volumeClaimTemplates
   * are immutable, so each existing PVC is patched in place (requires a StorageClass
   * with allowVolumeExpansion). Returns undefined when PVCs cannot be listed.
   */
  private async reconcileVolumeExpansion(
    cluster: FirebirdCluster,
    log: Logger,
  ): Promise<VolumeStatus[] | undefined> {
    const { name, namespace = 'default' } = cluster.metadata;
    const desiredSize = cluster.spec.storage.size;

    let pvcs;
    try {
      pvcs = await this.coreApi.listNamespacedPersistentVolumeClaim({
        namespace,
        labelSelector: `${CLUSTER_LABEL}=${name}`,
      });
    } catch (err) {
      log.warn({ err }, 'Unable to list PersistentVolumeClaims for volume expansion');
      return undefined;
    }

    const pattern = new RegExp(`^firebird-data-${name}-\\d+$`);
    const statuses: VolumeStatus[] = [];
    for (const pvc of pvcs.items.filter((p) => pattern.test(p.metadata?.name ?? ''))) {
      const plan = planVolumeExpansion(pvc, desiredSize);
      const previous = cluster.status?.volumes?.find((v) => v.name === plan.status.name)?.state;
      if (plan.expand) {
        log.info({ pvc: plan.status.name, size: desiredSize }, 'Expanding PersistentVolumeClaim');
        try {
          await this.coreApi.patchNamespacedPersistentVolumeClaim({
            name: plan.status.name,
            namespace,
            body: [{ op: 'replace', path: '/spec/resources/requests/storage', value: desiredSize }],
          });
        } catch (err) {
          const message = err instanceof Error ? err.message : String(err);
          log.warn({ err, pvc: plan.status.name }, 'PersistentVolumeClaim expansion rejected');
          if (previous !== 'ResizeFailed') {
            await this.event(cluster, 'Warning', EventReason.VolumeResizeFailed, `${plan.status.name}: ${message}`);
          }
          statuses.push({
            ...plan.status,
            requestedSize: pvc.spec?.resources?.requests?.storage,
            state: 'ResizeFailed',
            message,
          });
          continue;
        }
        await this.event(cluster, 'Normal', EventReason.VolumeResizing, `${plan.status.name}: expanding to ${desiredSize}`);
      } else if (plan.status.state === 'ShrinkRejected') {
        log.warn({ pvc: plan.status.name }, plan.status.message);
        if (previous !== 'ShrinkRejected') {
          await this.event(cluster, 'Warning', EventReason.VolumeResizeFailed, `${plan.status.name}: ${plan.status.message}`);
        }
      }
      statuses.push(plan.status);
    }

    return statuses.sort((a, b) => a.name.localeCompare(b.name, undefined, { numeric: true }));
  }

  /** Reconcile the StatefulSet for the cluster and return ready replica count and secret hash */
  private async reconcileStatefulSet(
    cluster: FirebirdCluster,
    log: Logger,
  ): Promise<{
    readyInstances: number;
    superuserSecretHash?: string;
    statefulSetExisted: boolean;
    statefulSet: V1StatefulSet;
  }> {
    const { name, namespace = 'default' } = cluster.metadata;
    let superuserSecretHash: string | undefined;

    if (cluster.spec.superuserSecret?.name) {
      try {
        const secret = await this.coreApi.readNamespacedSecret({
          name: cluster.spec.superuserSecret.name,
          namespace,
        });
        const dataStr = JSON.stringify(secret.data ?? {});
        superuserSecretHash = crypto.createHash('sha256').update(dataStr).digest('hex');
      } catch {
        log.debug('Superuser secret not found or unreadable during StatefulSet build');
      }
    }

    const desired = buildStatefulSet(cluster, { superuserSecretHash });

    let existing;
    try {
      const response = await this.appsApi.readNamespacedStatefulSet({ name, namespace });
      existing = response;
    } catch {
      log.info('Creating StatefulSet');
      const created = await this.appsApi.createNamespacedStatefulSet({
        namespace,
        body: desired,
      });
      return {
        readyInstances: created.status?.readyReplicas ?? 0,
        superuserSecretHash,
        statefulSetExisted: false,
        statefulSet: created,
      };
    }

    let current = existing;
    if (statefulSetNeedsUpdate(existing, desired)) {
      log.info('Updating StatefulSet');
      // volumeClaimTemplates are immutable; storage growth is handled by PVC expansion
      if (desired.spec && existing.spec?.volumeClaimTemplates) {
        desired.spec.volumeClaimTemplates = existing.spec.volumeClaimTemplates;
      }
      // a merge patch keeps the existing rollingUpdate settings, which OnDelete rejects
      const body =
        desired.spec?.updateStrategy?.type === 'OnDelete'
          ? { ...desired, spec: { ...desired.spec, updateStrategy: { type: 'OnDelete', rollingUpdate: null } } }
          : desired;
      current = await this.appsApi.patchNamespacedStatefulSet({
        name,
        namespace,
        body,
      }, MERGE_PATCH);
    } else {
      log.debug('StatefulSet is up to date, skipping');
    }

    return {
      readyInstances: current.status?.readyReplicas ?? 0,
      superuserSecretHash,
      statefulSetExisted: true,
      statefulSet: current,
    };
  }

  /**
   * Rolling update with the primary last (utils/rolling-update.ts): restarts at most one outdated
   * instance per reconcile, or asks for a switchover to an updated replica.
   */
  private async reconcileRollingUpdate(
    cluster: FirebirdCluster,
    statefulSet: V1StatefulSet,
    primaryPod: string,
    fenced: string[],
    busy: string | undefined,
    lastSwitchover: SwitchoverStatus | undefined,
    log: Logger,
  ): Promise<RollingUpdateStatus | undefined> {
    const { name, namespace = 'default' } = cluster.metadata;
    if (!operatorRollsPods(cluster) || cluster.spec.hibernated) return undefined;
    const pods = instancePods(
      (await this.coreApi.listNamespacedPod({ namespace, labelSelector: instancePodSelector(name) })).items,
      name,
    );
    const plan = planRollingUpdate({ cluster, statefulSet, pods, primaryPod, fenced, busy, lastSwitchover });
    // the StatefulSet controller has not observed the latest template yet: keep the last status
    if (!plan) return cluster.status?.rollingUpdate;
    const primary = pods.find((p) => p.metadata?.name === primaryPod);
    // automatic failover leaves the restarted primary alone until it is ready again
    let primaryRestart = cluster.status?.rollingUpdate?.primaryRestart;
    if (primaryRestart && (primaryRestart.pod !== primaryPod || (primary && isPodReady(primary) && primary.metadata?.uid !== primaryRestart.uid))) {
      primaryRestart = undefined;
    }
    if (plan.outdated.length === 0) {
      if (cluster.status?.rollingUpdate && !primaryRestart) {
        await this.event(cluster, 'Normal', EventReason.RollingUpdateCompleted, `all instances run revision ${plan.revision}`);
      }
      return primaryRestart
        ? { revision: plan.revision, outdatedInstances: [], message: `waiting for ${primaryPod} to be ready`, primaryRestart }
        : undefined;
    }

    if (plan.restart || plan.switchoverTo) {
      await this.event(cluster, 'Normal', EventReason.RollingUpdate, plan.message);
    }
    if (plan.restart) {
      log.info({ pod: plan.restart, revision: plan.revision }, 'Rolling update: restarting instance');
      if (plan.restart === primaryPod) {
        primaryRestart = { pod: primaryPod, uid: primary?.metadata?.uid ?? '', time: new Date().toISOString() };
      }
      try {
        await this.coreApi.deleteNamespacedPod({ name: plan.restart, namespace });
      } catch (err) {
        if (!isNotFound(err)) throw err;
      }
    } else if (plan.switchoverTo) {
      log.info({ from: primaryPod, target: plan.switchoverTo }, 'Rolling update: switching over to update the primary');
      await this.customApi.patchNamespacedCustomObject(
        {
          group: API_GROUP,
          version: API_VERSION,
          namespace,
          plural: RESOURCE_PLURAL,
          name,
          body: { metadata: { annotations: { [TARGET_PRIMARY_ANNOTATION]: plan.switchoverTo } } },
        },
        MERGE_PATCH,
      );
    } else {
      log.debug({ outdated: plan.outdated }, plan.message);
    }
    return {
      revision: plan.revision,
      outdatedInstances: plan.outdated,
      message: plan.message,
      ...(primaryRestart ? { primaryRestart } : {}),
    };
  }

  /** The SYSDBA password, which is also the segment servers' token */
  private async superuserPassword(cluster: FirebirdCluster): Promise<string | undefined> {
    const secret = cluster.spec.superuserSecret?.name;
    if (!secret) return 'masterkey'; // the pods' ISC_PASSWORD without a superuser Secret
    try {
      const data = (await this.coreApi.readNamespacedSecret({ name: secret, namespace: cluster.metadata.namespace ?? 'default' }))
        .data?.password;
      return data ? Buffer.from(data, 'base64').toString('utf8') : undefined;
    } catch {
      return undefined;
    }
  }

  /**
   * Measures each ready replica's replication lag from the segment servers (utils/replication-lag.ts),
   * publishes it as the replication-lag-seconds pod annotation read by read-only routing, and
   * returns it for status.replicationStatus. An unmeasurable replica loses the annotation (it is
   * then routed on readiness alone).
   */
  private async reconcileReplicationLag(
    cluster: FirebirdCluster,
    primaryPod: string,
    switchover: SwitchoverStatus | undefined,
    log: Logger,
  ): Promise<{ lastArchivedSequence?: number; replicas: ReplicaLagStatus[] } | undefined> {
    const { name, namespace = 'default' } = cluster.metadata;
    if (!replicationEnabled(cluster) || cluster.spec.hibernated || cluster.spec.instances < 2) return undefined;
    // positions are meaningless while the primary moves
    if (switchoverInFlight(switchover)) return cluster.status?.replicationStatus?.replicas ? {
      lastArchivedSequence: cluster.status.replicationStatus.lastArchivedSequence,
      replicas: cluster.status.replicationStatus.replicas,
    } : undefined;
    const token = await this.superuserPassword(cluster);
    if (token === undefined) return undefined;

    const pods = instancePods(
      (await this.coreApi.listNamespacedPod({ namespace, labelSelector: instancePodSelector(name) })).items,
      name,
    );
    const host = (pod: string) => `${instanceHost(cluster, pod)}.${namespace}.svc`;
    const primary = pods.find((p) => p.metadata?.name === primaryPod);
    if (!primary || !isPodReady(primary)) return undefined;
    let archived;
    try {
      archived = parseArchived(await this.segmentClient(host(primaryPod), SEGMENT_PORT, `${token} ARCHIVED`));
    } catch (err) {
      log.debug({ err }, 'Could not list the primary archived segments');
      return undefined;
    }

    const replicas: ReplicaLagStatus[] = [];
    for (const pod of pods) {
      const podName = pod.metadata?.name;
      if (!podName || podName === primaryPod || !isPodReady(pod)) continue;
      let entry: ReplicaLagStatus;
      try {
        const position = parsePosition(await this.segmentClient(host(podName), SEGMENT_PORT, `${token} POSITION`));
        entry = { name: podName, appliedSequence: position.sequence, pendingSegments: position.pending, ...computeLag(archived, position.sequence) };
      } catch (err) {
        entry = { name: podName, error: err instanceof Error ? err.message : String(err) };
      }
      replicas.push(entry);
      const annotation = entry.lagSeconds === undefined ? null : String(entry.lagSeconds);
      if ((pod.metadata?.annotations?.[REPLICATION_LAG_ANNOTATION] ?? null) !== annotation) {
        await this.coreApi.patchNamespacedPod(
          { name: podName, namespace, body: { metadata: { annotations: { [REPLICATION_LAG_ANNOTATION]: annotation } } } },
          MERGE_PATCH,
        );
      }
    }
    return {
      ...(archived.length ? { lastArchivedSequence: archived[archived.length - 1].sequence } : {}),
      replicas: replicas.sort((a, b) => a.name.localeCompare(b.name, undefined, { numeric: true })),
    };
  }

  /** Reconcile CronJob for replication journal continuous archiving to S3 */
  private async reconcileJournalArchiveCronJob(
    cluster: FirebirdCluster,
    primaryPod: string,
    log: Logger,
  ): Promise<void> {
    const { name, namespace = 'default' } = cluster.metadata;
    const cronJobName = `${name}-journal-archive`;

    if (cluster.spec.replication?.enabled && cluster.spec.replication.journalArchiveS3) {
      const built = buildJournalArchiveCronJob(cluster, primaryPod);
      if (!built) return;
      const desired = withHibernation(built, cluster);
      try {
        const existing = await this.batchApi.readNamespacedCronJob({ name: cronJobName, namespace });
        if (cronJobNeedsUpdate(existing, desired)) {
          log.info('Updating journal archive CronJob');
          await this.batchApi.patchNamespacedCronJob({
            name: cronJobName,
            namespace,
            body: desired,
          }, MERGE_PATCH);
        } else {
          log.debug('Journal archive CronJob up to date, skipping');
        }
      } catch {
        log.info('Creating journal archive CronJob');
        await this.batchApi.createNamespacedCronJob({
          namespace,
          body: desired,
        });
      }
    } else {
      try {
        await this.batchApi.readNamespacedCronJob({ name: cronJobName, namespace });
        log.info('Deleting disabled journal archive CronJob');
        await this.batchApi.deleteNamespacedCronJob({ name: cronJobName, namespace });
      } catch {
        log.debug('Journal archive CronJob does not exist, skipping deletion');
      }
    }
  }

  /** Reconcile the CronJob resource for database backups */
  private async reconcileBackupCronJob(
    cluster: FirebirdCluster,
    primaryPod: string,
    log: Logger,
  ): Promise<void> {
    const { name, namespace = 'default' } = cluster.metadata;
    const backupName = `${name}-backup`;

    if (cluster.spec.backup?.enabled) {
      const backup = cluster.spec.backup;
      const pods =
        backup.target === 'prefer-standby'
          ? (await this.coreApi.listNamespacedPod({ namespace, labelSelector: instancePodSelector(name) })).items
          : [];
      const instance = chooseBackupInstance({ cluster, primaryPod, pods, ...backup });
      const desired = withHibernation(buildBackupCronJob(cluster, instance), cluster);
      try {
        const existing = await this.batchApi.readNamespacedCronJob({ name: backupName, namespace });
        if (cronJobNeedsUpdate(existing, desired)) {
          log.info('Updating backup CronJob');
          await this.batchApi.patchNamespacedCronJob({
            name: backupName,
            namespace,
            body: desired,
          }, MERGE_PATCH);
        } else {
          log.debug('Backup CronJob up to date, skipping');
        }
      } catch {
        log.info('Creating backup CronJob');
        await this.batchApi.createNamespacedCronJob({
          namespace,
          body: desired,
        });
      }
    } else {
      try {
        await this.batchApi.readNamespacedCronJob({ name: backupName, namespace });
        log.info('Deleting disabled backup CronJob');
        await this.batchApi.deleteNamespacedCronJob({ name: backupName, namespace });
      } catch {
        log.debug('Backup CronJob does not exist, skipping deletion');
      }
    }
  }

  /** Reconcile the PodMonitor custom resource for Prometheus monitoring */
  private async reconcilePodMonitor(
    cluster: FirebirdCluster,
    log: Logger,
  ): Promise<void> {
    const { name, namespace = 'default' } = cluster.metadata;
    const podMonitorName = `${name}-podmonitor`;
    const group = 'monitoring.coreos.com';
    const version = 'v1';
    const plural = 'podmonitors';

    if (cluster.spec.monitoring?.enablePodMonitor) {
      const desired = buildPodMonitor(cluster);
      try {
        await this.customApi.getNamespacedCustomObject({
          group,
          version,
          namespace,
          plural,
          name: podMonitorName,
        });
        log.debug('PodMonitor already exists, skipping');
      } catch {
        log.info('Creating PodMonitor');
        await this.customApi.createNamespacedCustomObject({
          group,
          version,
          namespace,
          plural,
          body: desired,
        });
      }
    } else {
      try {
        await this.customApi.getNamespacedCustomObject({
          group,
          version,
          namespace,
          plural,
          name: podMonitorName,
        });
        log.info('Deleting disabled PodMonitor');
        await this.customApi.deleteNamespacedCustomObject({
          group,
          version,
          namespace,
          plural,
          name: podMonitorName,
        });
      } catch {
        log.debug('PodMonitor does not exist, skipping deletion');
      }
    }
  }

  /** Reconcile PodDisruptionBudget for multi-instance clusters (instances > 1) */
  private async reconcilePodDisruptionBudget(
    cluster: FirebirdCluster,
    log: Logger,
  ): Promise<void> {
    const { name, namespace = 'default' } = cluster.metadata;
    const pdbName = `${name}-pdb`;

    // A hibernated cluster has no pods to protect; drop the PDB so drains are not blocked
    if (cluster.spec.instances > 1 && !cluster.spec.hibernated) {
      const desired = buildPodDisruptionBudget(cluster);
      try {
        const existing = await this.policyApi.readNamespacedPodDisruptionBudget({ name: pdbName, namespace });
        if (podDisruptionBudgetNeedsUpdate(existing, desired)) {
          log.info('Updating PodDisruptionBudget');
          await this.policyApi.patchNamespacedPodDisruptionBudget({
            name: pdbName,
            namespace,
            body: desired,
          }, MERGE_PATCH);
        } else {
          log.debug('PodDisruptionBudget up to date, skipping');
        }
      } catch {
        log.info('Creating PodDisruptionBudget');
        await this.policyApi.createNamespacedPodDisruptionBudget({
          namespace,
          body: desired,
        });
      }
    } else {
      try {
        await this.policyApi.readNamespacedPodDisruptionBudget({ name: pdbName, namespace });
        log.info('Deleting PodDisruptionBudget for single instance or hibernated cluster');
        await this.policyApi.deleteNamespacedPodDisruptionBudget({ name: pdbName, namespace });
      } catch {
        log.debug('PodDisruptionBudget does not exist, skipping deletion');
      }
    }
  }

  /** Reconcile custom firebird.conf settings or init.sql script ConfigMap */
  private async reconcileConfigMap(
    cluster: FirebirdCluster,
    primaryPod: string,
    seedSourcePods: string[],
    reseed: Record<string, string>,
    log: Logger,
    switchover?: { promote: Record<string, string>; demote: Record<string, string> },
  ): Promise<void> {
    const { name, namespace = 'default' } = cluster.metadata;
    const configName = `${name}-config`;
    const desired = buildConfigMap(cluster, {
      primaryPod,
      seedSourcePods,
      reseed,
      promote: switchover?.promote,
      demote: switchover?.demote,
    });

    if (desired) {
      try {
        const existing = await this.coreApi.readNamespacedConfigMap({ name: configName, namespace });
        if (configMapNeedsUpdate(existing, desired)) {
          log.info('Updating cluster ConfigMap');
          await this.coreApi.patchNamespacedConfigMap({
            name: configName,
            namespace,
            body: desired,
          }, MERGE_PATCH);
        } else {
          log.debug('ConfigMap is up to date, skipping');
        }
      } catch {
        log.info('Creating cluster ConfigMap');
        await this.coreApi.createNamespacedConfigMap({
          namespace,
          body: desired,
        });
      }
    } else {
      try {
        await this.coreApi.readNamespacedConfigMap({ name: configName, namespace });
        log.info('Deleting unused ConfigMap');
        await this.coreApi.deleteNamespacedConfigMap({ name: configName, namespace });
      } catch {
        log.debug('ConfigMap does not exist, skipping deletion');
      }
    }
  }

  /** Reconcile the CronJob resource for periodic gfix database sweeping */
  private async reconcileAutoSweepCronJob(
    cluster: FirebirdCluster,
    primaryPod: string,
    log: Logger,
  ): Promise<void> {
    const { name, namespace = 'default' } = cluster.metadata;
    const sweepName = `${name}-sweep`;

    if (cluster.spec.autoSweep?.enabled) {
      const desired = withHibernation(buildAutoSweepCronJob(cluster, primaryPod), cluster);
      try {
        const existing = await this.batchApi.readNamespacedCronJob({ name: sweepName, namespace });
        if (autoSweepCronJobNeedsUpdate(existing, desired)) {
          log.info('Updating AutoSweep CronJob');
          await this.batchApi.patchNamespacedCronJob({
            name: sweepName,
            namespace,
            body: desired,
          }, MERGE_PATCH);
        } else {
          log.debug('AutoSweep CronJob up to date, skipping');
        }
      } catch {
        log.info('Creating AutoSweep CronJob');
        await this.batchApi.createNamespacedCronJob({
          namespace,
          body: desired,
        });
      }
    } else {
      try {
        await this.batchApi.readNamespacedCronJob({ name: sweepName, namespace });
        log.info('Deleting disabled AutoSweep CronJob');
        await this.batchApi.deleteNamespacedCronJob({ name: sweepName, namespace });
      } catch {
        log.debug('AutoSweep CronJob does not exist, skipping deletion');
      }
    }
  }

  /** Reconcile NetworkPolicy resource for cluster isolation */
  private async reconcileNetworkPolicy(
    cluster: FirebirdCluster,
    log: Logger,
  ): Promise<void> {
    const { name, namespace = 'default' } = cluster.metadata;
    const npName = `${name}-networkpolicy`;

    if (cluster.spec.networkPolicy?.enabled) {
      const desired = buildNetworkPolicy(cluster);
      try {
        const existing = await this.networkingApi.readNamespacedNetworkPolicy({ name: npName, namespace });
        if (networkPolicyNeedsUpdate(existing, desired)) {
          log.info('Updating NetworkPolicy');
          await this.networkingApi.patchNamespacedNetworkPolicy({
            name: npName,
            namespace,
            body: networkPolicyWireFormat(desired),
          }, MERGE_PATCH);
        } else {
          log.debug('NetworkPolicy is up to date, skipping');
        }
      } catch {
        log.info('Creating NetworkPolicy');
        await this.networkingApi.createNamespacedNetworkPolicy({
          namespace,
          body: desired,
        });
      }
    } else {
      try {
        await this.networkingApi.readNamespacedNetworkPolicy({ name: npName, namespace });
        log.info('Deleting disabled NetworkPolicy');
        await this.networkingApi.deleteNamespacedNetworkPolicy({ name: npName, namespace });
      } catch {
        log.debug('NetworkPolicy does not exist, skipping deletion');
      }
    }
  }

  /** Reconcile the primary leader lease object for HA election */
  private async reconcileLease(
    cluster: FirebirdCluster,
    log: Logger,
  ): Promise<void> {
    const { name, namespace = 'default' } = cluster.metadata;
    const leaseName = `${name}-lease`;
    const desired = buildLease(cluster);

    try {
      await this.coordinationApi.readNamespacedLease({ name: leaseName, namespace });
      log.debug('Leader lease already exists, skipping');
    } catch {
      log.info('Creating leader lease');
      await this.coordinationApi.createNamespacedLease({
        namespace,
        body: desired,
      });
    }
  }

  /** Reconcile cert-manager Certificate resource if TLS issuerRef is configured */
  private async reconcileCertificate(
    cluster: FirebirdCluster,
    log: Logger,
  ): Promise<void> {
    if (!cluster.spec.tls?.enabled || !cluster.spec.tls.issuerRef) return;
    const { name, namespace = 'default' } = cluster.metadata;
    const certName = `${name}-cert`;
    const desired = buildCertificate(cluster);

    try {
      await this.customApi.getNamespacedCustomObject({
        group: 'cert-manager.io',
        version: 'v1',
        namespace,
        plural: 'certificates',
        name: certName,
      });
      log.debug('cert-manager Certificate already exists, skipping');
    } catch {
      log.info('Creating cert-manager Certificate');
      await this.customApi.createNamespacedCustomObject({
        group: 'cert-manager.io',
        version: 'v1',
        namespace,
        plural: 'certificates',
        body: desired,
      });
    }
  }

  /** Reconcile the online database diagnostics CronJob (gfix -v -full) */
  private async reconcileDiagnosticsCronJob(
    cluster: FirebirdCluster,
    primaryPod: string,
    log: Logger,
  ): Promise<void> {
    const { name, namespace = 'default' } = cluster.metadata;
    const cronJobName = `${name}-diagnostics`;

    if (cluster.spec.diagnostics?.enabled) {
      const desired = withHibernation(buildDiagnosticsCronJob(cluster, primaryPod), cluster);
      try {
        const existing = await this.batchApi.readNamespacedCronJob({ name: cronJobName, namespace });
        if (diagnosticsCronJobNeedsUpdate(existing, desired)) {
          log.info('Updating Diagnostics CronJob');
          await this.batchApi.patchNamespacedCronJob({
            name: cronJobName,
            namespace,
            body: desired,
          }, MERGE_PATCH);
        } else {
          log.debug('Diagnostics CronJob is up to date, skipping');
        }
      } catch {
        log.info('Creating Diagnostics CronJob');
        await this.batchApi.createNamespacedCronJob({
          namespace,
          body: desired,
        });
      }
    } else {
      try {
        await this.batchApi.readNamespacedCronJob({ name: cronJobName, namespace });
        log.info('Deleting disabled Diagnostics CronJob');
        await this.batchApi.deleteNamespacedCronJob({ name: cronJobName, namespace });
      } catch {
        log.debug('Diagnostics CronJob does not exist, skipping deletion');
      }
    }
  }

  /** Reconcile the Grafana dashboard ConfigMap if enableGrafanaDashboard is true */
  private async reconcileGrafanaDashboard(
    cluster: FirebirdCluster,
    log: Logger,
  ): Promise<void> {
    const { name, namespace = 'default' } = cluster.metadata;
    const cmName = `${name}-grafana-dashboard`;

    if (cluster.spec.monitoring?.enableGrafanaDashboard) {
      const desired = buildGrafanaDashboardConfigMap(cluster);
      try {
        const existing = await this.coreApi.readNamespacedConfigMap({ name: cmName, namespace });
        if (configMapNeedsUpdate(existing, desired)) {
          log.info('Updating Grafana Dashboard ConfigMap');
          await this.coreApi.patchNamespacedConfigMap({
            name: cmName,
            namespace,
            body: desired,
          }, MERGE_PATCH);
        } else {
          log.debug('Grafana Dashboard ConfigMap is up to date, skipping');
        }
      } catch {
        log.info('Creating Grafana Dashboard ConfigMap');
        await this.coreApi.createNamespacedConfigMap({
          namespace,
          body: desired,
        });
      }
    } else {
      try {
        await this.coreApi.readNamespacedConfigMap({ name: cmName, namespace });
        log.info('Deleting disabled Grafana Dashboard ConfigMap');
        await this.coreApi.deleteNamespacedConfigMap({ name: cmName, namespace });
      } catch {
        log.debug('Grafana Dashboard ConfigMap does not exist, skipping deletion');
      }
    }
  }

  /** Records a Kubernetes event on the cluster */
  private event(cluster: FirebirdCluster, type: EventType, reason: string, message: string): Promise<void> {
    return this.events.record(
      { apiVersion: `${API_GROUP}/${API_VERSION}`, kind: RESOURCE_KIND, metadata: cluster.metadata },
      type,
      reason,
      message,
    );
  }

  /** Update the status sub-resource of a FirebirdCluster */
  async updateStatus(
    cluster: FirebirdCluster,
    status: Partial<FirebirdClusterStatus>,
  ): Promise<void> {
    const { name, namespace = 'default' } = cluster.metadata;

    const patch = [
      {
        op: 'replace' as const,
        path: '/status',
        value: {
          ...cluster.status,
          ...status,
        },
      },
    ];

    try {
      await this.customApi.patchNamespacedCustomObjectStatus({
        group: API_GROUP,
        version: API_VERSION,
        namespace,
        plural: RESOURCE_PLURAL,
        name,
        body: patch,
      });
    } catch (err) {
      logger.warn({ err, cluster: name }, 'Failed to update cluster status');
    }
  }

  /** Helper to create a status condition */
  private fencingCondition(fencing: FencingResult): FirebirdClusterCondition {
    const parts = [
      fencing.fenced.length ? `fenced: ${fencing.fenced.join(', ')}` : '',
      fencing.pending.length ? `changing: ${fencing.pending.join(', ')}` : '',
      fencing.failed.length ? `failed (retrying): ${fencing.failed.join(', ')}` : '',
    ].filter(Boolean);
    return this.makeCondition(
      'Fenced',
      fencing.fenced.length ? 'True' : 'False',
      fencing.failed.length ? 'FencingFailed' : fencing.pending.length ? 'FencingInProgress' : 'FencingApplied',
      parts.length ? parts.join('; ') : 'No instance is fenced',
    );
  }

  /**
   * Applies the fencedInstances annotation: one Job per instance puts its database into full
   * shutdown or back online, and status.fencedInstances records what has been applied.
   */
  private async reconcileFencing(cluster: FirebirdCluster, log: Logger): Promise<FencingResult> {
    const { name, namespace = 'default' } = cluster.metadata;
    const desired = new Set(desiredFencedInstances(cluster));
    // entries for instances removed by scaling down are kept: their volumes stay shut down
    const applied = new Set(cluster.status?.fencedInstances ?? []);
    const result: FencingResult = { fenced: [], pending: [], failed: [] };
    if (cluster.spec.hibernated) {
      result.fenced = [...applied].sort();
      return result;
    }

    for (let i = 0; i < cluster.spec.instances; i++) {
      const pod = `${name}-${i}`;
      const jobName = fencingJobName(pod);
      let job;
      try {
        job = await this.batchApi.readNamespacedJob({ name: jobName, namespace });
      } catch (err) {
        if (!isNotFound(err)) throw err;
      }
      const step = planFencing(desired.has(pod), applied.has(pod), job);
      switch (step.kind) {
        case 'create':
          log.info({ pod, action: step.action }, 'Starting fencing Job');
          try {
            await this.batchApi.createNamespacedJob({ namespace, body: buildFencingJob(cluster, pod, step.action) });
          } catch (err) {
            // the previous Job of this instance is still being deleted; retried on the next reconcile
            if ((err as { code?: number })?.code !== 409) throw err;
          }
          result.pending.push(pod);
          break;
        case 'wait':
          result.pending.push(pod);
          break;
        case 'applied':
          log.info({ pod, action: step.action }, step.action === 'fence' ? 'Instance fenced' : 'Instance unfenced');
          await this.event(
            cluster,
            'Normal',
            step.action === 'fence' ? EventReason.InstanceFenced : EventReason.InstanceUnfenced,
            step.action === 'fence' ? `${pod}: database shut down (fenced)` : `${pod}: database back online`,
          );
          if (step.action === 'fence') applied.add(pod);
          else applied.delete(pod);
          await this.batchApi.deleteNamespacedJob({ name: jobName, namespace, propagationPolicy: 'Background' });
          if (desired.has(pod) !== applied.has(pod)) result.pending.push(pod);
          break;
        case 'failed':
          log.warn({ pod, action: step.action }, 'Fencing Job failed; retrying on the next reconcile');
          await this.event(cluster, 'Warning', EventReason.FencingFailed, `${step.action} Job for ${pod} failed; retrying`);
          await this.batchApi.deleteNamespacedJob({ name: jobName, namespace, propagationPolicy: 'Background' });
          result.failed.push(pod);
          break;
      }
    }
    result.fenced = [...applied].sort();
    return result;
  }

  private makeCondition(
    type: FirebirdClusterCondition['type'],
    status: FirebirdClusterCondition['status'],
    reason: string,
    message: string,
  ): FirebirdClusterCondition {
    return {
      type,
      status,
      reason,
      message,
      lastTransitionTime: new Date().toISOString(),
    };
  }
}

/** Returns a copy of a string map with keys in sorted order for stable comparison */
function sortKeys(map: Record<string, string>): Record<string, string> {
  return Object.fromEntries(Object.entries(map).sort(([a], [b]) => a.localeCompare(b)));
}
