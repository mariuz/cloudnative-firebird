import { segmentTlsEnabled } from '../utils/segment-tls-pods';
import { ensureSegmentTlsSecret, reconcileSegmentTlsPeers } from '../utils/segment-tls-client';
import { nativeSidecarsSupported, segmentTlsDefault, segmentTlsRequired } from '../utils/segment-tls-default';
import { MIGRATION_ANNOTATION, migrationInProgress, migrationMode, migrationStep, notMigrating } from '../utils/segment-tls-migration';
import {
  notMigratingToPrimaryLease,
  primaryLeaseMigrationInProgress,
  primaryLeaseMigrationMode,
  primaryLeaseMigrationStep,
} from '../utils/primary-lease-migration';
import {
  buildPrimaryLeaseRole,
  buildPrimaryLeaseRoleBinding,
  leaseAgeSeconds,
  leaseExpired,
  PRIMARY_LEASE_ANNOTATION,
  primaryLeaseDefault,
  primaryLeaseDurationSeconds,
  primaryLeaseEnabled,
  primaryLeaseRoleName,
} from '../utils/primary-lease';
import { superuserPasswordFrom } from '../utils/restore-target';
import { POD_ANTI_AFFINITY_ANNOTATION } from '../utils/scheduling';
import {
  buildImageCheckJob,
  buildMajorUpgradeJob,
  imageChangeKind,
  imageCheckJobName,
  imageCheckMessage,
  imageCheckResults,
  instancesStartedOn,
  majorUpgradeInProgress,
  majorUpgradeJobName,
  withEffectiveImage,
} from '../utils/major-upgrade';
import { inPlaceResize, RESIZE_ANNOTATION, RESIZE_TIMEOUT_SECONDS, resizeApplied, resizeInfeasible } from '../utils/in-place';
import crypto from 'crypto';
import {
  AppsV1Api,
  BatchV1Api,
  CoordinationV1Api,
  RbacAuthorizationV1Api,
  CoreV1Api,
  CustomObjectsApi,
  KubeConfig,
  NetworkingV1Api,
  PatchStrategy,
  PolicyV1Api,
  setHeaderOptions,
  V1ConfigMap,
  V1Job,
  V1MicroTime,
  V1Pod,
  V1PodTemplateSpec,
  V1StatefulSet,
} from '@kubernetes/client-node';
import { Logger } from 'pino';
import {
  buildBackupCronJob,
  buildJournalArchiveCronJob,
  buildRecoveryBootstrapJob,
  instanceDataClaimName,
  journalArchiveListedSequence,
  journalArchivePodSelector,
  jobOutcome,
  recoveryBootstrapJobName,
} from '../utils/backup';
import {
  PENDING_DROP_USERS_ANNOTATION,
  buildPendingDropJob,
  formatPendingDrops,
  parsePendingDrops,
  pendingDropJobName,
  pendingDropsConfigMapName,
} from '../utils/pending-drops';
import { firebirdUsername } from '../utils/users';
import { metrics, recordClusterMetrics, recordReconcile } from '../utils/metrics';
import { RESEED_VOLUME, dataClaimName, planVolumeRecreation } from '../utils/volume-recreation';
import { EventReason, EventRecorder, EventType } from '../utils/events';
import { PRIMARY_RESTART_GRACE_SECONDS, REVISION_LABEL, planRollingUpdate, rollingUpdateTarget } from '../utils/rolling-update';
import {
  SyncPlanInput,
  attachedStandbys,
  syncUnsupportedReason,
  buildSyncStandbyJob,
  handoverForUpdate,
  planSynchronous,
  synchronousMembers,
  syncStandbyJobName,
  synchronousMode,
} from '../utils/synchronous';
import { chooseBackupInstance } from '../utils/backup-target';
import {
  effectiveFailoverDelaySeconds,
  primaryCutOff,
  TARGET_PRIMARY_ANNOTATION,
  buildFailoverJob,
  buildPromoteJob,
  buildSwitchoverJob,
  failoverJobName,
  parseElection,
  switchoverJobName,
  promoteJobName,
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
  buildConfigMap,
  buildDiagnosticsCronJob,
  buildGrafanaDashboardConfigMap,
  buildHeadlessService,
  buildLease,
  buildNetworkPolicy,
  CloneTarget,
  cloneTargets,
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
import {
  SegmentClient,
  computeLag,
  parseArchived,
  parsePosition,
  segmentRequest,
  segmentRetention,
} from '../utils/replication-lag';
import {
  RESEED_ANNOTATION,
  RESEED_KEY,
  SEGMENT_PORT,
  instanceHost,
  isolationCheckTimeoutSeconds,
  replicationEnabled,
} from '../utils/replication';
import { planVolumeExpansion } from '../utils/storage';
import { validateClusterSpec } from '../utils/validation';
import {
  API_GROUP,
  API_VERSION,
  FirebirdCluster,
  FirebirdClusterCondition,
  FirebirdClusterStatus,
  FirebirdUser,
  DEFAULT_FIREBIRD_IMAGE,
  ImageCheckStatus,
  MajorUpgradeStatus,
  ReplicaLagStatus,
  VolumeRecreationStatus,
  SegmentRetentionStatus,
  RollingUpdateStatus,
  SwitchoverStatus,
  SynchronousStatus,
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
  /** The stored rolling update status (the reconcile may have started from a stale watch copy) */
  rollingUpdate?: RollingUpdateStatus;
  /** A planned switchover is wanted (targetPrimary annotation) but not started */
  wantsSwitchover?: boolean;
  /** The stored synchronous replication state */
  synchronous?: SynchronousStatus;
  /** The stored status.journalArchiveSequence */
  journalArchiveSequence?: number;
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
  private readonly rbacApi: RbacAuthorizationV1Api;
  private readonly coreApi: CoreV1Api;
  private readonly customApi: CustomObjectsApi;
  private readonly networkingApi: NetworkingV1Api;
  private readonly policyApi: PolicyV1Api;
  private readonly events: EventRecorder;
  /** Clusters already warned about tls.secretName / tls.issuerRef */
  private readonly certificateWarned = new Set<string>();
  /** Firebird engine version per instance pod (by UID), from its segment server (VERSION) */
  private readonly engineVersions = new Map<string, string>();

  constructor(
    kubeConfig: KubeConfig,
    private readonly segmentClient: SegmentClient = segmentRequest,
  ) {
    this.appsApi = kubeConfig.makeApiClient(AppsV1Api);
    this.batchApi = kubeConfig.makeApiClient(BatchV1Api);
    this.coordinationApi = kubeConfig.makeApiClient(CoordinationV1Api);
    this.rbacApi = kubeConfig.makeApiClient(RbacAuthorizationV1Api);
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

      // Only a new cluster starts as Creating: rewriting the phase of a running cluster on every
      // reconcile made it read Creating until the end of the reconcile (e.g. every resync). An
      // existing one is only checked: rewriting its status from this (possibly stale) copy dropped
      // what the previous reconcile had just stored, e.g. the primary restart of a rolling update.
      const exists = cluster.status?.phase
        ? await this.clusterExists(cluster)
        : await this.updateStatus(cluster, { phase: 'Creating', phaseReason: 'Reconciliation started' });
      if (!exists) {
        // deleted since this reconcile was queued (e.g. a resync): recreate nothing
        log.info('Cluster no longer exists; nothing to reconcile');
        return;
      }
      await this.defaultSegmentTls(cluster, log);
      await this.defaultPrimaryLease(cluster, log);
      await this.defaultPodAntiAffinity(cluster, log);
      await this.reconcileSegmentTlsMigration(cluster, log);
      await this.reconcilePrimaryLeaseMigration(cluster, log);
      // a new image is checked first (major version upgrades): the rest of the reconcile sees the
      // image the instances may run, and a cluster stopped while its databases are converted
      const image = await this.reconcileImage(cluster, log);
      cluster = image.cluster;

      const switchover = await this.reconcileSwitchover(cluster, await this.resolvePrimaryPod(cluster, log), log);
      const primaryPod = switchover.primaryPod;
      // the synchronous standby is detached before it is re-seeded: it must never receive the
      // journal and the primary's synchronous changes together
      const syncHold = switchover.synchronous?.primary === primaryPod ? synchronousMembers(switchover.synchronous) : [];
      const volumeRecreation = await this.reconcileVolumeRecreation(cluster, primaryPod, log, syncHold);
      const recreatingVolumes = volumeRecreation.recreating;
      const reseed = await this.resolveReseeds(cluster, primaryPod, log, syncHold);
      Object.assign(reseed.requests, switchover.reseed);
      const instances = await this.resolveSeedSources(cluster, primaryPod);
      const seedSourcePods = instances.seedSources.filter(
        (pod) => !(pod in reseed.requests) && !(pod in switchover.promote) && !(pod in switchover.demote),
      );
      await this.reconcileConfigMap(cluster, primaryPod, seedSourcePods, reseed.requests, log, switchover, instances.peerAddresses);
      // the ConfigMap lists the requests before the pods restart into their init containers
      for (const pod of [...new Set([...reseed.restart, ...switchover.restart])]) {
        log.info({ pod }, 'Restarting instance into its init container');
        try {
          await this.coreApi.deleteNamespacedPod({ name: pod, namespace });
        } catch (err) {
          if (!isNotFound(err)) throw err; // already gone
        }
      }
      // a point-in-time bootstrap prepares the first volume before the StatefulSet exists
      const recovering = await this.reconcileRecoveryBootstrap(cluster, log);
      if (recovering) {
        await this.updateStatus(cluster, {
          phase: 'Creating',
          phaseReason: recovering,
          conditions: [
            this.makeCondition('Ready', 'False', 'Recovering', recovering),
            this.makeCondition('Progressing', 'True', 'Recovering', recovering),
          ],
        });
        return;
      }
      // lag is published as pod annotations before read-only routing evaluates them
      const lag = await this.reconcileReplicationLag(cluster, primaryPod, switchover.status, log);
      // Label pods before (re)pointing service selectors at the routing labels
      const readRouting = await this.reconcileReadRouting(cluster, primaryPod, log);
      await this.reconcileHeadlessService(cluster, log);
      await this.reconcileService(cluster, log);
      // segment TLS: the certificates exist (and are renewed) before pods mount them
      if (segmentTlsEnabled(cluster)) await ensureSegmentTlsSecret(this.coreApi, cluster);
      // before the StatefulSet: an instance restarted into the new mode reads it when it starts
      await reconcileSegmentTlsPeers(this.coreApi, cluster);
      const { readyInstances, superuserSecretHash, statefulSetExisted, statefulSet } =
        await this.reconcileStatefulSet(cluster, log);
      const volumes = statefulSetExisted
        ? await this.reconcileVolumeExpansion(cluster, log)
        : undefined;
      const fencing = await this.reconcileFencing(cluster, log);
      const { status: synchronous, rollingHold } = await this.reconcileSynchronous(
        cluster,
        primaryPod,
        switchover,
        [...Object.keys(reseed.requests), ...reseed.held, ...recreatingVolumes.map((r) => r.pod), ...volumeRecreation.held],
        fencing.fenced,
        statefulSetExisted ? statefulSet : undefined,
        log,
      );
      const busy = switchoverInFlight(switchover.status)
        ? `${switchover.status?.kind ?? 'switchover'} in progress`
        : recreatingVolumes.length > 0
          ? `volume of ${recreatingVolumes.map((r) => r.pod).join(', ')} being re-created`
        : Object.keys(reseed.requests).length > 0 || reseed.restart.length > 0 || switchover.restart.length > 0
          ? 'instances are being re-seeded or restarted'
        // the sync-standby Job shuts the primary down and needs both instances
        : synchronous?.phase === 'Attaching' || synchronous?.phase === 'Detaching'
          ? `the synchronous standby ${synchronous.standby} is being ${synchronous.phase === 'Attaching' ? 'attached' : 'detached'}`
          : undefined;
      const rollingUpdate = statefulSetExisted
        ? await this.reconcileRollingUpdate(cluster, statefulSet, primaryPod, fencing.fenced, busy, switchover, rollingHold, log)
        : undefined;

      if (cluster.spec.replication?.enabled) {
        await this.reconcileReplicaService(cluster, log);
      }

      await this.reconcileJournalArchiveCronJob(cluster, primaryPod, log);
      const journalArchiveSequence = await this.journalArchiveSequence(cluster, switchover.journalArchiveSequence);
      await this.reconcileLease(cluster, log);
      await this.reconcilePrimaryLeaseRbac(cluster, log);
      await this.warnUnusedCertificate(cluster, log);
      await this.reconcilePodDisruptionBudget(cluster, log);
      await this.reconcileNetworkPolicy(cluster, log);
      await this.reconcileBackupCronJob(cluster, primaryPod, log);
      await this.reconcilePendingUserDrops(cluster, log);
      await this.reconcileAutoSweepCronJob(cluster, primaryPod, log);
      await this.reconcileDiagnosticsCronJob(cluster, primaryPod, log);
      await this.reconcilePodMonitor(cluster, log);
      await this.reconcileGrafanaDashboard(cluster, log);

      if (cluster.spec.hibernated && image.upgrading) {
        const upgrading: Partial<FirebirdClusterStatus> = {
          phase: 'Upgrading',
          phaseReason: image.upgrading,
          instances: cluster.spec.instances,
          readyInstances,
          superuserSecretHash,
          fencedInstances: fencing.fenced,
          selector: podSelector(cluster),
          ...(volumes ? { volumes } : {}),
          conditions: [
            this.makeCondition('Ready', 'False', 'MajorUpgrade', image.upgrading),
            this.makeCondition('Progressing', 'True', 'MajorUpgrade', image.upgrading),
          ],
        };
        if (await this.updateStatus(cluster, upgrading)) {
          recordClusterMetrics(cluster, upgrading);
          recordReconcile(cluster, 'success');
        }
        log.info(image.upgrading);
        return;
      }

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
        if (await this.updateStatus(cluster, hibernated)) {
          recordClusterMetrics(cluster, hibernated);
          recordReconcile(cluster, 'success');
        }
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
        phase: image.upgrading ? 'Upgrading' : isReady ? (rollingUpdate ? 'Updating' : 'Running') : 'Creating',
        phaseReason: image.upgrading
          ? image.upgrading
          : isReady
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
        recreatingVolumes,
        rollingUpdate,
        ...(switchover.status ? { switchover: switchover.status } : {}),
        primaryNotReadySince: switchover.primaryNotReadySince,
        synchronous,
        journalArchiveSequence,
        ...(volumes ? { volumes } : {}),
        conditions: [
          this.makeCondition(
            'Ready',
            isReady ? 'True' : 'False',
            isReady ? 'ClusterReady' : 'PodsNotReady',
            isReady ? 'Cluster is ready' : `${readyInstances}/${expectedReady} ready`,
          ),
          this.fencingCondition(fencing),
          ...(await this.segmentTlsCondition(cluster)),
          ...image.conditions,
          this.makeCondition(
            'Progressing',
            isReady ? 'False' : 'True',
            isReady ? 'ReconciliationComplete' : 'PodsStarting',
            isReady ? 'Reconciliation completed' : 'Waiting for StatefulSet pods',
          ),
        ],
      };
      // a cluster deleted during the reconcile must not get its metric series back
      if (await this.updateStatus(cluster, status)) {
        recordClusterMetrics(cluster, status);
        recordReconcile(cluster, 'success');
      }

      log.info('Reconciliation complete');
    } catch (err) {
      // a cluster deleted while it was reconciled: its resources are garbage-collected under the
      // reconcile (e.g. the StatefulSet patch fails with 404), which is not a failure
      if (isNotFound(err) && !(await this.clusterExists(cluster))) {
        log.info('Cluster deleted during reconciliation; stopping');
        return;
      }
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

  /** Stores status fields now, and in the reconcile's copy (the final status write starts from it) */
  private async persistStatus(cluster: FirebirdCluster, patch: Partial<FirebirdClusterStatus>): Promise<void> {
    await this.updateStatus(cluster, patch);
    // in place: the reconcile's copies of the cluster (withEffectiveImage) share the status object
    Object.assign((cluster.status ??= {}), patch);
  }

  /**
   * A new spec.imageName (utils/major-upgrade.ts): checked against the image the instances run
   * before they get it; a newer major on-disk structure starts a major upgrade, an older one is
   * refused. Returns the cluster the rest of the reconcile works with (the image the instances may
   * run, stopped while a major upgrade converts the databases), the phase message of an upgrade in
   * progress, and conditions for the status.
   */
  private async reconcileImage(
    cluster: FirebirdCluster,
    log: Logger,
  ): Promise<{ cluster: FirebirdCluster; upgrading?: string; conditions: FirebirdClusterCondition[] }> {
    const { name, namespace = 'default' } = cluster.metadata;
    const desired = cluster.spec.imageName ?? DEFAULT_FIREBIRD_IMAGE;
    const upgrade = cluster.status?.majorUpgrade;
    if (majorUpgradeInProgress(upgrade)) return this.reconcileMajorUpgrade(cluster, upgrade!, desired, log);

    // what the instances run: the StatefulSet's image (none yet: a new cluster starts on spec)
    let running: string | undefined;
    try {
      const statefulSet = await this.appsApi.readNamespacedStatefulSet({ name, namespace });
      running = statefulSet.spec?.template?.spec?.containers?.find((c) => c.name === 'firebird')?.image;
    } catch (err) {
      if (!isNotFound(err)) throw err;
    }
    if (!running || running === desired) {
      if (cluster.status?.imageCheck) await this.persistStatus(cluster, { imageCheck: undefined });
      return { cluster, conditions: [] };
    }

    let check = cluster.status?.imageCheck;
    if (!check || check.from !== running || check.to !== desired) {
      check = { from: running, to: desired, phase: 'Checking' };
      log.info({ from: running, to: desired }, 'Checking the new image before the instances run it');
      await this.event(cluster, 'Normal', EventReason.ImageCheckStarted, imageCheckMessage(check));
      await this.persistStatus(cluster, { imageCheck: check });
    }
    if (check.phase === 'Failed') {
      // the failed Job deleted: check again
      try {
        await this.batchApi.readNamespacedJob({ name: imageCheckJobName(cluster, check.from, check.to), namespace });
      } catch (err) {
        if (!isNotFound(err)) throw err;
        check = { from: check.from, to: check.to, phase: 'Checking' };
        await this.persistStatus(cluster, { imageCheck: check });
      }
    }
    if (check.phase === 'Checking') {
      const decided = await this.imageCheck(cluster, check, log);
      if (decided) {
        check = decided;
        await this.persistStatus(cluster, { imageCheck: check });
        const warning = check.phase === 'Refused' || check.phase === 'Failed';
        await this.event(
          cluster,
          warning ? 'Warning' : 'Normal',
          warning ? EventReason.ImageRefused : EventReason.ImageChecked,
          imageCheckMessage(check),
        );
      }
    }

    const held = (message: string, reason: string) => ({
      cluster: withEffectiveImage(cluster, running!),
      conditions: [this.makeCondition('ImageChange', 'False', reason, message)],
    });
    switch (check.phase) {
      case 'Compatible':
        return { cluster, conditions: [] };
      case 'Upgrade': {
        // the upgrade stops every instance: not in the middle of a switchover or with the primary fenced
        const primary = replicationEnabled(cluster) ? await this.resolvePrimaryPod(cluster, log) : undefined;
        const switchover = cluster.status?.switchover;
        if (switchover && switchover.phase !== 'Completed' && switchover.phase !== 'Failed') {
          return held(`major upgrade to ${desired} waits for the ${switchover.kind ?? 'switchover'} in progress`, 'MajorUpgradeWaiting');
        }
        if (primary && (cluster.status?.fencedInstances ?? []).includes(primary)) {
          return held(`major upgrade to ${desired} waits for the primary ${primary} to be unfenced`, 'MajorUpgradeWaiting');
        }
        const started: MajorUpgradeStatus = {
          from: check.from,
          to: check.to,
          fromOds: check.fromOds!,
          toOds: check.toOds!,
          ...(check.fromVersion ? { fromVersion: check.fromVersion } : {}),
          ...(check.toVersion ? { toVersion: check.toVersion } : {}),
          ...(primary ? { primary } : {}),
          phase: 'Stopping',
          message: 'stopping every instance',
          startTime: new Date().toISOString(),
        };
        log.info({ from: started.from, to: started.to }, 'Major upgrade: stopping every instance');
        await this.event(
          cluster,
          'Normal',
          EventReason.MajorUpgradeStarted,
          `major upgrade from ${started.from} (ODS ${started.fromOds}) to ${started.to} (ODS ${started.toOds}): stopping every instance to convert the databases`,
        );
        await this.persistStatus(cluster, { imageCheck: undefined, majorUpgrade: started });
        return this.reconcileMajorUpgrade(cluster, started, desired, log);
      }
      case 'Refused':
        return held(imageCheckMessage(check), 'ImageRefused');
      case 'Failed':
        return held(imageCheckMessage(check), 'ImageCheckFailed');
      default:
        return held(imageCheckMessage(check), 'ImageChecking');
    }
  }

  /** Runs the image check Job of `check`; the decided check once it finished, else undefined */
  private async imageCheck(cluster: FirebirdCluster, check: ImageCheckStatus, log: Logger): Promise<ImageCheckStatus | undefined> {
    const namespace = cluster.metadata.namespace ?? 'default';
    const jobName = imageCheckJobName(cluster, check.from, check.to);
    let job;
    try {
      job = await this.batchApi.readNamespacedJob({ name: jobName, namespace });
    } catch (err) {
      if (!isNotFound(err)) throw err;
    }
    if (!job) {
      log.info({ job: jobName }, 'Starting the image check Job');
      await this.batchApi.createNamespacedJob({ namespace, body: buildImageCheckJob(cluster, check.from, check.to) });
      return undefined;
    }
    const outcome = jobOutcome(job);
    if (outcome === 'Running') return undefined;
    const pods = await this.coreApi.listNamespacedPod({ namespace, labelSelector: `job-name=${jobName}` });
    const results = imageCheckResults(pods.items);
    if (!results.from || !results.to) {
      // kept for its logs and events: deleting it checks again
      const missing = [!results.from ? check.from : undefined, !results.to ? check.to : undefined].filter(Boolean).join(' and ');
      return { ...check, phase: 'Failed', message: `no result from ${missing} (see Job ${jobName}; delete it to check again)` };
    }
    await this.batchApi.deleteNamespacedJob({ name: jobName, namespace, propagationPolicy: 'Background' });
    const decided: ImageCheckStatus = {
      ...check,
      fromOds: results.from.ods,
      toOds: results.to.ods,
      fromVersion: results.from.version,
      toVersion: results.to.version,
      phase: imageChangeKind(results.from, results.to),
    };
    return decided;
  }

  /**
   * A major upgrade in progress: stops the instances, converts every instance volume with a Job,
   * starts the instances on the new image, and records each step
   */
  private async reconcileMajorUpgrade(
    cluster: FirebirdCluster,
    upgrade: MajorUpgradeStatus,
    desired: string,
    log: Logger,
  ): Promise<{ cluster: FirebirdCluster; upgrading?: string; conditions: FirebirdClusterCondition[] }> {
    const { name, namespace = 'default' } = cluster.metadata;
    const persist = async (next: MajorUpgradeStatus) => {
      await this.persistStatus(cluster, { majorUpgrade: next });
      upgrade = next;
    };
    const jobs = async () => {
      const found = new Map<number, V1Job>();
      for (let ordinal = 0; ordinal < cluster.spec.instances; ordinal++) {
        try {
          found.set(ordinal, await this.batchApi.readNamespacedJob({ name: majorUpgradeJobName(cluster, ordinal), namespace }));
        } catch (err) {
          if (!isNotFound(err)) throw err;
        }
      }
      return found;
    };

    // spec.imageName set back to the old image: abandoned while no volume was converted
    if (desired === upgrade.from && upgrade.phase !== 'Starting' && (upgrade.converted ?? []).length === 0) {
      for (const ordinal of (await jobs()).keys()) {
        await this.batchApi.deleteNamespacedJob({ name: majorUpgradeJobName(cluster, ordinal), namespace, propagationPolicy: 'Background' });
      }
      const message = `major upgrade to ${upgrade.to} abandoned: spec.imageName is ${upgrade.from} again and no volume was converted`;
      log.info(message);
      await this.event(cluster, 'Normal', EventReason.MajorUpgradeAbandoned, message);
      await persist({ ...upgrade, phase: 'Failed', message, completionTime: new Date().toISOString() });
      // recorded as finished: the cluster starts on its image again
      await this.persistStatus(cluster, { majorUpgrade: undefined });
      return { cluster, conditions: [] };
    }
    const pending = desired !== upgrade.to ? ` (spec.imageName ${desired} is checked once it completes)` : '';

    if (upgrade.phase === 'Stopping') {
      const pods = instancePods((await this.coreApi.listNamespacedPod({ namespace, labelSelector: instancePodSelector(name) })).items, name);
      if (pods.length > 0) {
        return {
          cluster: withEffectiveImage(cluster, upgrade.from, true),
          upgrading: `Major upgrade to ${upgrade.to}: waiting for ${pods.length} instance(s) to stop${pending}`,
          conditions: [],
        };
      }
      log.info('Major upgrade: every instance stopped; converting the volumes');
      await this.event(cluster, 'Normal', EventReason.MajorUpgradeConverting, `every instance stopped: converting the databases to ${upgrade.to}`);
      await persist({ ...upgrade, phase: 'Converting', message: 'converting the instance volumes' });
    }

    if (upgrade.phase === 'Converting' || upgrade.phase === 'Failed') {
      const found = await jobs();
      const converted: string[] = [];
      const failed: string[] = [];
      let running = 0;
      for (let ordinal = 0; ordinal < cluster.spec.instances; ordinal++) {
        const pod = `${name}-${ordinal}`;
        const job = found.get(ordinal);
        if (!job) {
          const claimName = instanceDataClaimName(cluster, ordinal);
          try {
            await this.coreApi.readNamespacedPersistentVolumeClaim({ name: claimName, namespace });
          } catch (err) {
            if (!isNotFound(err)) throw err;
            // no volume yet: the instance starts on the new image with a new one
            converted.push(pod);
            continue;
          }
          log.info({ pod, job: majorUpgradeJobName(cluster, ordinal) }, 'Major upgrade: converting the instance volume');
          await this.batchApi.createNamespacedJob({ namespace, body: buildMajorUpgradeJob(cluster, upgrade, ordinal, claimName) });
          running++;
          continue;
        }
        const outcome = jobOutcome(job);
        if (outcome === 'Completed') converted.push(pod);
        else if (outcome === 'Failed') failed.push(majorUpgradeJobName(cluster, ordinal));
        else running++;
      }
      if (failed.length > 0) {
        const message =
          `conversion failed: ${failed.join(', ')} (kubectl logs job/<name> -c backup / -c restore); ` +
          `the cluster stays stopped: delete the Job to retry, or set spec.imageName back to ${upgrade.from} while no volume was converted`;
        if (upgrade.phase !== 'Failed' || upgrade.message !== message) {
          log.warn({ failed }, 'Major upgrade: a conversion Job failed');
          await this.event(cluster, 'Warning', EventReason.MajorUpgradeFailed, message);
          await persist({ ...upgrade, phase: 'Failed', converted, message });
        }
        return {
          cluster: withEffectiveImage(cluster, upgrade.from, true),
          upgrading: `Major upgrade to ${upgrade.to}: ${message}`,
          conditions: [],
        };
      }
      if (running > 0 || converted.length < cluster.spec.instances) {
        const message = `converting the instance volumes (${converted.length}/${cluster.spec.instances} done)`;
        if (upgrade.phase !== 'Converting' || upgrade.message !== message || (upgrade.converted ?? []).length !== converted.length) {
          await persist({ ...upgrade, phase: 'Converting', converted, message });
        }
        return {
          cluster: withEffectiveImage(cluster, upgrade.from, true),
          upgrading: `Major upgrade to ${upgrade.to}: ${message}${pending}`,
          conditions: [],
        };
      }
      log.info('Major upgrade: every volume converted; starting the instances on the new image');
      await this.event(cluster, 'Normal', EventReason.MajorUpgradeStarting, `every volume converted: starting the instances on ${upgrade.to}`);
      await persist({ ...upgrade, phase: 'Starting', converted, message: `starting the instances on ${upgrade.to}` });
      for (const ordinal of found.keys()) {
        await this.batchApi.deleteNamespacedJob({ name: majorUpgradeJobName(cluster, ordinal), namespace, propagationPolicy: 'Background' });
      }
    }

    // Starting: the instances run the new image; the replicas are seeded again from the primary
    const effective = withEffectiveImage(cluster, upgrade.to);
    const pods = instancePods((await this.coreApi.listNamespacedPod({ namespace, labelSelector: instancePodSelector(name) })).items, name);
    if (cluster.spec.hibernated || instancesStartedOn(cluster, pods, upgrade.to, cluster.status?.fencedInstances ?? [])) {
      const message = `major upgrade from ${upgrade.from} to ${upgrade.to} completed`;
      log.info(message);
      await this.event(cluster, 'Normal', EventReason.MajorUpgradeCompleted, message);
      await persist({ ...upgrade, phase: 'Completed', message, completionTime: new Date().toISOString() });
      return { cluster: effective, conditions: [] };
    }
    return {
      cluster: effective,
      upgrading: `Major upgrade to ${upgrade.to}: starting the instances on the new image (replicas are seeded again)${pending}`,
      conditions: [],
    };
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
      if (primaryLeaseEnabled(cluster)) {
        metrics.set(
          'firebird_cluster_primary_lease_age_seconds',
          'Seconds since the primary last renewed its Lease (replication.failover.primaryLease)',
          { namespace, cluster: name },
          Math.round(leaseAgeSeconds(lease) ?? -1),
        );
      }
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
    result.rollingUpdate = cluster.status?.rollingUpdate;
    result.journalArchiveSequence = cluster.status?.journalArchiveSequence;
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
    result.rollingUpdate = current.status?.rollingUpdate;
    result.journalArchiveSequence = current.status?.journalArchiveSequence ?? cluster.status?.journalArchiveSequence;
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
    result.wantsSwitchover = !inFlight && wantsSwitchover;
    result.synchronous = current.status?.synchronous;
    const sync = current.status?.synchronous?.primary === primaryPod ? current.status.synchronous : undefined;
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
    const primaryPodReady = Boolean(podOf(primaryPod) && isPodReady(podOf(primaryPod)!));
    // a ready primary that neither the operator nor any replica reaches counts as unavailable
    const cutOff = primaryPodReady && failover?.enabled && !fenced.includes(primaryPod)
      ? await this.primaryCutOffReason(cluster, primaryPod, pods.items, fenced)
      : undefined;
    const primaryReady = primaryPodReady && !cutOff;
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

    // A primary that fenced itself while cut off from the cluster (isolation check) and still
    // holds the Lease: nothing was promoted meanwhile, so it is brought back online
    // (a primary that fenced itself over its Lease rejoins itself once it holds the Lease again, utils/primary-lease.ts)
    if (!inFlight && failover?.enabled && !primaryReady && !fenced.includes(primaryPod) && podOf(primaryPod)) {
      if (await this.rejoinIsolatedPrimary(cluster, primaryPod, log)) return result;
    }

    // Automatic failover: the primary has not been ready for failover.delaySeconds
    if (!inFlight && failover?.enabled) {
      const restart = current.status?.rollingUpdate?.primaryRestart;
      const graceMs = Math.max(PRIMARY_RESTART_GRACE_SECONDS, effectiveFailoverDelaySeconds(cluster)) * 1000;
      const plannedRestart = restart?.pod === primaryPod && Date.now() - Date.parse(restart.time) < graceMs;
      // a sync-standby Job keeps the primary in full shutdown while it attaches or detaches
      const syncJob =
        (sync?.phase === 'Attaching' || sync?.phase === 'Detaching') && Date.now() - Date.parse(sync.time) < graceMs;
      if (primaryReady || fenced.includes(primaryPod) || plannedRestart || syncJob) {
        // a fenced primary is never failed over, a primary restarted by a rolling update or shut
        // down by a sync-standby Job not yet
        result.primaryNotReadySince = undefined;
      } else {
        const since = current.status?.primaryNotReadySince ?? now;
        result.primaryNotReadySince = since;
        if (!current.status?.primaryNotReadySince) {
          const delay = effectiveFailoverDelaySeconds(cluster, cutOff !== undefined);
          await this.event(
            cluster,
            'Warning',
            EventReason.PrimaryNotReady,
            `primary ${primaryPod} ${cutOff ?? 'is not ready'}; failover in ${delay}s unless it recovers`,
          );
        }
        // a cut-off primary may still take writes until it fenced itself (isolation check)
        const delayMs = effectiveFailoverDelaySeconds(cluster, cutOff !== undefined) * 1000;
        const recentFailure =
          state?.kind === 'failover' && state.phase === 'Failed' && state.from === primaryPod &&
          Date.now() - Date.parse(state.completionTime ?? now) < delayMs;
        if (Date.now() - Date.parse(since) >= delayMs && !recentFailure) {
          // the synchronous standby has every committed transaction: promoted without an election,
          // and the other replicas (which may lack the old primary's unarchived changes) re-seeded
          // any ready attached standby (not one being detached): the lowest ordinal
          const promotable = attachedStandbys(sync)
            .filter((s) => !(sync?.phase === 'Detaching' && sync.standby === s))
            .filter((s) => { const p = podOf(s); return p && isPodReady(p) && !fenced.includes(s); })
            .sort((a, b) => Number(a.slice(name.length + 1)) - Number(b.slice(name.length + 1)));
          const primaryVersion = this.engineVersions.get(podOf(primaryPod)?.metadata?.uid ?? '');
          const standby = syncUnsupportedReason(primaryVersion) ? undefined : promotable[0];
          const standbyPod = standby ? podOf(standby) : undefined;
          if (sync && standby && standbyPod) {
            const reseed: Record<string, string> = {};
            for (const pod of pods.items) {
              const podName = pod.metadata!.name!;
              if (podName !== standby) reseed[podName] = pod.metadata?.uid ?? '';
            }
            reseed[primaryPod] = podOf(primaryPod)?.metadata?.uid ?? `failover-${Date.parse(now)}`;
            log.warn({ primary: primaryPod, standby }, 'Primary unavailable: promoting the synchronous standby');
            await persist({
              kind: 'failover',
              target: standby,
              from: primaryPod,
              phase: 'Promoting',
              message: `primary unavailable since ${since}: promoting the synchronous standby ${standby} (no transaction lost); ${primaryPod} and the other replicas will be re-seeded`,
              startTime: now,
              targetToken: standbyPod.metadata?.uid ?? '',
              reseed,
            });
            await this.customApi.patchNamespacedCustomObject(
              {
                group: API_GROUP,
                version: API_VERSION,
                namespace,
                plural: RESOURCE_PLURAL,
                name,
                body: { metadata: { annotations: { [TARGET_PRIMARY_ANNOTATION]: standby } } },
              },
              MERGE_PATCH,
            );
            result.primaryNotReadySince = undefined;
            return result;
          }
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

    if (!inFlight && synchronousMembers(sync).length > 0) {
      // the standbys receive changes synchronously, not from the journal: detached first
      log.info({ standbys: synchronousMembers(sync), phase: sync?.phase }, 'Switchover waits for the synchronous standbys to be detached');
      return result;
    }

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
          body: buildSwitchoverJob(cluster, {
            from: primaryPod,
            target,
            replicas,
            archived: await this.journalArchiveSequence(cluster, current.status?.journalArchiveSequence),
          }),
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

      // the primary Lease (utils/primary-lease.ts): the old primary is replaced only once it has
      // stopped renewing its Lease, so it is down or fenced, never still taking writes
      if (primaryLeaseEnabled(cluster)) {
        const lease = await this.coordinationApi.readNamespacedLease({ name: `${name}-lease`, namespace }).catch(() => undefined);
        if (lease?.spec?.holderIdentity === phase.from && !leaseExpired(lease, primaryLeaseDurationSeconds(cluster))) {
          const message = `elected ${target}; waiting for the Lease of ${phase.from} to expire before promoting it`;
          log.warn({ primary: phase.from, target, ageSeconds: leaseAgeSeconds(lease) }, 'Failover waits for the primary Lease to expire');
          if (phase.message !== message) await persist({ ...phase, message }, EventReason.PrimaryLeaseHeld);
          return result;
        }
        // the Lease is taken over with the version that was read: a renewal by the old primary
        // that lands first (it reached the API server again and re-acquired it) wins, and the
        // failover looks again
        if (lease) {
          try {
            await this.coordinationApi.replaceNamespacedLease({
              name: `${name}-lease`,
              namespace,
              body: { ...lease, spec: { ...lease.spec, holderIdentity: target, renewTime: new V1MicroTime() } },
            });
          } catch (err) {
            if ((err as { code?: number })?.code !== 409) throw err;
            log.warn({ primary: phase.from, target }, 'The primary renewed its Lease while the failover took it over; looking again');
            return result;
          }
        }
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
          body: buildSwitchoverJob(cluster, {
            from: phase.from,
            target: phase.target,
            replicas: [],
            archived: await this.journalArchiveSequence(cluster, current.status?.journalArchiveSequence),
          }),
        });
        return result;
      }
      const conditions = job.status?.conditions ?? [];
      const jobFailed = conditions.some((c) => c.type === 'Failed' && c.status === 'True');
      // a Job that ended after promoting the target in place (before reporting it): the target is
      // the primary now, and the old primary must stay shut down
      const targetPromoted = jobFailed && (await this.promotedInPlace(cluster, phase.target));
      if (jobFailed && !targetPromoted) {
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
      if (!targetPromoted && !conditions.some((c) => c.type === 'Complete' && c.status === 'True')) return result;
      // "inplace <S>": the Job promoted the target without a restart (switchover.pl)
      const jobPods = await this.coreApi.listNamespacedPod({ namespace, labelSelector: `job-name=${jobName}` });
      const outcome = jobPods.items.map((p) => p.status?.containerStatuses?.[0]?.state?.terminated?.message ?? '').find((m) => m !== '');
      const promotedInPlace = targetPromoted || /^inplace \d+/.test(outcome ?? '');

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
        message: promotedInPlace
          ? 'target promoted in place; demoting the old primary'
          : 'promoting the target and demoting the old primary',
        ...(promotedInPlace ? { promotedInPlace: true } : {}),
        targetToken: podOf(phase.target)?.metadata?.uid ?? '',
        fromToken: podOf(phase.from)?.metadata?.uid ?? '',
        ...(Object.keys(lagging).length ? { reseed: lagging } : {}),
      };
      await persist(phase);
      await this.batchApi.deleteNamespacedJob({ name: jobName, namespace, propagationPolicy: 'Background' });
      log.info({ primary: phase.target, demoted: phase.from, promotedInPlace, outcome }, 'Promoting the switchover target');
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
    // a target promoted in place keeps running: ready is enough
    const inPlace = Boolean(phase.promotedInPlace);
    // a failover's target is promoted in place by a Job once the primary moved to it (the election
    // had to stay discardable); undecided until that Job ended, then restarted only if it failed
    const promotionPending = isFailover && phase.promotedInPlace === undefined;
    const targetDone = inPlace ? Boolean(podOf(phase.target) && isPodReady(podOf(phase.target)!)) : restarted(phase.target, phase.targetToken);
    if (targetDone && (isFailover || restarted(phase.from, phase.fromToken))) {
      log.info({ primary: phase.target }, 'Switchover completed');
      await persist({ ...phase, phase: 'Completed', message: `${phase.target} is the primary`, completionTime: now });
      return result;
    }
    // the promoted replica's journal continues after every segment the journal archive may hold
    const archived = await this.journalArchiveSequence(cluster, result.journalArchiveSequence);
    // (in place: the init container only records the directive, should the target restart)
    result.promote = { [phase.target]: `${phase.targetToken ?? ''}${archived !== undefined ? ` ${archived}` : ''}` };
    // after a failover the old primary diverged (its unshipped transactions): it is re-seeded
    result.demote = isFailover ? {} : { [phase.from]: phase.fromToken ?? '' };
    // pods still running with the UID they had when the primary moved (not yet restarted)
    const stillOld = (pod: string, token?: string) => {
      const p = podOf(pod);
      return Boolean(p && p.metadata?.uid === token && !p.metadata?.deletionTimestamp);
    };
    const restartable: Array<[string, string | undefined]> = isFailover
      ? inPlace || promotionPending
        ? []
        : [[phase.target, phase.targetToken]]
      : inPlace
        ? [[phase.from, phase.fromToken]]
        : [[phase.target, phase.targetToken], [phase.from, phase.fromToken]];
    for (const [pod, token] of restartable) {
      if (stillOld(pod, token)) result.restart.push(pod);
    }
    for (const [pod, token] of Object.entries(phase.reseed ?? {})) {
      result.reseed[pod] = token;
      if (stillOld(pod, token)) result.restart.push(pod);
    }
    if (promotionPending) {
      const promoted = await this.failoverPromotion(cluster, phase.target, archived, log);
      if (promoted !== undefined) {
        phase = {
          ...phase,
          promotedInPlace: promoted.ok,
          message: promoted.ok
            ? `${phase.target} promoted in place (${promoted.reply}); ${phase.from} will be re-seeded`
            : `in-place promotion of ${phase.target} failed (${promoted.reply}): restarting it to promote it offline`,
        };
        await persist(phase);
        if (!promoted.ok && stillOld(phase.target, phase.targetToken)) result.restart.push(phase.target);
      }
    }
    result.status = phase;
    return result;
  }

  /**
   * Runs the promote Job of a failover (buildPromoteJob) and reads its outcome: undefined while it
   * runs, then whether the target was promoted in place and the segment server's reply.
   */
  private async failoverPromotion(
    cluster: FirebirdCluster,
    target: string,
    archived: number | undefined,
    log: Logger,
  ): Promise<{ ok: boolean; reply: string } | undefined> {
    const namespace = cluster.metadata.namespace ?? 'default';
    const jobName = promoteJobName(cluster);
    let job;
    try {
      job = await this.batchApi.readNamespacedJob({ name: jobName, namespace });
    } catch (err) {
      if (!isNotFound(err)) throw err;
    }
    if (!job || job.metadata?.annotations?.[TARGET_PRIMARY_ANNOTATION] !== target) {
      if (job) {
        // left over from an earlier failover
        await this.batchApi.deleteNamespacedJob({ name: jobName, namespace, propagationPolicy: 'Background' });
        return undefined;
      }
      log.info({ target }, 'Promoting the failover target in place');
      await this.batchApi.createNamespacedJob({ namespace, body: buildPromoteJob(cluster, target, archived) });
      return undefined;
    }
    const conditions = job.status?.conditions ?? [];
    const failed = conditions.some((c) => (c.type === 'Failed' || c.type === 'FailureTarget') && c.status === 'True');
    const complete = conditions.some((c) => c.type === 'Complete' && c.status === 'True');
    if (!failed && !complete) return undefined;
    const jobPods = await this.coreApi.listNamespacedPod({ namespace, labelSelector: `job-name=${jobName}` });
    const reply =
      jobPods.items.map((p) => p.status?.containerStatuses?.[0]?.state?.terminated?.message ?? '').find((m) => m !== '')?.trim() ??
      'no reply';
    await this.batchApi.deleteNamespacedJob({ name: jobName, namespace, propagationPolicy: 'Background' });
    return { ok: complete && /^OK \d+$/.test(reply), reply: failed ? `Job failed: ${reply}` : reply };
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
    syncStandbys: string[] = [],
  ): Promise<{ requests: Record<string, string>; restart: string[]; held: string[] }> {
    const result = { requests: {} as Record<string, string>, restart: [] as string[], held: [] as string[] };
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
      if (syncStandbys.includes(podName)) {
        log.info({ pod: podName }, 'Re-seed of the synchronous standby waits until it is detached');
        result.held.push(podName);
        continue;
      }
      result.requests[podName] = pod.metadata.uid ?? '';
      result.restart.push(podName);
      await this.event(cluster, 'Normal', EventReason.ReseedStarted, `re-seeding ${podName} (reseed annotation)`);
    }
    return result;
  }

  /**
   * Re-creates the volume of replicas annotated reseed=volume (see utils/volume-recreation.ts):
   * deletes the claim and the pod, and the pod again until the StatefulSet made a new claim.
   */
  private async reconcileVolumeRecreation(
    cluster: FirebirdCluster,
    primaryPod: string,
    log: Logger,
    syncStandbys: string[] = [],
  ): Promise<{ recreating: VolumeRecreationStatus[]; held: string[] }> {
    const { name, namespace = 'default' } = cluster.metadata;
    // only replicas of a replication cluster can be re-created (others would start empty)
    if (!replicationEnabled(cluster) || cluster.spec.hibernated) return { recreating: cluster.status?.recreatingVolumes ?? [], held: [] };
    const listed = instancePods(
      (await this.coreApi.listNamespacedPod({ namespace, labelSelector: instancePodSelector(name) })).items,
      name,
    );
    // the synchronous standby's request waits until it is detached
    const held = listed
      .filter((p) => syncStandbys.includes(p.metadata?.name ?? '') && p.metadata?.annotations?.[RESEED_ANNOTATION] === RESEED_VOLUME)
      .filter((p) => !(cluster.status?.recreatingVolumes ?? []).some((r) => r.pod === p.metadata?.name))
      .map((p) => p.metadata!.name!);
    const pods = listed.map((p) =>
      held.includes(p.metadata?.name ?? '')
        ? { ...p, metadata: { ...p.metadata, annotations: { ...p.metadata?.annotations, [RESEED_ANNOTATION]: '' } } }
        : p,
    );
    if (held.length) log.info({ pod: held[0] }, 'Volume re-creation of the synchronous standby waits until it is detached');
    const requested = pods.some((p) => p.metadata?.annotations?.[RESEED_ANNOTATION] === RESEED_VOLUME);
    if (!requested && !cluster.status?.recreatingVolumes?.length) return { recreating: [], held };
    const claims = await this.coreApi.listNamespacedPersistentVolumeClaim({
      namespace,
      labelSelector: `${CLUSTER_LABEL}=${name}`,
    });
    const plan = planVolumeRecreation({
      cluster,
      primaryPod,
      pods,
      claims: claims.items,
      replication: replicationEnabled(cluster),
    });
    for (const { pod, reason } of plan.ignored) {
      log.warn({ pod, reason }, 'Ignoring the reseed=volume annotation');
    }
    // the requests outlive the annotated pods: recorded before anything is deleted, and kept in the
    // in-memory status so a failure later in this reconcile does not drop them
    cluster.status = { ...(cluster.status ?? {}), recreatingVolumes: plan.recreating };
    if (plan.started.length > 0) await this.updateStatus(cluster, {});
    for (const pod of plan.started) {
      log.info({ pod }, 'Re-creating the volume of a replica');
      await this.event(cluster, 'Normal', EventReason.VolumeRecreating, `re-creating the volume of ${pod} (reseed=volume annotation)`);
    }
    for (const pod of plan.deleteClaims) {
      try {
        await this.coreApi.deleteNamespacedPersistentVolumeClaim({ name: dataClaimName(pod), namespace });
      } catch (err) {
        if (!isNotFound(err)) throw err;
      }
    }
    // the claim is released (pvc-protection) once no pod uses it; a pod recreated against the old
    // claim, or waiting for a claim the StatefulSet only creates with a new pod, is deleted again
    for (const pod of plan.deletePods) {
      try {
        await this.coreApi.deleteNamespacedPod({ name: pod, namespace });
      } catch (err) {
        if (!isNotFound(err)) throw err;
      }
    }
    for (const pod of plan.completed) {
      log.info({ pod }, 'Replica volume re-created and seeded');
      await this.event(cluster, 'Normal', EventReason.VolumeRecreated, `${pod} runs on a new volume and was seeded`);
    }
    return { recreating: plan.recreating, held };
  }

  /**
   * Ready replicas that can serve seed copies to new replicas, so seeding does not lock or
   * load the primary (see ISSUES.md, issue 2).
   */
  private async resolveSeedSources(cluster: FirebirdCluster, primaryPod: string): Promise<{ seedSources: string[]; peerAddresses: string[] }> {
    if (!replicationEnabled(cluster) || cluster.spec.hibernated) return { seedSources: [], peerAddresses: [] };
    const { name, namespace = 'default' } = cluster.metadata;
    const pods = {
      items: instancePods(
        (await this.coreApi.listNamespacedPod({ namespace, labelSelector: instancePodSelector(name) })).items,
        name,
      ),
    };
    return {
      seedSources: pods.items
        .filter((pod) => pod.metadata?.name && pod.metadata.name !== primaryPod && isPodReady(pod))
        .map((pod) => pod.metadata!.name!)
        .sort(),
      // every instance with an address, ready or not: the isolation check's peers without DNS
      peerAddresses: [
        ...new Set(pods.items.filter((pod) => !pod.metadata?.deletionTimestamp).map((pod) => pod.status?.podIP).filter((ip): ip is string => !!ip)),
      ].sort(),
    };
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
  /**
   * Point-in-time bootstrap (bootstrap.recovery.pointInTime). Before the StatefulSet exists, the
   * operator creates the first instance's volume under the StatefulSet's claim name (the
   * StatefulSet adopts it) and runs a Job that leaves the recovered database on it. Returns a
   * status message while the recovery runs, undefined once the StatefulSet may be created.
   */
  private async reconcileRecoveryBootstrap(cluster: FirebirdCluster, log: Logger): Promise<string | undefined> {
    if (!cluster.spec.bootstrap?.recovery?.pointInTime) return undefined;
    const { name, namespace = 'default' } = cluster.metadata;
    try {
      await this.appsApi.readNamespacedStatefulSet({ name, namespace });
      return undefined; // recovered (or created) already
    } catch (err) {
      if (!isNotFound(err)) throw err;
    }

    const claimName = instanceDataClaimName(cluster, 0);
    try {
      await this.coreApi.readNamespacedPersistentVolumeClaim({ name: claimName, namespace });
    } catch (err) {
      if (!isNotFound(err)) throw err;
      const template = buildStatefulSet(cluster).spec!.volumeClaimTemplates![0];
      log.info({ claimName }, 'Creating the first instance volume for point-in-time recovery');
      await this.coreApi.createNamespacedPersistentVolumeClaim({
        namespace,
        body: {
          apiVersion: 'v1',
          kind: 'PersistentVolumeClaim',
          metadata: { ...template.metadata, name: claimName, namespace },
          spec: template.spec,
        },
      });
    }

    const jobName = recoveryBootstrapJobName(cluster);
    let job: V1Job;
    try {
      job = await this.batchApi.readNamespacedJob({ name: jobName, namespace });
    } catch (err) {
      if (!isNotFound(err)) throw err;
      log.info({ jobName }, 'Starting point-in-time recovery');
      await this.batchApi.createNamespacedJob({ namespace, body: buildRecoveryBootstrapJob(cluster) });
      await this.event(cluster, 'Normal', EventReason.RecoveryStarted, `recovering the database to a point in time (Job ${jobName})`);
      return `Recovering the database to a point in time (Job ${jobName})`;
    }
    switch (jobOutcome(job)) {
      case 'Completed':
        log.info({ jobName }, 'Point-in-time recovery completed');
        await this.event(cluster, 'Normal', EventReason.RecoveryCompleted, `database recovered by Job ${jobName}; starting the instances`);
        return undefined;
      case 'Failed':
        throw new Error(`point-in-time recovery Job ${jobName} failed; see its pod logs, then delete the Job to retry`);
      default:
        return `Recovering the database to a point in time (Job ${jobName})`;
    }
  }

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

    // podManagementPolicy is immutable: a StatefulSet created before Parallel is replaced, its
    // pods orphaned and adopted by the new one (same template: nothing restarts)
    if ((existing.spec?.podManagementPolicy ?? 'OrderedReady') !== desired.spec?.podManagementPolicy) {
      log.info('Re-creating the StatefulSet for parallel pod management; the pods keep running');
      await this.appsApi.deleteNamespacedStatefulSet({ name, namespace, propagationPolicy: 'Orphan' });
      let gone = false;
      for (let i = 0; i < 20 && !gone; i++) {
        try {
          await this.appsApi.readNamespacedStatefulSet({ name, namespace });
          await new Promise((resolve) => setTimeout(resolve, 500));
        } catch (err) {
          if (!isNotFound(err)) throw err;
          gone = true;
        }
      }
      // still being deleted: the next reconcile creates it
      if (!gone) return { readyInstances: existing.status?.readyReplicas ?? 0, superuserSecretHash, statefulSetExisted: true, statefulSet: existing };
      if (desired.spec && existing.spec?.volumeClaimTemplates) desired.spec.volumeClaimTemplates = existing.spec.volumeClaimTemplates;
      const created = await this.appsApi.createNamespacedStatefulSet({ namespace, body: desired });
      return { readyInstances: existing.status?.readyReplicas ?? 0, superuserSecretHash, statefulSetExisted: true, statefulSet: created };
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
  /**
   * Drops users whose FirebirdUser was deleted while an instance was not ready (utils/pending-drops.ts)
   * once that instance is ready, through a Job, and clears the entries. A user that a FirebirdUser of
   * the cluster declares again is not dropped; an instance scaled away with its volume gone needs
   * nothing.
   */
  private async reconcilePendingUserDrops(cluster: FirebirdCluster, log: Logger): Promise<void> {
    const { name, namespace = 'default' } = cluster.metadata;
    if (cluster.spec.hibernated) return;
    const cmName = pendingDropsConfigMapName(name);
    let cm: V1ConfigMap;
    try {
      cm = await this.coreApi.readNamespacedConfigMap({ name: cmName, namespace });
    } catch (err) {
      if (isNotFound(err)) return;
      throw err;
    }
    const entries = Object.entries(cm.data ?? {});
    if (entries.length === 0) return;

    const users = (await this.customApi.listNamespacedCustomObject({
      group: API_GROUP,
      version: API_VERSION,
      namespace,
      plural: 'firebirdusers',
    })) as { items?: FirebirdUser[] };
    const declared = new Set(
      (users.items ?? [])
        .filter((u) => u.spec?.clusterName === name && !u.metadata?.deletionTimestamp)
        .map((u) => firebirdUsername(u)),
    );
    const [pods, pvcs] = await Promise.all([
      this.coreApi.listNamespacedPod({ namespace, labelSelector: instancePodSelector(name) }),
      this.coreApi.listNamespacedPersistentVolumeClaim({ namespace, labelSelector: `${CLUSTER_LABEL}=${name}` }),
    ]);

    const data: Record<string, string> = {};
    for (const [pod, value] of entries) {
      let drops = parsePendingDrops(String(value)).filter((d) => !declared.has(d.username));
      const ordinal = Number(pod.slice(name.length + 1));
      if (ordinal >= cluster.spec.instances && !pvcs.items.some((p) => p.metadata?.name === dataClaimName(pod))) {
        drops = [];
      }
      const jobName = pendingDropJobName(pod);
      let job: V1Job | undefined;
      try {
        job = await this.batchApi.readNamespacedJob({ name: jobName, namespace });
      } catch (err) {
        if (!isNotFound(err)) throw err;
      }
      if (job) {
        const outcome = jobOutcome(job);
        if (outcome === 'Completed') {
          const done = String(job.metadata?.annotations?.[PENDING_DROP_USERS_ANNOTATION] ?? '').split(' ');
          drops = drops.filter((d) => !done.includes(d.username));
          log.info({ pod, users: done }, 'Pending user drops applied');
        }
        if (outcome !== 'Running') {
          // a failed Job is retried on a later reconcile
          await this.batchApi.deleteNamespacedJob({ name: jobName, namespace, propagationPolicy: 'Background' });
        }
      } else if (drops.length > 0) {
        const podObj = pods.items.find((p) => p.metadata?.name === pod);
        if (podObj && isPodReady(podObj)) {
          const body = buildPendingDropJob(cluster, pod, drops.map((d) => d.username), !replicationEnabled(cluster));
          try {
            await this.batchApi.createNamespacedJob({ namespace, body });
            log.info({ pod, users: drops.map((d) => d.username) }, 'Dropping users left pending on an instance');
          } catch (err) {
            if ((err as { code?: number })?.code !== 409) throw err;
          }
        }
      }
      if (drops.length > 0) data[pod] = formatPendingDrops(drops);
    }

    const unchanged =
      Object.keys(data).length === entries.length && entries.every(([pod, value]) => data[pod] === formatPendingDrops(parsePendingDrops(String(value))));
    if (unchanged) return;
    if (Object.keys(data).length === 0) {
      await this.coreApi.deleteNamespacedConfigMap({ name: cmName, namespace });
    } else {
      // a concurrent change (a new pending drop) fails the replace; retried on the next reconcile
      await this.coreApi.replaceNamespacedConfigMap({ name: cmName, namespace, body: { ...cm, data } });
    }
  }

  private async reconcileRollingUpdate(
    cluster: FirebirdCluster,
    statefulSet: V1StatefulSet,
    primaryPod: string,
    fenced: string[],
    busy: string | undefined,
    switchover: SwitchoverResult,
    syncStandbys: Array<{ pod: string; hold: boolean }> | undefined,
    log: Logger,
  ): Promise<RollingUpdateStatus | undefined> {
    const lastSwitchover = switchover.status;
    // from the stored status: a stale copy would lose the primary restart (and with it the grace
    // automatic failover gives the restarted primary)
    const stored = switchover.rollingUpdate;
    const { name, namespace = 'default' } = cluster.metadata;
    if (cluster.spec.hibernated) return undefined;
    const pods = instancePods(
      (await this.coreApi.listNamespacedPod({ namespace, labelSelector: instancePodSelector(name) })).items,
      name,
    );
    const plan = planRollingUpdate({ cluster, statefulSet, pods, primaryPod, fenced, busy, lastSwitchover, syncStandbys });
    // the StatefulSet controller has not observed the latest template yet: keep the last status
    if (!plan) return stored;
    const primary = pods.find((p) => p.metadata?.name === primaryPod);
    // automatic failover leaves the restarted primary alone until it is ready again
    let primaryRestart = stored?.primaryRestart;
    if (primaryRestart && (primaryRestart.pod !== primaryPod || (primary && isPodReady(primary) && primary.metadata?.uid !== primaryRestart.uid))) {
      primaryRestart = undefined;
    }
    if (plan.outdated.length === 0) {
      if (stored && !primaryRestart) {
        await this.event(cluster, 'Normal', EventReason.RollingUpdateCompleted, `all instances run revision ${plan.revision}`);
      }
      return primaryRestart
        ? { revision: plan.revision, outdatedInstances: [], message: `waiting for ${primaryPod} to be ready`, primaryRestart }
        : undefined;
    }

    // pods whose only change is container resources are resized in place instead (in-place.ts)
    const inPlace = busy ? new Set<string>() : await this.resizeInPlace(cluster, pods, plan.revision, log);
    if (plan.restart && inPlace.has(plan.restart)) {
      return {
        revision: plan.revision,
        outdatedInstances: plan.outdated,
        message: `resizing ${[...inPlace].sort().join(', ')} in place to revision ${plan.revision}`,
        ...(primaryRestart ? { primaryRestart } : {}),
      };
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

  /**
   * Resizes the outdated instance pods whose revision differs from the new one only in container
   * resources (in-place.ts), and relabels each with the new revision once the kubelet applied it.
   * Returns the pods handled in place (resizing or done); the others are restarted as usual.
   */
  private async resizeInPlace(
    cluster: FirebirdCluster,
    pods: V1Pod[],
    revision: string,
    log: Logger,
  ): Promise<Set<string>> {
    const namespace = cluster.metadata.namespace ?? 'default';
    const handled = new Set<string>();
    const templates = new Map<string, V1PodTemplateSpec | undefined>();
    // the revision label (and the StatefulSet's updateRevision) is the ControllerRevision's name
    const template = async (revisionName: string) => {
      if (!templates.has(revisionName)) {
        try {
          const rev = await this.appsApi.readNamespacedControllerRevision({ name: revisionName, namespace });
          templates.set(revisionName, (rev.data as { spec?: { template?: V1PodTemplateSpec } } | undefined)?.spec?.template);
        } catch (err) {
          if (!isNotFound(err)) throw err;
          templates.set(revisionName, undefined);
        }
      }
      return templates.get(revisionName);
    };
    const target = await template(revision);
    if (!target) return handled;
    for (const pod of pods) {
      const podName = pod.metadata?.name ?? '';
      const current = pod.metadata?.labels?.[REVISION_LABEL];
      if (!current || current === revision || pod.metadata?.deletionTimestamp) continue;
      const marker = pod.metadata?.annotations?.[RESIZE_ANNOTATION] ?? '';
      if (marker === `failed ${revision}`) continue;
      const source = await template(current);
      const resizes = source ? inPlaceResize(source, target) : undefined;
      if (!resizes) continue;
      const setMetadata = (labels: Record<string, string | null>, annotations: Record<string, string | null>) =>
        this.coreApi.patchNamespacedPod(
          { name: podName, namespace, body: { metadata: { labels, annotations } } },
          MERGE_PATCH,
        );
      const [requested, since] = marker.split(' ');
      if (requested === revision) {
        if (resizeApplied(pod, resizes)) {
          await setMetadata({ [REVISION_LABEL]: revision }, { [RESIZE_ANNOTATION]: null });
          log.info({ pod: podName, revision }, 'Rolling update: resized in place');
          await this.event(cluster, 'Normal', EventReason.RollingUpdate, `${podName} resized in place to revision ${revision} (no restart)`);
          handled.add(podName);
        } else if (resizeInfeasible(pod) || Date.now() - Number(since) > RESIZE_TIMEOUT_SECONDS * 1000) {
          await setMetadata({}, { [RESIZE_ANNOTATION]: `failed ${revision}` });
          log.warn({ pod: podName, revision }, 'Rolling update: in-place resize not applied; restarting instead');
        } else {
          handled.add(podName);
        }
        continue;
      }
      try {
        await this.coreApi.patchNamespacedPodResize(
          { name: podName, namespace, body: { spec: { containers: resizes.map((r) => ({ name: r.name, resources: r.resources })) } } },
          setHeaderOptions('Content-Type', PatchStrategy.StrategicMergePatch),
        );
      } catch (err) {
        // e.g. a cluster without in-place pod resize: restarted as before
        log.warn({ err, pod: podName }, 'Rolling update: in-place resize refused; restarting instead');
        await setMetadata({}, { [RESIZE_ANNOTATION]: `failed ${revision}` });
        continue;
      }
      await setMetadata({}, { [RESIZE_ANNOTATION]: `${revision} ${Date.now()}` });
      log.info({ pod: podName, revision, containers: resizes.map((r) => r.name) }, 'Rolling update: resizing in place');
      await this.event(cluster, 'Normal', EventReason.RollingUpdate, `resizing ${podName} in place (${resizes.map((r) => r.name).join(', ')})`);
      handled.add(podName);
    }
    return handled;
  }

  /** The SYSDBA password, which is also the segment servers' token */
  private async superuserPassword(cluster: FirebirdCluster): Promise<string | undefined> {
    const secret = cluster.spec.superuserSecret?.name;
    if (!secret) return superuserPasswordFrom(cluster, undefined);
    try {
      return superuserPasswordFrom(cluster, await this.coreApi.readNamespacedSecret({ name: secret, namespace: cluster.metadata.namespace ?? 'default' }));
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
  ): Promise<
    | { lastArchivedSequence?: number; replicas?: ReplicaLagStatus[]; segmentRetention?: SegmentRetentionStatus }
    | undefined
  > {
    const { name, namespace = 'default' } = cluster.metadata;
    if (!replicationEnabled(cluster) || cluster.spec.hibernated) return undefined;
    const previous = cluster.status?.replicationStatus;
    // the retention floor is kept (not re-measured) whenever the replicas cannot be measured
    const keep = () => (previous?.segmentRetention ? { segmentRetention: previous.segmentRetention } : undefined);
    // positions are meaningless while the primary moves
    if (switchoverInFlight(switchover)) return previous?.replicas ? {
      lastArchivedSequence: previous.lastArchivedSequence,
      replicas: previous.replicas,
      ...keep(),
    } : keep();
    // a single instance has no replica to measure, only a floor to clear
    if (cluster.spec.instances < 2 && previous?.segmentRetention?.floorSequence === undefined) return undefined;
    const token = await this.superuserPassword(cluster);
    if (token === undefined) return keep();

    const pods = instancePods(
      (await this.coreApi.listNamespacedPod({ namespace, labelSelector: instancePodSelector(name) })).items,
      name,
    );
    const host = (pod: string) => `${instanceHost(cluster, pod)}.${namespace}.svc`;
    const primary = pods.find((p) => p.metadata?.name === primaryPod);
    if (!primary || !isPodReady(primary)) return keep();
    if (cluster.spec.instances < 2) {
      try {
        await this.retainSegments(host(primaryPod), token, undefined);
        return undefined;
      } catch (err) {
        log.debug({ err }, 'Could not clear the segment retention floor');
        return keep();
      }
    }
    let archived;
    try {
      archived = parseArchived(await this.segmentClient(host(primaryPod), SEGMENT_PORT, `${token} ARCHIVED`));
    } catch (err) {
      log.debug({ err }, 'Could not list the primary archived segments');
      return keep();
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
    // the primary keeps the segments the replicas (ready or not) have not applied yet
    const replicaNames = Array.from({ length: cluster.spec.instances }, (_, i) => `${name}-${i}`).filter(
      (pod) => pod !== primaryPod,
    );
    const retention = segmentRetention({ replicaNames, measured: replicas, previous: previous?.segmentRetention });
    try {
      await this.retainSegments(host(primaryPod), token, retention.floorSequence);
    } catch (err) {
      log.debug({ err }, 'Could not send the segment retention floor to the primary');
    }
    return {
      ...(archived.length ? { lastArchivedSequence: archived[archived.length - 1].sequence } : {}),
      replicas: replicas.sort((a, b) => a.name.localeCompare(b.name, undefined, { numeric: true })),
      segmentRetention: retention,
    };
  }

  /** Tells the primary's segment server which archived segments the replicas still need */
  private async retainSegments(host: string, token: string, floorSequence: number | undefined): Promise<void> {
    const reply = await this.segmentClient(host, SEGMENT_PORT, `${token} RETAIN ${floorSequence ?? 'none'}`);
    if (reply[0] !== 'OK') throw new Error(reply[0] ?? 'empty RETAIN reply');
  }

  /** Reconcile CronJob for replication journal continuous archiving to S3 */
  /** status.journalArchiveSequence: see journalArchiveListedSequence (utils/backup.ts) */
  private async journalArchiveSequence(cluster: FirebirdCluster, stored?: number): Promise<number | undefined> {
    if (!replicationEnabled(cluster) || !cluster.spec.replication?.journalArchiveS3) return stored;
    const pods = (
      await this.coreApi.listNamespacedPod({
        namespace: cluster.metadata.namespace ?? 'default',
        labelSelector: journalArchivePodSelector(cluster),
      })
    ).items;
    return journalArchiveListedSequence(pods, stored);
  }

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
      const desired = withHibernation(buildBackupCronJob(cluster, instance, instance !== primaryPod), cluster);
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
    peerAddresses?: string[],
  ): Promise<void> {
    const { name, namespace = 'default' } = cluster.metadata;
    const configName = `${name}-config`;
    const desired = buildConfigMap(cluster, {
      primaryPod,
      seedSourcePods,
      peerAddresses,
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
      const clones = await this.cloneTargets(cluster, log);
      const desired = buildNetworkPolicy(cluster, clones ?? []);
      try {
        const existing = await this.networkingApi.readNamespacedNetworkPolicy({ name: npName, namespace });
        // without the list of clones, an update could drop the rule a running clone relies on
        if (clones && networkPolicyNeedsUpdate(existing, desired)) {
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

  /** Clusters in any namespace cloning from this one (undefined when they cannot be listed) */
  private async cloneTargets(cluster: FirebirdCluster, log: Logger): Promise<CloneTarget[] | undefined> {
    try {
      const list = (await this.customApi.listClusterCustomObject({
        group: API_GROUP,
        version: API_VERSION,
        plural: RESOURCE_PLURAL,
      })) as { items?: FirebirdCluster[] };
      return cloneTargets(cluster, list.items ?? []);
    } catch (err) {
      log.warn({ err }, 'Could not list the clusters cloning from this one');
      return undefined;
    }
  }

  /**
   * Why a primary whose pod is ready is cut off all the same, if it is (see primaryCutOff in
   * utils/switchover.ts): the operator asks its segment server, and every ready replica's segment
   * server when its puller last reached it (PRIMARYSEEN).
   */
/** Whether an instance reports itself as the primary (POSITION "OK primary": promoted in place) */
  private async promotedInPlace(cluster: FirebirdCluster, pod: string): Promise<boolean> {
    const token = await this.superuserPassword(cluster);
    if (token === undefined) return false;
    const host = `${instanceHost(cluster, pod)}.${cluster.metadata.namespace ?? 'default'}.svc`;
    try {
      return (await this.segmentClient(host, SEGMENT_PORT, `${token} POSITION`))[0] === 'OK primary';
    } catch {
      return false;
    }
  }

    private async primaryCutOffReason(
    cluster: FirebirdCluster,
    primaryPod: string,
    pods: V1Pod[],
    fenced: string[],
  ): Promise<string | undefined> {
    const token = await this.superuserPassword(cluster);
    if (token === undefined) return undefined;
    const namespace = cluster.metadata.namespace ?? 'default';
    const address = (pod: string) => `${instanceHost(cluster, pod)}.${namespace}.svc`;
    const ask = async (pod: string, command: string): Promise<string | undefined> => {
      try {
        return (await this.segmentClient(address(pod), SEGMENT_PORT, `${token} ${command}`, 3000))[0];
      } catch {
        return undefined;
      }
    };
    if ((await ask(primaryPod, 'ISOLATION')) !== undefined) return undefined;
    const replicas = pods
      .map((p) => p.metadata?.name ?? '')
      .filter((pod) => pod !== primaryPod && !fenced.includes(pod) && isPodReady(pods.find((p) => p.metadata?.name === pod)!));
    const contacts = await Promise.all(replicas.map(async (pod) => ({ pod, reply: await ask(pod, 'PRIMARYSEEN') })));
    return primaryCutOff({ primaryHost: instanceHost(cluster, primaryPod), operatorReached: false, replicas: contacts });
  }

  /**
   * Asks the primary's segment server whether its isolation check fenced it, and if so brings its
   * database back online (REJOIN). The caller has checked that the pod still holds the Lease and
   * no switchover or failover is under way. Returns true when the primary was rejoined.
   */
  private async rejoinIsolatedPrimary(cluster: FirebirdCluster, primaryPod: string, log: Logger): Promise<boolean> {
    if (isolationCheckTimeoutSeconds(cluster) === undefined) return false;
    const namespace = cluster.metadata.namespace ?? 'default';
    const token = await this.superuserPassword(cluster);
    if (token === undefined) return false;
    const host = `${instanceHost(cluster, primaryPod)}.${namespace}.svc`;
    try {
      const reply = await this.segmentClient(host, SEGMENT_PORT, `${token} ISOLATION`);
      if (!reply[0]?.startsWith('OK fenced')) return false;
      // fenced by its Lease holder (the Lease could not be renewed, or names another instance): the
      // holder lifts it once it holds the Lease again, or the failover replaces the primary
      if (reply[0].endsWith(' lease')) return false;
      const answer = await this.segmentClient(host, SEGMENT_PORT, `${token} REJOIN`);
      if (answer[0] !== 'OK') throw new Error(answer[0] ?? 'no answer');
    } catch (err) {
      log.debug({ err, primary: primaryPod }, 'Could not check or lift the isolation fence of the primary');
      return false;
    }
    log.warn({ primary: primaryPod }, 'Primary had fenced itself while isolated; still the primary: brought back online');
    await this.event(
      cluster,
      'Normal',
      EventReason.PrimaryRejoined,
      `primary ${primaryPod} fenced itself while cut off from the cluster and still holds the Lease: database back online`,
    );
    return true;
  }

  /**
   * Synchronous replication (utils/synchronous.ts): attaches a synchronous standby to the primary
   * and detaches it when needed, one sync-standby Job at a time. Each phase is stored before it is
   * acted upon. Returns the state to keep in status.synchronous, and the standby of the primary for
   * the rolling update (hold: not to be restarted now, see utils/rolling-update.ts).
   */
  private async reconcileSynchronous(
    cluster: FirebirdCluster,
    primaryPod: string,
    switchover: SwitchoverResult,
    reseeding: string[],
    fenced: string[],
    statefulSet: V1StatefulSet | undefined,
    log: Logger,
  ): Promise<{ status?: SynchronousStatus; rollingHold?: Array<{ pod: string; hold: boolean }> }> {
    const { name, namespace = 'default' } = cluster.metadata;
    const stored = switchover.synchronous;
    if (!replicationEnabled(cluster)) return {};
    // the primary keeps its sync_replica entry while hibernated: so does the status
    if (cluster.spec.hibernated) return { status: stored ?? cluster.status?.synchronous };
    if (!stored && !synchronousMode(cluster)) return {};
    const jobName = syncStandbyJobName(cluster);
    let job: V1Job | undefined;
    try {
      job = await this.batchApi.readNamespacedJob({ name: jobName, namespace });
    } catch (err) {
      if (!isNotFound(err)) throw err;
    }
    let jobOutcome: string | undefined;
    if (job && (job.status?.conditions ?? []).some((c) => (c.type === 'Complete' || c.type === 'Failed') && c.status === 'True')) {
      // the last attempt's termination message
      const jobPods = (await this.coreApi.listNamespacedPod({ namespace, labelSelector: `job-name=${jobName}` })).items
        .slice()
        .sort((a, b) => new Date(a.metadata?.creationTimestamp ?? 0).getTime() - new Date(b.metadata?.creationTimestamp ?? 0).getTime());
      jobOutcome = jobPods.at(-1)?.status?.containerStatuses?.[0]?.state?.terminated?.message;
    }
    const pods = instancePods(
      (await this.coreApi.listNamespacedPod({ namespace, labelSelector: instancePodSelector(name) })).items,
      name,
    );
    const busy = switchoverInFlight(switchover.status)
      ? `${switchover.status?.kind ?? 'switchover'} in progress`
      : switchover.wantsSwitchover
        ? 'a switchover is requested'
        : undefined;
    // Firebird 4 commits without an unreachable sync_replica: nothing is attached there
    const primaryObj = pods.find((p) => p.metadata?.name === primaryPod);
    const engine = synchronousMode(cluster) || stored ? await this.engineVersion(cluster, primaryObj) : undefined;
    const unsupported = syncUnsupportedReason(engine);
    const input: SyncPlanInput = {
      cluster,
      primaryPod,
      pods,
      status: stored,
      unsupported,
      // attached only once the primary's engine is known to fail commits without them
      attachBlocked: engine === undefined ? "the primary's Firebird version is not known yet" : undefined,
      job,
      jobOutcome,
      busy,
      fenced: [...new Set([...fenced, ...desiredFencedInstances(cluster)])],
      reseeding,
      rollingTarget: rollingUpdateTarget(
        cluster,
        statefulSet,
        pods,
        primaryPod,
        fenced,
        stored?.primary === primaryPod ? synchronousMembers(stored) : [],
      ),
      now: Date.now(),
    };
    const step = planSynchronous(input);
    // the standby of the primary is restarted by the rolling update once neither being attached or
    // detached nor to be handed over first
    const rolling = (status: SynchronousStatus | undefined) =>
      status?.primary === primaryPod
        ? synchronousMembers(status).map((pod) => ({
            pod,
            hold:
              ((status.phase === 'Attaching' || status.phase === 'Detaching') && status.standby === pod) ||
              (pod === input.rollingTarget && handoverForUpdate(input)),
          }))
        : [];
    const persist = async (status: SynchronousStatus | undefined) => {
      if (JSON.stringify(status) === JSON.stringify(stored)) return;
      await this.customApi.patchNamespacedCustomObjectStatus({
        group: API_GROUP,
        version: API_VERSION,
        namespace,
        plural: RESOURCE_PLURAL,
        name,
        body: [status ? { op: 'add', path: '/status/synchronous', value: status } : { op: 'remove', path: '/status/synchronous' }],
      }).catch((err) => {
        // removing an absent field
        if (status || (err as { code?: number })?.code !== 422) throw err;
      });
    };

    if (step.kind === 'none') {
      if (unsupported && step.status?.message === unsupported && stored?.message !== unsupported) {
        log.warn({ primary: primaryPod }, unsupported);
        await this.event(cluster, 'Warning', EventReason.SyncStandbyFailed, unsupported);
      }
      await persist(step.status);
      return { status: step.status, rollingHold: rolling(step.status) };
    }
    if (step.kind === 'start') {
      if (job) {
        // a previous Job not yet deleted
        await this.batchApi.deleteNamespacedJob({ name: jobName, namespace, propagationPolicy: 'Background' }).catch(() => undefined);
        return { status: stored, rollingHold: rolling(stored) };
      }
      log.info({ action: step.action, standby: step.standby, primary: primaryPod }, `Synchronous standby: ${step.status.message}`);
      await persist(step.status);
      await this.event(cluster, 'Normal', step.action === 'attach' ? EventReason.SyncStandbyAttaching : EventReason.SyncStandbyDetaching, step.status.message ?? '');
      try {
        // the standbys that stay attached are named in every sync_replica change
        const others = attachedStandbys(step.status).filter((pod) => pod !== step.standby);
        await this.batchApi.createNamespacedJob({ namespace, body: buildSyncStandbyJob(cluster, step.action, primaryPod, step.standby, others) });
      } catch (err) {
        if ((err as { code?: number })?.code !== 409) throw err;
      }
      return { status: step.status, rollingHold: rolling(step.status) };
    }
    // finished
    log.info({ standby: step.status.standby, phase: step.status.phase }, `Synchronous standby: ${step.status.message}`);
    await persist(step.status);
    await this.event(cluster, step.status.phase === 'Failed' ? 'Warning' : 'Normal', step.event, step.status.message ?? '');
    if (job) await this.batchApi.deleteNamespacedJob({ name: jobName, namespace, propagationPolicy: 'Background' }).catch(() => undefined);
    if (step.reseed) {
      await this.coreApi
        .patchNamespacedPod(
          { name: step.reseed, namespace, body: { metadata: { annotations: { [RESEED_ANNOTATION]: 'true' } } } },
          MERGE_PATCH,
        )
        .catch((err) => {
          if (!isNotFound(err)) throw err;
        });
    }
    return { status: step.status, rollingHold: rolling(step.status) };
  }

  /**
   * The Firebird engine version of an instance (segment server VERSION), cached per pod UID;
   * undefined while it cannot be read (pod not ready, older segment server)
   */
  private async engineVersion(cluster: FirebirdCluster, pod?: V1Pod): Promise<string | undefined> {
    const uid = pod?.metadata?.uid;
    if (!pod || !uid) return undefined;
    const cached = this.engineVersions.get(uid);
    if (cached) return cached;
    if (!isPodReady(pod)) return undefined;
    const token = await this.superuserPassword(cluster);
    if (token === undefined) return undefined;
    const host = `${instanceHost(cluster, pod.metadata!.name!)}.${cluster.metadata.namespace ?? 'default'}.svc`;
    try {
      const reply = await this.segmentClient(host, SEGMENT_PORT, `${token} VERSION`);
      const version = /^OK ([\d.]+)$/.exec(reply[0] ?? '')?.[1];
      if (version) this.engineVersions.set(uid, version);
      return version;
    } catch {
      return undefined;
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

  /**
   * The Role and RoleBinding through which the instances renew their cluster's Lease (the
   * lease-holder sidecar, utils/primary-lease.ts); removed again when the primary Lease is off
   */
  private async reconcilePrimaryLeaseRbac(cluster: FirebirdCluster, log: Logger): Promise<void> {
    const namespace = cluster.metadata.namespace ?? 'default';
    const name = primaryLeaseRoleName(cluster);
    if (!primaryLeaseEnabled(cluster)) {
      try {
        await this.rbacApi.readNamespacedRole({ name, namespace });
      } catch (err) {
        if (!isNotFound(err)) throw err;
        return;
      }
      log.info({ role: name }, 'Removing the primary Lease Role: the primary no longer holds its Lease');
      for (const remove of [
        () => this.rbacApi.deleteNamespacedRoleBinding({ name, namespace }),
        () => this.rbacApi.deleteNamespacedRole({ name, namespace }),
      ]) {
        await remove().catch((err) => {
          if (!isNotFound(err)) throw err;
        });
      }
      return;
    }
    const role = buildPrimaryLeaseRole(cluster);
    const binding = buildPrimaryLeaseRoleBinding(cluster);
    try {
      await this.rbacApi.readNamespacedRole({ name, namespace });
      await this.rbacApi.patchNamespacedRole({ name, namespace, body: role }, MERGE_PATCH);
    } catch (err) {
      if (!isNotFound(err)) throw err;
      log.info({ role: name }, 'Creating the primary Lease Role');
      await this.rbacApi.createNamespacedRole({ namespace, body: role });
    }
    try {
      await this.rbacApi.readNamespacedRoleBinding({ name, namespace });
      await this.rbacApi.patchNamespacedRoleBinding({ name, namespace, body: binding }, MERGE_PATCH);
    } catch (err) {
      if (!isNotFound(err)) throw err;
      await this.rbacApi.createNamespacedRoleBinding({ namespace, body: binding });
    }
  }

  /**
   * tls.secretName and tls.issuerRef are no longer used: Firebird has no TLS listener, so the
   * certificate was mounted but never read (tls.enabled is wire encryption). Warned once per cluster.
   */
  private async warnUnusedCertificate(cluster: FirebirdCluster, log: Logger): Promise<void> {
    const tls = cluster.spec.tls;
    const uid = cluster.metadata.uid ?? cluster.metadata.name;
    if (!tls?.enabled || (!tls.secretName && !tls.issuerRef) || this.certificateWarned.has(uid)) return;
    this.certificateWarned.add(uid);
    const message =
      'tls.secretName and tls.issuerRef are ignored: Firebird has no TLS listener; tls.enabled ' +
      'enforces wire encryption (WireCrypt = Required, ChaCha only) and needs no certificate';
    log.warn(message);
    await this.event(cluster, 'Warning', EventReason.TLSCertificateIgnored, message);
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

  /**
   * Writes the segment TLS default into a cluster's spec when it has none (segment-tls-default.ts):
   * a new cluster gets the operator's default, one that already has a StatefulSet (created by an
   * earlier version) keeps plain segment shipping. Written once, so it never changes later.
   */
  private async defaultSegmentTls(cluster: FirebirdCluster, log: Logger): Promise<void> {
    if (typeof cluster.spec.segmentTLS?.enabled === 'boolean') return;
    const { name, namespace = 'default' } = cluster.metadata;
    let existing = true;
    try {
      await this.appsApi.readNamespacedStatefulSet({ name, namespace });
    } catch (err) {
      if (!isNotFound(err)) throw err;
      existing = false;
    }
    const enabled = !existing && segmentTlsDefault();
    // pinned by the operator, not chosen by the owner: SEGMENT_TLS_MIGRATE=pinned may move it later
    const annotations = existing ? { [MIGRATION_ANNOTATION]: 'pinned' } : undefined;
    await this.customApi.patchNamespacedCustomObject(
      {
        group: API_GROUP,
        version: API_VERSION,
        namespace,
        plural: RESOURCE_PLURAL,
        name,
        body: { ...(annotations ? { metadata: { annotations } } : {}), spec: { segmentTLS: { enabled } } },
      },
      MERGE_PATCH,
    );
    cluster.spec.segmentTLS = { ...cluster.spec.segmentTLS, enabled };
    if (annotations) cluster.metadata.annotations = { ...cluster.metadata.annotations, ...annotations };
    const reason = existing ? 'an existing cluster keeps plain segment shipping' : 'the operator default for new clusters';
    log.info({ enabled }, `Defaulted spec.segmentTLS.enabled: ${reason}`);
    await this.event(cluster, 'Normal', 'SegmentTLSDefaulted', `spec.segmentTLS.enabled set to ${enabled} (${reason})`);
  }

  /**
   * Records whether a cluster gets the primary Lease by default (utils/primary-lease.ts) when it
   * is first reconciled: new clusters as PRIMARY_LEASE_DEFAULT says, clusters that already have a
   * StatefulSet (created by an earlier version) pinned to off, since the sidecar would restart
   * their instances. The owner's failover.primaryLease.enabled always wins over it.
   */
  /**
   * Records whether the instances repel each other by default (utils/scheduling.ts): on for a new
   * cluster, pinned off for one that already has a StatefulSet (adding it would restart its
   * instances). spec.podAntiAffinity.enabled overrides it.
   */
  private async defaultPodAntiAffinity(cluster: FirebirdCluster, log: Logger): Promise<void> {
    if (cluster.metadata.annotations?.[POD_ANTI_AFFINITY_ANNOTATION]) return;
    const { name, namespace = 'default' } = cluster.metadata;
    let existing = true;
    try {
      await this.appsApi.readNamespacedStatefulSet({ name, namespace });
    } catch (err) {
      if (!isNotFound(err)) throw err;
      existing = false;
    }
    const value = existing ? 'pinned' : 'enabled';
    await this.customApi.patchNamespacedCustomObject(
      {
        group: API_GROUP,
        version: API_VERSION,
        namespace,
        plural: RESOURCE_PLURAL,
        name,
        body: { metadata: { annotations: { [POD_ANTI_AFFINITY_ANNOTATION]: value } } },
      },
      MERGE_PATCH,
    );
    cluster.metadata.annotations = { ...cluster.metadata.annotations, [POD_ANTI_AFFINITY_ANNOTATION]: value };
    const reason = existing ? 'an existing cluster keeps its scheduling' : 'the default for new clusters';
    log.info({ enabled: !existing }, `Recorded the pod anti-affinity default: ${reason}`);
    await this.event(
      cluster,
      'Normal',
      EventReason.PodAntiAffinityDefaulted,
      `the instances repel each other (pod anti-affinity): ${!existing} (${reason}; spec.podAntiAffinity.enabled overrides it)`,
    );
  }

  private async defaultPrimaryLease(cluster: FirebirdCluster, log: Logger): Promise<void> {
    if (cluster.metadata.annotations?.[PRIMARY_LEASE_ANNOTATION]) return;
    const { name, namespace = 'default' } = cluster.metadata;
    let existing = true;
    try {
      await this.appsApi.readNamespacedStatefulSet({ name, namespace });
    } catch (err) {
      if (!isNotFound(err)) throw err;
      existing = false;
    }
    const enabled = !existing && primaryLeaseDefault();
    const value = enabled ? 'enabled' : 'pinned';
    await this.customApi.patchNamespacedCustomObject(
      {
        group: API_GROUP,
        version: API_VERSION,
        namespace,
        plural: RESOURCE_PLURAL,
        name,
        body: { metadata: { annotations: { [PRIMARY_LEASE_ANNOTATION]: value } } },
      },
      MERGE_PATCH,
    );
    cluster.metadata.annotations = { ...cluster.metadata.annotations, [PRIMARY_LEASE_ANNOTATION]: value };
    const reason = existing ? 'an existing cluster keeps its Lease with the operator' : 'the operator default for new clusters';
    log.info({ enabled }, `Recorded the primary Lease default: ${reason}`);
    await this.event(
      cluster,
      'Normal',
      EventReason.PrimaryLeaseDefaulted,
      `the primary holds its Lease with automatic failover: ${enabled} (${reason}; failover.primaryLease.enabled overrides it)`,
    );
  }

  /**
   * With SEGMENT_TLS_REQUIRED, a cluster still shipping segments in plain text (pinned on upgrade,
   * or chosen before the setting) is reported, not restarted: a SegmentTLS condition and a warning
   * event, until its owner or SEGMENT_TLS_MIGRATE moves it over
   */
  private async segmentTlsCondition(cluster: FirebirdCluster): Promise<FirebirdClusterCondition[]> {
    if (!segmentTlsRequired() || segmentTlsEnabled(cluster)) return [];
    const message = 'segment shipping is plain text, which this operator refuses for new clusters (SEGMENT_TLS_REQUIRED): set spec.segmentTLS.enabled to true, or SEGMENT_TLS_MIGRATE on the operator';
    await this.event(cluster, 'Warning', 'SegmentTLSRequired', message);
    return [this.makeCondition('SegmentTLS', 'False', 'PlainSegmentShipping', message)];
  }

  /**
   * Moves a cluster pinned to plain segment shipping over to segment TLS when the operator is told
   * to (SEGMENT_TLS_MIGRATE, segment-tls-migration.ts), one cluster at a time, and records when its
   * instances all run the proxy
   */
  private async reconcileSegmentTlsMigration(cluster: FirebirdCluster, log: Logger): Promise<void> {
    const mode = migrationMode();
    const inProgress = migrationInProgress(cluster);
    if (!inProgress && notMigrating(cluster, mode, nativeSidecarsSupported())) return;
    const { name, namespace = 'default' } = cluster.metadata;
    const pods = inProgress
      ? instancePods((await this.coreApi.listNamespacedPod({ namespace, labelSelector: instancePodSelector(name) })).items, name)
      : [];
    const others = inProgress
      ? []
      : ((await this.customApi.listClusterCustomObject({ group: API_GROUP, version: API_VERSION, plural: RESOURCE_PLURAL })) as { items?: FirebirdCluster[] })
          .items ?? [];
    const step = migrationStep(cluster, mode, nativeSidecarsSupported(), pods, others);
    if (step.action === 'none') {
      if (step.reason && !inProgress) log.debug({ reason: step.reason }, 'Segment TLS migration waits');
      return;
    }
    const value = step.action === 'start' ? 'in-progress' : step.action === 'finish' ? 'done' : 'skip';
    const body = {
      metadata: { annotations: { [MIGRATION_ANNOTATION]: value } },
      ...(step.action === 'start' ? { spec: { segmentTLS: { enabled: true } } } : {}),
    };
    await this.customApi.patchNamespacedCustomObject(
      { group: API_GROUP, version: API_VERSION, namespace, plural: RESOURCE_PLURAL, name, body },
      MERGE_PATCH,
    );
    cluster.metadata.annotations = { ...cluster.metadata.annotations, [MIGRATION_ANNOTATION]: value };
    if (step.action === 'start') {
      cluster.spec.segmentTLS = { ...cluster.spec.segmentTLS, enabled: true };
      log.info('Moving the cluster to segment TLS (SEGMENT_TLS_MIGRATE)');
      await this.event(cluster, 'Normal', 'SegmentTLSMigrationStarted', 'switching segment TLS on (operator setting SEGMENT_TLS_MIGRATE); the instances restart one by one');
    } else if (step.action === 'finish') {
      log.info('Segment TLS migration complete');
      await this.event(cluster, 'Normal', 'SegmentTLSMigrated', 'every instance runs the segment TLS proxy');
    } else {
      log.info('Segment TLS was turned off during its migration: the cluster is skipped from now on');
      await this.event(cluster, 'Normal', 'SegmentTLSMigrationSkipped', 'segment TLS was turned off during the migration; annotation set to skip');
    }
  }

  /**
   * Moves a cluster pinned to the operator-moved Lease over to the primary Lease when the operator
   * is told to (PRIMARY_LEASE_MIGRATE, primary-lease-migration.ts), one cluster at a time, and
   * records when its instances run the sidecar
   */
  private async reconcilePrimaryLeaseMigration(cluster: FirebirdCluster, log: Logger): Promise<void> {
    const mode = primaryLeaseMigrationMode();
    const inProgress = primaryLeaseMigrationInProgress(cluster);
    if (!inProgress && notMigratingToPrimaryLease(cluster, mode, nativeSidecarsSupported())) return;
    const { name, namespace = 'default' } = cluster.metadata;
    const pods = inProgress
      ? instancePods((await this.coreApi.listNamespacedPod({ namespace, labelSelector: instancePodSelector(name) })).items, name)
      : [];
    const others = inProgress
      ? []
      : ((await this.customApi.listClusterCustomObject({ group: API_GROUP, version: API_VERSION, plural: RESOURCE_PLURAL })) as { items?: FirebirdCluster[] })
          .items ?? [];
    const step = primaryLeaseMigrationStep(cluster, mode, nativeSidecarsSupported(), pods, others);
    if (step.action === 'none') {
      if (step.reason && !inProgress) log.debug({ reason: step.reason }, 'Primary Lease migration waits');
      return;
    }
    const value = step.action === 'start' ? 'in-progress' : step.action === 'finish' ? 'done' : 'skip';
    await this.customApi.patchNamespacedCustomObject(
      { group: API_GROUP, version: API_VERSION, namespace, plural: RESOURCE_PLURAL, name, body: { metadata: { annotations: { [PRIMARY_LEASE_ANNOTATION]: value } } } },
      MERGE_PATCH,
    );
    cluster.metadata.annotations = { ...cluster.metadata.annotations, [PRIMARY_LEASE_ANNOTATION]: value };
    if (step.action === 'start') {
      const restart = primaryLeaseEnabled(cluster);
      log.info({ restart }, 'Moving the cluster to the primary Lease (PRIMARY_LEASE_MIGRATE)');
      await this.event(
        cluster,
        'Normal',
        'PrimaryLeaseMigrationStarted',
        restart
          ? 'the primary holds its Lease from now on (operator setting PRIMARY_LEASE_MIGRATE); the instances restart one by one to add the lease-holder sidecar'
          : 'the primary holds its Lease once automatic failover is enabled (operator setting PRIMARY_LEASE_MIGRATE); nothing restarts now',
      );
    } else if (step.action === 'finish') {
      log.info('Primary Lease migration complete');
      await this.event(cluster, 'Normal', 'PrimaryLeaseMigrated', 'the cluster holds its Lease: every instance runs what it needs');
    } else {
      log.info('failover.primaryLease.enabled was set during the migration: the owner decides from now on');
      await this.event(cluster, 'Normal', 'PrimaryLeaseMigrationSkipped', 'failover.primaryLease.enabled was set during the migration; annotation set to skip');
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

  /**
   * Update the status sub-resource of a FirebirdCluster. Returns false when the cluster no longer
   * exists (deleted while a reconcile of it was running); other failures are logged.
   */
  /** Whether the cluster resource still exists (errors other than 404 count as existing) */
  private async clusterExists(cluster: FirebirdCluster): Promise<boolean> {
    const { name, namespace = 'default' } = cluster.metadata;
    try {
      await this.customApi.getNamespacedCustomObject({
        group: API_GROUP,
        version: API_VERSION,
        namespace,
        plural: RESOURCE_PLURAL,
        name,
      });
      return true;
    } catch (err) {
      return !isNotFound(err);
    }
  }

  async updateStatus(
    cluster: FirebirdCluster,
    status: Partial<FirebirdClusterStatus>,
  ): Promise<boolean> {
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
      if (isNotFound(err)) {
        logger.debug({ cluster: name, namespace }, 'Cluster no longer exists; status not updated');
        return false;
      }
      logger.warn({ err, cluster: name }, 'Failed to update cluster status');
    }
    return true;
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
