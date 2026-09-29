import { KubeConfig, Watch } from '@kubernetes/client-node';
import { logger } from './utils/logger';
import { HealthServer } from './utils/health';
import { metrics } from './utils/metrics';
import { FirebirdClusterController } from './controllers/firebirdcluster.controller';
import { FirebirdBackupController } from './controllers/backup.controller';
import { FirebirdUserController } from './controllers/user.controller';
import { FENCED_INSTANCES_ANNOTATION } from './utils/fencing';
import { TARGET_PRIMARY_ANNOTATION } from './utils/switchover';
import { RESEED_ANNOTATION } from './utils/replication';
import { CLUSTER_LABEL, instancePods } from './utils/resources';
import { isPodReady } from './utils/routing';
import {
  API_GROUP,
  API_VERSION,
  FirebirdBackup,
  FirebirdCluster,
  FirebirdRestore,
  FirebirdScheduledBackup,
  FirebirdUser,
  RESOURCE_PLURAL,
  reconciliationDisabled,
} from './types';

/** Annotations that drive reconciliation (they do not bump the generation) */
function drivingAnnotations(cluster: FirebirdCluster): string {
  const annotations = cluster.metadata.annotations ?? {};
  return JSON.stringify([annotations[FENCED_INSTANCES_ANNOTATION], annotations[TARGET_PRIMARY_ANNOTATION]]);
}

/** Kubernetes object fields the operator's event handling relies on */
interface WatchedObject {
  metadata: { name: string; namespace?: string; generation?: number; annotations?: Record<string, string> };
}

/** A watched resource kind other than FirebirdCluster (backups, restores, users) and how to reconcile it */
interface BackupKind {
  plural: string;
  reconcile: (obj: WatchedObject) => Promise<void>;
}

/** Default interval for periodic re-reconciliation of known clusters */
export const DEFAULT_RESYNC_INTERVAL_MS = 30_000;

/** Pod events of one cluster within this window are coalesced into one reconcile */
export const POD_EVENT_DEBOUNCE_MS = 1_000;

/** Instance pods of every cluster (Job pods have other components) */
export const INSTANCE_POD_WATCH_SELECTOR =
  'app.kubernetes.io/managed-by=cloudnative-firebird-operator,app.kubernetes.io/component=database';

/** Instance pod fields the operator's event handling relies on */
interface WatchedPod {
  metadata?: {
    name?: string;
    namespace?: string;
    uid?: string;
    labels?: Record<string, string>;
    annotations?: Record<string, string>;
    deletionTimestamp?: unknown;
  };
  status?: { phase?: string; conditions?: Array<{ type: string; status: string }> };
}

/**
 * The state of an instance pod the cluster reconcile acts on: readiness (rolling updates, failover,
 * routing, fencing), termination, revision and the reseed request. The labels and annotations the
 * operator itself writes (role, read-routable, replication lag) are left out, so its own pod
 * patches do not trigger reconciles.
 */
export function podFingerprint(pod: WatchedPod): string {
  return JSON.stringify([
    pod.metadata?.uid,
    pod.status?.phase,
    isPodReady(pod as Parameters<typeof isPodReady>[0]),
    Boolean(pod.metadata?.deletionTimestamp),
    pod.metadata?.labels?.['controller-revision-hash'],
    pod.metadata?.annotations?.[RESEED_ANNOTATION],
  ]);
}

/**
 * Operator watches for FirebirdCluster resources and triggers reconciliation.
 * Known clusters are also re-reconciled periodically so that state outside the
 * FirebirdCluster object (pod readiness, replication lag, PVC resize progress) converges.
 */
export class Operator {
  private readonly kubeConfig: KubeConfig;
  private readonly controller: FirebirdClusterController;
  private readonly backupController: FirebirdBackupController;
  private readonly userController: FirebirdUserController;
  private readonly watch: Watch;
  private readonly healthServer: HealthServer;
  private readonly watchRequests = new Map<string, { abort: () => void }>();
  private readonly watchTimers = new Map<string, NodeJS.Timeout>();
  private readonly backupKinds: BackupKind[];
  /** Latest observed backup, scheduled backup and restore objects, keyed by plural/namespace/name */
  private readonly knownBackupObjects = new Map<string, { kind: BackupKind; obj: WatchedObject }>();
  private resyncTimer: NodeJS.Timeout | null = null;
  private readonly resyncIntervalMs: number;
  private readonly knownClusters = new Map<string, FirebirdCluster>();
  /** fencedInstances / targetPrimary annotations at the last reconcile, per cluster */
  private readonly reconciledFencing = new Map<string, string>();
  /** Reconcile in progress per cluster, and clusters to reconcile again once it finishes */
  private readonly running = new Map<string, Promise<void>>();
  private readonly rerun = new Set<string>();
  /** metadata.generation of the last successful reconcile, per cluster */
  private readonly reconciledGenerations = new Map<string, number>();
  /** reconciliationDisabled state of the last reconcile, per backup, restore or user */
  private readonly reconciledPause = new Map<string, boolean>();
  /** podFingerprint of every watched instance pod, keyed by namespace/name */
  private readonly podStates = new Map<string, string>();
  /** Pending pod-triggered reconciles, per cluster */
  private readonly podTriggers = new Map<string, NodeJS.Timeout>();

  constructor(kubeConfig: KubeConfig, healthPort = 8080, resyncIntervalMs = DEFAULT_RESYNC_INTERVAL_MS) {
    this.resyncIntervalMs = resyncIntervalMs;
    this.kubeConfig = kubeConfig;
    this.controller = new FirebirdClusterController(kubeConfig);
    this.backupController = new FirebirdBackupController(kubeConfig);
    this.userController = new FirebirdUserController(kubeConfig);
    this.backupKinds = [
      {
        plural: 'firebirdbackups',
        reconcile: (obj) => this.backupController.reconcileBackup(obj as FirebirdBackup),
      },
      {
        plural: 'firebirdscheduledbackups',
        reconcile: (obj) => this.backupController.reconcileScheduledBackup(obj as FirebirdScheduledBackup),
      },
      {
        plural: 'firebirdrestores',
        reconcile: (obj) => this.backupController.reconcileRestore(obj as FirebirdRestore),
      },
      {
        plural: 'firebirdusers',
        reconcile: (obj) => this.userController.reconcileUser(obj as FirebirdUser),
      },
    ];
    this.watch = new Watch(kubeConfig);
    this.healthServer = new HealthServer(healthPort);
  }

  /**
   * Start the operator: begins watching FirebirdCluster and backup resources
   * across all namespaces and reconciling them.
   */
  async start(): Promise<void> {
    logger.info('Starting cloudnative-firebird operator');
    this.healthServer.start();
    await this.watchPath(`/apis/${API_GROUP}/${API_VERSION}/${RESOURCE_PLURAL}`, (phase, obj) =>
      this.handleEvent(phase, obj as FirebirdCluster),
    );
    for (const kind of this.backupKinds) {
      await this.watchPath(`/apis/${API_GROUP}/${API_VERSION}/${kind.plural}`, (phase, obj) =>
        this.handleBackupEvent(kind, phase, obj as WatchedObject),
      );
    }
    // instance pods: rolling updates, re-seeding, failover and routing react to readiness
    // changes as they happen instead of on the next resync
    await this.watchPath(
      '/api/v1/pods',
      async (phase, obj) => this.handlePodEvent(phase, obj as WatchedPod),
      { labelSelector: INSTANCE_POD_WATCH_SELECTOR },
    );
    if (this.resyncIntervalMs > 0) {
      this.resyncTimer = setInterval(() => this.resync(), this.resyncIntervalMs);
    }
    this.healthServer.setReady(true);
  }

  /** Stop the operator and abort any active watch */
  stop(): void {
    logger.info('Stopping cloudnative-firebird operator');
    this.healthServer.setReady(false);
    for (const request of this.watchRequests.values()) request.abort();
    this.watchRequests.clear();
    for (const timer of this.watchTimers.values()) clearTimeout(timer);
    this.watchTimers.clear();
    for (const timer of this.podTriggers.values()) clearTimeout(timer);
    this.podTriggers.clear();
    this.podStates.clear();
    if (this.resyncTimer) {
      clearInterval(this.resyncTimer);
      this.resyncTimer = null;
    }
    this.knownClusters.clear();
    this.knownBackupObjects.clear();
    this.rerun.clear();
    this.reconciledGenerations.clear();
    this.reconciledPause.clear();
    this.reconciledFencing.clear();
    this.healthServer.stop();
  }

  /** Watches a resource collection, restarting the watch whenever the stream ends */
  private async watchPath(
    path: string,
    onEvent: (phase: string, obj: unknown) => Promise<void>,
    params: Record<string, string> = {},
  ): Promise<void> {
    logger.info({ path }, 'Starting watch');

    const schedule = (delayMs: number): void => {
      const existing = this.watchTimers.get(path);
      if (existing) clearTimeout(existing);
      this.watchTimers.set(
        path,
        setTimeout(() => {
          restartWatch().catch((restartErr) => {
            logger.error({ err: restartErr, path }, 'Failed to restart watch');
          });
        }, delayMs),
      );
    };

    const restartWatch = async (): Promise<void> => {
      try {
        const request = await this.watch.watch(
          path,
          params,
          (phase: string, obj: unknown) => {
            onEvent(phase, obj).catch((err) => {
              logger.error({ err, phase, path }, 'Unhandled error in event handler');
            });
          },
          (err: unknown) => {
            if (err) {
              logger.error({ err, path }, 'Watch stream ended with error, restarting');
            } else {
              logger.info({ path }, 'Watch stream ended gracefully, restarting');
            }
            schedule(5000);
          },
        );
        this.watchRequests.set(path, request);
      } catch (err) {
        logger.error({ err, path }, 'Failed to start watch, retrying in 10s');
        schedule(10000);
      }
    };

    await restartWatch();
  }

  /**
   * Re-reconcile every known cluster with its latest observed spec, and every known backup,
   * scheduled backup and restore (their status follows Jobs and CronJobs the watch does not cover)
   */
  private resync(): void {
    for (const [key, cluster] of this.knownClusters) {
      const { name, namespace = 'default' } = cluster.metadata;
      this.reconcileCluster(key).catch((err) => {
        logger.error({ err, cluster: name, namespace }, 'Periodic resync reconcile failed');
      });
    }
    for (const { kind, obj } of this.knownBackupObjects.values()) {
      const { name, namespace = 'default' } = obj.metadata;
      kind.reconcile(obj).catch((err) => {
        logger.error({ err, kind: kind.plural, name, namespace }, 'Periodic resync reconcile failed');
      });
    }
  }

  /**
   * Reconciles the pod's cluster when the pod's fingerprint changes (created, deleted, readiness,
   * termination, revision, reseed request). Events of one cluster are coalesced.
   */
  private async handlePodEvent(phase: string, pod: WatchedPod): Promise<void> {
    const { name, namespace = 'default', labels } = pod?.metadata ?? {};
    const cluster = labels?.[CLUSTER_LABEL];
    if (!name || !cluster || instancePods([{ metadata: { name } }], cluster).length === 0) return;
    const podKey = `${namespace}/${name}`;
    if (phase === 'DELETED') {
      this.podStates.delete(podKey);
    } else if (phase === 'ADDED' || phase === 'MODIFIED') {
      const fingerprint = podFingerprint(pod);
      if (this.podStates.get(podKey) === fingerprint) return;
      this.podStates.set(podKey, fingerprint);
    } else {
      return;
    }
    const key = `${namespace}/${cluster}`;
    if (!this.knownClusters.has(key) || this.podTriggers.has(key)) return;
    this.podTriggers.set(
      key,
      setTimeout(() => {
        this.podTriggers.delete(key);
        logger.debug({ cluster, namespace, pod: name, phase }, 'Instance pod changed, reconciling');
        this.reconcileCluster(key).catch((err) => {
          logger.error({ err, cluster, namespace }, 'Pod-triggered reconcile failed');
        });
      }, POD_EVENT_DEBOUNCE_MS),
    );
  }

  private async handleBackupEvent(kind: BackupKind, phase: string, obj: WatchedObject): Promise<void> {
    if (phase === 'ERROR' || !obj?.metadata) {
      logger.error({ phase, kind: kind.plural, event: obj }, 'Received error event from watch stream');
      return;
    }
    const { name, namespace = 'default', generation } = obj.metadata;
    const key = `${kind.plural}/${namespace}/${name}`;
    const log = logger.child({ kind: kind.plural, name, namespace, phase });

    switch (phase) {
      case 'ADDED':
      case 'MODIFIED':
        this.knownBackupObjects.set(key, { kind, obj });
        // status-only updates (the controller's own) are left to the resync; pausing or resuming
        // with the reconciliationDisabled annotation does not bump the generation either
        if (
          phase === 'MODIFIED' &&
          generation !== undefined &&
          this.reconciledGenerations.get(key) === generation &&
          this.reconciledPause.get(key) === reconciliationDisabled(obj)
        ) {
          break;
        }
        if (generation !== undefined) this.reconciledGenerations.set(key, generation);
        this.reconciledPause.set(key, reconciliationDisabled(obj));
        log.info('Received event, reconciling');
        await kind.reconcile(obj);
        break;
      case 'DELETED':
        this.knownBackupObjects.delete(key);
        this.reconciledGenerations.delete(key);
        this.reconciledPause.delete(key);
        break;
      default:
        log.debug('Ignoring watch event');
    }
  }

  /**
   * Reconciles a cluster, never concurrently with itself: a request that arrives while a reconcile
   * runs is coalesced into one more run with the latest observed object. Overlapping reconciles
   * of one cluster act on each other's stale status (e.g. a switchover phase).
   */
  private reconcileCluster(key: string): Promise<void> {
    const inProgress = this.running.get(key);
    if (inProgress) {
      this.rerun.add(key);
      return inProgress;
    }
    const run = (async () => {
      let first: unknown;
      do {
        this.rerun.delete(key);
        const cluster = this.knownClusters.get(key);
        if (!cluster) break;
        try {
          await this.controller.reconcile(cluster);
        } catch (err) {
          if (first === undefined) first = err;
          else logger.error({ err, cluster: key }, 'Reconcile failed');
        }
      } while (this.rerun.has(key));
      if (first !== undefined) throw first;
    })().finally(() => this.running.delete(key));
    this.running.set(key, run);
    return run;
  }

  /**
   * A new clone: its source's NetworkPolicy must admit the clone's instances before they copy the
   * database, so the source is reconciled now rather than on the next resync
   */
  private reconcileCloneSource(cluster: FirebirdCluster): void {
    const clone = cluster.spec?.bootstrap?.clone;
    if (!clone) return;
    const sourceKey = `${clone.namespace ?? cluster.metadata.namespace ?? 'default'}/${clone.sourceCluster}`;
    const source = this.knownClusters.get(sourceKey);
    if (!source?.spec.networkPolicy?.enabled) return;
    this.reconcileCluster(sourceKey).catch((err) => {
      logger.error({ err, cluster: sourceKey }, 'Reconcile of the clone source failed');
    });
  }

  private async handleEvent(phase: string, cluster: FirebirdCluster): Promise<void> {
    // ERROR events carry a Status object, not a cluster
    if (phase === 'ERROR' || !cluster?.metadata) {
      logger.error({ phase, event: cluster }, 'Received error event from watch stream');
      return;
    }
    const { name, namespace = 'default' } = cluster.metadata;
    const log = logger.child({ cluster: name, namespace, phase });
    const key = `${namespace}/${name}`;
    const generation = cluster.metadata.generation;

    switch (phase) {
      case 'ADDED':
      case 'MODIFIED':
        if (!this.knownClusters.has(key)) this.reconcileCloneSource(cluster);
        this.knownClusters.set(key, cluster);
        // Status-only updates (including the operator's own) do not bump metadata.generation;
        // skip them to avoid a reconcile → status patch → MODIFIED feedback loop.
        // Periodic resync still converges anything observed outside the spec, and retries
        // failed reconciles at the resync interval instead of in a tight event loop.
        // Annotations do not bump the generation either; fencing and switchover are driven by them.
        if (
          phase === 'MODIFIED' &&
          generation !== undefined &&
          this.reconciledGenerations.get(key) === generation &&
          this.reconciledFencing.get(key) === drivingAnnotations(cluster)
        ) {
          log.debug({ generation }, 'Spec unchanged since last reconcile, skipping');
          break;
        }
        log.info('Received cluster event, reconciling');
        if (generation !== undefined) this.reconciledGenerations.set(key, generation);
        this.reconciledFencing.set(key, drivingAnnotations(cluster));
        await this.reconcileCluster(key);
        break;

      case 'DELETED':
        this.knownClusters.delete(key);
        this.reconciledGenerations.delete(key);
        this.reconciledFencing.delete(key);
        metrics.remove({ namespace: cluster.metadata.namespace ?? 'default', cluster: cluster.metadata.name });
        log.info('FirebirdCluster deleted; owned resources will be garbage collected');
        break;

      case 'ERROR':
        log.error('Received error event from watch stream');
        break;

      default:
        log.warn({ phase }, 'Unknown watch event phase');
    }
  }
}

