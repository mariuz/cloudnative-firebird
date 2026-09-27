import { KubeConfig, Watch } from '@kubernetes/client-node';
import { logger } from './utils/logger';
import { HealthServer } from './utils/health';
import { FirebirdClusterController } from './controllers/firebirdcluster.controller';
import { FirebirdBackupController } from './controllers/backup.controller';
import { FirebirdUserController } from './controllers/user.controller';
import { FENCED_INSTANCES_ANNOTATION } from './utils/fencing';
import { TARGET_PRIMARY_ANNOTATION } from './utils/switchover';
import {
  API_GROUP,
  API_VERSION,
  FirebirdBackup,
  FirebirdCluster,
  FirebirdRestore,
  FirebirdScheduledBackup,
  FirebirdUser,
  RESOURCE_PLURAL,
} from './types';

/** Annotations that drive reconciliation (they do not bump the generation) */
function drivingAnnotations(cluster: FirebirdCluster): string {
  const annotations = cluster.metadata.annotations ?? {};
  return JSON.stringify([annotations[FENCED_INSTANCES_ANNOTATION], annotations[TARGET_PRIMARY_ANNOTATION]]);
}

/** Kubernetes object fields the operator's event handling relies on */
interface WatchedObject {
  metadata: { name: string; namespace?: string; generation?: number };
}

/** A watched resource kind other than FirebirdCluster (backups, restores, users) and how to reconcile it */
interface BackupKind {
  plural: string;
  reconcile: (obj: WatchedObject) => Promise<void>;
}

/** Default interval for periodic re-reconciliation of known clusters */
export const DEFAULT_RESYNC_INTERVAL_MS = 30_000;

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
  /** metadata.generation of the last successful reconcile, per cluster */
  private readonly reconciledGenerations = new Map<string, number>();

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
    if (this.resyncTimer) {
      clearInterval(this.resyncTimer);
      this.resyncTimer = null;
    }
    this.knownClusters.clear();
    this.knownBackupObjects.clear();
    this.reconciledGenerations.clear();
    this.reconciledFencing.clear();
    this.healthServer.stop();
  }

  /** Watches a resource collection, restarting the watch whenever the stream ends */
  private async watchPath(path: string, onEvent: (phase: string, obj: unknown) => Promise<void>): Promise<void> {
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
          {},
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
    for (const cluster of this.knownClusters.values()) {
      const { name, namespace = 'default' } = cluster.metadata;
      this.controller.reconcile(cluster).catch((err) => {
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
        // status-only updates (the controller's own) are left to the resync
        if (phase === 'MODIFIED' && generation !== undefined && this.reconciledGenerations.get(key) === generation) {
          break;
        }
        if (generation !== undefined) this.reconciledGenerations.set(key, generation);
        log.info('Received event, reconciling');
        await kind.reconcile(obj);
        break;
      case 'DELETED':
        this.knownBackupObjects.delete(key);
        this.reconciledGenerations.delete(key);
        break;
      default:
        log.debug('Ignoring watch event');
    }
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
        await this.controller.reconcile(cluster);
        break;

      case 'DELETED':
        this.knownClusters.delete(key);
        this.reconciledGenerations.delete(key);
        this.reconciledFencing.delete(key);
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

