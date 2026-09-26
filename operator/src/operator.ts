import { KubeConfig, Watch } from '@kubernetes/client-node';
import { logger } from './utils/logger';
import { HealthServer } from './utils/health';
import { FirebirdClusterController } from './controllers/firebirdcluster.controller';
import { FirebirdBackupController } from './controllers/backup.controller';
import {
  API_GROUP,
  API_VERSION,
  FirebirdCluster,
  RESOURCE_PLURAL,
} from './types';

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
  private readonly watch: Watch;
  private readonly healthServer: HealthServer;
  private watchRequest: { abort: () => void } | null = null;
  private watchTimer: NodeJS.Timeout | null = null;
  private resyncTimer: NodeJS.Timeout | null = null;
  private readonly resyncIntervalMs: number;
  private readonly knownClusters = new Map<string, FirebirdCluster>();
  /** metadata.generation of the last successful reconcile, per cluster */
  private readonly reconciledGenerations = new Map<string, number>();

  constructor(kubeConfig: KubeConfig, healthPort = 8080, resyncIntervalMs = DEFAULT_RESYNC_INTERVAL_MS) {
    this.resyncIntervalMs = resyncIntervalMs;
    this.kubeConfig = kubeConfig;
    this.controller = new FirebirdClusterController(kubeConfig);
    this.backupController = new FirebirdBackupController(kubeConfig);
    this.watch = new Watch(kubeConfig);
    this.healthServer = new HealthServer(healthPort);
  }

  /**
   * Start the operator: begins watching FirebirdCluster resources
   * across all namespaces and reconciling them.
   */
  async start(): Promise<void> {
    logger.info('Starting cloudnative-firebird operator');
    this.healthServer.start();
    await this.startWatching();
    if (this.resyncIntervalMs > 0) {
      this.resyncTimer = setInterval(() => this.resync(), this.resyncIntervalMs);
    }
    this.healthServer.setReady(true);
  }

  /** Stop the operator and abort any active watch */
  stop(): void {
    logger.info('Stopping cloudnative-firebird operator');
    this.healthServer.setReady(false);
    this.watchRequest?.abort();
    this.watchRequest = null;
    if (this.watchTimer) {
      clearTimeout(this.watchTimer);
      this.watchTimer = null;
    }
    if (this.resyncTimer) {
      clearInterval(this.resyncTimer);
      this.resyncTimer = null;
    }
    this.knownClusters.clear();
    this.reconciledGenerations.clear();
    this.healthServer.stop();
  }

  private async startWatching(): Promise<void> {
    const path = `/apis/${API_GROUP}/${API_VERSION}/${RESOURCE_PLURAL}`;

    logger.info({ path }, 'Starting watch on FirebirdCluster resources');

    const restartWatch = async (): Promise<void> => {
      try {
        this.watchRequest = await this.watch.watch(
          path,
          {},
          (phase: string, obj: FirebirdCluster) => {
            this.handleEvent(phase, obj).catch((err) => {
              logger.error({ err, phase }, 'Unhandled error in event handler');
            });
          },
          (err: unknown) => {
            if (err) {
              logger.error({ err }, 'Watch stream ended with error, restarting');
            } else {
              logger.info('Watch stream ended gracefully, restarting');
            }
            // Restart the watch after a short delay
            if (this.watchTimer) clearTimeout(this.watchTimer);
            this.watchTimer = setTimeout(() => {
              restartWatch().catch((restartErr) => {
                logger.error({ err: restartErr }, 'Failed to restart watch');
              });
            }, 5000);
          },
        );
      } catch (err) {
        logger.error({ err }, 'Failed to start watch, retrying in 10s');
        if (this.watchTimer) clearTimeout(this.watchTimer);
        this.watchTimer = setTimeout(() => {
          restartWatch().catch((retryErr) => {
            logger.error({ err: retryErr }, 'Failed to restart watch after error');
          });
        }, 10000);
      }
    };

    await restartWatch();
  }

  /** Re-reconcile every known cluster with its latest observed spec */
  private resync(): void {
    for (const cluster of this.knownClusters.values()) {
      const { name, namespace = 'default' } = cluster.metadata;
      this.controller.reconcile(cluster).catch((err) => {
        logger.error({ err, cluster: name, namespace }, 'Periodic resync reconcile failed');
      });
    }
  }

  private async handleEvent(phase: string, cluster: FirebirdCluster): Promise<void> {
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
        if (
          phase === 'MODIFIED' &&
          generation !== undefined &&
          this.reconciledGenerations.get(key) === generation
        ) {
          log.debug({ generation }, 'Spec unchanged since last reconcile, skipping');
          break;
        }
        log.info('Received cluster event, reconciling');
        if (generation !== undefined) this.reconciledGenerations.set(key, generation);
        await this.controller.reconcile(cluster);
        break;

      case 'DELETED':
        this.knownClusters.delete(key);
        this.reconciledGenerations.delete(key);
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

