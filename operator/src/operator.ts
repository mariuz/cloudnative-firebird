import { KubeConfig, Watch } from '@kubernetes/client-node';
import { logger } from './utils/logger';
import { HealthServer } from './utils/health';
import { FirebirdClusterController } from './controllers/firebirdcluster.controller';
import {
  API_GROUP,
  API_VERSION,
  FirebirdCluster,
  RESOURCE_PLURAL,
} from './types';

/**
 * Operator watches for FirebirdCluster resources and triggers reconciliation.
 */
export class Operator {
  private readonly kubeConfig: KubeConfig;
  private readonly controller: FirebirdClusterController;
  private readonly watch: Watch;
  private readonly healthServer: HealthServer;
  private watchRequest: { abort: () => void } | null = null;

  constructor(kubeConfig: KubeConfig, healthPort = 8080) {
    this.kubeConfig = kubeConfig;
    this.controller = new FirebirdClusterController(kubeConfig);
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
    this.healthServer.setReady(true);
  }

  /** Stop the operator and abort any active watch */
  stop(): void {
    logger.info('Stopping cloudnative-firebird operator');
    this.healthServer.setReady(false);
    this.watchRequest?.abort();
    this.watchRequest = null;
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
            setTimeout(() => {
              restartWatch().catch((restartErr) => {
                logger.error({ err: restartErr }, 'Failed to restart watch');
              });
            }, 5000);
          },
        );
      } catch (err) {
        logger.error({ err }, 'Failed to start watch, retrying in 10s');
        setTimeout(() => {
          restartWatch().catch((retryErr) => {
            logger.error({ err: retryErr }, 'Failed to restart watch after error');
          });
        }, 10000);
      }
    };

    await restartWatch();
  }

  private async handleEvent(phase: string, cluster: FirebirdCluster): Promise<void> {
    const { name, namespace = 'default' } = cluster.metadata;
    const log = logger.child({ cluster: name, namespace, phase });

    switch (phase) {
      case 'ADDED':
      case 'MODIFIED':
        log.info('Received cluster event, reconciling');
        await this.controller.reconcile(cluster);
        break;

      case 'DELETED':
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

