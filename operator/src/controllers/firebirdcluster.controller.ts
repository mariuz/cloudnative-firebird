import {
  AppsV1Api,
  CoreV1Api,
  CustomObjectsApi,
  KubeConfig,
} from '@kubernetes/client-node';
import { Logger } from 'pino';
import { logger } from '../utils/logger';
import {
  buildHeadlessService,
  buildService,
  buildStatefulSet,
  statefulSetNeedsUpdate,
} from '../utils/resources';
import {
  API_GROUP,
  API_VERSION,
  FirebirdCluster,
  FirebirdClusterCondition,
  FirebirdClusterStatus,
  RESOURCE_PLURAL,
} from '../types';

/**
 * FirebirdClusterController reconciles FirebirdCluster resources
 * to the desired state by managing StatefulSets, Services, and PVCs.
 */
export class FirebirdClusterController {
  private readonly appsApi: AppsV1Api;
  private readonly coreApi: CoreV1Api;
  private readonly customApi: CustomObjectsApi;

  constructor(kubeConfig: KubeConfig) {
    this.appsApi = kubeConfig.makeApiClient(AppsV1Api);
    this.coreApi = kubeConfig.makeApiClient(CoreV1Api);
    this.customApi = kubeConfig.makeApiClient(CustomObjectsApi);
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
      await this.updateStatus(cluster, {
        phase: 'Creating',
        phaseReason: 'Reconciliation started',
      });

      await this.reconcileHeadlessService(cluster, log);
      await this.reconcileService(cluster, log);
      await this.reconcileStatefulSet(cluster, log);

      await this.updateStatus(cluster, {
        phase: 'Running',
        phaseReason: 'All resources reconciled successfully',
        instances: cluster.spec.instances,
        conditions: [
          this.makeCondition('Ready', 'True', 'ClusterReady', 'Cluster is ready'),
          this.makeCondition(
            'Progressing',
            'False',
            'ReconciliationComplete',
            'Reconciliation completed',
          ),
        ],
      });

      log.info('Reconciliation complete');
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      log.error({ err }, 'Reconciliation failed');

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

    try {
      await this.coreApi.readNamespacedService({ name, namespace });
      log.debug('Service already exists, skipping');
    } catch {
      log.info('Creating cluster service');
      await this.coreApi.createNamespacedService({
        namespace,
        body: desired,
      });
    }
  }

  /** Reconcile the StatefulSet for the cluster */
  private async reconcileStatefulSet(
    cluster: FirebirdCluster,
    log: Logger,
  ): Promise<void> {
    const { name, namespace = 'default' } = cluster.metadata;
    const desired = buildStatefulSet(cluster);

    let existing;
    try {
      const response = await this.appsApi.readNamespacedStatefulSet({ name, namespace });
      existing = response;
    } catch {
      log.info('Creating StatefulSet');
      await this.appsApi.createNamespacedStatefulSet({
        namespace,
        body: desired,
      });
      return;
    }

    if (statefulSetNeedsUpdate(existing, desired)) {
      log.info('Updating StatefulSet');
      await this.appsApi.patchNamespacedStatefulSet({
        name,
        namespace,
        body: desired,
      });
    } else {
      log.debug('StatefulSet is up to date, skipping');
    }
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
