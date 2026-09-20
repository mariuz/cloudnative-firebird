import {
  AppsV1Api,
  BatchV1Api,
  CoordinationV1Api,
  CoreV1Api,
  CustomObjectsApi,
  KubeConfig,
  NetworkingV1Api,
  PolicyV1Api,
} from '@kubernetes/client-node';
import { Logger } from 'pino';
import { logger } from '../utils/logger';
import {
  buildAutoSweepCronJob,
  buildBackupCronJob,
  buildCertificate,
  buildConfigMap,
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
  networkPolicyNeedsUpdate,
  podDisruptionBudgetNeedsUpdate,
  statefulSetNeedsUpdate,
} from '../utils/resources';
import { validateClusterSpec } from '../utils/validation';
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

  constructor(kubeConfig: KubeConfig) {
    this.appsApi = kubeConfig.makeApiClient(AppsV1Api);
    this.batchApi = kubeConfig.makeApiClient(BatchV1Api);
    this.coordinationApi = kubeConfig.makeApiClient(CoordinationV1Api);
    this.coreApi = kubeConfig.makeApiClient(CoreV1Api);
    this.customApi = kubeConfig.makeApiClient(CustomObjectsApi);
    this.networkingApi = kubeConfig.makeApiClient(NetworkingV1Api);
    this.policyApi = kubeConfig.makeApiClient(PolicyV1Api);
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

      await this.reconcileConfigMap(cluster, log);
      await this.reconcileHeadlessService(cluster, log);
      await this.reconcileService(cluster, log);
      const readyInstances = await this.reconcileStatefulSet(cluster, log);

      if (cluster.spec.replication?.enabled) {
        await this.reconcileReplicaService(cluster, log);
      }

      await this.reconcileLease(cluster, log);
      await this.reconcileCertificate(cluster, log);
      await this.reconcilePodDisruptionBudget(cluster, log);
      await this.reconcileNetworkPolicy(cluster, log);
      await this.reconcileBackupCronJob(cluster, log);
      await this.reconcileAutoSweepCronJob(cluster, log);
      await this.reconcilePodMonitor(cluster, log);

      const targetInstances = cluster.spec.instances;
      const isReady = readyInstances === targetInstances;

      await this.updateStatus(cluster, {
        phase: isReady ? 'Running' : 'Creating',
        phaseReason: isReady
          ? 'All resources reconciled successfully'
          : `Waiting for pods: ${readyInstances}/${targetInstances} ready`,
        instances: targetInstances,
        readyInstances,
        conditions: [
          this.makeCondition(
            'Ready',
            isReady ? 'True' : 'False',
            isReady ? 'ClusterReady' : 'PodsNotReady',
            isReady ? 'Cluster is ready' : `${readyInstances}/${targetInstances} ready`,
          ),
          this.makeCondition(
            'Progressing',
            isReady ? 'False' : 'True',
            isReady ? 'ReconciliationComplete' : 'PodsStarting',
            isReady ? 'Reconciliation completed' : 'Waiting for StatefulSet pods',
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

  /** Reconcile the read-replica service for replication-enabled clusters */
  private async reconcileReplicaService(
    cluster: FirebirdCluster,
    log: Logger,
  ): Promise<void> {
    const { name, namespace = 'default' } = cluster.metadata;
    const replicaName = `${name}-replica`;
    const desired = buildReplicaService(cluster);

    try {
      await this.coreApi.readNamespacedService({ name: replicaName, namespace });
      log.debug('Replica service already exists, skipping');
    } catch {
      log.info('Creating replica service');
      await this.coreApi.createNamespacedService({
        namespace,
        body: desired,
      });
    }
  }

  /** Reconcile the StatefulSet for the cluster and return number of ready replicas */
  private async reconcileStatefulSet(
    cluster: FirebirdCluster,
    log: Logger,
  ): Promise<number> {
    const { name, namespace = 'default' } = cluster.metadata;
    const desired = buildStatefulSet(cluster);

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
      return created.status?.readyReplicas ?? 0;
    }

    let current = existing;
    if (statefulSetNeedsUpdate(existing, desired)) {
      log.info('Updating StatefulSet');
      current = await this.appsApi.patchNamespacedStatefulSet({
        name,
        namespace,
        body: desired,
      });
    } else {
      log.debug('StatefulSet is up to date, skipping');
    }

    return current.status?.readyReplicas ?? 0;
  }

  /** Reconcile the CronJob resource for database backups */
  private async reconcileBackupCronJob(
    cluster: FirebirdCluster,
    log: Logger,
  ): Promise<void> {
    const { name, namespace = 'default' } = cluster.metadata;
    const backupName = `${name}-backup`;

    if (cluster.spec.backup?.enabled) {
      const desired = buildBackupCronJob(cluster);
      try {
        const existing = await this.batchApi.readNamespacedCronJob({ name: backupName, namespace });
        if (cronJobNeedsUpdate(existing, desired)) {
          log.info('Updating backup CronJob');
          await this.batchApi.patchNamespacedCronJob({
            name: backupName,
            namespace,
            body: desired,
          });
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

    if (cluster.spec.instances > 1) {
      const desired = buildPodDisruptionBudget(cluster);
      try {
        const existing = await this.policyApi.readNamespacedPodDisruptionBudget({ name: pdbName, namespace });
        if (podDisruptionBudgetNeedsUpdate(existing, desired)) {
          log.info('Updating PodDisruptionBudget');
          await this.policyApi.patchNamespacedPodDisruptionBudget({
            name: pdbName,
            namespace,
            body: desired,
          });
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
        log.info('Deleting PodDisruptionBudget for single instance cluster');
        await this.policyApi.deleteNamespacedPodDisruptionBudget({ name: pdbName, namespace });
      } catch {
        log.debug('PodDisruptionBudget does not exist, skipping deletion');
      }
    }
  }

  /** Reconcile custom firebird.conf settings or init.sql script ConfigMap */
  private async reconcileConfigMap(
    cluster: FirebirdCluster,
    log: Logger,
  ): Promise<void> {
    const { name, namespace = 'default' } = cluster.metadata;
    const configName = `${name}-config`;
    const desired = buildConfigMap(cluster);

    if (desired) {
      try {
        const existing = await this.coreApi.readNamespacedConfigMap({ name: configName, namespace });
        if (configMapNeedsUpdate(existing, desired)) {
          log.info('Updating cluster ConfigMap');
          await this.coreApi.patchNamespacedConfigMap({
            name: configName,
            namespace,
            body: desired,
          });
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
    log: Logger,
  ): Promise<void> {
    const { name, namespace = 'default' } = cluster.metadata;
    const sweepName = `${name}-sweep`;

    if (cluster.spec.autoSweep?.enabled) {
      const desired = buildAutoSweepCronJob(cluster);
      try {
        const existing = await this.batchApi.readNamespacedCronJob({ name: sweepName, namespace });
        if (autoSweepCronJobNeedsUpdate(existing, desired)) {
          log.info('Updating AutoSweep CronJob');
          await this.batchApi.patchNamespacedCronJob({
            name: sweepName,
            namespace,
            body: desired,
          });
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
            body: desired,
          });
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
