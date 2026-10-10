import { V1Pod } from '@kubernetes/client-node';
import { FirebirdCluster } from '../types';
import { isPodReady } from './routing';
import { PRIMARY_LEASE_ANNOTATION, PRIMARY_LEASE_CONTAINER, primaryLeaseEnabled } from './primary-lease';

/**
 * Moving clusters pinned to the operator-moved Lease over to the primary Lease
 * (PRIMARY_LEASE_MIGRATE), one at a time, as SEGMENT_TLS_MIGRATE does for segment TLS.
 *
 * Clusters that existed when v0.87.0 first saw them carry the primary-lease annotation "pinned":
 * the operator keeps moving their Lease itself, since the lease-holder sidecar would restart
 * their instances. With PRIMARY_LEASE_MIGRATE=pinned the operator moves them itself: the
 * annotation becomes "in-progress", which makes the primary Lease effective (primary-lease.ts),
 * so the instances of a cluster with automatic failover restart one by one through the usual
 * rolling update (the primary last) to add the sidecar; a cluster without failover changes
 * nothing now and holds its Lease once failover is enabled. Once every instance runs what the
 * cluster needs, the annotation becomes "done". An owner who sets failover.primaryLease.enabled
 * during the migration gets "skip" (the owner's value wins anyway); a cluster annotated "skip"
 * by hand is left alone. A cluster is moved only when it is idle: running, every instance
 * ready, no rolling update, switchover or fencing, and no other cluster migrating.
 */

export type PrimaryLeaseMigrationMode = 'off' | 'pinned';

export function primaryLeaseMigrationMode(env = process.env): PrimaryLeaseMigrationMode {
  return (env.PRIMARY_LEASE_MIGRATE ?? '').trim().toLowerCase() === 'pinned' ? 'pinned' : 'off';
}

export type PrimaryLeaseMigrationStep =
  | { action: 'start' }
  | { action: 'finish' }
  | { action: 'abandon' }
  | { action: 'none'; reason?: string };

const annotation = (cluster: FirebirdCluster) => cluster.metadata.annotations?.[PRIMARY_LEASE_ANNOTATION];
const key = (cluster: FirebirdCluster) => `${cluster.metadata.namespace ?? 'default'}/${cluster.metadata.name}`;

export function primaryLeaseMigrationInProgress(cluster: FirebirdCluster): boolean {
  return annotation(cluster) === 'in-progress';
}

/** Why the cluster cannot be moved now, or undefined when it is a candidate */
export function notMigratingToPrimaryLease(cluster: FirebirdCluster, mode: PrimaryLeaseMigrationMode, nativeSidecars: boolean | undefined): string | undefined {
  if (mode === 'off') return 'off';
  if (annotation(cluster) !== 'pinned') return `annotation ${annotation(cluster) ?? 'missing'}`;
  if (cluster.spec.replication?.failover?.primaryLease?.enabled !== undefined) return 'the owner decided';
  if (nativeSidecars !== true) return 'Kubernetes without native sidecars (1.29 or later)';
  if (cluster.spec.hibernated || cluster.spec.suspended) return 'hibernated or suspended';
  const status = cluster.status ?? {};
  if (status.phase !== 'Running') return `phase ${status.phase ?? 'unknown'}`;
  if (status.rollingUpdate) return 'rolling update in progress';
  if (status.switchover && !['Completed', 'Failed'].includes(status.switchover.phase)) return 'switchover in progress';
  if ((status.fencedInstances ?? []).length > 0) return 'fenced instances';
  if ((status.readyInstances ?? 0) < cluster.spec.instances) return 'not every instance is ready';
  return undefined;
}

/**
 * The next step for a cluster: `pods` are its instance pods (needed while it migrates), `others`
 * every cluster (needed to start one)
 */
export function primaryLeaseMigrationStep(
  cluster: FirebirdCluster,
  mode: PrimaryLeaseMigrationMode,
  nativeSidecars: boolean | undefined,
  pods: V1Pod[] = [],
  others: FirebirdCluster[] = [],
): PrimaryLeaseMigrationStep {
  if (primaryLeaseMigrationInProgress(cluster)) {
    if (cluster.spec.replication?.failover?.primaryLease?.enabled !== undefined) return { action: 'abandon' };
    // with automatic failover every instance runs the sidecar once rolled; without, nothing changes
    const needsSidecar = primaryLeaseEnabled(cluster);
    const switched =
      !cluster.status?.rollingUpdate &&
      (!needsSidecar ||
        (pods.length >= cluster.spec.instances &&
          pods.every((p) => (p.spec?.initContainers ?? []).some((c) => c.name === PRIMARY_LEASE_CONTAINER) && isPodReady(p))));
    return switched ? { action: 'finish' } : { action: 'none', reason: 'instances restarting' };
  }
  const reason = notMigratingToPrimaryLease(cluster, mode, nativeSidecars);
  if (reason) return { action: 'none', reason };
  const busy = others.find((o) => key(o) !== key(cluster) && primaryLeaseMigrationInProgress(o));
  if (busy) return { action: 'none', reason: `${key(busy)} is migrating` };
  return { action: 'start' };
}
