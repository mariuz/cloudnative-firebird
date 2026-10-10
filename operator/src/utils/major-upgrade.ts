import { createHash } from 'crypto';
import { V1EnvVar, V1Job, V1Pod } from '@kubernetes/client-node';
import { API_GROUP, FirebirdCluster, ImageCheckStatus, MajorUpgradeStatus, RESOURCE_KIND } from '../types';
import {
  clusterLabels,
  databaseName,
  FIREBIRD_DATA_DIR,
  instancePodSecurityContext,
  instanceSecurityContext,
  jobPodSpec,
  SECURITY_DB_PATH,
  serviceAccount,
} from './resources';
import { OPERATOR_CONFIG_DIR, replicationDirectories, replicationEnabled, UPGRADE_SCRIPTS } from './replication';
import { isPodReady } from './routing';

/**
 * Major version upgrades (CloudNativePG's offline major upgrade, adapted to Firebird).
 *
 * A Firebird engine opens the databases of its own major on-disk structure (ODS) only: Firebird 6
 * (ODS 14) refuses the ODS 13 of Firebird 4 and 5. Firebird 5 opens the ODS 13.0 of Firebird 4
 * as it is, so 4 to 5 is an ordinary image change. Changing spec.imageName therefore goes through
 * a check first: an image check Job runs both images (image-check.sh) and reports the ODS of a
 * database each creates, and the server version. Meanwhile the instances keep their image.
 *
 * - Same major ODS: the new image is rolled out as usual (rolling update, primary last).
 * - Newer major ODS: a major upgrade. The instances stop (the cluster is offline, as hibernated:
 *   no failover, no rolling update, scheduled work suspended), a Job per instance volume converts
 *   its databases (major-upgrade.sh: gbak backup with the old image, restore with the new one,
 *   the old files kept), and the instances start on the new image: the primary's init container
 *   writes a new seed, the replicas (their databases discarded by the Jobs) are seeded from it.
 *   A failed Job leaves the cluster stopped; deleting the Job retries it, and setting
 *   spec.imageName back abandons the upgrade while no volume was converted.
 * - Older major ODS: refused, the instances keep their image (a database cannot be moved back to
 *   an older ODS; restore a logical backup into a new cluster instead).
 */

export const IMAGE_CHECK_COMPONENT = 'image-check';
export const MAJOR_UPGRADE_COMPONENT = 'major-upgrade';

/** What the image check reports about an image */
export interface ImageFacts {
  /** ODS "major.minor" of a database the image creates */
  ods: string;
  version: string;
}

/** Parses an image check termination message ("ODS 13.1 VERSION 5.0.4.1812") */
export function parseImageCheck(message: string | undefined): ImageFacts | undefined {
  const match = /^ODS (\d+\.\d+) VERSION (\S+)$/.exec((message ?? '').trim());
  return match ? { ods: match[1], version: match[2] } : undefined;
}

export function odsMajor(ods: string): number {
  return Number(ods.split('.')[0]);
}

/** What an image change means for the databases */
export function imageChangeKind(from: ImageFacts, to: ImageFacts): 'Compatible' | 'Upgrade' | 'Refused' {
  const delta = odsMajor(to.ods) - odsMajor(from.ods);
  return delta === 0 ? 'Compatible' : delta > 0 ? 'Upgrade' : 'Refused';
}

/** The message of a decided image check */
export function imageCheckMessage(check: ImageCheckStatus): string {
  const from = `${check.from} (Firebird ${check.fromVersion ?? '?'}, ODS ${check.fromOds ?? '?'})`;
  const to = `${check.to} (Firebird ${check.toVersion ?? '?'}, ODS ${check.toOds ?? '?'})`;
  switch (check.phase) {
    case 'Compatible':
      return `${from} to ${to}: same on-disk structure, rolled out as usual`;
    case 'Upgrade':
      return `${from} to ${to}: newer on-disk structure, the databases are converted (major upgrade)`;
    case 'Refused':
      return (
        `${to} cannot open the databases of ${from}: a newer on-disk structure cannot be moved back; ` +
        `the instances keep ${check.from} (restore a logical backup into a new cluster instead)`
      );
    case 'Failed':
      return `checking ${check.to} failed: ${check.message ?? 'unknown error'}; the instances keep ${check.from}`;
    default:
      return `checking ${check.to} before the instances run it (they keep ${check.from} meanwhile)`;
  }
}

const shortHash = (text: string) => createHash('sha256').update(text).digest('hex').slice(0, 8);

function ownerReference(cluster: FirebirdCluster) {
  return {
    apiVersion: `${API_GROUP}/v1`,
    kind: RESOURCE_KIND,
    name: cluster.metadata.name,
    uid: cluster.metadata.uid ?? '',
    controller: true,
    blockOwnerDeletion: true,
  };
}

/** Name of the Job checking an image change (one per pair of images) */
export function imageCheckJobName(cluster: FirebirdCluster, from: string, to: string): string {
  return `${cluster.metadata.name}-image-check-${shortHash(`${from}>${to}`)}`;
}

/** Container names of the image check Job: the image the instances run, and the new one */
export const IMAGE_CHECK_CONTAINERS = { from: 'current', to: 'new' } as const;

/** Job running the image check (image-check.sh) on both images, each in its own container */
export function buildImageCheckJob(cluster: FirebirdCluster, from: string, to: string): V1Job {
  const { name, namespace = 'default' } = cluster.metadata;
  const labels = { ...clusterLabels(name), 'app.kubernetes.io/component': IMAGE_CHECK_COMPONENT };
  const container = (containerName: string, image: string) => ({
    name: containerName,
    image,
    command: ['/bin/sh', '-c', UPGRADE_SCRIPTS['image-check.sh']],
    // embedded: the user name only
    env: [{ name: 'ISC_USER', value: 'SYSDBA' }],
    resources: { requests: { cpu: '10m', memory: '64Mi' } },
  });
  return {
    apiVersion: 'batch/v1',
    kind: 'Job',
    metadata: { name: imageCheckJobName(cluster, from, to), namespace, labels, ownerReferences: [ownerReference(cluster)] },
    spec: {
      backoffLimit: 1,
      template: {
        metadata: { labels },
        spec: jobPodSpec(cluster, {
          restartPolicy: 'Never',
          containers: [container(IMAGE_CHECK_CONTAINERS.from, from), container(IMAGE_CHECK_CONTAINERS.to, to)],
          ...(cluster.spec.nodeSelector ? { nodeSelector: cluster.spec.nodeSelector } : {}),
          ...(cluster.spec.tolerations ? { tolerations: cluster.spec.tolerations } : {}),
        }),
      },
    },
  };
}

/** The facts each container of a finished image check Job pod reported */
export function imageCheckResults(pods: V1Pod[]): { from?: ImageFacts; to?: ImageFacts } {
  const result: { from?: ImageFacts; to?: ImageFacts } = {};
  for (const pod of pods) {
    for (const status of pod.status?.containerStatuses ?? []) {
      const facts = parseImageCheck(status.state?.terminated?.message);
      if (!facts) continue;
      if (status.name === IMAGE_CHECK_CONTAINERS.from) result.from = facts;
      if (status.name === IMAGE_CHECK_CONTAINERS.to) result.to = facts;
    }
  }
  return result;
}

/** Name of the Job converting the volume of instance `ordinal` */
export function majorUpgradeJobName(cluster: FirebirdCluster, ordinal: number): string {
  return `${cluster.metadata.name}-major-upgrade-${ordinal}`;
}

/** What the conversion of an instance volume does (major-upgrade.sh ROLE) */
export type MajorUpgradeRole = 'primary' | 'replica' | 'standalone';

export function majorUpgradeRole(cluster: FirebirdCluster, upgrade: MajorUpgradeStatus, pod: string): MajorUpgradeRole {
  if (!replicationEnabled(cluster)) return 'standalone';
  return pod === (upgrade.primary ?? `${cluster.metadata.name}-0`) ? 'primary' : 'replica';
}

/**
 * Job converting the databases on one instance volume (major-upgrade.sh): an init container backs
 * them up with the old image, the container restores them with the new one. It runs with the
 * instance pods' security contexts and scheduling, while the instance is stopped.
 */
export function buildMajorUpgradeJob(cluster: FirebirdCluster, upgrade: MajorUpgradeStatus, ordinal: number, claimName: string): V1Job {
  const { name, namespace = 'default' } = cluster.metadata;
  const pod = `${name}-${ordinal}`;
  const role = majorUpgradeRole(cluster, upgrade, pod);
  const labels = { ...clusterLabels(name), 'app.kubernetes.io/component': MAJOR_UPGRADE_COMPONENT };
  const dirs = replicationDirectories(cluster, FIREBIRD_DATA_DIR);
  const env: V1EnvVar[] = [
    { name: 'ISC_USER', value: 'SYSDBA' },
    { name: 'ROLE', value: role },
    { name: 'TARGET_ODS', value: String(odsMajor(upgrade.toOds)) },
    { name: 'DATA_DIR', value: FIREBIRD_DATA_DIR },
    { name: 'DATABASE_PATH', value: `${FIREBIRD_DATA_DIR}/${databaseName(cluster)}` },
    { name: 'SECURITY_DB', value: SECURITY_DB_PATH },
    { name: 'SCRIPT_DIR', value: OPERATOR_CONFIG_DIR },
    ...(replicationEnabled(cluster)
      ? [
          { name: 'REPLICATION_DIR', value: dirs.base },
          { name: 'JOURNAL_DIR', value: dirs.journal },
          { name: 'ARCHIVE_DIR', value: dirs.archive },
          { name: 'SOURCE_DIR', value: dirs.source },
          { name: 'STATE_FILE', value: dirs.state },
        ]
      : []),
  ];
  const mounts = [
    { name: 'firebird-data', mountPath: FIREBIRD_DATA_DIR },
    { name: 'cluster-config', mountPath: OPERATOR_CONFIG_DIR, readOnly: true },
  ];
  const step = (containerName: string, image: string, mode: string) => ({
    name: containerName,
    image,
    command: ['/bin/sh', '-c', UPGRADE_SCRIPTS['major-upgrade.sh']],
    env: [...env, { name: 'MODE', value: mode }],
    volumeMounts: mounts,
    securityContext: instanceSecurityContext(cluster),
    ...(cluster.spec.resources ? { resources: cluster.spec.resources } : {}),
  });
  return {
    apiVersion: 'batch/v1',
    kind: 'Job',
    metadata: {
      name: majorUpgradeJobName(cluster, ordinal),
      namespace,
      labels,
      annotations: { [`${API_GROUP}/major-upgrade`]: `${upgrade.from} > ${upgrade.to}`, [`${API_GROUP}/role`]: role },
      ownerReferences: [ownerReference(cluster)],
    },
    spec: {
      backoffLimit: 1,
      template: {
        metadata: { labels },
        spec: {
          restartPolicy: 'Never',
          ...serviceAccount(cluster),
          securityContext: instancePodSecurityContext(cluster),
          initContainers: [step('backup', upgrade.from, 'backup')],
          containers: [step('restore', upgrade.to, 'restore')],
          volumes: [
            { name: 'firebird-data', persistentVolumeClaim: { claimName } },
            { name: 'cluster-config', configMap: { name: `${name}-config` } },
          ],
          ...(cluster.spec.nodeSelector ? { nodeSelector: cluster.spec.nodeSelector } : {}),
          ...(cluster.spec.affinity ? { affinity: cluster.spec.affinity } : {}),
          ...(cluster.spec.tolerations ? { tolerations: cluster.spec.tolerations } : {}),
        },
      },
    },
  };
}

/** Phases in which the instances are stopped */
export function majorUpgradeOffline(upgrade: MajorUpgradeStatus | undefined): boolean {
  return upgrade?.phase === 'Stopping' || upgrade?.phase === 'Converting' || upgrade?.phase === 'Failed';
}

/** Phases of an upgrade not finished yet */
export function majorUpgradeInProgress(upgrade: MajorUpgradeStatus | undefined): boolean {
  return majorUpgradeOffline(upgrade) || upgrade?.phase === 'Starting';
}

/**
 * The cluster as the rest of the reconcile sees it: the image the instances run (held at `image`
 * while a change is checked or refused), and stopped (as hibernated) while `offline`. The spec of
 * the stored object is unchanged.
 */
export function withEffectiveImage(cluster: FirebirdCluster, image: string, offline = false): FirebirdCluster {
  return { ...cluster, spec: { ...cluster.spec, imageName: image, ...(offline ? { hibernated: true } : {}) } };
}

/** Whether every instance runs `image` and is ready (fenced instances aside) */
export function instancesStartedOn(cluster: FirebirdCluster, pods: V1Pod[], image: string, fenced: string[] = []): boolean {
  const running = pods.filter((p) => !p.metadata?.deletionTimestamp);
  if (running.length < cluster.spec.instances) return false;
  return running.every((p) => {
    const onImage = (p.spec?.containers ?? []).find((c) => c.name === 'firebird')?.image === image;
    return onImage && (isPodReady(p) || fenced.includes(p.metadata?.name ?? ''));
  });
}

/** Why the cluster's instances are stopped (hibernated, or a major upgrade converting them), if they are */
export function clusterStoppedReason(cluster: FirebirdCluster): string | undefined {
  if (cluster.spec.hibernated) return 'cluster is hibernated';
  if (majorUpgradeOffline(cluster.status?.majorUpgrade)) return 'cluster is stopped for a major upgrade';
  return undefined;
}
