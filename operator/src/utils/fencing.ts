import { V1Job } from '@kubernetes/client-node';
import { API_GROUP, DEFAULT_FIREBIRD_IMAGE, FirebirdCluster } from '../types';
import { clusterLabels, databaseName, FIREBIRD_DATA_DIR, superuserClientEnv, jobPodSpec } from './resources';
import { instanceHost } from './replication';
import { ValidationError } from './validation';

/**
 * Instance fencing, following CloudNativePG's `cnpg.io/fencedInstances`: the annotation holds a
 * JSON list of instance (pod) names, `["*"]` fences every instance, and an empty list or no
 * annotation fences nothing.
 *
 * CloudNativePG stops the postmaster and keeps the pod. The Firebird server is the container's
 * main process, so a fenced instance keeps its server running and its database is put into
 * full shutdown instead (`gfix -shut full -force 0` through the service manager): every
 * attachment is closed and no client, replica apply or backup can attach until the fence is
 * lifted. The shutdown is recorded in the database header, so it survives pod restarts. The
 * readiness probe reports a shut-down database as not ready, so a fenced instance leaves the
 * Services, read routing and replica seeding. A fenced primary is not failed over.
 */
export const FENCED_INSTANCES_ANNOTATION = `${API_GROUP}/fencedInstances`;

/** Label on fencing Jobs naming the action they apply */
export const FENCING_ACTION_LABEL = `${API_GROUP}/fencing-action`;

export type FencingAction = 'fence' | 'unfence';

/**
 * Returns the instances the annotation fences, limited to the cluster's current instances.
 * Throws a ValidationError for a malformed annotation.
 */
export function desiredFencedInstances(cluster: FirebirdCluster): string[] {
  const raw = cluster.metadata.annotations?.[FENCED_INSTANCES_ANNOTATION];
  if (raw === undefined || raw.trim() === '') return [];
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new ValidationError(`${FENCED_INSTANCES_ANNOTATION} must be a JSON list of instance names, got ${raw}`);
  }
  if (!Array.isArray(parsed) || parsed.some((v) => typeof v !== 'string')) {
    throw new ValidationError(`${FENCED_INSTANCES_ANNOTATION} must be a JSON list of instance names, got ${raw}`);
  }
  const name = cluster.metadata.name;
  const instances = Array.from({ length: cluster.spec.instances }, (_, i) => `${name}-${i}`);
  if (parsed.includes('*')) return instances;
  const unknown = (parsed as string[]).filter((p) => !/^.+-\d+$/.test(p) || !p.startsWith(`${name}-`));
  if (unknown.length > 0) {
    throw new ValidationError(`${FENCED_INSTANCES_ANNOTATION} names instances of another cluster: ${unknown.join(', ')}`);
  }
  return instances.filter((pod) => (parsed as string[]).includes(pod));
}

/** Name of the Job that applies fencing changes to an instance */
export function fencingJobName(pod: string): string {
  return `${pod}-fencing`;
}

/**
 * Shell check used by the readiness probe: the local server answers the service manager and the
 * database is not shut down (fenced, or shut down by an administrator).
 */
export function databaseOnlineCheck(databasePath: string): string {
  return (
    `out=$(fbsvcmgr localhost:service_mgr action_db_stats dbname ${databasePath} sts_hdr_pages 2>&1) ` +
    `&& ! echo "$out" | grep -q 'shutdown'`
  );
}

/**
 * Builds the Job that fences or unfences one instance through its service manager. The service
 * actions fail when the database is already in the target mode, so the Job reads the header first
 * and is idempotent.
 */
export function buildFencingJob(cluster: FirebirdCluster, pod: string, action: FencingAction): V1Job {
  const { name, namespace = 'default', uid } = cluster.metadata;
  const labels = {
    ...clusterLabels(name),
    'app.kubernetes.io/component': 'fencing',
    [FENCING_ACTION_LABEL]: action,
  };
  // Firebird 6 refuses header statistics for a database in full shutdown ("database ... shutdown"):
  // that answer means fenced as well
  const header =
    'hdr=$(fbsvcmgr "$FIREBIRD_HOST:service_mgr" action_db_stats dbname "$DATABASE_PATH" sts_hdr_pages 2>&1) || ' +
    '{ echo "$hdr" | grep -q "^database .* shutdown" || { echo "$hdr" >&2; exit 1; }; hdr="full shutdown"; }; ';
  const script =
    action === 'fence'
      ? header +
        'if echo "$hdr" | grep -q "full shutdown"; then echo "$FIREBIRD_HOST already fenced"; else ' +
        'fbsvcmgr "$FIREBIRD_HOST:service_mgr" action_properties dbname "$DATABASE_PATH" prp_shutdown_mode prp_sm_full prp_force_shutdown 0; ' +
        'echo "fenced $FIREBIRD_HOST: database in full shutdown"; fi'
      : header +
        'if ! echo "$hdr" | grep -q "shutdown"; then echo "$FIREBIRD_HOST already online"; else ' +
        'fbsvcmgr "$FIREBIRD_HOST:service_mgr" action_properties dbname "$DATABASE_PATH" prp_online_mode prp_sm_normal; ' +
        'echo "unfenced $FIREBIRD_HOST: database online"; fi';

  return {
    apiVersion: 'batch/v1',
    kind: 'Job',
    metadata: {
      name: fencingJobName(pod),
      namespace,
      labels,
      ownerReferences: [
        {
          apiVersion: `${API_GROUP}/v1`,
          kind: 'FirebirdCluster',
          name,
          uid: uid ?? '',
          controller: true,
          blockOwnerDeletion: true,
        },
      ],
    },
    spec: {
      backoffLimit: 3,
      ttlSecondsAfterFinished: 3600,
      template: {
        metadata: { labels },
        spec: jobPodSpec(cluster, {
          restartPolicy: 'Never',
          containers: [
            {
              name: 'fencing',
              image: cluster.spec.imageName ?? DEFAULT_FIREBIRD_IMAGE,
              command: ['/bin/sh', '-c'],
              args: [`set -eu; ${script}`],
              env: [
                ...superuserClientEnv(cluster),
                { name: 'FIREBIRD_HOST', value: instanceHost(cluster, pod) },
                { name: 'DATABASE_PATH', value: `${FIREBIRD_DATA_DIR}/${databaseName(cluster)}` },
              ],
            },
          ],
        }),
      },
    },
  };
}

/** What to do about one instance's fencing Job */
export type FencingStep =
  | { kind: 'none' }
  | { kind: 'create'; action: FencingAction }
  | { kind: 'wait' }
  /** the Job finished: record its action as applied and delete it */
  | { kind: 'applied'; action: FencingAction }
  /** the Job failed: delete it, so the next reconcile retries if the change is still wanted */
  | { kind: 'failed'; action: FencingAction };

/**
 * Decides the next fencing step for an instance from the desired and applied state and its
 * current Job. There is at most one Job per instance and a running Job is never interrupted; a
 * finished Job is recorded for the action it performed, even if the annotation changed meanwhile,
 * and the next reconcile starts a new Job if the instance still differs from the annotation.
 */
export function planFencing(desired: boolean, applied: boolean, job: V1Job | undefined): FencingStep {
  if (!job) return desired === applied ? { kind: 'none' } : { kind: 'create', action: desired ? 'fence' : 'unfence' };
  const action: FencingAction = job.metadata?.labels?.[FENCING_ACTION_LABEL] === 'fence' ? 'fence' : 'unfence';
  const conditions = job.status?.conditions ?? [];
  if (conditions.some((c) => c.type === 'Complete' && c.status === 'True')) return { kind: 'applied', action };
  if (conditions.some((c) => c.type === 'Failed' && c.status === 'True')) return { kind: 'failed', action };
  return { kind: 'wait' };
}
