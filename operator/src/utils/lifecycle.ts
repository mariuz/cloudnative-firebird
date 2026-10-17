import { V1Lifecycle, V1Probe } from '@kubernetes/client-node';
import { FirebirdCluster, ProbeTimings } from '../types';

/**
 * How instance pods start and stop (CloudNativePG startDelay, stopDelay, smartShutdownTimeout and
 * probes). Firebird has no "smart" shutdown that leaves no trace: a database shutdown is recorded
 * in its header and fences the instance (utils/fencing.ts). The smart shutdown here is a preStop
 * hook instead: once the pod is terminating it is out of the Services, so no new clients arrive,
 * and the hook waits for the remote attachments to end before the server gets its signal.
 */

/** Kubernetes' default terminationGracePeriodSeconds */
export const DEFAULT_STOP_DELAY = 30;
/** Period of the startup probe */
export const STARTUP_PROBE_PERIOD = 10;
/** Failures the startup probe allows when only spec.probes.startup sets it */
export const DEFAULT_STARTUP_FAILURES = 30;
/** Seconds between the smart shutdown's attachment counts */
export const SMART_SHUTDOWN_POLL = 2;

const TIMING_FIELDS = ['initialDelaySeconds', 'periodSeconds', 'timeoutSeconds', 'failureThreshold', 'successThreshold'] as const;

/** A probe with the timings of spec.probes over it */
function withTimings(probe: V1Probe, timings?: ProbeTimings): V1Probe {
  const result: V1Probe = { ...probe };
  for (const field of TIMING_FIELDS) if (timings?.[field] !== undefined) result[field] = timings[field];
  return result;
}

/** The firebird container's probes: liveness and readiness always, startup with startDelay or probes.startup */
export function instanceProbes(
  cluster: FirebirdCluster,
  readinessCommand: string[],
): { livenessProbe: V1Probe; readinessProbe: V1Probe; startupProbe?: V1Probe } {
  const { probes, startDelay } = cluster.spec;
  const livenessProbe = withTimings({ tcpSocket: { port: 3050 }, initialDelaySeconds: 30, periodSeconds: 10, failureThreshold: 5 }, probes?.liveness);
  // a short delay: the first checks may fail while the server starts, which only delays
  // readiness, never a restart (the liveness probe starts later)
  const readinessProbe = withTimings(
    { exec: { command: readinessCommand }, initialDelaySeconds: 5, periodSeconds: 5, timeoutSeconds: 5, failureThreshold: 3 },
    probes?.readiness,
  );
  if (startDelay === undefined && !probes?.startup) return { livenessProbe, readinessProbe };
  const failureThreshold = startDelay !== undefined ? Math.max(1, Math.ceil(startDelay / STARTUP_PROBE_PERIOD)) : DEFAULT_STARTUP_FAILURES;
  const startupProbe = withTimings({ tcpSocket: { port: 3050 }, periodSeconds: STARTUP_PROBE_PERIOD, failureThreshold }, probes?.startup);
  return { livenessProbe, readinessProbe, startupProbe };
}

/** The pod's terminationGracePeriodSeconds (spec.stopDelay), or none for Kubernetes' default */
export function terminationGracePeriod(cluster: FirebirdCluster): { terminationGracePeriodSeconds?: number } {
  return cluster.spec.stopDelay !== undefined ? { terminationGracePeriodSeconds: cluster.spec.stopDelay } : {};
}

/**
 * The smart shutdown: counts the remote user attachments (not the operator's sidecars and probes,
 * which attach through localhost) every few seconds until there are none or the timeout passes. A
 * server that does not answer has nothing to wait for. Its lines go to the container's log.
 */
export function smartShutdownScript(databasePath: string, timeoutSeconds: number): string {
  const query =
    "SELECT 'remote=' || COUNT(*) FROM MON$ATTACHMENTS WHERE MON$SYSTEM_FLAG = 0 AND MON$ATTACHMENT_ID <> CURRENT_CONNECTION " +
    "AND MON$REMOTE_ADDRESS IS NOT NULL AND MON$REMOTE_ADDRESS NOT STARTING WITH '127.' AND MON$REMOTE_ADDRESS <> '::1' " +
    "AND MON$REMOTE_ADDRESS NOT STARTING WITH '::ffff:127.';";
  return [
    'wait_clients() {',
    `end=$(( $(date +%s) + ${timeoutSeconds} ))`,
    'while :; do',
    // the query is single-quoted for the shell: its own quotes are written as '\\''
    `  n=$(echo '${query.replace(/'/g, "'\\''")}' | isql -q "localhost:${databasePath}" 2>/dev/null | sed -n 's/.*remote=\\([0-9][0-9]*\\).*/\\1/p')`,
    '  if [ -z "$n" ]; then echo "smart shutdown: the server does not answer, stopping"; return 0; fi',
    '  if [ "$n" = 0 ]; then echo "smart shutdown: no remote attachments, stopping"; return 0; fi',
    `  if [ "$(date +%s)" -ge "$end" ]; then echo "smart shutdown: $n remote attachments after ${timeoutSeconds}s, stopping anyway"; return 0; fi`,
    '  echo "smart shutdown: waiting for $n remote attachments"',
    `  sleep ${SMART_SHUTDOWN_POLL}`,
    'done',
    '}',
    // the hook's output is not kept by Kubernetes, the container's log is (when it can be written)
    'wait_clients >/proc/1/fd/1 2>&1 || wait_clients',
  ].join('\n');
}

/** The firebird container's preStop hook (spec.smartShutdownTimeout), or none */
export function smartShutdownLifecycle(cluster: FirebirdCluster, databasePath: string): V1Lifecycle | undefined {
  const timeout = cluster.spec.smartShutdownTimeout;
  if (!timeout) return undefined;
  return { preStop: { exec: { command: ['/bin/sh', '-c', smartShutdownScript(databasePath, timeout)] } } };
}

/** Errors of stopDelay, smartShutdownTimeout, startDelay and probes */
export function lifecycleErrors(spec: FirebirdCluster['spec']): string[] {
  const errors: string[] = [];
  const positive = (field: string, value: unknown, min = 1) => {
    if (value === undefined) return true;
    if (typeof value !== 'number' || !Number.isInteger(value) || value < min) {
      errors.push(`${field} must be an integer of at least ${min}`);
      return false;
    }
    return true;
  };
  positive('stopDelay', spec.stopDelay);
  if (positive('smartShutdownTimeout', spec.smartShutdownTimeout) && spec.smartShutdownTimeout !== undefined) {
    const stop = spec.stopDelay ?? DEFAULT_STOP_DELAY;
    if (spec.smartShutdownTimeout >= stop) errors.push(`smartShutdownTimeout (${spec.smartShutdownTimeout}) must be below stopDelay (${stop}): the server needs time to stop after it`);
  }
  positive('startDelay', spec.startDelay);
  for (const kind of ['liveness', 'readiness', 'startup'] as const) {
    const timings = spec.probes?.[kind];
    if (!timings) continue;
    positive(`probes.${kind}.initialDelaySeconds`, timings.initialDelaySeconds, 0);
    for (const field of ['periodSeconds', 'timeoutSeconds', 'failureThreshold', 'successThreshold'] as const) positive(`probes.${kind}.${field}`, timings[field]);
    if (kind !== 'readiness' && timings.successThreshold !== undefined && timings.successThreshold !== 1) {
      errors.push(`probes.${kind}.successThreshold must be 1 (Kubernetes)`);
    }
  }
  return errors;
}
