import { V1Job } from '@kubernetes/client-node';
import { API_GROUP, DEFAULT_FIREBIRD_IMAGE, FirebirdCluster } from '../types';
import { clusterPodSettings, withInheritedMetadata, withPullPolicy } from '../utils/pod-settings';

/**
 * kubectl firebird bench: a load test of a cluster (CloudNativePG's kubectl cnpg pgbench). A Job
 * in the cluster's image connects through the primary Service as SYSDBA and runs, from several
 * clients at once, short read/write transactions against a table of its own (KF_BENCH: an
 * insert, an update and a read each), then prints the transactions per second. The table is
 * created first and dropped afterwards unless kept.
 */

/** The table the bench creates and drops: a name no application uses */
export const BENCH_TABLE = 'KF_BENCH';
/** The user and group the operator's Jobs run as (utils/resources.ts FIREBIRD_UID) */
export const BENCH_UID = 84;

export interface BenchOptions {
  /** Transactions per client */
  transactions: number;
  clients: number;
  /** Database file in the data directory */
  databaseFile: string;
  jobName: string;
  /** Keep the table (and its rows) afterwards */
  keep: boolean;
}

/** The cluster's image, as the operator runs it (utils/image-catalog.ts clusterImage) */
function benchImage(cluster: FirebirdCluster): string {
  if (cluster.spec.imageName) return cluster.spec.imageName;
  if (cluster.spec.imageCatalogRef && cluster.status?.image) return cluster.status.image;
  return DEFAULT_FIREBIRD_IMAGE;
}

/**
 * The bench script. Each client is one isql session running a generated script of its
 * transactions; a client fails when isql exits with an error or reports a failed statement.
 * Environment: HOST, DATABASE, CLIENTS, TRANSACTIONS, KEEP (1 to keep the table).
 */
export function benchScript(): string {
  return [
    'set -u',
    'db="$HOST:$DATABASE"',
    'work=$(mktemp -d)',
    `echo "RECREATE TABLE ${BENCH_TABLE} (ID BIGINT NOT NULL PRIMARY KEY, CLIENT INTEGER, N INTEGER, PAYLOAD VARCHAR(100)); COMMIT;" | isql -q "$db" >"$work/init.log" 2>&1`,
    'if [ $? -ne 0 ] || grep -q "Statement failed" "$work/init.log"; then cat "$work/init.log"; echo "bench: could not create the table"; exit 1; fi',
    // one script per client: insert, update and read per transaction
    'c=1',
    'while [ "$c" -le "$CLIENTS" ]; do',
    // the quote comes in as a variable (awk implementations differ on escapes); IDs past 2^31
    // are printed with %.0f (mawk's %d is 32-bit)
    '  awk -v c="$c" -v n="$TRANSACTIONS" -v q="\'" \'BEGIN { for (i = 1; i <= n; i++) { id = c * 100000000 + i; ' +
      `printf "INSERT INTO ${BENCH_TABLE} VALUES (%.0f, %d, %d, %sbench%s); UPDATE ${BENCH_TABLE} SET N = N + 1 WHERE ID = %.0f; ` +
      `SELECT COUNT(*) FROM ${BENCH_TABLE} WHERE CLIENT = %d AND ID <= %.0f; COMMIT;\\n", id, c, i, q, q, id, c, id } }' >"$work/client-$c.sql"`,
    '  c=$((c + 1))',
    'done',
    'echo "bench: $CLIENTS clients x $TRANSACTIONS transactions against $db"',
    'start=$(date +%s%N)',
    'c=1',
    'while [ "$c" -le "$CLIENTS" ]; do',
    '  ( isql -q "$db" -i "$work/client-$c.sql" >"$work/client-$c.log" 2>&1; echo $? >"$work/client-$c.status" ) &',
    '  c=$((c + 1))',
    'done',
    'wait',
    'end=$(date +%s%N)',
    'failed=0',
    'c=1',
    'while [ "$c" -le "$CLIENTS" ]; do',
    '  if [ "$(cat "$work/client-$c.status")" != 0 ] || grep -q "Statement failed" "$work/client-$c.log"; then',
    '    echo "bench: client $c failed:"; grep -m 5 -A 3 "Statement failed\\|SQLSTATE" "$work/client-$c.log" || tail -5 "$work/client-$c.log"; failed=1',
    '  fi',
    '  c=$((c + 1))',
    'done',
    `rows=$(echo "SELECT 'rows=' || COUNT(*) FROM ${BENCH_TABLE};" | isql -q "$db" | sed -n 's/.*rows=\\([0-9]*\\).*/\\1/p')`,
    `if [ "\${KEEP:-0}" != 1 ]; then echo "DROP TABLE ${BENCH_TABLE}; COMMIT;" | isql -q "$db" >/dev/null 2>&1 || echo "bench: note: could not drop ${BENCH_TABLE}"; fi`,
    'if [ "$failed" != 0 ]; then exit 1; fi',
    'total=$((CLIENTS * TRANSACTIONS))',
    'if [ "$rows" != "$total" ]; then echo "bench: $rows rows, expected $total"; exit 1; fi',
    'awk -v total="$total" -v ns="$((end - start))" -v clients="$CLIENTS" \'BEGIN { s = ns / 1e9; if (s <= 0) s = 0.001; ' +
      'printf "transactions: %d\\nclients: %d\\nelapsed: %.3f s\\ntps: %.1f\\nlatency (average): %.2f ms\\n", total, clients, s, total / s, s * 1000 * clients / total }\'',
  ].join('\n');
}

/** The Job of a bench run */
export function buildBenchJob(cluster: FirebirdCluster, options: BenchOptions): V1Job {
  const { name, namespace = 'default' } = cluster.metadata;
  const secret = cluster.spec.superuserSecret;
  const labels = {
    'app.kubernetes.io/name': 'firebird',
    'app.kubernetes.io/component': 'bench',
    'app.kubernetes.io/managed-by': 'kubectl-firebird',
    [`${API_GROUP}/cluster`]: name,
  };
  // the cluster's pod settings (service account, priority class, pull secrets) and inherited metadata
  return withInheritedMetadata(cluster, {
    apiVersion: 'batch/v1',
    kind: 'Job',
    metadata: { name: options.jobName, namespace, labels },
    spec: {
      backoffLimit: 0,
      // the result stays readable (kubectl logs) for a day
      ttlSecondsAfterFinished: 86400,
      template: {
        metadata: { labels },
        spec: {
          restartPolicy: 'Never',
          ...clusterPodSettings(cluster),
          securityContext: { runAsNonRoot: true, runAsUser: BENCH_UID, runAsGroup: BENCH_UID, seccompProfile: { type: 'RuntimeDefault' } },
          containers: [
            withPullPolicy(cluster, {
              name: 'bench',
              image: benchImage(cluster),
              command: ['/bin/bash', '-c', benchScript()],
              env: [
                { name: 'HOST', value: name },
                { name: 'DATABASE', value: `/var/lib/firebird/data/${options.databaseFile}` },
                { name: 'CLIENTS', value: String(options.clients) },
                { name: 'TRANSACTIONS', value: String(options.transactions) },
                { name: 'KEEP', value: options.keep ? '1' : '0' },
                { name: 'ISC_USER', value: 'SYSDBA' },
                secret
                  ? { name: 'ISC_PASSWORD', valueFrom: { secretKeyRef: { name: secret.name, key: 'password' } } }
                  : { name: 'ISC_PASSWORD', value: 'masterkey' },
              ],
              securityContext: { allowPrivilegeEscalation: false, capabilities: { drop: ['ALL'] } },
            }),
          ],
        },
      },
    },
  });
}
