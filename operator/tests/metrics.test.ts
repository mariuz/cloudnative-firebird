import { describe, it, expect, beforeEach } from 'vitest';
import { metrics, MetricsRegistry, recordClusterMetrics, recordReconcile } from '../src/utils/metrics';
import { FirebirdCluster } from '../src/types';

describe('MetricsRegistry', () => {
  it('renders gauges and counters in the Prometheus text format', () => {
    const r = new MetricsRegistry();
    r.set('b_gauge', 'A gauge', { cluster: 'db', note: 'a "quoted"\\value\n' }, 1.5);
    r.inc('a_total', 'A counter', { result: 'success' });
    r.inc('a_total', 'A counter', { result: 'success' }, 2);
    r.set('c_empty', 'No labels', {}, NaN);
    expect(r.render()).toBe(
      [
        '# HELP a_total A counter',
        '# TYPE a_total counter',
        'a_total{result="success"} 3',
        '# HELP b_gauge A gauge',
        '# TYPE b_gauge gauge',
        'b_gauge{cluster="db",note="a \\"quoted\\"\\\\value\\n"} 1.5',
        '# HELP c_empty No labels',
        '# TYPE c_empty gauge',
        'c_empty NaN',
        '',
      ].join('\n'),
    );
  });

  it('removes the series matching labels and omits empty families', () => {
    const r = new MetricsRegistry();
    r.set('g', 'G', { namespace: 'a', cluster: 'x' }, 1);
    r.set('g', 'G', { namespace: 'a', cluster: 'y' }, 2);
    r.set('h', 'H', { namespace: 'a', cluster: 'x', pod: 'x-1' }, 3);
    r.remove({ namespace: 'a', cluster: 'x' });
    expect(r.render()).toBe('# HELP g G\n# TYPE g gauge\ng{namespace="a",cluster="y"} 2\n');
    r.removeFamilies(['g'], { cluster: 'y' });
    expect(r.render()).toBe('\n');
  });
});

describe('cluster metrics', () => {
  const cluster: FirebirdCluster = {
    apiVersion: 'firebird.cloudnative-firebird.io/v1',
    kind: 'FirebirdCluster',
    metadata: { name: 'db', namespace: 'prod', uid: 'c' },
    spec: { instances: 3, storage: { size: '1Gi' }, replication: { enabled: true } },
  };
  const lines = () => metrics.render().split('\n').filter((l) => l.includes('cluster="db"'));

  beforeEach(() => metrics.remove({ namespace: 'prod', cluster: 'db' }));

  it('exports the instances and the measured replication lag per replica', () => {
    recordClusterMetrics(cluster, {
      phase: 'Running',
      readyInstances: 3,
      fencedInstances: [],
      replicationStatus: {
        primaryPod: 'db-0',
        lastArchivedSequence: 22,
        replicas: [
          { name: 'db-1', appliedSequence: 22, pendingSegments: 0, lagSegments: 0, lagSeconds: 0 },
          { name: 'db-2', appliedSequence: 20, pendingSegments: 2, lagSegments: 2, lagSeconds: 90 },
          { name: 'db-3', error: 'connect ECONNREFUSED' },
        ],
      },
    });
    recordReconcile(cluster, 'success');
    recordReconcile(cluster, 'success');
    recordReconcile(cluster, 'error');
    expect(lines()).toEqual(
      expect.arrayContaining([
        'firebird_cluster_instances{namespace="prod",cluster="db"} 3',
        'firebird_cluster_ready_instances{namespace="prod",cluster="db"} 3',
        'firebird_cluster_fenced_instances{namespace="prod",cluster="db"} 0',
        'firebird_cluster_ready{namespace="prod",cluster="db"} 1',
        'firebird_replication_last_archived_sequence{namespace="prod",cluster="db"} 22',
        'firebird_replication_lag_seconds{namespace="prod",cluster="db",pod="db-1"} 0',
        'firebird_replication_lag_seconds{namespace="prod",cluster="db",pod="db-2"} 90',
        'firebird_replication_lag_segments{namespace="prod",cluster="db",pod="db-2"} 2',
        'firebird_replication_pending_segments{namespace="prod",cluster="db",pod="db-2"} 2',
        'firebird_replication_applied_sequence{namespace="prod",cluster="db",pod="db-2"} 20',
        'firebird_operator_reconciles_total{namespace="prod",cluster="db",result="success"} 2',
        'firebird_operator_reconciles_total{namespace="prod",cluster="db",result="error"} 1',
      ]),
    );
    // an unmeasured replica has no lag series rather than a stale or zero one
    expect(lines().filter((l) => l.includes('pod="db-3"'))).toEqual([]);
  });

  it('drops the lag series of replicas no longer reported', () => {
    const replica = (name: string) => ({ name, appliedSequence: 5, pendingSegments: 0, lagSegments: 0, lagSeconds: 0 });
    recordClusterMetrics(cluster, {
      phase: 'Running',
      readyInstances: 3,
      replicationStatus: { primaryPod: 'db-0', lastArchivedSequence: 5, replicas: [replica('db-1'), replica('db-2')] },
    });
    recordClusterMetrics(cluster, {
      phase: 'Creating',
      readyInstances: 1,
      replicationStatus: { primaryPod: 'db-1', replicas: [] },
    });
    expect(lines().filter((l) => l.startsWith('firebird_replication_'))).toEqual([]);
    expect(lines()).toContain('firebird_cluster_ready{namespace="prod",cluster="db"} 0');
  });
});
