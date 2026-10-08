/**
 * Tests for the Operator class: Watch setup, event routing, and lifecycle.
 *
 * The K8s client, controller, and health-server are all fully mocked so
 * these tests run without any real cluster connectivity.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

// Capture the watch callback so tests can simulate incoming events.
// capturedEventCallback / capturedDoneCallback belong to the FirebirdCluster watch; every
// watch's callbacks are kept by path.
let capturedEventCallback: ((phase: string, obj: unknown) => void) | null = null;
let capturedDoneCallback: ((err: unknown) => void) | null = null;
const eventCallbacks = new Map<string, (phase: string, obj: unknown) => void>();
const CLUSTERS_PATH = '/apis/firebird.cloudnative-firebird.io/v1/firebirdclusters';
const mockWatchAbort = vi.fn();
const mockWatchFn = vi
  .fn()
  .mockImplementation(
    (
      _path: string,
      _params: object,
      eventCb: (phase: string, obj: unknown) => void,
      doneCb: (err: unknown) => void,
    ) => {
      eventCallbacks.set(_path, eventCb);
      if (_path === CLUSTERS_PATH) {
        capturedEventCallback = eventCb;
        capturedDoneCallback = doneCb;
      }
      return Promise.resolve({ abort: mockWatchAbort });
    },
  );

// Mock the ESM-only @kubernetes/client-node package
vi.mock('@kubernetes/client-node', () => {
  const makeApiClient = vi.fn();
  class KubeConfig {
    loadFromDefault = vi.fn();
    makeApiClient = makeApiClient;
  }
  class Watch {
    watch = mockWatchFn;
  }
  class AppsV1Api {}
  class CoreV1Api {}
  class BatchV1Api {}
  class CustomObjectsApi {}
  class PolicyV1Api {}
  class VersionApi {}
  return { KubeConfig, Watch, AppsV1Api, CoreV1Api, BatchV1Api, CustomObjectsApi, PolicyV1Api, VersionApi };
});

// Mock the controller so we can track reconcile() calls without real K8s
const mockReconcile = vi.fn().mockResolvedValue(undefined);
vi.mock('../src/controllers/firebirdcluster.controller', () => ({
  FirebirdClusterController: vi.fn().mockImplementation(() => ({
    reconcile: mockReconcile,
  })),
}));

// Mock the backup controller so backup watch events can be observed
const mockReconcileBackup = vi.fn().mockResolvedValue(undefined);
const mockReconcileScheduledBackup = vi.fn().mockResolvedValue(undefined);
const mockReconcileRestore = vi.fn().mockResolvedValue(undefined);
vi.mock('../src/controllers/backup.controller', () => ({
  FirebirdBackupController: vi.fn().mockImplementation(() => ({
    reconcileBackup: mockReconcileBackup,
    reconcileScheduledBackup: mockReconcileScheduledBackup,
    reconcileRestore: mockReconcileRestore,
  })),
}));

const mockReconcileUser = vi.fn().mockResolvedValue(undefined);
vi.mock('../src/controllers/user.controller', () => ({
  FirebirdUserController: vi.fn().mockImplementation(() => ({ reconcileUser: mockReconcileUser })),
}));

const mockReconcileRole = vi.fn().mockResolvedValue(undefined);
vi.mock('../src/controllers/role.controller', () => ({
  FirebirdRoleController: vi.fn().mockImplementation(() => ({ reconcileRole: mockReconcileRole })),
}));

// Mock the health server so no real HTTP port is opened during tests
const mockHealthStart = vi.fn();
const mockHealthStop = vi.fn();
const mockHealthSetReady = vi.fn();
vi.mock('../src/utils/health', () => ({
  HealthServer: vi.fn().mockImplementation(() => ({
    start: mockHealthStart,
    stop: mockHealthStop,
    setReady: mockHealthSetReady,
  })),
}));

import { KubeConfig } from '@kubernetes/client-node';
import { INSTANCE_POD_WATCH_SELECTOR, Operator, POD_EVENT_DEBOUNCE_MS } from '../src/operator';
import { makeCluster, makeNamedCluster } from './helpers/factories';

/** Lets pending (coalesced) reconciles run */
const flush = () => new Promise<void>((resolve) => setTimeout(resolve, 0));

// Helpers
function makeOperator(): { operator: Operator; mockKubeConfig: KubeConfig } {
  const mockKubeConfig = new KubeConfig();
  const operator = new Operator(mockKubeConfig);
  return { operator, mockKubeConfig };
}

describe('Operator – lifecycle', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    capturedEventCallback = null;
    capturedDoneCallback = null;
  });

  describe('start()', () => {
    it('starts the health server', async () => {
      const { operator } = makeOperator();
      await operator.start();
      expect(mockHealthStart).toHaveBeenCalledTimes(1);
    });

    it('watches FirebirdCluster, backup, restore and user resources', async () => {
      const { operator } = makeOperator();
      await operator.start();
      expect(mockWatchFn.mock.calls.map((c) => c[0])).toEqual([
        CLUSTERS_PATH,
        '/apis/firebird.cloudnative-firebird.io/v1/firebirdbackups',
        '/apis/firebird.cloudnative-firebird.io/v1/firebirdscheduledbackups',
        '/apis/firebird.cloudnative-firebird.io/v1/firebirdrestores',
        '/apis/firebird.cloudnative-firebird.io/v1/firebirdusers',
        '/apis/firebird.cloudnative-firebird.io/v1/firebirdroles',
        '/api/v1/pods',
      ]);
      // only instance pods, not Job pods
      expect(mockWatchFn.mock.calls[6][1]).toEqual({ labelSelector: INSTANCE_POD_WATCH_SELECTOR });
    });

    it('watches the correct API path for FirebirdCluster resources', async () => {
      const { operator } = makeOperator();
      await operator.start();
      const watchedPath: string = mockWatchFn.mock.calls[0][0];
      expect(watchedPath).toBe(
        '/apis/firebird.cloudnative-firebird.io/v1/firebirdclusters',
      );
    });

    it('marks the operator as ready after the watch is established', async () => {
      const { operator } = makeOperator();
      await operator.start();
      expect(mockHealthSetReady).toHaveBeenCalledWith(true);
    });

    it('starts the health server before setting ready', async () => {
      const callOrder: string[] = [];
      mockHealthStart.mockImplementation(() => callOrder.push('start'));
      mockHealthSetReady.mockImplementation(() => callOrder.push('setReady'));

      const { operator } = makeOperator();
      await operator.start();

      expect(callOrder).toEqual(['start', 'setReady']);
    });
  });

  describe('stop()', () => {
    it('aborts every active watch request', async () => {
      const { operator } = makeOperator();
      await operator.start();
      operator.stop();
      expect(mockWatchAbort).toHaveBeenCalledTimes(7);
    });

    it('marks the operator as not ready', async () => {
      const { operator } = makeOperator();
      await operator.start();

      vi.clearAllMocks();
      operator.stop();

      expect(mockHealthSetReady).toHaveBeenCalledWith(false);
    });

    it('stops the health server', async () => {
      const { operator } = makeOperator();
      await operator.start();

      vi.clearAllMocks();
      operator.stop();

      expect(mockHealthStop).toHaveBeenCalledTimes(1);
    });

    it('does not throw when stop() is called before start()', () => {
      const { operator } = makeOperator();
      expect(() => operator.stop()).not.toThrow();
    });
  });
});

describe('Operator – event handling', () => {
  let activeOperator: Operator | null = null;

  beforeEach(async () => {
    vi.clearAllMocks();
    capturedEventCallback = null;
    capturedDoneCallback = null;
    // Start the operator so watch callbacks are registered
    const { operator } = makeOperator();
    activeOperator = operator;
    await operator.start();
  });

  afterEach(() => {
    activeOperator?.stop();
    activeOperator = null;
  });

  describe('ADDED events', () => {
    it('calls controller.reconcile() when an ADDED event arrives', async () => {
      const cluster = makeCluster();
      capturedEventCallback!('ADDED', cluster);
      await Promise.resolve(); // flush microtasks
      expect(mockReconcile).toHaveBeenCalledTimes(1);
    });

    it('passes the cluster object to controller.reconcile() on ADDED', async () => {
      const cluster = makeCluster();
      capturedEventCallback!('ADDED', cluster);
      await Promise.resolve();
      expect(mockReconcile).toHaveBeenCalledWith(cluster);
    });

    it('reconciles clusters from any namespace on ADDED', async () => {
      const cluster = makeNamedCluster('prod-db', 'production');
      capturedEventCallback!('ADDED', cluster);
      await Promise.resolve();
      expect(mockReconcile).toHaveBeenCalledWith(cluster);
    });
  });

  describe('MODIFIED events', () => {
    it('calls controller.reconcile() when a MODIFIED event arrives', async () => {
      const cluster = makeCluster();
      capturedEventCallback!('MODIFIED', cluster);
      await Promise.resolve();
      expect(mockReconcile).toHaveBeenCalledTimes(1);
    });

    it('passes the cluster object to controller.reconcile() on MODIFIED', async () => {
      const cluster = makeCluster({ instances: 3 });
      capturedEventCallback!('MODIFIED', cluster);
      await Promise.resolve();
      expect(mockReconcile).toHaveBeenCalledWith(cluster);
    });
  });

  describe('DELETED events', () => {
    it('does not call controller.reconcile() on a DELETED event', async () => {
      const cluster = makeCluster();
      capturedEventCallback!('DELETED', cluster);
      await Promise.resolve();
      expect(mockReconcile).not.toHaveBeenCalled();
    });

    it("drops the deleted cluster's metrics", async () => {
      const { metrics } = await import('../src/utils/metrics');
      const cluster = makeNamedCluster('gone', 'production');
      metrics.set('firebird_cluster_ready', 'Ready', { namespace: 'production', cluster: 'gone' }, 1);
      metrics.set('firebird_cluster_ready', 'Ready', { namespace: 'production', cluster: 'kept' }, 1);
      capturedEventCallback!('DELETED', cluster);
      await Promise.resolve();
      expect(metrics.render()).not.toContain('cluster="gone"');
      expect(metrics.render()).toContain('cluster="kept"');
    });
  });

  describe('ERROR events', () => {
    it('does not call controller.reconcile() on an ERROR event', async () => {
      capturedEventCallback!('ERROR', {});
      await Promise.resolve();
      expect(mockReconcile).not.toHaveBeenCalled();
    });
  });

  describe('unknown event phases', () => {
    it('does not call controller.reconcile() for an unknown phase', async () => {
      const cluster = makeCluster();
      capturedEventCallback!('UNKNOWN_PHASE', cluster);
      await Promise.resolve();
      expect(mockReconcile).not.toHaveBeenCalled();
    });
  });

  describe('reconcile error containment', () => {
    it('does not propagate reconcile errors out of the event handler', async () => {
      mockReconcile.mockRejectedValueOnce(new Error('reconcile failed'));
      const cluster = makeCluster();

      // The event handler catches errors internally; firing it should not throw
      expect(() => capturedEventCallback!('ADDED', cluster)).not.toThrow();

      // Allow the async error path to flush without an unhandled rejection
      await new Promise((resolve) => setTimeout(resolve, 0));
    });

    it('still processes subsequent events after a reconcile error', async () => {
      mockReconcile
        .mockRejectedValueOnce(new Error('first fails'))
        .mockResolvedValueOnce(undefined);

      const cluster = makeCluster();
      capturedEventCallback!('ADDED', cluster);
      await new Promise((resolve) => setTimeout(resolve, 0));

      capturedEventCallback!('MODIFIED', cluster);
      await Promise.resolve();

      expect(mockReconcile).toHaveBeenCalledTimes(2);
    });
  });

  describe('watch done callback', () => {
    it('does not throw when the watch stream ends gracefully (err=null)', () => {
      expect(() => capturedDoneCallback!(null)).not.toThrow();
    });

    it('does not throw when the watch stream ends with an error', () => {
      expect(() => capturedDoneCallback!(new Error('stream error'))).not.toThrow();
    });
  });
});

describe('Operator – periodic resync', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('re-reconciles known clusters on the resync interval', async () => {
    const operator = new Operator(new KubeConfig(), 8080, 1000);
    await operator.start();
    const cluster = makeCluster();
    capturedEventCallback!('ADDED', cluster);
    expect(mockReconcile).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(1000);
    expect(mockReconcile).toHaveBeenCalledTimes(2);
    expect(mockReconcile).toHaveBeenLastCalledWith(cluster);
    operator.stop();
  });

  it('uses the latest observed cluster object and forgets deleted clusters', async () => {
    const operator = new Operator(new KubeConfig(), 8080, 1000);
    await operator.start();
    const a = makeNamedCluster('a');
    const aUpdated = makeNamedCluster('a', 'default', { instances: 3 });
    const b = makeNamedCluster('b');
    capturedEventCallback!('ADDED', a);
    capturedEventCallback!('ADDED', b);
    capturedEventCallback!('MODIFIED', aUpdated);
    capturedEventCallback!('DELETED', b);
    await vi.advanceTimersByTimeAsync(0); // let the coalesced reconcile of a finish
    mockReconcile.mockClear();

    await vi.advanceTimersByTimeAsync(1000);
    expect(mockReconcile).toHaveBeenCalledTimes(1);
    expect(mockReconcile).toHaveBeenCalledWith(aUpdated);
    operator.stop();
  });

  it('stops resyncing after stop()', async () => {
    const operator = new Operator(new KubeConfig(), 8080, 1000);
    await operator.start();
    capturedEventCallback!('ADDED', makeCluster());
    operator.stop();
    mockReconcile.mockClear();

    await vi.advanceTimersByTimeAsync(5000);
    expect(mockReconcile).not.toHaveBeenCalled();
  });

  it('disables resync when the interval is 0', async () => {
    const operator = new Operator(new KubeConfig(), 8080, 0);
    await operator.start();
    capturedEventCallback!('ADDED', makeCluster());
    mockReconcile.mockClear();

    await vi.advanceTimersByTimeAsync(60_000);
    expect(mockReconcile).not.toHaveBeenCalled();
    operator.stop();
  });
});

describe('Operator – generation-based event filtering', () => {
  let operator: Operator;

  beforeEach(async () => {
    vi.clearAllMocks();
    operator = new Operator(new KubeConfig(), 8080, 0);
    await operator.start();
  });

  afterEach(() => operator.stop());

  const withGeneration = (generation: number) => {
    const cluster = makeCluster();
    cluster.metadata.generation = generation;
    return cluster;
  };

  it('skips MODIFIED events whose generation was already reconciled (status-only updates)', async () => {
    capturedEventCallback!('ADDED', withGeneration(1));
    await flush();
    capturedEventCallback!('MODIFIED', withGeneration(1));
    await flush();

    expect(mockReconcile).toHaveBeenCalledTimes(1);
  });

  it('reconciles MODIFIED events with a new generation', async () => {
    capturedEventCallback!('ADDED', withGeneration(1));
    await flush();
    capturedEventCallback!('MODIFIED', withGeneration(2));
    await flush();

    expect(mockReconcile).toHaveBeenCalledTimes(2);
  });

  it('does not retry a failed generation from status-only events (resync retries instead)', async () => {
    mockReconcile.mockRejectedValueOnce(new Error('boom'));
    capturedEventCallback!('ADDED', withGeneration(1));
    await new Promise((resolve) => setTimeout(resolve, 0));
    // The Degraded status patch after the failure produces a MODIFIED event
    capturedEventCallback!('MODIFIED', withGeneration(1));
    await flush();

    expect(mockReconcile).toHaveBeenCalledTimes(1);
  });

  it('reconciles a fencedInstances annotation change although the generation is unchanged', async () => {
    capturedEventCallback!('ADDED', withGeneration(1));
    await flush();
    const fenced = withGeneration(1);
    fenced.metadata.annotations = { 'firebird.cloudnative-firebird.io/fencedInstances': '["test-cluster-0"]' };
    capturedEventCallback!('MODIFIED', fenced);
    await flush();
    capturedEventCallback!('MODIFIED', fenced);
    await flush();

    expect(mockReconcile).toHaveBeenCalledTimes(2);
  });

  it('reconciles a targetPrimary annotation change although the generation is unchanged', async () => {
    capturedEventCallback!('ADDED', withGeneration(1));
    await flush();
    const switched = withGeneration(1);
    switched.metadata.annotations = { 'firebird.cloudnative-firebird.io/targetPrimary': 'test-cluster-1' };
    capturedEventCallback!('MODIFIED', switched);
    await flush();

    expect(mockReconcile).toHaveBeenCalledTimes(2);
  });

  it('always reconciles ADDED events (e.g. after a watch restart)', async () => {
    capturedEventCallback!('ADDED', withGeneration(1));
    await flush();
    capturedEventCallback!('ADDED', withGeneration(1));
    await flush();

    expect(mockReconcile).toHaveBeenCalledTimes(2);
  });

  it('forgets the reconciled generation when the cluster is deleted', async () => {
    capturedEventCallback!('ADDED', withGeneration(1));
    await flush();
    capturedEventCallback!('DELETED', withGeneration(1));
    capturedEventCallback!('MODIFIED', withGeneration(1));
    await flush();

    expect(mockReconcile).toHaveBeenCalledTimes(2);
  });
});

describe('Operator – backup resources', () => {
  const backup = (generation: number, name = 'b1') => ({
    apiVersion: 'firebird.cloudnative-firebird.io/v1',
    kind: 'FirebirdBackup',
    metadata: { name, namespace: 'default', generation },
    spec: { clusterName: 'c' },
  });
  const emit = (plural: string, phase: string, obj: unknown) =>
    eventCallbacks.get(`/apis/firebird.cloudnative-firebird.io/v1/${plural}`)!(phase, obj);

  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers();
    eventCallbacks.clear();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('routes each kind to its reconcile method', async () => {
    const operator = new Operator(new KubeConfig(), 8080, 0);
    await operator.start();
    emit('firebirdbackups', 'ADDED', backup(1));
    emit('firebirdscheduledbackups', 'ADDED', backup(1, 's1'));
    emit('firebirdrestores', 'ADDED', backup(1, 'r1'));
    emit('firebirdusers', 'ADDED', backup(1, 'u1'));
    expect(mockReconcileUser).toHaveBeenCalledTimes(1);
    expect(mockReconcileBackup).toHaveBeenCalledTimes(1);
    expect(mockReconcileScheduledBackup).toHaveBeenCalledTimes(1);
    expect(mockReconcileRestore).toHaveBeenCalledTimes(1);
    expect(mockReconcile).not.toHaveBeenCalled();
    operator.stop();
  });

  it('leaves status-only updates to the resync, which follows Job progress', async () => {
    const operator = new Operator(new KubeConfig(), 8080, 1000);
    await operator.start();
    emit('firebirdbackups', 'ADDED', backup(1));
    emit('firebirdbackups', 'MODIFIED', backup(1));
    expect(mockReconcileBackup).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(1000);
    expect(mockReconcileBackup).toHaveBeenCalledTimes(2);

    emit('firebirdbackups', 'DELETED', backup(1));
    mockReconcileBackup.mockClear();
    await vi.advanceTimersByTimeAsync(1000);
    expect(mockReconcileBackup).not.toHaveBeenCalled();
    operator.stop();
  });

  it('reconciles right away when the reconciliationDisabled annotation is added or removed', async () => {
    const operator = new Operator(new KubeConfig(), 8080, 0);
    await operator.start();
    const paused = (value: string) => ({
      ...backup(1),
      metadata: { ...backup(1).metadata, annotations: { 'firebird.cloudnative-firebird.io/reconciliationDisabled': value } },
    });
    emit('firebirdbackups', 'ADDED', backup(1));
    emit('firebirdbackups', 'MODIFIED', paused('true'));
    emit('firebirdbackups', 'MODIFIED', paused('true'));
    emit('firebirdbackups', 'MODIFIED', backup(1));
    expect(mockReconcileBackup).toHaveBeenCalledTimes(3);
    operator.stop();
  });
});

describe('Operator – clone sources', () => {
  let operator: Operator;
  beforeEach(async () => {
    vi.clearAllMocks();
    ({ operator } = makeOperator());
    await operator.start();
  });
  afterEach(() => operator.stop());

  it('reconciles the source when a clone appears, so its NetworkPolicy admits the clone', async () => {
    const source = makeNamedCluster('src', 'prod');
    source.spec.networkPolicy = { enabled: true };
    capturedEventCallback!('ADDED', source);
    await flush();
    mockReconcile.mockClear();

    const clone = makeNamedCluster('copy', 'staging');
    clone.spec.bootstrap = { clone: { sourceCluster: 'src', namespace: 'prod' } };
    capturedEventCallback!('ADDED', clone);
    await flush();
    const reconciled = mockReconcile.mock.calls.map((c) => `${c[0].metadata.namespace}/${c[0].metadata.name}`);
    expect(reconciled).toEqual(expect.arrayContaining(['prod/src', 'staging/copy']));

    // not again on later events of the clone
    mockReconcile.mockClear();
    capturedEventCallback!('MODIFIED', { ...clone, metadata: { ...clone.metadata, generation: 2 } });
    await flush();
    expect(mockReconcile.mock.calls.map((c) => c[0].metadata.name)).toEqual(['copy']);
  });
});

describe('Operator – reconcile serialization', () => {
  beforeEach(() => vi.clearAllMocks());

  it('never reconciles a cluster concurrently and coalesces requests into one run with the latest object', async () => {
    const operator = new Operator(new KubeConfig(), 8080, 0);
    await operator.start();
    let active = 0;
    let maxActive = 0;
    const seen: unknown[] = [];
    mockReconcile.mockImplementation(async (cluster: unknown) => {
      active++;
      maxActive = Math.max(maxActive, active);
      seen.push(cluster);
      await new Promise((resolve) => setTimeout(resolve, 5));
      active--;
    });
    const v = (generation: number) => {
      const c = makeCluster();
      c.metadata.generation = generation;
      return c;
    };
    capturedEventCallback!('ADDED', v(1));
    capturedEventCallback!('MODIFIED', v(2));
    capturedEventCallback!('MODIFIED', v(3));
    await new Promise((resolve) => setTimeout(resolve, 30));

    expect(maxActive).toBe(1);
    expect(seen).toHaveLength(2);
    expect((seen[1] as { metadata: { generation: number } }).metadata.generation).toBe(3);
    mockReconcile.mockReset();
    mockReconcile.mockResolvedValue(undefined);
    operator.stop();
  });
});

describe('Operator – instance pod watch', () => {
  const PODS_PATH = '/api/v1/pods';
  let operator: Operator;

  const pod = (
    name: string,
    options: { ready?: boolean; cluster?: string; revision?: string; annotations?: Record<string, string>; deleting?: boolean } = {},
  ) => ({
    metadata: {
      name,
      namespace: 'default',
      uid: `uid-${name}`,
      labels: {
        'firebird.cloudnative-firebird.io/cluster': options.cluster ?? 'test-cluster',
        'controller-revision-hash': options.revision ?? 'rev-1',
      },
      annotations: options.annotations ?? {},
      ...(options.deleting ? { deletionTimestamp: '2026-01-01T00:00:00Z' } : {}),
    },
    status: { phase: 'Running', conditions: [{ type: 'Ready', status: options.ready === false ? 'False' : 'True' }] },
  });
  const emitPod = (phase: string, obj: unknown) => eventCallbacks.get(PODS_PATH)!(phase, obj);

  beforeEach(async () => {
    vi.clearAllMocks();
    vi.useFakeTimers();
    ({ operator } = makeOperator());
    await operator.start();
    capturedEventCallback!('ADDED', makeCluster());
    await vi.advanceTimersByTimeAsync(0);
    mockReconcile.mockClear();
  });

  afterEach(() => {
    operator.stop();
    vi.useRealTimers();
  });

  it('reconciles the cluster when an instance becomes ready or unready, once per burst', async () => {
    emitPod('ADDED', pod('test-cluster-0', { ready: false }));
    emitPod('ADDED', pod('test-cluster-1', { ready: false }));
    await vi.advanceTimersByTimeAsync(POD_EVENT_DEBOUNCE_MS);
    expect(mockReconcile).toHaveBeenCalledTimes(1);

    emitPod('MODIFIED', pod('test-cluster-1'));
    await vi.advanceTimersByTimeAsync(POD_EVENT_DEBOUNCE_MS);
    expect(mockReconcile).toHaveBeenCalledTimes(2);
    expect(mockReconcile).toHaveBeenLastCalledWith(expect.objectContaining({ metadata: expect.objectContaining({ name: 'test-cluster' }) }));
  });

  it("ignores the operator's own label and annotation patches", async () => {
    emitPod('ADDED', pod('test-cluster-1'));
    await vi.advanceTimersByTimeAsync(POD_EVENT_DEBOUNCE_MS);
    mockReconcile.mockClear();

    const patched = pod('test-cluster-1', {
      annotations: { 'firebird.cloudnative-firebird.io/replication-lag-seconds': '4' },
    });
    patched.metadata.labels['firebird.cloudnative-firebird.io/role'] = 'replica';
    emitPod('MODIFIED', patched);
    await vi.advanceTimersByTimeAsync(POD_EVENT_DEBOUNCE_MS);
    expect(mockReconcile).not.toHaveBeenCalled();
  });

  it('reacts to termination, deletion, a new revision and a reseed request', async () => {
    emitPod('ADDED', pod('test-cluster-1'));
    await vi.advanceTimersByTimeAsync(POD_EVENT_DEBOUNCE_MS);
    const changes = [
      ['MODIFIED', pod('test-cluster-1', { annotations: { 'firebird.cloudnative-firebird.io/reseed': 'true' } })],
      ['MODIFIED', pod('test-cluster-1', { deleting: true })],
      ['DELETED', pod('test-cluster-1', { deleting: true })],
      ['ADDED', pod('test-cluster-1', { ready: false, revision: 'rev-2' })],
    ] as const;
    for (const [phase, obj] of changes) {
      mockReconcile.mockClear();
      emitPod(phase, obj);
      await vi.advanceTimersByTimeAsync(POD_EVENT_DEBOUNCE_MS);
      expect(mockReconcile, phase).toHaveBeenCalledTimes(1);
    }
  });

  it('ignores pods of unknown clusters and pods that are not instances', async () => {
    emitPod('ADDED', pod('other-0', { cluster: 'other' }));
    emitPod('ADDED', pod('test-cluster-backup-x7k2p', {}));
    emitPod('ERROR', { kind: 'Status' });
    await vi.advanceTimersByTimeAsync(POD_EVENT_DEBOUNCE_MS);
    expect(mockReconcile).not.toHaveBeenCalled();
  });
});
