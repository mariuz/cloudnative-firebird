/**
 * Tests for the Operator class: Watch setup, event routing, and lifecycle.
 *
 * The K8s client, controller, and health-server are all fully mocked so
 * these tests run without any real cluster connectivity.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

// Capture the watch callback so tests can simulate incoming events.
let capturedEventCallback: ((phase: string, obj: unknown) => void) | null = null;
let capturedDoneCallback: ((err: unknown) => void) | null = null;
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
      capturedEventCallback = eventCb;
      capturedDoneCallback = doneCb;
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
  return { KubeConfig, Watch, AppsV1Api, CoreV1Api, BatchV1Api, CustomObjectsApi, PolicyV1Api };
});

// Mock the controller so we can track reconcile() calls without real K8s
const mockReconcile = vi.fn().mockResolvedValue(undefined);
vi.mock('../src/controllers/firebirdcluster.controller', () => ({
  FirebirdClusterController: vi.fn().mockImplementation(() => ({
    reconcile: mockReconcile,
  })),
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
import { Operator } from '../src/operator';
import { makeCluster, makeNamedCluster } from './helpers/factories';

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

    it('calls watch.watch() to begin watching FirebirdCluster resources', async () => {
      const { operator } = makeOperator();
      await operator.start();
      expect(mockWatchFn).toHaveBeenCalledTimes(1);
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
    it('aborts the active watch request', async () => {
      const { operator } = makeOperator();
      await operator.start();
      operator.stop();
      expect(mockWatchAbort).toHaveBeenCalledTimes(1);
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
