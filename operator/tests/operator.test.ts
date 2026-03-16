/**
 * Tests for the Operator class: Watch setup, event routing, and lifecycle.
 *
 * The K8s client, controller, and health-server are all fully mocked so
 * these tests run without any real cluster connectivity.
 */

// ---------------------------------------------------------------------------
// Module mocks – must be declared before any imports
// ---------------------------------------------------------------------------

// Capture the watch callback so tests can simulate incoming events.
let capturedEventCallback: ((phase: string, obj: unknown) => void) | null = null;
let capturedDoneCallback: ((err: unknown) => void) | null = null;
const mockWatchAbort = jest.fn();
const mockWatchFn = jest
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
jest.mock('@kubernetes/client-node', () => {
  const makeApiClient = jest.fn();
  class KubeConfig {
    loadFromDefault = jest.fn();
    makeApiClient = makeApiClient;
  }
  class Watch {
    watch = mockWatchFn;
  }
  class AppsV1Api {}
  class CoreV1Api {}
  class CustomObjectsApi {}
  return { KubeConfig, Watch, AppsV1Api, CoreV1Api, CustomObjectsApi };
});

// Mock the controller so we can track reconcile() calls without real K8s
const mockReconcile = jest.fn().mockResolvedValue(undefined);
jest.mock('../src/controllers/firebirdcluster.controller', () => ({
  FirebirdClusterController: jest.fn().mockImplementation(() => ({
    reconcile: mockReconcile,
  })),
}));

// Mock the health server so no real HTTP port is opened during tests
const mockHealthStart = jest.fn();
const mockHealthStop = jest.fn();
const mockHealthSetReady = jest.fn();
jest.mock('../src/utils/health', () => ({
  HealthServer: jest.fn().mockImplementation(() => ({
    start: mockHealthStart,
    stop: mockHealthStop,
    setReady: mockHealthSetReady,
  })),
}));

// ---------------------------------------------------------------------------
// Imports (after mocks are hoisted)
// ---------------------------------------------------------------------------
import { KubeConfig } from '@kubernetes/client-node';
import { Operator } from '../src/operator';
import { makeCluster, makeNamedCluster } from './helpers/factories';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function makeOperator(): { operator: Operator; mockKubeConfig: KubeConfig } {
  const mockKubeConfig = new KubeConfig();
  const operator = new Operator(mockKubeConfig);
  return { operator, mockKubeConfig };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
describe('Operator – lifecycle', () => {
  beforeEach(() => {
    jest.clearAllMocks();
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

      jest.clearAllMocks();
      operator.stop();

      expect(mockHealthSetReady).toHaveBeenCalledWith(false);
    });

    it('stops the health server', async () => {
      const { operator } = makeOperator();
      await operator.start();

      jest.clearAllMocks();
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
  beforeEach(async () => {
    jest.clearAllMocks();
    capturedEventCallback = null;
    capturedDoneCallback = null;
    // Start the operator so watch callbacks are registered
    const { operator } = makeOperator();
    await operator.start();
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
