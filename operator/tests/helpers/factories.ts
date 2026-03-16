/**
 * Shared test data factories for FirebirdCluster fixtures.
 *
 * These helpers centralise cluster fixture construction so that individual
 * test files can build rich, varied test data without duplicating the
 * baseline spec everywhere.
 */
import { FirebirdCluster } from '../../src/types';

/** Build a minimal FirebirdCluster fixture, optionally overriding spec fields */
export const makeCluster = (overrides: Partial<FirebirdCluster['spec']> = {}): FirebirdCluster => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdCluster',
  metadata: {
    name: 'test-cluster',
    namespace: 'default',
    uid: 'test-uid-1234',
  },
  spec: {
    instances: 1,
    storage: { size: '1Gi' },
    ...overrides,
  },
});

/** Build a cluster with replication enabled (defaults to async mode) */
export const makeClusterWithReplication = (mode?: 'sync' | 'async'): FirebirdCluster =>
  makeCluster({ replication: { enabled: true, mode } });

/** Build a cluster with CPU/memory resource requests and limits */
export const makeClusterWithResources = (): FirebirdCluster =>
  makeCluster({
    resources: {
      requests: { cpu: '100m', memory: '256Mi' },
      limits: { cpu: '500m', memory: '512Mi' },
    },
  });

/** Build a cluster that references a Kubernetes Secret for the SYSDBA password */
export const makeClusterWithSecret = (secretName: string): FirebirdCluster =>
  makeCluster({ superuserSecret: { name: secretName } });

/**
 * Build a cluster with a custom name/namespace, useful when tests need
 * to distinguish multiple clusters or exercise non-default namespaces.
 */
export const makeNamedCluster = (
  name: string,
  namespace = 'default',
  overrides: Partial<FirebirdCluster['spec']> = {},
): FirebirdCluster => ({
  ...makeCluster(overrides),
  metadata: { name, namespace, uid: `uid-${name}` },
});

/**
 * Kubernetes HTTP 404 error shape emitted by @kubernetes/client-node when a
 * resource is not found.  Reuse this instead of constructing it per-test.
 */
export const notFoundError = Object.assign(new Error('Not Found'), { statusCode: 404 });
