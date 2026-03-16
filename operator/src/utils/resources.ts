import { V1StatefulSet, V1Service } from '@kubernetes/client-node';
import {
  FirebirdCluster,
  DEFAULT_FIREBIRD_IMAGE,
  API_GROUP,
  RESOURCE_KIND,
} from '../types';

/** The label key used to identify cluster resources */
export const CLUSTER_LABEL = `${API_GROUP}/cluster`;

/** Returns the set of labels to apply to all cluster resources */
export function clusterLabels(name: string): Record<string, string> {
  return {
    'app.kubernetes.io/name': 'firebird',
    'app.kubernetes.io/component': 'database',
    'app.kubernetes.io/managed-by': 'cloudnative-firebird-operator',
    [CLUSTER_LABEL]: name,
  };
}

/**
 * Builds the StatefulSet for a FirebirdCluster.
 */
export function buildStatefulSet(cluster: FirebirdCluster): V1StatefulSet {
  const { name, namespace = 'default' } = cluster.metadata;
  const spec = cluster.spec;
  const image = spec.imageName ?? DEFAULT_FIREBIRD_IMAGE;
  const labels = clusterLabels(name);
  const storageClassName = spec.storage.storageClass;

  const env = [
    // Enable Firebird SuperUser password from secret or default
    ...(spec.superuserSecret
      ? [
          {
            name: 'ISC_PASSWORD',
            valueFrom: {
              secretKeyRef: {
                name: spec.superuserSecret.name,
                key: 'password',
              },
            },
          },
        ]
      : [{ name: 'ISC_PASSWORD', value: 'masterkey' }]),
    // Additional env vars from spec
    ...(spec.env ?? []),
  ];

  const statefulSet: V1StatefulSet = {
    apiVersion: 'apps/v1',
    kind: 'StatefulSet',
    metadata: {
      name,
      namespace,
      labels,
      ownerReferences: [
        {
          apiVersion: `${API_GROUP}/v1`,
          kind: RESOURCE_KIND,
          name: cluster.metadata.name,
          uid: cluster.metadata.uid ?? '',
          controller: true,
          blockOwnerDeletion: true,
        },
      ],
    },
    spec: {
      // Must reference the headless service for stable pod DNS
      serviceName: `${name}-headless`,
      replicas: spec.instances,
      selector: {
        matchLabels: labels,
      },
      template: {
        metadata: {
          labels,
        },
        spec: {
          securityContext: {
            fsGroup: 999,
          },
          containers: [
            {
              name: 'firebird',
              image,
              ports: [
                {
                  name: 'firebird',
                  containerPort: 3050,
                  protocol: 'TCP',
                },
              ],
              env,
              resources: spec.resources,
              volumeMounts: [
                {
                  name: 'firebird-data',
                  mountPath: '/firebird/data',
                },
              ],
              livenessProbe: {
                tcpSocket: { port: 3050 },
                initialDelaySeconds: 30,
                periodSeconds: 10,
                failureThreshold: 5,
              },
              readinessProbe: {
                tcpSocket: { port: 3050 },
                initialDelaySeconds: 15,
                periodSeconds: 5,
                failureThreshold: 3,
              },
            },
          ],
        },
      },
      volumeClaimTemplates: [
        {
          metadata: {
            name: 'firebird-data',
            labels,
          },
          spec: {
            accessModes: ['ReadWriteOnce'],
            ...(storageClassName ? { storageClassName } : {}),
            resources: {
              requests: {
                storage: spec.storage.size,
              },
            },
          },
        },
      ],
    },
  };

  return statefulSet;
}

/**
 * Builds the primary Service for a FirebirdCluster.
 * This is the read-write service that clients connect to.
 */
export function buildService(cluster: FirebirdCluster): V1Service {
  const { name, namespace = 'default' } = cluster.metadata;
  const labels = clusterLabels(name);

  const service: V1Service = {
    apiVersion: 'v1',
    kind: 'Service',
    metadata: {
      name,
      namespace,
      labels,
      ownerReferences: [
        {
          apiVersion: `${API_GROUP}/v1`,
          kind: RESOURCE_KIND,
          name: cluster.metadata.name,
          uid: cluster.metadata.uid ?? '',
          controller: true,
          blockOwnerDeletion: true,
        },
      ],
    },
    spec: {
      type: 'ClusterIP',
      selector: labels,
      ports: [
        {
          name: 'firebird',
          port: 3050,
          targetPort: 3050,
          protocol: 'TCP',
        },
      ],
    },
  };

  return service;
}

/**
 * Builds the headless Service used by the StatefulSet for pod DNS discovery.
 */
export function buildHeadlessService(cluster: FirebirdCluster): V1Service {
  const { name, namespace = 'default' } = cluster.metadata;
  const labels = clusterLabels(name);

  const service: V1Service = {
    apiVersion: 'v1',
    kind: 'Service',
    metadata: {
      name: `${name}-headless`,
      namespace,
      labels,
      annotations: {
        'service.alpha.kubernetes.io/tolerate-unready-endpoints': 'true',
      },
      ownerReferences: [
        {
          apiVersion: `${API_GROUP}/v1`,
          kind: RESOURCE_KIND,
          name: cluster.metadata.name,
          uid: cluster.metadata.uid ?? '',
          controller: true,
          blockOwnerDeletion: true,
        },
      ],
    },
    spec: {
      clusterIP: 'None',
      publishNotReadyAddresses: true,
      selector: labels,
      ports: [
        {
          name: 'firebird',
          port: 3050,
          targetPort: 3050,
          protocol: 'TCP',
        },
      ],
    },
  };

  return service;
}

/**
 * Checks if two StatefulSet specs are semantically equal
 * (ignoring server-set fields like resourceVersion).
 */
export function statefulSetNeedsUpdate(
  existing: V1StatefulSet,
  desired: V1StatefulSet,
): boolean {
  const existingSpec = existing.spec;
  const desiredSpec = desired.spec;

  if (!existingSpec || !desiredSpec) return true;

  if (existingSpec.replicas !== desiredSpec.replicas) return true;

  const existingContainer = existingSpec.template?.spec?.containers?.[0];
  const desiredContainer = desiredSpec.template?.spec?.containers?.[0];

  if (!existingContainer || !desiredContainer) return true;
  if (existingContainer.image !== desiredContainer.image) return true;

  return false;
}
