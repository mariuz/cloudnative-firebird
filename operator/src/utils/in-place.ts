import { V1Pod, V1PodSpec, V1PodTemplateSpec, V1ResourceRequirements } from '@kubernetes/client-node';
import { API_GROUP } from '../types';
import { parseQuantity } from './storage';

/**
 * In-place updates of the instance pods (CloudNativePG applies what it can without a restart).
 * Firebird reads firebird.conf when the server starts, so configuration changes restart the
 * instances; container resources, however, can be resized in place (Kubernetes in-place pod
 * resize, GA in 1.35). When the only difference between a pod's StatefulSet revision and the new
 * one is container resources, the rolling update resizes the running pod and relabels it with the
 * new revision instead of restarting it. Not in place (restarted as before):
 *
 * - any other template change (image, environment, configuration, init containers, ...);
 * - a lower memory limit (the running server may use more than the new limit);
 * - a change of QoS class, which Kubernetes does not resize.
 */

/** Pod annotation: the revision a resize was requested for, or "failed:<revision>" */
export const RESIZE_ANNOTATION = `${API_GROUP}/resize-revision`;
/** A resize not applied within this time is given up: the pod is restarted instead */
export const RESIZE_TIMEOUT_SECONDS = 300;

export interface ContainerResize {
  name: string;
  resources: V1ResourceRequirements;
}

const RESOURCE_NAMES = ['cpu', 'memory'] as const;

function qosClass(spec: V1PodSpec | undefined): 'Guaranteed' | 'Burstable' | 'BestEffort' {
  const containers = spec?.containers ?? [];
  let any = false;
  let guaranteed = containers.length > 0;
  for (const c of containers) {
    const requests = c.resources?.requests ?? {};
    const limits = c.resources?.limits ?? {};
    if (Object.keys(requests).length || Object.keys(limits).length) any = true;
    for (const r of RESOURCE_NAMES) {
      const limit = limits[r];
      const request = requests[r] ?? limit;
      if (!limit || parseQuantity(limit) !== parseQuantity(request)) guaranteed = false;
    }
  }
  if (!any) return 'BestEffort';
  return guaranteed ? 'Guaranteed' : 'Burstable';
}

/** The template without what can be resized in place (container resources) */
function withoutResources(template: V1PodTemplateSpec): unknown {
  return {
    ...template,
    spec: { ...template.spec, containers: (template.spec?.containers ?? []).map((c) => ({ ...c, resources: undefined })) },
  };
}

/**
 * The containers to resize to go from one pod template to the other, or undefined when that
 * cannot be done in place (see above). An empty list: the templates are the same.
 */
export function inPlaceResize(from: V1PodTemplateSpec, to: V1PodTemplateSpec): ContainerResize[] | undefined {
  if (JSON.stringify(withoutResources(from)) !== JSON.stringify(withoutResources(to))) return undefined;
  if (qosClass(from.spec) !== qosClass(to.spec)) return undefined;
  const resizes: ContainerResize[] = [];
  for (const target of to.spec?.containers ?? []) {
    const source = from.spec?.containers.find((c) => c.name === target.name);
    if (JSON.stringify(source?.resources ?? {}) === JSON.stringify(target.resources ?? {})) continue;
    const oldMemory = parseQuantity(source?.resources?.limits?.memory);
    const newMemory = parseQuantity(target.resources?.limits?.memory);
    // a lower (or newly set) memory limit could be below what the server uses
    if (newMemory !== null && (oldMemory === null || newMemory < oldMemory)) return undefined;
    resizes.push({ name: target.name, resources: target.resources ?? {} });
  }
  return resizes;
}

function sameQuantities(actual: Record<string, string> | undefined, desired: Record<string, string> | undefined): boolean {
  return RESOURCE_NAMES.every((r) => parseQuantity(actual?.[r]) === parseQuantity(desired?.[r]));
}

/** Whether the kubelet runs the pod's containers with the requested resources */
export function resizeApplied(pod: V1Pod, resizes: ContainerResize[]): boolean {
  return resizes.every((r) => {
    const status = pod.status?.containerStatuses?.find((s) => s.name === r.name);
    const actual = status?.resources;
    return Boolean(actual) && sameQuantities(actual!.requests, r.resources.requests) && sameQuantities(actual!.limits, r.resources.limits);
  });
}

/** Whether the kubelet refused the resize for good (PodResizePending, reason Infeasible) */
export function resizeInfeasible(pod: V1Pod): boolean {
  const conditions = pod.status?.conditions ?? [];
  return conditions.some((c) => c.type === 'PodResizePending' && c.status === 'True' && c.reason === 'Infeasible');
}
