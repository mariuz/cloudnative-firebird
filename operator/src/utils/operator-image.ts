import { CoreV1Api, KubeConfig } from '@kubernetes/client-node';
import { logger } from './logger';

/**
 * The operator's own image, which instance pods and Jobs run as the segment TLS proxy
 * (segment-tls.ts): OPERATOR_IMAGE when set, otherwise read from the operator's pod at startup
 * (POD_NAME, OPERATOR_NAMESPACE), so it always matches the running operator.
 */
export const DEFAULT_OPERATOR_IMAGE = 'ghcr.io/mariuz/cloudnative-firebird:latest';

let image = process.env.OPERATOR_IMAGE?.trim() || '';

export function operatorImage(): string {
  return image || DEFAULT_OPERATOR_IMAGE;
}

/** Tests */
export function setOperatorImage(value: string): void {
  image = value;
}

/** Reads the image of the operator container from the operator's own pod (unless OPERATOR_IMAGE is set) */
export async function discoverOperatorImage(kubeConfig: KubeConfig, env = process.env): Promise<string> {
  if (image) return image;
  const name = env.POD_NAME;
  const namespace = env.OPERATOR_NAMESPACE;
  if (!name || !namespace) return operatorImage();
  try {
    const pod = await kubeConfig.makeApiClient(CoreV1Api).readNamespacedPod({ name, namespace });
    const containers = pod.spec?.containers ?? [];
    const own = containers.find((c) => c.name === 'operator') ?? containers[0];
    if (own?.image) image = own.image;
  } catch (err) {
    logger.warn({ err }, `Could not read the operator's own image; segment TLS uses ${operatorImage()}`);
  }
  return operatorImage();
}
