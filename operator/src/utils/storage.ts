import { V1PersistentVolumeClaim } from '@kubernetes/client-node';
import { VolumeStatus } from '../types';

const BINARY_SUFFIXES: Record<string, number> = {
  Ki: 2 ** 10,
  Mi: 2 ** 20,
  Gi: 2 ** 30,
  Ti: 2 ** 40,
  Pi: 2 ** 50,
  Ei: 2 ** 60,
};

const DECIMAL_SUFFIXES: Record<string, number> = {
  m: 1e-3,
  '': 1,
  k: 1e3,
  M: 1e6,
  G: 1e9,
  T: 1e12,
  P: 1e15,
  E: 1e18,
};

const QUANTITY_PATTERN = /^([+]?[0-9]*\.?[0-9]+)(?:([eE][+-]?[0-9]+)|(Ki|Mi|Gi|Ti|Pi|Ei|m|k|M|G|T|P|E))?$/;

/**
 * Parses a Kubernetes resource quantity (e.g. "10Gi", "500M", "1e9") into bytes.
 * Returns null when the quantity is not valid.
 */
export function parseQuantity(quantity: string | undefined): number | null {
  if (!quantity) return null;
  const match = QUANTITY_PATTERN.exec(quantity.trim());
  if (!match) return null;

  const [, number, exponent, suffix] = match;
  const value = parseFloat(number);
  if (exponent) return value * 10 ** parseInt(exponent.slice(1), 10);
  if (suffix && suffix in BINARY_SUFFIXES) return value * BINARY_SUFFIXES[suffix];
  return value * DECIMAL_SUFFIXES[suffix ?? ''];
}

/** Outcome of comparing an instance PVC against the desired storage size */
export interface VolumeExpansionPlan {
  /** Whether the PVC storage request must be patched to the desired size */
  expand: boolean;
  /** Volume status to report for this PVC */
  status: VolumeStatus;
}

/**
 * Decides whether a PVC needs expanding to reach the desired size and derives its status.
 * PVCs can only grow, so a smaller desired size is reported as ShrinkRejected.
 */
export function planVolumeExpansion(
  pvc: V1PersistentVolumeClaim,
  desiredSize: string,
): VolumeExpansionPlan {
  const name = pvc.metadata?.name ?? '';
  const requestedSize = pvc.spec?.resources?.requests?.storage;
  const capacity = pvc.status?.capacity?.storage;
  const desired = parseQuantity(desiredSize);
  const requested = parseQuantity(requestedSize);
  const actual = parseQuantity(capacity);
  const base = {
    name,
    ...(requestedSize ? { requestedSize } : {}),
    ...(capacity ? { capacity } : {}),
  };

  if (desired === null || requested === null) {
    return { expand: false, status: { ...base, state: 'Ready' } };
  }

  if (desired > requested) {
    return {
      expand: true,
      status: {
        ...base,
        requestedSize: desiredSize,
        state: 'Resizing',
        message: `Expanding volume from ${requestedSize} to ${desiredSize}`,
      },
    };
  }

  if (desired < requested) {
    return {
      expand: false,
      status: {
        ...base,
        state: 'ShrinkRejected',
        message: `Cannot shrink volume from ${requestedSize} to ${desiredSize}`,
      },
    };
  }

  const resizeCondition = pvc.status?.conditions?.find(
    (c) => (c.type === 'Resizing' || c.type === 'FileSystemResizePending') && c.status === 'True',
  );
  if (resizeCondition || (actual !== null && actual < requested)) {
    return {
      expand: false,
      status: {
        ...base,
        state: 'Resizing',
        message: resizeCondition?.type === 'FileSystemResizePending'
          ? 'Waiting for pod restart to complete file system resize'
          : 'Volume expansion in progress',
      },
    };
  }

  return { expand: false, status: { ...base, state: 'Ready' } };
}
