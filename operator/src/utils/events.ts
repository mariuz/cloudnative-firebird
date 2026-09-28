import { randomBytes } from 'crypto';
import { CoreV1Api, CoreV1Event, PatchStrategy, setHeaderOptions } from '@kubernetes/client-node';
import { logger } from './logger';

/**
 * Kubernetes Events for operator actions (CloudNativePG 1.29 / 1.30): switchover and failover,
 * fencing, re-seeding, rolling updates, backups, restores and users. Shown by
 * `kubectl describe` and `kubectl get events`.
 *
 * Events are recorded like client-go's EventRecorder: repeating the same event on the same object
 * increments its count instead of creating a new one. Recording never fails a reconcile.
 */

/** Source component of the events */
export const EVENT_SOURCE = 'cloudnative-firebird-operator';

/** How long a repeated event is aggregated into the previous one (client-go uses 10 minutes) */
export const EVENT_AGGREGATION_MS = 10 * 60 * 1000;

const MERGE_PATCH = setHeaderOptions('Content-Type', PatchStrategy.MergePatch);
const MAX_TRACKED_EVENTS = 1000;

export type EventType = 'Normal' | 'Warning';

/** The object an event is about (a custom resource of this operator) */
export interface EventObject {
  apiVersion?: string;
  kind?: string;
  metadata: { name?: string; namespace?: string; uid?: string; resourceVersion?: string };
}

/** Event reasons, one per transition */
export const EventReason = {
  SwitchoverStarted: 'SwitchoverStarted',
  SwitchoverPromoting: 'SwitchoverPromoting',
  SwitchoverCompleted: 'SwitchoverCompleted',
  SwitchoverFailed: 'SwitchoverFailed',
  PrimaryNotReady: 'PrimaryNotReady',
  FailoverStarted: 'FailoverStarted',
  FailoverCancelled: 'FailoverCancelled',
  FailingOver: 'FailingOver',
  FailoverCompleted: 'FailoverCompleted',
  FailoverFailed: 'FailoverFailed',
  InstanceFenced: 'InstanceFenced',
  InstanceUnfenced: 'InstanceUnfenced',
  FencingFailed: 'FencingFailed',
  ReseedStarted: 'ReseedStarted',
  ReseedCompleted: 'ReseedCompleted',
  RollingUpdate: 'RollingUpdate',
  RollingUpdateCompleted: 'RollingUpdateCompleted',
  ReplicaLagging: 'ReplicaLagging',
  VolumeResizing: 'VolumeResizing',
  VolumeResizeFailed: 'VolumeResizeFailed',
  ReconcileFailed: 'ReconcileFailed',
  BackupStarted: 'BackupStarted',
  BackupCompleted: 'BackupCompleted',
  BackupFailed: 'BackupFailed',
  RestoreStarted: 'RestoreStarted',
  RestoreCompleted: 'RestoreCompleted',
  RestoreFailed: 'RestoreFailed',
  UserApplied: 'UserApplied',
  UserFailed: 'UserFailed',
  UserDropped: 'UserDropped',
} as const;

export class EventRecorder {
  private readonly recent = new Map<string, { name: string; namespace: string; count: number; last: number }>();

  constructor(
    private readonly coreApi: CoreV1Api,
    private readonly now: () => number = Date.now,
  ) {}

  /** Records an event on the object; errors are logged, never thrown */
  async record(object: EventObject, type: EventType, reason: string, message: string): Promise<void> {
    const { name, namespace = 'default', uid } = object.metadata;
    if (!name) return;
    const key = [namespace, object.kind, name, uid, type, reason, message].join('\u0000');
    const now = this.now();
    const timestamp = new Date(now);
    try {
      const previous = this.recent.get(key);
      if (previous && now - previous.last < EVENT_AGGREGATION_MS) {
        try {
          await this.coreApi.patchNamespacedEvent(
            {
              name: previous.name,
              namespace: previous.namespace,
              body: { count: previous.count + 1, lastTimestamp: timestamp.toISOString() },
            },
            MERGE_PATCH,
          );
          this.remember(key, { ...previous, count: previous.count + 1, last: now });
          return;
        } catch {
          // expired or deleted: record a new event
        }
      }
      const body: CoreV1Event = {
        metadata: { name: `${name}.${now.toString(16)}${randomBytes(4).toString('hex')}`, namespace },
        involvedObject: {
          apiVersion: object.apiVersion,
          kind: object.kind,
          name,
          namespace,
          uid,
        },
        type,
        reason,
        message: message.length > 1024 ? `${message.slice(0, 1021)}...` : message,
        source: { component: EVENT_SOURCE },
        reportingComponent: EVENT_SOURCE,
        firstTimestamp: timestamp,
        lastTimestamp: timestamp,
        count: 1,
      };
      const created = await this.coreApi.createNamespacedEvent({ namespace, body });
      this.remember(key, { name: created?.metadata?.name ?? body.metadata.name!, namespace, count: 1, last: now });
    } catch (err) {
      logger.debug({ err, reason, object: name }, 'Failed to record event');
    }
  }

  private remember(key: string, entry: { name: string; namespace: string; count: number; last: number }): void {
    this.recent.delete(key);
    this.recent.set(key, entry);
    if (this.recent.size > MAX_TRACKED_EVENTS) {
      this.recent.delete(this.recent.keys().next().value!);
    }
  }
}
