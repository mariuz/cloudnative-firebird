import { describe, it, expect } from 'vitest';
import { V1PodSpec } from '@kubernetes/client-node';
import { buildBackupJob, buildJournalArchiveCronJob } from '../src/utils/backup';
import {
  FIREBIRD_UID,
  INSTANCE_CAPABILITIES,
  buildAutoSweepCronJob,
  buildStatefulSet,
  statefulSetNeedsUpdate,
} from '../src/utils/resources';
import { buildFencingJob } from '../src/utils/fencing';
import { buildFailoverJob } from '../src/utils/switchover';
import { FirebirdBackup, FirebirdCluster } from '../src/types';

const cluster = (spec: Partial<FirebirdCluster['spec']> = {}): FirebirdCluster => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdCluster',
  metadata: { name: 'db', namespace: 'default', uid: 'c' },
  spec: {
    instances: 2,
    storage: { size: '1Gi' },
    replication: { enabled: true, journalArchiveS3: { bucket: 'b' } },
    bootstrap: { recovery: { s3: { bucket: 'b' }, sourcePath: 'x.fbk' } },
    autoSweep: { enabled: true },
    ...spec,
  },
});
const allContainers = (spec: V1PodSpec) => [...(spec.initContainers ?? []), ...spec.containers];

describe('instance pod security', () => {
  it('drops every capability except CHOWN, DAC_OVERRIDE and FOWNER, and uses RuntimeDefault seccomp', () => {
    const pod = buildStatefulSet(cluster()).spec!.template.spec!;
    expect(pod.securityContext).toEqual({ fsGroup: 999, seccompProfile: { type: 'RuntimeDefault' } });
    const containers = allContainers(pod);
    expect(containers.map((c) => c.name)).toEqual(
      expect.arrayContaining(['security-db-init', 'bootstrap-download', 'replication-init', 'firebird', 'segment-server']),
    );
    for (const c of containers) {
      expect(c.securityContext, c.name).toEqual({
        allowPrivilegeEscalation: false,
        capabilities: { drop: ['ALL'], add: INSTANCE_CAPABILITIES },
      });
    }
  });

  it('merges spec.podSecurityContext and spec.securityContext over the defaults', () => {
    const pod = buildStatefulSet(
      cluster({
        podSecurityContext: { fsGroup: 2000, supplementalGroups: [3000] },
        securityContext: { readOnlyRootFilesystem: false, capabilities: { drop: ['ALL'], add: ['DAC_OVERRIDE'] } },
      }),
    ).spec!.template.spec!;
    expect(pod.securityContext).toEqual({ fsGroup: 2000, supplementalGroups: [3000], seccompProfile: { type: 'RuntimeDefault' } });
    expect(pod.containers[0].securityContext).toEqual({
      allowPrivilegeEscalation: false,
      readOnlyRootFilesystem: false,
      capabilities: { drop: ['ALL'], add: ['DAC_OVERRIDE'] },
    });
  });

  it('rolls the instances when a security context changes', () => {
    const existing = buildStatefulSet(cluster());
    expect(statefulSetNeedsUpdate(existing, buildStatefulSet(cluster()))).toBe(false);
    expect(statefulSetNeedsUpdate(existing, buildStatefulSet(cluster({ podSecurityContext: { fsGroup: 5 } })))).toBe(true);
    expect(statefulSetNeedsUpdate(existing, buildStatefulSet(cluster({ securityContext: { privileged: false } })))).toBe(true);
    // StatefulSets created by earlier versions (fsGroup only, no container contexts) are updated
    const legacy = buildStatefulSet(cluster());
    legacy.spec!.template.spec!.securityContext = { fsGroup: 999 };
    for (const c of allContainers(legacy.spec!.template.spec!)) delete c.securityContext;
    expect(statefulSetNeedsUpdate(legacy, buildStatefulSet(cluster()))).toBe(true);
  });
});

describe('Job pod security (restricted Pod Security Standard)', () => {
  const backup: FirebirdBackup = {
    apiVersion: 'firebird.cloudnative-firebird.io/v1',
    kind: 'FirebirdBackup',
    metadata: { name: 'b', namespace: 'default', uid: 'b' },
    spec: { clusterName: 'db', s3: { bucket: 'b' } },
  };
  const c = cluster();
  const specs: Array<[string, V1PodSpec]> = [
    ['backup to S3', buildBackupJob(backup, c, 'db-0').spec!.template.spec!],
    ['journal archive', buildJournalArchiveCronJob(c, 'db-0')!.spec!.jobTemplate.spec!.template.spec!],
    ['sweep', buildAutoSweepCronJob(c)!.spec!.jobTemplate.spec!.template.spec!],
    ['fencing', buildFencingJob(c, 'db-1', 'fence').spec!.template.spec!],
    ['failover', buildFailoverJob(c, ['db-1']).spec!.template.spec!],
  ];

  for (const [what, spec] of specs) {
    it(`${what}: non-root, no privilege escalation, no capabilities, RuntimeDefault seccomp`, () => {
      expect(spec.securityContext).toEqual({
        runAsNonRoot: true,
        runAsUser: FIREBIRD_UID,
        runAsGroup: FIREBIRD_UID,
        seccompProfile: { type: 'RuntimeDefault' },
      });
      for (const container of allContainers(spec)) {
        expect(container.securityContext, container.name).toEqual({
          allowPrivilegeEscalation: false,
          capabilities: { drop: ['ALL'] },
        });
      }
    });
  }
});
