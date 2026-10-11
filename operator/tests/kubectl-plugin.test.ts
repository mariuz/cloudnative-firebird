import { describe, it, expect, vi, beforeEach } from 'vitest';
import { V1Pod } from '@kubernetes/client-node';
import {
  apiErrorMessage,
  FENCED_INSTANCES_ANNOTATION,
  formatStatus,
  nextFencedList,
  parseArgs,
  PluginApi,
  PluginIo,
  RESEED_ANNOTATION,
  run,
  TARGET_PRIMARY_ANNOTATION,
} from '../src/plugin/cli';
import { FENCED_INSTANCES_ANNOTATION as OPERATOR_FENCED } from '../src/utils/fencing';
import { TARGET_PRIMARY_ANNOTATION as OPERATOR_TARGET } from '../src/utils/switchover';
import { RESEED_ANNOTATION as OPERATOR_RESEED } from '../src/utils/replication';
import { FirebirdBackup, FirebirdCluster } from '../src/types';

const makeCluster = (spec: Partial<FirebirdCluster['spec']> = {}, status?: FirebirdCluster['status']): FirebirdCluster => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdCluster',
  metadata: { name: 'db', namespace: 'prod' },
  spec: { instances: 3, storage: { size: '1Gi' }, replication: { enabled: true }, ...spec },
  ...(status ? { status } : {}),
});

const pod = (name: string, ready = true, role?: string): V1Pod => ({
  metadata: { name, labels: role ? { 'firebird.cloudnative-firebird.io/role': role } : {}, creationTimestamp: new Date('2026-10-11T10:00:00Z') },
  spec: { containers: [], nodeName: 'node-a' },
  status: { phase: 'Running', conditions: [{ type: 'Ready', status: ready ? 'True' : 'False' }] },
});

describe('kubectl firebird', () => {
  let cluster: FirebirdCluster;
  let pods: V1Pod[];
  let api: { [K in keyof PluginApi]: ReturnType<typeof vi.fn> };
  let out: string[];
  let errs: string[];
  let io: PluginIo;
  let kubectlArgs: string[] | undefined;

  beforeEach(() => {
    cluster = makeCluster({}, { phase: 'Running', readyInstances: 3, replicationStatus: { primaryPod: 'db-1' } });
    pods = [pod('db-0', true, 'replica'), pod('db-1', true, 'primary'), pod('db-2', false, 'replica')];
    api = {
      getCluster: vi.fn().mockImplementation((_ns, name) =>
        name === 'db' ? Promise.resolve(cluster) : Promise.reject(Object.assign(new Error('x'), { code: 404, body: '{"message":"firebirdclusters \\"nope\\" not found"}' })),
      ),
      patchCluster: vi.fn().mockResolvedValue(undefined),
      listPods: vi.fn().mockImplementation(() => Promise.resolve(pods)),
      patchPod: vi.fn().mockResolvedValue(undefined),
      deletePod: vi.fn().mockResolvedValue(undefined),
      patchStatefulSet: vi.fn().mockResolvedValue(undefined),
      listBackups: vi.fn().mockResolvedValue([]),
      listScheduledBackups: vi.fn().mockResolvedValue([]),
      listDatabases: vi.fn().mockResolvedValue([]),
      createBackup: vi.fn().mockResolvedValue(undefined),
    };
    out = [];
    errs = [];
    kubectlArgs = undefined;
    io = {
      out: (t) => out.push(t),
      err: (t) => errs.push(t),
      kubectl: (args) => {
        kubectlArgs = args;
        return Promise.resolve(0);
      },
      interactive: false,
      now: () => new Date('2026-10-11T12:00:00Z'),
    };
  });

  const exec = (...argv: string[]) => run(argv, api as unknown as PluginApi, io, 'prod');

  it('writes the annotations the operator reads', () => {
    expect(FENCED_INSTANCES_ANNOTATION).toBe(OPERATOR_FENCED);
    expect(TARGET_PRIMARY_ANNOTATION).toBe(OPERATOR_TARGET);
    expect(RESEED_ANNOTATION).toBe(OPERATOR_RESEED);
  });

  it('parses flags, values and the arguments after --', () => {
    expect(parseArgs(['status', 'db', '-n', 'x', '--context=k'])).toEqual({
      positional: ['status', 'db'],
      flags: new Map([['namespace', 'x'], ['context', 'k']]),
      rest: [],
    });
    expect(parseArgs(['isql', 'db', '--replica', '--', '-q', '-i', 'f.sql']).rest).toEqual(['-q', '-i', 'f.sql']);
    expect(() => parseArgs(['status', '--bogus'])).toThrow('unknown flag');
    expect(() => parseArgs(['status', '-n'])).toThrow('needs a value');
  });

  it('prints usage, and fails on unknown commands', async () => {
    expect(await exec()).toBe(1);
    expect(out[0]).toContain('Usage: kubectl firebird');
    expect(await exec('--help')).toBe(0);
    expect(await exec('frobnicate')).toBe(2);
    expect(errs.at(-1)).toContain('unknown command frobnicate');
  });

  it('reports the cluster, its instances, replication and backups', async () => {
    cluster.status = {
      ...cluster.status,
      fencedInstances: ['db-2'],
      replicationStatus: {
        primaryPod: 'db-1',
        lastArchivedSequence: 42,
        replicas: [
          { name: 'db-0', appliedSequence: 41, lagSegments: 1, lagSeconds: 3 },
          { name: 'db-2', error: 'segment server unreachable' },
        ],
      },
      conditions: [{ type: 'Ready', status: 'True', reason: 'AllReady', lastTransitionTime: '' }],
    };
    const backups: FirebirdBackup[] = [
      {
        apiVersion: 'firebird.cloudnative-firebird.io/v1',
        kind: 'FirebirdBackup',
        metadata: { name: 'nightly' },
        spec: { clusterName: 'db', type: 'volumeSnapshot' },
        status: { phase: 'Completed', startTime: '2026-10-11T11:00:00Z', location: 'VolumeSnapshot prod/nightly' },
      },
      { apiVersion: 'firebird.cloudnative-firebird.io/v1', kind: 'FirebirdBackup', metadata: { name: 'other' }, spec: { clusterName: 'elsewhere' } },
    ];
    api.listBackups.mockResolvedValue(backups);
    api.listDatabases.mockResolvedValue([
      {
        apiVersion: 'firebird.cloudnative-firebird.io/v1',
        kind: 'FirebirdDatabase',
        metadata: { name: 'sales' },
        spec: { clusterName: 'db', reclaimPolicy: 'delete' },
        status: { phase: 'Ready', path: '/var/lib/firebird/data/sales.fdb' },
      },
    ]);
    expect(await exec('status', 'db')).toBe(0);
    const text = out.join('\n');
    expect(api.listPods).toHaveBeenCalledWith('prod', 'firebird.cloudnative-firebird.io/cluster=db,app.kubernetes.io/component=database');
    expect(text).toContain('Cluster:      prod/db');
    expect(text).toContain('Primary:      db-1');
    expect(text).toContain('Replication:  asynchronous, last archived segment 42');
    expect(text).toContain('Fenced:       db-2');
    expect(text).toMatch(/db-0\s+replica\s+true\s+Running\s+node-a\s+41\s+1 seg \/ 3s\s+2h/);
    expect(text).toMatch(/db-1\s+primary\s+true/);
    expect(text).toMatch(/db-2\s+replica \(fenced\)\s+false.*error: segment server unreachable/);
    expect(text).toMatch(/nightly\s+volumeSnapshot\s+Completed\s+1h ago\s+VolumeSnapshot prod\/nightly/);
    expect(text).not.toContain('other');
    expect(text).toMatch(/sales\s+\/var\/lib\/firebird\/data\/sales\.fdb\s+Ready\s+delete/);
    expect(text).toContain('Ready=True AllReady');
  });

  it('lists no databases when the CRD is missing', async () => {
    api.listDatabases.mockRejectedValue(Object.assign(new Error('not found'), { code: 404 }));
    expect(await exec('status', 'db')).toBe(0);
    expect(out.join('\n')).not.toContain('Databases:');
  });

  it('formats a cluster without replication or pods', () => {
    const text = formatStatus(makeCluster({ replication: undefined, instances: 1 }), [], [], [], new Date());
    expect(text).toContain('Replication:  off');
    expect(text).toContain('Primary:      db-0');
    expect(text).toContain('(no instance pods)');
    expect(text).toContain('(none)');
  });

  it('promotes a replica through the targetPrimary annotation', async () => {
    expect(await exec('promote', 'db', '0')).toBe(0);
    expect(api.patchCluster).toHaveBeenCalledWith('prod', 'db', { metadata: { annotations: { [TARGET_PRIMARY_ANNOTATION]: 'db-0' } } });
    expect(out.at(-1)).toContain('db-1 -> db-0');
    // not ready: warned, still requested
    expect(await exec('promote', 'db', 'db-2')).toBe(0);
    expect(errs.at(-1)).toContain('not ready');
    expect(await exec('promote', 'db', '1')).toBe(2);
    expect(errs.at(-1)).toContain('the primary already');
    expect(await exec('promote', 'db', '5')).toBe(2);
    expect(errs.at(-1)).toContain('not an instance of cluster db');
    cluster.spec.replication = undefined;
    expect(await exec('promote', 'db', '0')).toBe(2);
    expect(errs.at(-1)).toContain('no replication');
  });

  it('fences and unfences instances', async () => {
    expect(nextFencedList(cluster, true, 'db-2')).toEqual(['db-2']);
    expect(nextFencedList(cluster, true, '*')).toEqual(['*']);
    cluster.metadata.annotations = { [FENCED_INSTANCES_ANNOTATION]: '["*"]' };
    // unfencing one of "all" keeps the others
    expect(nextFencedList(cluster, false, 'db-1')).toEqual(['db-0', 'db-2']);
    expect(nextFencedList(cluster, false, '*')).toEqual([]);

    cluster.metadata.annotations = { [FENCED_INSTANCES_ANNOTATION]: '["db-0"]' };
    expect(await exec('fence', 'on', 'db', '2')).toBe(0);
    expect(api.patchCluster).toHaveBeenLastCalledWith('prod', 'db', { metadata: { annotations: { [FENCED_INSTANCES_ANNOTATION]: '["db-0","db-2"]' } } });
    expect(await exec('fence', 'off', 'db', 'db-0')).toBe(0);
    // the last one: the annotation is removed
    cluster.metadata.annotations = { [FENCED_INSTANCES_ANNOTATION]: '["db-0"]' };
    expect(await exec('fence', 'off', 'db', '0')).toBe(0);
    expect(api.patchCluster).toHaveBeenLastCalledWith('prod', 'db', { metadata: { annotations: { [FENCED_INSTANCES_ANNOTATION]: null } } });
    expect(await exec('fence', 'on', 'db', 'all')).toBe(0);
    expect(api.patchCluster).toHaveBeenLastCalledWith('prod', 'db', { metadata: { annotations: { [FENCED_INSTANCES_ANNOTATION]: '["*"]' } } });
    expect(await exec('fence', 'maybe', 'db', '0')).toBe(2);
  });

  it('hibernates, restarts and re-seeds', async () => {
    expect(await exec('hibernate', 'on', 'db')).toBe(0);
    expect(api.patchCluster).toHaveBeenLastCalledWith('prod', 'db', { spec: { hibernated: true } });
    expect(await exec('hibernate', 'off', 'db')).toBe(0);
    expect(api.patchCluster).toHaveBeenLastCalledWith('prod', 'db', { spec: { hibernated: false } });

    expect(await exec('restart', 'db')).toBe(0);
    expect(api.patchStatefulSet).toHaveBeenCalledWith('prod', 'db', {
      spec: { template: { metadata: { annotations: { 'kubectl.kubernetes.io/restartedAt': '2026-10-11T12:00:00.000Z' } } } },
    });
    expect(await exec('restart', 'db', '2')).toBe(0);
    expect(api.deletePod).toHaveBeenCalledWith('prod', 'db-2');

    expect(await exec('reseed', 'db', '0')).toBe(0);
    expect(api.patchPod).toHaveBeenCalledWith('prod', 'db-0', { metadata: { annotations: { [RESEED_ANNOTATION]: 'true' } } });
    expect(await exec('reseed', 'db', '1')).toBe(2);
    expect(errs.at(-1)).toContain('is the primary');
  });

  it('creates on-demand backups', async () => {
    expect(await exec('backup', 'db')).toBe(0);
    expect(api.createBackup.mock.calls[0][1]).toMatchObject({
      kind: 'FirebirdBackup',
      metadata: { name: 'db-20261011120000', namespace: 'prod' },
      spec: { clusterName: 'db', type: 'logical' },
    });
    cluster.spec.backup = { enabled: true, s3: { bucket: 'bk', secretRef: { name: 's' } } };
    expect(await exec('backup', 'db', '--type', 'physical', '--level', '1', '--name', 'inc')).toBe(0);
    expect(api.createBackup.mock.calls[1][1].spec).toEqual({ clusterName: 'db', type: 'physical', level: 1, s3: { bucket: 'bk', secretRef: { name: 's' } } });
    expect(await exec('backup', 'db', '--type', 'volumeSnapshot', '--snapshot-class', 'csi')).toBe(0);
    expect(api.createBackup.mock.calls[2][1].spec).toEqual({ clusterName: 'db', type: 'volumeSnapshot', volumeSnapshot: { className: 'csi' } });
    expect(await exec('backup', 'db', '--type', 'logical', '--level', '1')).toBe(2);
    expect(await exec('backup', 'db', '--type', 'tape')).toBe(2);
    expect(await exec('backup', 'db', '--snapshot-class', 'csi')).toBe(2);
  });

  it('runs isql in the firebird container of the primary or a replica', async () => {
    expect(await exec('isql', 'db', '--', '-q')).toBe(0);
    expect(kubectlArgs).toEqual(['-n', 'prod', 'exec', '-i', 'db-1', '-c', 'firebird', '--', 'isql', '-q', 'localhost:/var/lib/firebird/data/mydb.fdb']);
    io.interactive = true;
    expect(await exec('isql', 'db', '--replica', '--context', 'kind')).toBe(0);
    expect(kubectlArgs).toEqual(['--context', 'kind', '-n', 'prod', 'exec', '-it', 'db-0', '-c', 'firebird', '--', 'isql', 'localhost:/var/lib/firebird/data/mydb.fdb']);
    expect(await exec('isql', 'db', '--instance', '2')).toBe(0);
    expect(kubectlArgs?.[4]).toBe('db-2');
    pods = [pod('db-1', true, 'primary')];
    expect(await exec('isql', 'db', '--replica')).toBe(2);
  });

  it('reports API errors with the server message', async () => {
    expect(await exec('status', 'nope')).toBe(1);
    expect(errs.at(-1)).toBe('error: firebirdclusters "nope" not found');
    expect(apiErrorMessage(Object.assign(new Error('boom'), { code: 500 }))).toBe('boom');
    expect(apiErrorMessage({ code: 404 })).toBe('not found');
  });
});
