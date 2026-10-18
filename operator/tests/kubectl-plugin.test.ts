import { describe, it, expect, vi, beforeEach } from 'vitest';
import { V1Pod } from '@kubernetes/client-node';
import {
  apiErrorMessage,
  FENCED_INSTANCES_ANNOTATION,
  formatBackups,
  formatStatus,
  nextFencedList,
  parseArgs,
  PluginApi,
  PluginIo,
  RESEED_ANNOTATION,
  RESEED_VOLUME,
  run,
  TARGET_PRIMARY_ANNOTATION,
} from '../src/plugin/cli';
import { FENCED_INSTANCES_ANNOTATION as OPERATOR_FENCED } from '../src/utils/fencing';
import { TARGET_PRIMARY_ANNOTATION as OPERATOR_TARGET } from '../src/utils/switchover';
import { RESEED_ANNOTATION as OPERATOR_RESEED } from '../src/utils/replication';
import { RESEED_VOLUME as OPERATOR_RESEED_VOLUME } from '../src/utils/volume-recreation';
import { formatEvents, operatorLogLines, sanitize } from '../src/plugin/report';
import { BENCH_UID, benchScript } from '../src/plugin/bench';
import { FIREBIRD_UID } from '../src/utils/resources';
import { spawnSync } from 'child_process';
import { chmodSync, mkdtempSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
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
  let files: Map<string, string>;
  let dirs: string[];

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
      createRestore: vi.fn().mockResolvedValue(undefined),
      createJob: vi.fn().mockResolvedValue(undefined),
      getStatefulSet: vi.fn().mockResolvedValue({ metadata: { name: 'db', managedFields: [{ manager: 'x' }] }, spec: {} }),
      listServices: vi.fn().mockResolvedValue([{ metadata: { name: 'db' } }, { metadata: { name: 'db-replica' } }]),
      listClaims: vi.fn().mockRejectedValue(Object.assign(new Error('x'), { code: 403, body: '{"message":"persistentvolumeclaims is forbidden"}' })),
      listEvents: vi.fn().mockResolvedValue([]),
      readPodLog: vi.fn().mockImplementation((_ns, name, container) => Promise.resolve(`log of ${name}/${container}\n`)),
    };
    files = new Map();
    dirs = [];
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
      makeDir: (path) => {
        if (dirs.includes(path)) return Promise.reject(new Error(`EEXIST: file already exists, mkdir '${path}'`));
        dirs.push(path);
        return Promise.resolve();
      },
      writeFile: (path, content) => {
        files.set(path, content);
        return Promise.resolve();
      },
    };
  });

  const exec = (...argv: string[]) => run(argv, api as unknown as PluginApi, io, 'prod');

  it('writes the annotations the operator reads', () => {
    expect(FENCED_INSTANCES_ANNOTATION).toBe(OPERATOR_FENCED);
    expect(TARGET_PRIMARY_ANNOTATION).toBe(OPERATOR_TARGET);
    expect(RESEED_ANNOTATION).toBe(OPERATOR_RESEED);
    expect(RESEED_VOLUME).toBe(OPERATOR_RESEED_VOLUME);
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

  it('shows the image a catalog resolves to', () => {
    const text = formatStatus(
      { ...makeCluster({ imageCatalogRef: { kind: 'FirebirdClusterImageCatalog', name: 'firebird', major: 5 } }), status: { image: 'fb:5.0.3' } },
      [],
      [],
      [],
      new Date(),
    );
    expect(text).toContain('Image:        fb:5.0.3 (FirebirdClusterImageCatalog firebird, Firebird 5)');
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
    expect(await exec('backup', 'db', '--database', 'sales', '--name', 'sales-1')).toBe(0);
    expect(api.createBackup.mock.calls.at(-1)[1].spec).toEqual({ clusterName: 'db', type: 'logical', database: 'sales', s3: { bucket: 'bk', secretRef: { name: 's' } } });
    expect(out.at(-1)).toContain('(logical of FirebirdDatabase sales, to s3://bk)');
    expect(await exec('backup', 'db', '--type', 'physical', '--database', 'sales')).toBe(2);
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

  it('streams the logs of every instance, or of one', async () => {
    expect(await exec('logs', 'db')).toBe(0);
    expect(kubectlArgs).toEqual([
      '-n', 'prod', 'logs', '-l', 'firebird.cloudnative-firebird.io/cluster=db,app.kubernetes.io/component=database',
      '--prefix', '--max-log-requests=3', '-c', 'firebird', '--tail=-1',
    ]);
    expect(await exec('logs', 'db', '--instance', '2', '--container', 'segment-server', '--tail', '50', '-f', '--context', 'kind')).toBe(0);
    expect(kubectlArgs).toEqual(['--context', 'kind', '-n', 'prod', 'logs', 'db-2', '-c', 'segment-server', '--tail=50', '-f']);
    expect(await exec('logs', 'db', '--tail', 'all')).toBe(2);
    expect(await exec('logs', 'db', '--instance', '7')).toBe(2);
  });

  it('destroys a replica through the volume re-creation the operator runs', async () => {
    expect(await exec('destroy', 'db', '2')).toBe(0);
    expect(api.patchPod).toHaveBeenCalledWith('prod', 'db-2', { metadata: { annotations: { [RESEED_ANNOTATION]: 'volume' } } });
    expect(out.at(-1)).toContain('db-2 destroyed');
    // never the primary, a cluster without replication, a hibernated one or an instance without a pod
    expect(await exec('destroy', 'db', 'db-1')).toBe(2);
    expect(errs.at(-1)).toContain('is the primary');
    pods = pods.filter((p) => p.metadata?.name !== 'db-0');
    expect(await exec('destroy', 'db', '0')).toBe(2);
    expect(errs.at(-1)).toContain('has no pod');
    cluster.spec.hibernated = true;
    expect(await exec('destroy', 'db', '2')).toBe(2);
    cluster.spec.hibernated = false;
    cluster.spec.replication = { enabled: false };
    expect(await exec('destroy', 'db', '2')).toBe(2);
    expect(errs.at(-1)).toContain('empty database');
    expect(api.patchPod).toHaveBeenCalledTimes(1);
  });

  it('writes a report of the cluster, listing what it could not collect', async () => {
    pods[0].spec = { initContainers: [{ name: 'segment-tls' }], containers: [{ name: 'firebird' }] };
    pods[0].spec.containers[0].env = [{ name: 'ISC_PASSWORD', value: 'masterkey' }, { name: 'ISC_USER', value: 'SYSDBA' }];
    api.listPods.mockImplementation((ns: string) =>
      Promise.resolve(ns === 'cloudnative-firebird-system' ? [{ metadata: { name: 'op-1' }, spec: { containers: [{ name: 'operator' }] } }] : pods),
    );
    api.readPodLog.mockImplementation((ns: string, name: string, container: string) =>
      ns === 'cloudnative-firebird-system'
        ? Promise.resolve(['{"msg":"a","cluster":"db","namespace":"prod"}', '{"msg":"b","cluster":"db","namespace":"other"}', '{"msg":"c","cluster":"x"}', 'plain'].join('\n'))
        : Promise.resolve(`log of ${name}/${container}\n`),
    );
    api.listBackups.mockResolvedValue([
      { metadata: { name: 'mine' }, spec: { clusterName: 'db' } },
      { metadata: { name: 'theirs' }, spec: { clusterName: 'other' } },
    ]);
    expect(await exec('report', 'db')).toBe(0);
    const dir = 'report-db-20261011120000';
    expect(dirs).toEqual([dir, `${dir}/logs`]);
    expect([...files.keys()].map((k) => k.slice(dir.length + 1)).sort()).toEqual([
      'backups.yaml', 'cluster.yaml', 'databases.yaml', 'errors.txt', 'events.txt',
      'logs/db-0-firebird.log', 'logs/db-0-segment-tls.log', 'operator.log', 'pods.yaml', 'scheduled-backups.yaml',
      'services.yaml', 'statefulset.yaml', 'status.txt',
    ]);
    expect(files.get(`${dir}/pods.yaml`)).toContain('value: <redacted>');
    expect(files.get(`${dir}/pods.yaml`)).not.toContain('masterkey');
    expect(files.get(`${dir}/pods.yaml`)).toContain('value: SYSDBA');
    expect(files.get(`${dir}/statefulset.yaml`)).not.toContain('managedFields');
    expect(files.get(`${dir}/backups.yaml`)).toContain('mine');
    expect(files.get(`${dir}/backups.yaml`)).not.toContain('theirs');
    expect(files.get(`${dir}/status.txt`)).toContain('Cluster:      prod/db');
    expect(files.get(`${dir}/operator.log`)).toBe('# cloudnative-firebird-system/op-1\n{"msg":"a","cluster":"db","namespace":"prod"}\n');
    expect(files.get(`${dir}/errors.txt`)).toBe('pvcs.yaml: persistentvolumeclaims is forbidden\n');
    expect(out.at(-1)).toBe(`report of cluster db written to ${dir}/ (12 files; 1 could not be collected: see errors.txt)`);

    // an existing directory is not overwritten; --no-logs leaves the logs out
    expect(await exec('report', 'db')).toBe(1);
    expect(errs.at(-1)).toContain('EEXIST');
    files.clear();
    expect(await exec('report', 'db', '-o', 'out', '--no-logs')).toBe(0);
    expect([...files.keys()].some((k) => k.includes('log'))).toBe(false);
  });

  it('formats events and redacts literal credentials', () => {
    const events = [
      { involvedObject: { kind: 'Pod', name: 'db-0' }, reason: 'Started', type: 'Normal', message: 'b', lastTimestamp: new Date('2026-10-11T10:00:02Z') },
      { involvedObject: { kind: 'FirebirdCluster', name: 'db' }, reason: 'Created', type: 'Normal', message: 'a', count: 2, lastTimestamp: new Date('2026-10-11T10:00:01Z') },
      { involvedObject: { kind: 'Pod', name: 'dbx-0' }, reason: 'Other', message: 'c' },
    ];
    expect(formatEvents(events as never, 'db')).toBe(
      '2026-10-11T10:00:01.000Z  Normal  Created  FirebirdCluster/db  (x2) a\n2026-10-11T10:00:02.000Z  Normal  Started  Pod/db-0  b\n',
    );
    expect(formatEvents([], 'db')).toBe('(no events)\n');
    expect(sanitize({ env: [{ name: 'AWS_SECRET_ACCESS_KEY', value: 's' }, { name: 'X', valueFrom: {} }] })).toEqual({
      env: [{ name: 'AWS_SECRET_ACCESS_KEY', value: '<redacted>' }, { name: 'X', valueFrom: {} }],
    });
    expect(sanitize({ metadata: { creationTimestamp: new Date('2026-10-11T10:00:00Z') } })).toEqual({ metadata: { creationTimestamp: '2026-10-11T10:00:00.000Z' } });
    expect(operatorLogLines('{"cluster":"db"}\n{"cluster":"db2"}', 'db', 'prod')).toBe('{"cluster":"db"}\n');
  });

  const backupOf = (name: string, spec: Partial<FirebirdBackup['spec']>, status?: FirebirdBackup['status']): FirebirdBackup => ({
    apiVersion: 'firebird.cloudnative-firebird.io/v1',
    kind: 'FirebirdBackup',
    metadata: { name },
    spec: { clusterName: 'db', ...spec },
    ...(status ? { status } : {}),
  });

  it("lists the cluster's backups, newest first", async () => {
    const backups = [
      backupOf('old', {}, { phase: 'Completed', startTime: '2026-10-10T12:00:00Z', completionTime: '2026-10-10T12:02:00Z', location: '/var/lib/firebird/data/backup-old.fbk', verified: true }),
      backupOf('inc', { type: 'physical', level: 1 }, { phase: 'Failed', startTime: '2026-10-11T11:00:00Z', error: 'Job failed' }),
      backupOf('sales', { database: 'sales' }, { phase: 'Running', startTime: '2026-10-11T11:50:00Z' }),
      backupOf('other', { clusterName: 'x' }, { phase: 'Completed' }),
    ];
    const text = formatBackups(backups, 'db', new Date('2026-10-11T12:00:00Z'));
    const lines = text.split('\n');
    expect(lines[0]).toMatch(/^NAME +TYPE +PHASE +STARTED +DURATION +DATABASE +LOCATION$/);
    expect(lines.slice(1).map((l) => l.split(/ {2,}/)[0])).toEqual(['sales', 'inc', 'old']);
    expect(lines[1]).toMatch(/^sales +logical +Running +10m ago +- +sales +-$/);
    expect(lines[2]).toMatch(/^inc +physical \(level 1\) +Failed +1h ago +- +\(cluster\) +Job failed$/);
    expect(lines[3]).toMatch(/^old +logical +Completed \(verified\) +24h ago +2m +\(cluster\) +\/var\/lib\/firebird\/data\/backup-old.fbk$/);
    expect(formatBackups([], 'db', new Date())).toBe('no backups of cluster db');

    api.listBackups.mockResolvedValue(backups);
    expect(await exec('backups', 'db')).toBe(0);
    expect(out.at(-1)).toBe(formatBackups(backups, 'db', new Date('2026-10-11T12:00:00Z')));
  });

  it('restores a completed backup into a new file, and refuses the others', async () => {
    api.listBackups.mockResolvedValue([
      backupOf('done', {}, { phase: 'Completed' }),
      backupOf('running', {}, { phase: 'Running' }),
      backupOf('snap', { type: 'volumeSnapshot' }, { phase: 'Completed' }),
      backupOf('theirs', { clusterName: 'x' }, { phase: 'Completed' }),
    ]);
    expect(await exec('restore', 'db', 'done', '--target', 'copy.fdb', '--name', 'r1')).toBe(0);
    expect(api.createRestore).toHaveBeenCalledWith('prod', {
      apiVersion: 'firebird.cloudnative-firebird.io/v1',
      kind: 'FirebirdRestore',
      metadata: { name: 'r1', namespace: 'prod' },
      spec: { clusterName: 'db', backupName: 'done', targetDatabase: 'copy.fdb' },
    });
    expect(out.at(-1)).toContain('done into copy.fdb on the primary');
    expect(await exec('restore', 'db', 'done')).toBe(0);
    expect(api.createRestore.mock.calls[1][1].metadata.name).toBe('done-restore-20261011120000');
    expect(out.at(-1)).toContain('into restore-done-restore-20261011120000.fdb');
    for (const [backup, message] of [
      ['running', 'only a completed backup'],
      ['snap', 'bootstraps from it'],
      ['theirs', 'backup of cluster x'],
      ['missing', 'no FirebirdBackup missing'],
    ]) {
      expect(await exec('restore', 'db', backup)).toBe(2);
      expect(errs.at(-1)).toContain(message);
    }
    expect(api.createRestore).toHaveBeenCalledTimes(2);
  });

  it('starts and ends a node maintenance window', async () => {
    expect(await exec('maintenance', 'on', 'db')).toBe(0);
    expect(api.patchCluster).toHaveBeenLastCalledWith('prod', 'db', { spec: { nodeMaintenanceWindow: { inProgress: true, reusePVC: null } } });
    expect(out.at(-1)).toContain('node drains may evict its instances;');
    expect(await exec('maintenance', 'off', 'db')).toBe(0);
    expect(api.patchCluster).toHaveBeenLastCalledWith('prod', 'db', { spec: { nodeMaintenanceWindow: { inProgress: false, reusePVC: null } } });
    expect(await exec('maintenance', 'maybe', 'db')).toBe(2);
    expect(await exec('maintenance', 'on', 'db', '--no-reuse-pvc')).toBe(0);
    expect(api.patchCluster).toHaveBeenLastCalledWith('prod', 'db', { spec: { nodeMaintenanceWindow: { inProgress: true, reusePVC: false } } });
    expect(out.at(-1)).toContain('gets a new volume');
    // ending the window removes reusePVC, so the next window reuses volumes unless asked again
    expect(await exec('maintenance', 'off', 'db')).toBe(0);
    expect(api.patchCluster).toHaveBeenLastCalledWith('prod', 'db', { spec: { nodeMaintenanceWindow: { inProgress: false, reusePVC: null } } });
    expect(await exec('maintenance', 'off', 'db', '--no-reuse-pvc')).toBe(2);
    cluster.spec.replication = { enabled: false };
    expect(await exec('maintenance', 'on', 'db', '--no-reuse-pvc')).toBe(2);
    cluster.spec.replication = { enabled: true };
    cluster.spec.nodeMaintenanceWindow = { inProgress: true };
    expect(formatStatus(cluster, pods, [], [], new Date())).toMatch(/^Maintenance: +node maintenance window in progress/m);
  });

  it('starts a bench Job against the primary Service', async () => {
    expect(BENCH_UID).toBe(FIREBIRD_UID);
    cluster.spec.superuserSecret = { name: 'db-superuser' };
    expect(await exec('bench', 'db')).toBe(0);
    const job = api.createJob.mock.calls[0][1];
    expect(job.metadata.name).toBe('db-bench-20261011120000');
    expect(job.spec.backoffLimit).toBe(0);
    const pod = job.spec.template.spec;
    expect(pod.securityContext).toMatchObject({ runAsNonRoot: true, runAsUser: 84 });
    const env = Object.fromEntries(pod.containers[0].env.map((e: { name: string; value?: string; valueFrom?: unknown }) => [e.name, e.value ?? e.valueFrom]));
    expect(env).toEqual({
      HOST: 'db',
      DATABASE: '/var/lib/firebird/data/mydb.fdb',
      CLIENTS: '4',
      TRANSACTIONS: '1000',
      KEEP: '0',
      ISC_USER: 'SYSDBA',
      ISC_PASSWORD: { secretKeyRef: { name: 'db-superuser', key: 'password' } },
    });
    expect(out.at(-1)).toContain('kubectl logs -f job/db-bench-20261011120000 -n prod');

    api.listDatabases.mockResolvedValue([{ metadata: { name: 'sales' }, spec: { clusterName: 'db', databaseName: 'sales-2026.fdb' } }]);
    expect(await exec('bench', 'db', '--clients', '8', '--transactions', '50', '--database', 'sales', '--keep', '--name', 'b1')).toBe(0);
    const env2 = Object.fromEntries(api.createJob.mock.calls[1][1].spec.template.spec.containers[0].env.map((e: { name: string; value?: string }) => [e.name, e.value]));
    expect(env2).toMatchObject({ DATABASE: '/var/lib/firebird/data/sales-2026.fdb', CLIENTS: '8', TRANSACTIONS: '50', KEEP: '1' });
    expect(api.createJob.mock.calls[1][1].metadata.name).toBe('b1');

    for (const args of [['--clients', '0'], ['--clients', '65'], ['--transactions', 'many'], ['--database', 'nope']]) {
      expect(await exec('bench', 'db', ...args)).toBe(2);
    }
    cluster.spec.hibernated = true;
    expect(await exec('bench', 'db')).toBe(2);
    expect(api.createJob).toHaveBeenCalledTimes(2);
  });

  it('runs the bench script: the transactions of every client, the rate, and failures', () => {
    // a fake isql: counts the inserts it is given, answers the row count, fails on demand
    const dir = mkdtempSync(join(tmpdir(), 'bench-'));
    writeFileSync(
      join(dir, 'isql'),
      [
        '#!/bin/bash',
        `state=${dir}/rows`,
        'input=""',
        'while [ $# -gt 0 ]; do case "$1" in -i) input=$(cat "$2"); shift 2;; *) shift;; esac; done',
        '[ -z "$input" ] && input=$(cat)',
        'if echo "$input" | grep -q "RECREATE TABLE"; then echo 0 > $state; exit 0; fi',
        'if echo "$input" | grep -q "rows="; then echo "rows=$(cat $state)"; exit 0; fi',
        'if echo "$input" | grep -q "DROP TABLE"; then echo dropped > ' + dir + '/dropped; exit 0; fi',
        'echo "$input" > ' + dir + '/last.sql',
        '[ "${FAIL:-0}" = 1 ] && echo "Statement failed, SQLSTATE = 23000"',
        'n=$(echo "$input" | grep -o "INSERT INTO KF_BENCH" | wc -l)',
        'flock $state -c "echo \\$((\\$(cat $state) + $n)) > $state"',
      ].join('\n'),
    );
    chmodSync(join(dir, 'isql'), 0o755);
    const runBench = (extra: Record<string, string>) =>
      spawnSync('bash', ['-c', benchScript()], {
        env: { PATH: `${dir}:${process.env.PATH}`, HOST: 'db', DATABASE: '/var/lib/firebird/data/mydb.fdb', CLIENTS: '3', TRANSACTIONS: '20', KEEP: '0', ...extra },
        encoding: 'utf8',
      });
    const ok = runBench({});
    expect(ok.status).toBe(0);
    expect(ok.stdout).toMatch(/^transactions: 60$/m);
    expect(ok.stdout).toMatch(/^clients: 3$/m);
    expect(ok.stdout).toMatch(/^tps: [0-9.]+$/m);
    expect(spawnSync('cat', [join(dir, 'dropped')]).status).toBe(0);
    // an id past 2^31 (client 64) is printed whole, and the quote of the payload survives awk
    const wide = runBench({ CLIENTS: '64', TRANSACTIONS: '1' });
    expect(wide.status).toBe(0);
    expect(spawnSync('cat', [join(dir, 'last.sql')], { encoding: 'utf8' }).stdout).toMatch(/VALUES \((\d{9,10}), \d+, 1, 'bench'\)/);
    const failed = runBench({ FAIL: '1' });
    expect(failed.status).toBe(1);
    expect(failed.stdout).toContain('bench: client 1 failed');
  }, 30000); // a shell script with 64 parallel clients: slow on a loaded CI runner
});
