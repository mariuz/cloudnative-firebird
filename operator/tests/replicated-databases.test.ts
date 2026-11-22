import { describe, it, expect } from 'vitest';
import { spawnSync } from 'child_process';
import { chmodSync, mkdtempSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { V1Job } from '@kubernetes/client-node';
import { buildFailoverJob, buildPromoteJob, buildSwitchoverJob } from '../src/utils/switchover';
import { AGENT_PATH } from '../src/utils/agent';
import { buildConfigMap, buildStatefulSet, replicationConfigHash } from '../src/utils/resources';
import { buildReplicationConf } from '../src/utils/replication';
import { FirebirdCluster } from '../src/types';
import { makeCluster } from './helpers/factories';
import { FirebirdClusterController } from '../src/controllers/firebirdcluster.controller';

/** docs/multi-database-replication.md: what the instances get for the declarative databases (status.replicatedDatabases) */
function annotated(databases?: string[]): FirebirdCluster {
  const cluster = makeCluster({ instances: 3, replication: { enabled: true } });
  if (databases) cluster.status = { ...(cluster.status ?? {}), replicatedDatabases: databases };
  return cluster;
}

const replicationInit = (cluster: FirebirdCluster) =>
  buildStatefulSet(cluster).spec!.template.spec!.initContainers!.find((c) => c.name === 'replication-init')!;
const env = (cluster: FirebirdCluster, container: string) => {
  const spec = buildStatefulSet(cluster).spec!.template.spec!;
  const c = [...(spec.initContainers ?? []), ...spec.containers].find((x) => x.name === container)!;
  return Object.fromEntries((c.env ?? []).map((e) => [e.name, e.value]));
};

describe('replicated declarative databases', () => {
  it('gives each its own replication.conf section and tree', () => {
    const conf = buildReplicationConf(annotated(['sales.fdb', 'app.fdb']), '/var/lib/firebird/data/mydb.fdb', '/var/lib/firebird/data');
    const sections = conf.split('\n').filter((line) => line.startsWith('database = '));
    expect(sections).toEqual([
      'database = /var/lib/firebird/data/mydb.fdb',
      'database = /var/lib/firebird/data/app.fdb',
      'database = /var/lib/firebird/data/sales.fdb',
    ]);
    const tree = '/var/lib/firebird/data/replication/databases/app.fdb';
    for (const line of [`journal_directory = ${tree}/journal`, `journal_archive_directory = ${tree}/archive`, `journal_source_directory = ${tree}/source`, `include ${tree}/sync.conf`]) {
      expect(conf).toContain(`    ${line}`);
    }
    // the cluster database's directories stay where they are
    expect(conf).toContain('    journal_directory = /var/lib/firebird/data/replication/journal');
  });

  it('lists them in the ConfigMap and points the agent at the list', () => {
    const cluster = annotated(['app.fdb']);
    expect(buildConfigMap(cluster, { primaryPod: 'test-cluster-0' })?.data?.['replicated-databases']).toBe('app.fdb\n');
    for (const container of ['replication-init', 'segment-server', 'segment-puller']) {
      expect(env(cluster, container).REPLICATED_DATABASES_FILE).toBe('/etc/firebird-operator/replicated-databases');
    }
    expect(replicationInit(cluster).command).toEqual([
      'sh',
      '-c',
      'sh /etc/firebird-operator/init-instance.sh && sh /etc/firebird-operator/init-databases.sh',
    ]);
    // after the last one is removed: an empty list, the same pods, so the replicas remove its copies
    expect(buildConfigMap(annotated([]), { primaryPod: 'test-cluster-0' })?.data?.['replicated-databases']).toBe('');
    expect(env(annotated([]), 'segment-server').REPLICATED_DATABASES_FILE).toBe('/etc/firebird-operator/replicated-databases');
  });

  it('rolls the instances when the list changes', () => {
    expect(replicationConfigHash(annotated(['app.fdb']))).not.toBe(replicationConfigHash(annotated()));
    expect(replicationConfigHash(annotated([]))).not.toBe(replicationConfigHash(annotated(['app.fdb'])));
    expect(replicationConfigHash(annotated(['app.fdb', 'sales.fdb']))).not.toBe(replicationConfigHash(annotated(['app.fdb'])));
  });

  it('changes nothing before the first database, nor on a replica cluster; the old annotation is not read', () => {
    const before = makeCluster({ instances: 3, replication: { enabled: true } });
    // the annotation that gated them until v0.185.0 changes nothing either
    before.metadata.annotations = { 'firebird.cloudnative-firebird.io/replicated-databases': 'experimental' };
    const s3 = { bucket: 'dr', prefix: 'prod', endpoint: 'http://s3:9000', secretRef: { name: 's3' } };
    const replica = makeCluster({
      instances: 1,
      replication: { enabled: true },
      replica: { enabled: true, journalS3: s3, pollIntervalSeconds: 10 },
      bootstrap: { recovery: { sourcePath: 'nbackup-l0.nbk', s3 } },
    });
    replica.status = { ...(replica.status ?? {}), replicatedDatabases: ['app.fdb'] };
    for (const cluster of [before, replica]) {
      expect(buildConfigMap(cluster, { primaryPod: 'test-cluster-0' })?.data?.['replicated-databases']).toBeUndefined();
      expect(env(cluster, 'segment-server').REPLICATED_DATABASES_FILE).toBeUndefined();
      expect(replicationInit(cluster).command).toEqual(['sh', '/etc/firebird-operator/init-instance.sh']);
      expect(buildReplicationConf(cluster, '/d/mydb.fdb', '/d')).not.toContain('app.fdb');
    }
    expect(replicationConfigHash(makeCluster({ instances: 3, replication: { enabled: true } }))).toBe(replicationConfigHash(before));
    expect(buildStatefulSet(makeCluster({ instances: 3, replication: { enabled: true } })).spec!.template).toEqual(buildStatefulSet(before).spec!.template);
  });

  it('has the switchover, promotion and failover Jobs cover them (phase 3)', () => {
    const jobEnv = (job: V1Job) => Object.fromEntries((job.spec!.template.spec!.containers[0].env ?? []).map((e) => [e.name, e.value]));
    const jobs = (cluster: FirebirdCluster) => [
      buildSwitchoverJob(cluster, { from: 'test-cluster-0', target: 'test-cluster-1', replicas: ['test-cluster-2'] }),
      buildPromoteJob(cluster, 'test-cluster-1'),
      buildFailoverJob(cluster, ['test-cluster-1', 'test-cluster-2']),
    ];
    for (const job of jobs(annotated(['sales.fdb', 'app.fdb']))) expect(jobEnv(job).DATABASES).toBe('app.fdb sales.fdb');
    for (const job of jobs(annotated())) expect(jobEnv(job).DATABASES).toBeUndefined();
    for (const job of jobs(annotated([]))) expect(jobEnv(job).DATABASES).toBeUndefined();
  });

  it('has the promotion Job promote each in place, and report an error for the first that is not', () => {
    const dir = mkdtempSync(join(tmpdir(), 'promote-job-'));
    const stub = join(dir, 'fb-agent');
    // the stub agent: "request <host> <words...>", answered per request; the calls recorded
    writeFileSync(
      stub,
      '#!/bin/sh\nshift 2\necho "$*" >> "$(dirname "$0")/calls"\ncase "$*" in\n' +
        '"PROMOTE none") echo "OK 53 1" ;;\n"DB app.fdb PROMOTE none") echo "OK 9 1" ;;\n' +
        '"DB sales.fdb PROMOTE none") echo "${SALES:-OK 8 1}" ;;\n*) echo ERR ;;\nesac\n',
    );
    chmodSync(stub, 0o755);
    const script = buildPromoteJob(annotated(['app.fdb', 'sales.fdb']), 'test-cluster-1').spec!.template.spec!.containers[0].command![2].split(AGENT_PATH).join(stub);
    const run = (env: Record<string, string>) =>
      spawnSync('sh', ['-c', script], { env: { PATH: process.env.PATH ?? '', TARGET: 'h', DATABASES: 'app.fdb sales.fdb', ...env }, encoding: 'utf8' });
    const ok = run({});
    expect(ok.status).toBe(0);
    expect(ok.stdout.trim().split('\n').at(-1)).toBe('OK 53 1');
    const failed = run({ SALES: 'ERR cannot shut down' });
    expect(failed.stdout.trim().split('\n').at(-1)).toBe('ERR sales.fdb: ERR cannot shut down');
    // the operator's reading: in place only for "OK <S>"
    expect(/^OK \d+/.test(failed.stdout.trim().split('\n').at(-1)!)).toBe(false);
  });
});

describe('status.replicatedDatabases', () => {
  const log = { info: () => undefined, warn: () => undefined } as never;
  const db = (name: string, clusterName = 'test-cluster', created = true) => ({
    metadata: { name },
    spec: { clusterName },
    status: created ? { instances: [{ name: 'test-cluster-0', volume: 'v' }] } : {},
  });
  // the private resolver, against a list of FirebirdDatabases (or a failing list)
  const resolve = async (cluster: FirebirdCluster, items: unknown[] | Error) => {
    const controller = Object.create(FirebirdClusterController.prototype) as { customApi: unknown };
    controller.customApi = {
      listNamespacedCustomObject: async () => {
        if (items instanceof Error) throw items;
        return { items };
      },
    };
    await (controller as unknown as { resolveReplicatedDatabases: (c: FirebirdCluster, l: never) => Promise<void> }).resolveReplicatedDatabases(cluster, log);
    return cluster.status?.replicatedDatabases;
  };

  it('is absent until the first database, lists those created on the primary, and empty after the last', async () => {
    expect(await resolve(annotated(), [])).toBeUndefined();
    expect(await resolve(annotated(), [db('other', 'elsewhere'), db('pending', 'test-cluster', false)])).toBeUndefined();
    expect(await resolve(annotated(), [db('sales'), db('app')])).toEqual(['app.fdb', 'sales.fdb']);
    // the last one removed: empty, so the instances stay set up and the replicas remove the copies
    expect(await resolve(annotated(['app.fdb']), [])).toEqual([]);
    expect(await resolve(annotated([]), [])).toEqual([]);
    // a list that fails leaves it as it is
    expect(await resolve(annotated(['app.fdb']), new Error('forbidden'))).toEqual(['app.fdb']);
  });

  it('is absent without replication', async () => {
    const plain = makeCluster({ instances: 1 });
    plain.status = { replicatedDatabases: [] };
    expect(await resolve(plain, [db('app')])).toBeUndefined();
  });
});
