import { describe, it, expect } from 'vitest';
import { buildConfigMap, buildStatefulSet, replicationConfigHash } from '../src/utils/resources';
import { buildReplicationConf, REPLICATED_DATABASES_ANNOTATION } from '../src/utils/replication';
import { FirebirdCluster } from '../src/types';
import { makeCluster } from './helpers/factories';

/** docs/multi-database-replication.md, phase 2: what the instances get for the declarative databases */
function annotated(databases?: string[], annotation = 'experimental'): FirebirdCluster {
  const cluster = makeCluster({ instances: 3, replication: { enabled: true } });
  cluster.metadata.annotations = { [REPLICATED_DATABASES_ANNOTATION]: annotation };
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
    // annotated without databases yet: an empty list, the same pods
    expect(buildConfigMap(annotated(), { primaryPod: 'test-cluster-0' })?.data?.['replicated-databases']).toBe('');
  });

  it('rolls the instances when the list changes', () => {
    expect(replicationConfigHash(annotated(['app.fdb']))).not.toBe(replicationConfigHash(annotated()));
    expect(replicationConfigHash(annotated(['app.fdb', 'sales.fdb']))).not.toBe(replicationConfigHash(annotated(['app.fdb'])));
  });

  it('changes nothing without the annotation, or with another value', () => {
    for (const cluster of [makeCluster({ instances: 3, replication: { enabled: true } }), annotated(['app.fdb'], 'yes')]) {
      cluster.status = { ...(cluster.status ?? {}), replicatedDatabases: ['app.fdb'] };
      expect(buildConfigMap(cluster, { primaryPod: 'test-cluster-0' })?.data?.['replicated-databases']).toBeUndefined();
      expect(env(cluster, 'segment-server').REPLICATED_DATABASES_FILE).toBeUndefined();
      expect(replicationInit(cluster).command).toEqual(['sh', '/etc/firebird-operator/init-instance.sh']);
      expect(buildReplicationConf(cluster, '/d/mydb.fdb', '/d')).not.toContain('app.fdb');
    }
    expect(replicationConfigHash(makeCluster({ instances: 3, replication: { enabled: true } }))).toBe(
      replicationConfigHash(annotated(['app.fdb'], 'yes')),
    );
  });
});
