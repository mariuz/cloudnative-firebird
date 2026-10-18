import { describe, it, expect, vi, type Mock } from 'vitest';
import { KubeConfig } from '@kubernetes/client-node';
import { FirebirdClusterController } from '../src/controllers/firebirdcluster.controller';
import { spawnSync } from 'child_process';
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { FirebirdCluster } from '../src/types';
import { initSqlFiles, initSqlRefsErrors, initSqlVolume } from '../src/utils/init-sql';
import { buildStatefulSet } from '../src/utils/resources';
import { validateClusterSpec } from '../src/utils/validation';
import { makeCluster, notFoundError } from './helpers/factories';

const refs = {
  secretRefs: [{ name: 'app-users', key: 'users.sql' }, { name: 'app-grants', key: 'grants.sql' }],
  configMapRefs: [{ name: 'app-schema', key: 'schema.sql' }],
};
const withRefs = (extra: Partial<FirebirdCluster['spec']> = {}, initSql?: string) =>
  makeCluster({ ...extra, bootstrap: { ...(initSql ? { initSql } : {}), initSqlRefs: refs } });

describe('init SQL from Secrets and ConfigMaps', () => {
  it('projects initSql, then the Secrets, then the ConfigMaps, as numbered files', () => {
    const cluster = withRefs({}, 'CREATE TABLE t (id INT);');
    expect(initSqlFiles(cluster)).toEqual(['00-init.sql', '10-secret-00.sql', '10-secret-01.sql', '20-configmap-00.sql']);
    // the order a directory listing gives
    expect([...initSqlFiles(cluster)].sort()).toEqual(initSqlFiles(cluster));
    const volume = initSqlVolume(cluster)!;
    expect(volume.projected!.defaultMode).toBe(0o440);
    expect(volume.projected!.sources).toEqual([
      { configMap: { name: 'test-cluster-config', items: [{ key: 'init.sql', path: '00-init.sql' }] } },
      { secret: { name: 'app-users', items: [{ key: 'users.sql', path: '10-secret-00.sql' }], optional: true } },
      { secret: { name: 'app-grants', items: [{ key: 'grants.sql', path: '10-secret-01.sql' }], optional: true } },
      { configMap: { name: 'app-schema', items: [{ key: 'schema.sql', path: '20-configmap-00.sql' }], optional: true } },
    ]);
    expect(initSqlVolume(makeCluster())).toBeUndefined();
    expect(initSqlFiles(withRefs())).toEqual(['10-secret-00.sql', '10-secret-01.sql', '20-configmap-00.sql']);
  });

  it("go to the entrypoint's directory without replication", () => {
    const pod = buildStatefulSet(withRefs()).spec!.template.spec!;
    expect(pod.volumes!.find((v) => v.name === 'init-sql')).toBeDefined();
    const firebird = pod.containers.find((c) => c.name === 'firebird')!;
    expect(firebird.volumeMounts).toContainEqual({ name: 'init-sql', mountPath: '/docker-entrypoint-initdb.d', readOnly: true });
  });

  it('go to the replication init container with replication, not to the entrypoint', () => {
    const pod = buildStatefulSet(withRefs({ replication: { enabled: true } })).spec!.template.spec!;
    const init = pod.initContainers!.find((c) => c.name === 'replication-init')!;
    expect(init.volumeMounts).toContainEqual({ name: 'init-sql', mountPath: '/firebird-init-sql', readOnly: true });
    expect(init.env).toContainEqual({ name: 'INIT_SQL_DIR', value: '/firebird-init-sql' });
    const firebird = pod.containers.find((c) => c.name === 'firebird')!;
    expect(firebird.volumeMounts!.some((m) => m.name === 'init-sql')).toBe(false);
    // without init SQL nothing changes
    const plain = buildStatefulSet(makeCluster({ replication: { enabled: true } })).spec!.template.spec!;
    expect(plain.volumes!.some((v) => v.name === 'init-sql')).toBe(false);
    expect(plain.initContainers!.find((c) => c.name === 'replication-init')!.env!.some((e) => e.name === 'INIT_SQL_DIR')).toBe(false);
  });

  it('are run in order by the replication init on a created database', { timeout: 20000 }, () => {
    // the init-instance.sh fragment, against a fake isql that records what it runs
    const script = readFileSync(join(__dirname, '..', 'src', 'replication', 'init-instance.sh'), 'utf8');
    const start = script.indexOf('    # bootstrap.initSql, then initSqlRefs');
    const fragment = script.slice(start, script.indexOf('    write_seed "$work"', start));
    const dir = mkdtempSync(join(tmpdir(), 'init-sql-'));
    mkdirSync(join(dir, 'sql'));
    for (const f of ['20-configmap-00.sql', '00-init.sql', '10-secret-00.sql']) writeFileSync(join(dir, 'sql', f), `-- ${f}\n`);
    writeFileSync(join(dir, 'isql'), `#!/bin/sh\necho "$4 $3" >> "${dir}/ran"\n`);
    chmodSync(join(dir, 'isql'), 0o755);
    const run = (env: Record<string, string>) =>
      spawnSync('sh', ['-c', `origin=created; work=/data/.init.fdb; SCRIPT_DIR=${dir}/scripts\n${fragment}`], {
        env: { ...process.env, PATH: `${dir}:${process.env.PATH}`, ...env },
        encoding: 'utf8',
      });
    const r = run({ INIT_SQL_DIR: join(dir, 'sql') });
    expect(r.status).toBe(0);
    expect(readFileSync(join(dir, 'ran'), 'utf8').trim().split('\n')).toEqual(
      ['00-init.sql', '10-secret-00.sql', '20-configmap-00.sql'].map((f) => `/data/.init.fdb ${join(dir, 'sql', f)}`),
    );
    expect(r.stdout).toContain('running 10-secret-00.sql');
    // pods from before: the cluster ConfigMap's init.sql
    mkdirSync(join(dir, 'scripts'));
    writeFileSync(join(dir, 'scripts', 'init.sql'), '-- old\n');
    writeFileSync(join(dir, 'ran'), '');
    expect(run({}).status).toBe(0);
    expect(readFileSync(join(dir, 'ran'), 'utf8').trim()).toBe(`/data/.init.fdb ${join(dir, 'scripts', 'init.sql')}`);
  });

  it('validates the references', () => {
    expect(initSqlRefsErrors(withRefs())).toEqual([]);
    expect(() => validateClusterSpec(withRefs())).not.toThrow();
    const bad = (initSqlRefs: object) => makeCluster({ bootstrap: { initSqlRefs: initSqlRefs as never } });
    expect(initSqlRefsErrors(bad({ secretRefs: [{ name: 'Bad_Name', key: 'k' }] }))).toEqual(['bootstrap.initSqlRefs.secretRefs[0].name must be a DNS subdomain name']);
    expect(initSqlRefsErrors(bad({ configMapRefs: [{ name: 'ok' }] }))).toEqual(['bootstrap.initSqlRefs.configMapRefs[0].key must be a ConfigMap key']);
    expect(initSqlRefsErrors(bad({ secretRefs: [{ name: 'ok', key: 'a/b' }] }))).toEqual(['bootstrap.initSqlRefs.secretRefs[0].key must be a Secret key']);
    expect(initSqlRefsErrors(bad({ configMapRefs: 'x' }))).toEqual(['bootstrap.initSqlRefs.configMapRefs must be a list']);
    expect(initSqlRefsErrors(bad({ secretRefs: Array.from({ length: 51 }, (_, i) => ({ name: `s${i}`, key: 'k' })) }))).toEqual([
      'bootstrap.initSqlRefs.secretRefs: at most 50 references',
    ]);
    expect(() => validateClusterSpec(bad({ secretRefs: [{ name: '', key: 'k' }] }))).toThrow('secretRefs[0].name');
  });
});

describe('the StatefulSet with init SQL references', () => {
  const mocks = (overrides: Record<string, Mock>) => {
    const calls: Record<string, Mock> = {};
    const fn = (m: string): Mock =>
      (calls[m] ??=
        overrides[m] ??
        (m.startsWith('read') || m.startsWith('get') ? vi.fn().mockRejectedValue(notFoundError) : m.startsWith('list') ? vi.fn().mockResolvedValue({ items: [] }) : vi.fn().mockResolvedValue({})));
    const kubeConfig = new KubeConfig();
    vi.spyOn(kubeConfig, 'makeApiClient').mockReturnValue(new Proxy({}, { get: (_t, p: string) => fn(p) }) as never);
    return { kubeConfig, api: fn };
  };

  it('is created only once every referenced key exists', async () => {
    const missing = mocks({
      readNamespacedSecret: vi.fn().mockImplementation(async ({ name }) => (name === 'app-users' ? { data: { 'users.sql': 'eA==' } } : Promise.reject(notFoundError))),
      readNamespacedConfigMap: vi.fn().mockImplementation(async ({ name }) => (name === 'app-schema' ? { data: { other: 'x' } } : Promise.reject(notFoundError))),
    });
    await expect(new FirebirdClusterController(missing.kubeConfig).reconcile(withRefs())).rejects.toThrow(
      'bootstrap.initSqlRefs: Secret app-grants does not exist; ConfigMap app-schema has no key schema.sql',
    );
    expect(missing.api('createNamespacedStatefulSet')).not.toHaveBeenCalled();
    const events = missing.api('createNamespacedEvent').mock.calls.map((c) => c[0].body);
    expect(events.some((e) => e.reason === 'ReconcileFailed' && e.message.includes('app-grants'))).toBe(true);

    const present = mocks({
      readNamespacedSecret: vi.fn().mockImplementation(async ({ name }) => ({ data: { 'users.sql': 'eA==', 'grants.sql': 'eA==', name } })),
      readNamespacedConfigMap: vi.fn().mockImplementation(async ({ name }) =>
        name === 'app-schema' ? { data: { 'schema.sql': 'x' } } : Promise.reject(notFoundError),
      ),
    });
    await new FirebirdClusterController(present.kubeConfig).reconcile(withRefs());
    expect(present.api('createNamespacedStatefulSet')).toHaveBeenCalledTimes(1);
  });
});
