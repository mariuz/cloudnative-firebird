import { describe, it, expect, vi, Mock } from 'vitest';
import { KubeConfig } from '@kubernetes/client-node';
import { FirebirdClusterController } from '../src/controllers/firebirdcluster.controller';
import { buildConfigMap } from '../src/utils/resources';
import { RESEED_ANNOTATION, REPLICATION_SCRIPTS } from '../src/utils/replication';
import { FirebirdCluster } from '../src/types';

const makeCluster = (overrides: Partial<FirebirdCluster['spec']> = {}): FirebirdCluster => ({
  apiVersion: 'firebird.cloudnative-firebird.io/v1',
  kind: 'FirebirdCluster',
  metadata: { name: 'db', namespace: 'default', uid: 'c' },
  spec: { instances: 3, storage: { size: '1Gi' }, replication: { enabled: true }, ...overrides },
});

const pod = (name: string, uid: string, opts: { reseed?: boolean; ready?: boolean } = {}) => ({
  metadata: { name, uid, annotations: opts.reseed ? { [RESEED_ANNOTATION]: 'true' } : {} },
  status: { phase: 'Running', conditions: [{ type: 'Ready', status: opts.ready === false ? 'False' : 'True' }] },
});

function setup(pods: object[], reseedData = '') {
  const notFound = Object.assign(new Error('Not Found'), { code: 404 });
  const api: Record<string, Mock> = {
    listNamespacedPod: vi.fn().mockResolvedValue({ items: pods }),
    readNamespacedConfigMap: vi.fn().mockResolvedValue({ data: { reseed: reseedData } }),
    readNamespacedLease: vi.fn().mockResolvedValue({ spec: { holderIdentity: 'db-0' } }),
    readNamespacedJob: vi.fn().mockRejectedValue(notFound),
  };
  const calls: Record<string, Mock> = {};
  const fn = (m: string): Mock =>
    (calls[m] ??=
      api[m] ??
      (m.startsWith('read') || m.startsWith('get')
        ? vi.fn().mockRejectedValue(notFound)
        : m.startsWith('list')
          ? vi.fn().mockResolvedValue({ items: [] })
          : vi.fn().mockResolvedValue({})));
  const kubeConfig = new KubeConfig();
  vi.spyOn(kubeConfig, 'makeApiClient').mockReturnValue(new Proxy({}, { get: (_t, p: string) => fn(p) }) as never);
  const reseedPatched = () => {
    const patches = fn('patchNamespacedConfigMap').mock.calls;
    return patches.length ? patches[patches.length - 1][0].body.data.reseed : undefined;
  };
  return { controller: new FirebirdClusterController(kubeConfig), fn, reseedPatched };
}

describe('replica re-seeding', () => {
  it('lists the request with the pod UID in the ConfigMap before restarting the replica', async () => {
    const { controller, fn, reseedPatched } = setup([pod('db-0', 'u0'), pod('db-1', 'u1'), pod('db-2', 'u2', { reseed: true })]);
    await controller.reconcile(makeCluster());
    expect(reseedPatched()).toBe('db-2 u2\n');
    expect(fn('deleteNamespacedPod')).toHaveBeenCalledWith({ name: 'db-2', namespace: 'default' });
    const cmOrder = fn('patchNamespacedConfigMap').mock.invocationCallOrder[0];
    expect(cmOrder).toBeLessThan(fn('deleteNamespacedPod').mock.invocationCallOrder[0]);
  });

  it('never re-seeds the primary', async () => {
    const { controller, fn, reseedPatched } = setup([pod('db-0', 'u0', { reseed: true }), pod('db-1', 'u1')]);
    await controller.reconcile(makeCluster());
    expect(fn('deleteNamespacedPod')).not.toHaveBeenCalled();
    expect(reseedPatched() ?? '').toBe('');
  });

  it('keeps the request until the new pod is ready, then clears it', async () => {
    // new pod (other UID) still initialising
    const waiting = setup([pod('db-0', 'u0'), pod('db-1', 'u1'), pod('db-2', 'new', { ready: false })], 'db-2 u2\n');
    await waiting.controller.reconcile(makeCluster());
    expect(waiting.fn('deleteNamespacedPod')).not.toHaveBeenCalled();
    expect(waiting.reseedPatched() ?? 'db-2 u2\n').toBe('db-2 u2\n');

    const done = setup([pod('db-0', 'u0'), pod('db-1', 'u1'), pod('db-2', 'new')], 'db-2 u2\n');
    await done.controller.reconcile(makeCluster());
    expect(done.reseedPatched()).toBe('');
  });

  it('does not restart a pod twice for the same request', async () => {
    // the annotated pod is still terminating or not yet deleted after the first reconcile
    const { controller, fn } = setup([pod('db-0', 'u0'), pod('db-2', 'u2', { reseed: true })], 'db-2 u2\n');
    await controller.reconcile(makeCluster());
    expect(fn('deleteNamespacedPod')).not.toHaveBeenCalled();
  });

  it('excludes replicas being re-seeded from the seed sources', async () => {
    const { controller, fn } = setup([pod('db-0', 'u0'), pod('db-1', 'u1'), pod('db-2', 'u2', { reseed: true })]);
    await controller.reconcile(makeCluster());
    const data = fn('patchNamespacedConfigMap').mock.calls[0][0].body.data;
    expect(data['seed-sources']).toBe('db-1.db-headless\n');
  });

  it('always publishes the reseed key so that finished requests are cleared by a merge patch', () => {
    expect(buildConfigMap(makeCluster())?.data?.reseed).toBe('');
    expect(buildConfigMap(makeCluster(), { reseed: { 'db-2': 'u2', 'db-1': 'u1' } })?.data?.reseed).toBe(
      'db-1 u1\ndb-2 u2\n',
    );
  });

  it('init-instance.sh discards only the database and replication state, never on the primary', () => {
    const script = REPLICATION_SCRIPTS['init-instance.sh'];
    expect(script).toContain('rm -f "$DATABASE_PATH" "$STATE_FILE"');
    expect(script).toContain('find "$SOURCE_DIR" "$JOURNAL_DIR" "$ARCHIVE_DIR" -mindepth 1 -maxdepth 1 -exec rm -rf {} +');
    expect(script).toContain('ignoring the re-seed request: this instance is the primary');
    expect(script).not.toMatch(/rm .*system/);
    // the token is recorded only after the seeded database is in place
    const lines = script.split('\n');
    const recorded = lines.findIndex((l) => l.includes('> "$REPLICATION_DIR/.reseeded"'));
    expect(recorded).toBeGreaterThan(lines.findIndex((l) => l.includes('mv "$work" "$DATABASE_PATH"') && l.startsWith('mv')));
  });
});

describe('enabling replication on an existing cluster (init-instance.sh)', () => {
  const script = REPLICATION_SCRIPTS['init-instance.sh'];
  const lines = script.split('\n');
  const at = (text: string) => lines.findIndex((l) => l.includes(text));
  const nothingToDo = at('echo "database exists, nothing to initialise"');

  it('converts before concluding that an existing database needs nothing', () => {
    expect(at('existing primary database now publishes')).toBeGreaterThan(0);
    expect(at('existing primary database now publishes')).toBeLessThan(nothingToDo);
    expect(at('keeping it as $keep')).toBeLessThan(nothingToDo);
  });

  it("only renames databases (resumable), never deletes an instance's own database", () => {
    expect(script).toContain('mv "$DATABASE_PATH" "$keep"');
    expect(script).not.toMatch(/rm [^\n]*\$keep/);
    // an interrupted conversion is resumed before anything looks at the database path
    expect(at('if [ -f "$en" ] && [ ! -f "$DATABASE_PATH" ]')).toBeLessThan(at('existing primary database now publishes'));
  });

  it('does not enable publication twice: a database that already publishes gets its seed from the refresh', () => {
    const check = at('RDB$ACTIVE_FLAG');
    expect(check).toBeGreaterThan(0);
    expect(at('already publishing, without a seed: written by the refresh below')).toBeGreaterThan(check);
    expect(at('offline bootstrap seed refreshed')).toBeLessThan(nothingToDo);
  });
});

describe('refreshing the offline bootstrap seed (init-instance.sh)', () => {
  const script = REPLICATION_SCRIPTS['init-instance.sh'];
  const lines = script.split('\n');
  const at = (text: string) => lines.findIndex((l) => l.includes(text));

  it('refreshes a missing or stale seed on the primary, before the server starts', () => {
    const block = script.slice(script.indexOf('if [ -f "$DATABASE_PATH" ] && is_primary && ! is_replica_db'), script.indexOf('echo "database exists, nothing to initialise"'));
    // stale: the database moved on and the segment after the seed is no longer archived
    expect(block).toContain('if [ "$current" -le "$seed_seq" ] || { [ -n "$first" ] && [ "$first" -le $((seed_seq + 1)) ]; }; then usable=yes; fi');
    expect(block).toContain('write_seed "$DATABASE_PATH"');
    expect(at('offline bootstrap seed refreshed')).toBeLessThan(at('echo "database exists, nothing to initialise"'));
  });

  it('never refreshes while a journal segment is still in use (an unclean stop)', () => {
    const inUse = at('if [ "$usable" = no ] && segment_in_use "$current"; then');
    expect(inUse).toBeGreaterThan(0);
    expect(inUse).toBeLessThan(at('write_seed "$DATABASE_PATH"'));
    // segment header: u16 state at offset 14 (0 = free), u64 sequence at offset 32
    expect(script).toContain('od -An -tu2 -j14 -N2 "$f"');
    expect(script).toContain('od -An -tu8 -j32 -N8 "$f"');
  });

  it('records the seed sequence next to every seed it writes', () => {
    expect(script).toContain('seq_of "$1" > "$REPLICATION_DIR/bootstrap-seed.seq.tmp"');
    expect(script).not.toContain('cp "$work" "$REPLICATION_DIR/bootstrap-seed.fdb.tmp"');
    expect(script).toContain('rm -f "$REPLICATION_DIR/bootstrap-seed.fdb" "$REPLICATION_DIR/bootstrap-seed.seq"');
  });
});
