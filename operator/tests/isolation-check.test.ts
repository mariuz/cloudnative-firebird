import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { spawn, spawnSync, ChildProcess } from 'child_process';
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { createServer, Server, Socket } from 'net';
import { REPLICATION_SCRIPTS } from '../src/utils/replication';

/** One request to a segment server (the shared client is mocked in unit tests) */
const segmentRequest = (port: number, line: string): Promise<string[]> =>
  new Promise((resolve, reject) => {
    const socket = new Socket();
    let data = '';
    socket.once('error', reject);
    socket.on('data', (chunk) => (data += chunk.toString()));
    socket.once('end', () => resolve(data.split('\n').filter((l) => l !== '')));
    socket.connect(port, '127.0.0.1', () => socket.write(`${line}\n`));
  });

const hasPerl = spawnSync('perl', ['-v']).status === 0;

/**
 * A directory with the scripts and a fake fbsvcmgr: it logs its arguments, answers header
 * statistics with the state in "<dir>/state" (online or shutdown), and fails when "<dir>/fail"
 * exists.
 */
function workspace() {
  const dir = mkdtempSync(join(tmpdir(), 'fb-isolation-'));
  for (const name of ['isolation-check.pl', 'segment-server.pl']) writeFileSync(join(dir, name), REPLICATION_SCRIPTS[name]);
  mkdirSync(join(dir, 'bin'));
  const fake = join(dir, 'bin', 'fbsvcmgr');
  writeFileSync(
    fake,
    `#!/bin/sh
echo "$*" >> "${dir}/calls"
[ -f "${dir}/fail" ] && exit 1
case "$*" in
  *sts_hdr_pages*) [ "$(cat "${dir}/state" 2>/dev/null)" = shutdown ] && echo "Attributes force write, full shutdown" || echo "Attributes force write";;
esac
exit 0
`,
  );
  chmodSync(fake, 0o755);
  writeFileSync(join(dir, 'primary'), 'db-0.db-headless\n');
  const calls = () => (existsSync(join(dir, 'calls')) ? readFileSync(join(dir, 'calls'), 'utf8').trim().split('\n') : []);
  return { dir, calls, marker: join(dir, 'self-fenced') };
}

/** Runs one isolation check */
function check(ws: ReturnType<typeof workspace>, env: Record<string, string>): string {
  const r = spawnSync('perl', [join(ws.dir, 'isolation-check.pl')], {
    env: {
      PATH: `${join(ws.dir, 'bin')}:${process.env.PATH}`,
      DATABASE_PATH: '/data/db.fdb',
      PRIMARY_FILE: join(ws.dir, 'primary'),
      SELF_FENCED_FILE: ws.marker,
      POD_NAME: 'db-0',
      ISOLATION_TIMEOUT_SECONDS: '20',
      CONNECT_TIMEOUT: '1',
      ONCE: 'true',
      ...env,
    },
    encoding: 'utf8',
  });
  expect(r.status).toBe(0);
  return r.stdout;
}

const longAgo = String(Math.floor(Date.now() / 1000) - 60);
// a port nothing listens on
const closedPort = '1';

describe('isolation-check.pl', () => {
  let listener: Server;
  let port: string;
  beforeAll(async () => {
    listener = createServer((s) => s.end());
    await new Promise<void>((resolve) => listener.listen(0, '127.0.0.1', resolve));
    port = String((listener.address() as { port: number }).port);
  });
  afterAll(() => listener.close());

  it('fences a primary that reached neither the API server nor another instance for the timeout', () => {
    if (!hasPerl) return;
    const ws = workspace();
    const out = check(ws, { KUBERNETES_SERVICE_HOST: '127.0.0.1', KUBERNETES_SERVICE_PORT: closedPort, LAST_CONNECTED: longAgo });
    expect(out).toMatch(/fencing the primary/);
    expect(ws.calls()).toEqual([
      'localhost:service_mgr action_properties dbname /data/db.fdb prp_shutdown_mode prp_sm_full prp_force_shutdown 0',
    ]);
    expect(existsSync(ws.marker)).toBe(true);
  });

  it('waits for the timeout before fencing', () => {
    if (!hasPerl) return;
    const ws = workspace();
    const out = check(ws, { KUBERNETES_SERVICE_HOST: '127.0.0.1', KUBERNETES_SERVICE_PORT: closedPort });
    expect(out).toMatch(/isolated for \d+s \(fencing after 20s\)/);
    expect(ws.calls()).toEqual([]);
  });

  it('leaves a primary alone while it reaches the API server', () => {
    if (!hasPerl) return;
    const ws = workspace();
    check(ws, { KUBERNETES_SERVICE_HOST: '127.0.0.1', KUBERNETES_SERVICE_PORT: port, LAST_CONNECTED: longAgo });
    expect(ws.calls()).toEqual([]);
    expect(existsSync(ws.marker)).toBe(false);
  });

  it('leaves a primary alone while it reaches another instance, but does not count itself', () => {
    if (!hasPerl) return;
    const peers = { PEERS_SERVICE: 'localhost', SEGMENT_PORT: port, LAST_CONNECTED: longAgo };
    const ws = workspace();
    check(ws, peers);
    expect(ws.calls()).toEqual([]);
    // the only address of the Service is this pod's own
    const alone = workspace();
    check(alone, { ...peers, POD_IP: '127.0.0.1' });
    expect(alone.calls()).toHaveLength(1);
  });

  it('never fences a replica', () => {
    if (!hasPerl) return;
    const ws = workspace();
    writeFileSync(join(ws.dir, 'primary'), 'db-1.db-headless\n');
    check(ws, { LAST_CONNECTED: longAgo });
    expect(ws.calls()).toEqual([]);
  });

  it('forgets a fence it could not apply, and one lifted by bringing the database online', () => {
    if (!hasPerl) return;
    const ws = workspace();
    writeFileSync(join(ws.dir, 'fail'), '');
    expect(check(ws, { LAST_CONNECTED: longAgo })).toMatch(/full shutdown failed/);
    expect(existsSync(ws.marker)).toBe(false);

    const fenced = workspace();
    writeFileSync(fenced.marker, '1700000000\n');
    writeFileSync(join(fenced.dir, 'state'), 'shutdown');
    check(fenced, { LAST_CONNECTED: longAgo });
    expect(existsSync(fenced.marker)).toBe(true);
    writeFileSync(join(fenced.dir, 'state'), 'online');
    expect(check(fenced, {})).toMatch(/isolation fence lifted/);
    expect(existsSync(fenced.marker)).toBe(false);
  });

  it('does nothing when disabled', () => {
    if (!hasPerl) return;
    const ws = workspace();
    check(ws, { ISOLATION_TIMEOUT_SECONDS: '0', LAST_CONNECTED: longAgo });
    expect(ws.calls()).toEqual([]);
  });
});

describe('segment server ISOLATION and REJOIN', () => {
  let server: ChildProcess | undefined;
  afterAll(() => server?.kill());

  it('reports the isolation fence and lifts it on request', { timeout: 30_000 }, async () => {
    if (!hasPerl) return;
    const ws = workspace();
    const base = join(ws.dir, 'repl');
    for (const d of ['archive', 'source']) mkdirSync(join(base, d), { recursive: true });
    const port = 41000 + Math.floor(Math.random() * 2000);
    server = spawn('perl', [join(ws.dir, 'segment-server.pl')], {
      env: {
        PATH: `${join(ws.dir, 'bin')}:${process.env.PATH}`,
        ARCHIVE_DIR: join(base, 'archive'),
        SOURCE_DIR: join(base, 'source'),
        REPLICATION_DIR: base,
        DATABASE_PATH: '/data/db.fdb',
        PRIMARY_FILE: join(ws.dir, 'primary'),
        POD_NAME: 'db-0',
        ISC_PASSWORD: 'tok',
        SEGMENT_PORT: String(port),
        SCRIPT_DIR: ws.dir,
      },
    });
    const ask = async (line: string) => {
      for (let i = 0; ; i++) {
        try {
          return await segmentRequest(port, `tok ${line}`);
        } catch (err) {
          if (i > 50) throw err;
          await new Promise((r) => setTimeout(r, 100));
        }
      }
    };
    expect(await ask('ISOLATION')).toEqual(['OK online']);
    expect(await ask('REJOIN')).toEqual(['OK']);
    expect(ws.calls()).toEqual([]);

    writeFileSync(join(base, 'self-fenced'), '1700000000\n');
    expect(await ask('ISOLATION')).toEqual(['OK fenced 1700000000']);
    writeFileSync(join(ws.dir, 'fail'), '');
    expect((await ask('REJOIN'))[0]).toMatch(/^ERR cannot bring/);
    expect(existsSync(join(base, 'self-fenced'))).toBe(true);
    spawnSync('rm', [join(ws.dir, 'fail')]);
    expect(await ask('REJOIN')).toEqual(['OK']);
    expect(ws.calls().pop()).toBe('localhost:service_mgr action_properties dbname /data/db.fdb prp_online_mode prp_sm_normal');
    expect(existsSync(join(base, 'self-fenced'))).toBe(false);
    expect(await ask('ISOLATION')).toEqual(['OK online']);
  });
});
