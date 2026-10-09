import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { spawn, spawnSync, ChildProcess } from 'child_process';
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, utimesSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { createServer, Server, Socket } from 'net';
import { signLine } from './helpers/segment-auth';
import { REPLICATION_SCRIPTS } from '../src/utils/replication';

/** One request to a segment server (the shared client is mocked in unit tests) */
const rawRequest = (port: number, line: string): Promise<string[]> =>
  new Promise((resolve, reject) => {
    const socket = new Socket();
    let data = '';
    socket.once('error', reject);
    socket.on('data', (chunk) => (data += chunk.toString()));
    socket.once('end', () => resolve(data.split('\n').filter((l) => l !== '')));
    socket.connect(port, '127.0.0.1', () => socket.write(`${line}\n`));
  });
/** "<token> <request>", signed with the token as the clients send it */
const segmentRequest = (port: number, line: string): Promise<string[]> => rawRequest(port, signLine(line));

const hasPerl = spawnSync('perl', ['-v']).status === 0;

/** A port no other server listens on */
const freePort = (): Promise<number> =>
  new Promise((resolve, reject) => {
    const probe = createServer();
    probe.once('error', reject);
    probe.listen(0, () => {
      const { port } = probe.address() as { port: number };
      probe.close(() => resolve(port));
    });
  });

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

  describe('a primary nothing reaches any more (CONTACT_TIMEOUT_SECONDS)', () => {
    const minutesAgo = (m: number) => Math.floor(Date.now() / 1000) - m * 60;
    /** API server reachable; the headless Service lists another instance (localhost) */
    const env = (ws: ReturnType<typeof workspace>, extra: Record<string, string> = {}) => ({
      KUBERNETES_SERVICE_HOST: '127.0.0.1',
      KUBERNETES_SERVICE_PORT: port,
      PEERS_SERVICE: 'localhost',
      SEGMENT_PORT: closedPort,
      CONTACT_TIMEOUT_SECONDS: '60',
      LAST_CONTACT_FILE: join(ws.dir, 'last-contact'),
      LAST_CONTACT_BASE: String(minutesAgo(10)),
      ...extra,
    });
    const contactedAt = (ws: ReturnType<typeof workspace>, epoch: number) => {
      writeFileSync(join(ws.dir, 'last-contact'), `${epoch}\n`);
      utimesSync(join(ws.dir, 'last-contact'), epoch, epoch);
    };

    it('fences it although it reaches the API server', () => {
      if (!hasPerl) return;
      const ws = workspace();
      contactedAt(ws, minutesAgo(2));
      expect(check(ws, env(ws))).toMatch(/neither the operator nor any replica has reached this primary for 1[0-9]{2}s; fencing/);
      expect(ws.calls()).toHaveLength(1);
      expect(existsSync(ws.marker)).toBe(true);
    });

    it('leaves it alone while the operator or a replica reached it recently', () => {
      if (!hasPerl) return;
      const ws = workspace();
      contactedAt(ws, minutesAgo(0));
      check(ws, env(ws));
      expect(ws.calls()).toEqual([]);
    });

    it('counts from when the instance became the primary, not from an earlier contact', () => {
      if (!hasPerl) return;
      const ws = workspace();
      contactedAt(ws, minutesAgo(5));
      check(ws, env(ws, { LAST_CONTACT_BASE: String(minutesAgo(0)) }));
      expect(ws.calls()).toEqual([]);
    });

    describe('when cluster DNS fails', () => {
      const cache = (ws: ReturnType<typeof workspace>) => join(ws.dir, 'self-fenced.peers');

      it('remembers the peers of every answer', () => {
        if (!hasPerl) return;
        const ws = workspace();
        contactedAt(ws, minutesAgo(0));
        check(ws, env(ws));
        expect(readFileSync(cache(ws), 'utf8')).toBe('127.0.0.1\n');
      });

      it('fences a primary nothing reaches when none of the peers it knew answers either', () => {
        if (!hasPerl) return;
        const ws = workspace();
        writeFileSync(cache(ws), '127.0.0.1\n');
        contactedAt(ws, minutesAgo(2));
        // a DNS server that never answers: the lookup is given up after DNS_TIMEOUT
        const started = Date.now();
        const out = check(ws, env(ws, { TEST_DNS: 'hang', DNS_TIMEOUT: '1' }));
        expect(Date.now() - started).toBeLessThan(5_000);
        expect(out).toMatch(/cluster DNS did not answer for localhost: using the 1 peer address\(es\) known/);
        expect(out).toMatch(/has reached this primary for 1[0-9]{2}s \(cluster DNS failing, and no known peer answers\); fencing/);
        expect(existsSync(ws.marker)).toBe(true);
        // the cache is kept for the next check
        expect(readFileSync(cache(ws), 'utf8')).toBe('127.0.0.1\n');
      });

      it('leaves it alone while a peer it knew still answers: a DNS outage alone', () => {
        if (!hasPerl) return;
        const ws = workspace();
        writeFileSync(cache(ws), '127.0.0.1\n');
        contactedAt(ws, minutesAgo(2));
        check(ws, env(ws, { TEST_DNS: 'fail', SEGMENT_PORT: port }));
        expect(ws.calls()).toEqual([]);
      });

      it('also goes by the addresses the operator publishes: a peer that restarted with a new address', () => {
        if (!hasPerl) return;
        const ws = workspace();
        // the last answer is stale (an address nothing answers on any more) ...
        writeFileSync(cache(ws), '192.0.2.1\n');
        // ... the operator publishes the current ones, this pod's own among them
        writeFileSync(join(ws.dir, 'peer-addresses'), '127.0.0.1\n');
        contactedAt(ws, minutesAgo(2));
        const out = check(ws, env(ws, { TEST_DNS: 'fail', SEGMENT_PORT: port, PEER_ADDRESSES_FILE: join(ws.dir, 'peer-addresses') }));
        expect(out).toMatch(/using the 2 peer address\(es\) known from the last answer and the operator/);
        expect(ws.calls()).toEqual([]);
        // its own address does not count
        const own = workspace();
        writeFileSync(cache(own), '192.0.2.1\n');
        writeFileSync(join(own.dir, 'peer-addresses'), '127.0.0.1\n');
        contactedAt(own, minutesAgo(2));
        check(own, env(own, { TEST_DNS: 'fail', SEGMENT_PORT: port, POD_IP: '127.0.0.1', PEER_ADDRESSES_FILE: join(own.dir, 'peer-addresses') }));
        expect(existsSync(own.marker)).toBe(true);
      });

      it('without known peers, goes by the operator\'s list of ready replicas', () => {
        if (!hasPerl) return;
        const listed = workspace();
        writeFileSync(join(listed.dir, 'seed-sources'), 'db-1.db-headless\n');
        contactedAt(listed, minutesAgo(2));
        check(listed, env(listed, { TEST_DNS: 'fail', SEED_SOURCES_FILE: join(listed.dir, 'seed-sources') }));
        expect(existsSync(listed.marker)).toBe(true);
        const none = workspace();
        writeFileSync(join(none.dir, 'seed-sources'), '');
        contactedAt(none, minutesAgo(2));
        check(none, env(none, { TEST_DNS: 'fail', SEED_SOURCES_FILE: join(none.dir, 'seed-sources') }));
        expect(none.calls()).toEqual([]);
      });

      it('trusts an answer of "no such name": no peers', () => {
        if (!hasPerl) return;
        const ws = workspace();
        writeFileSync(cache(ws), '127.0.0.1\n');
        contactedAt(ws, minutesAgo(2));
        check(ws, env(ws, { PEERS_SERVICE: 'no-such-peers.invalid' }));
        expect(ws.calls()).toEqual([]);
        expect(readFileSync(cache(ws), 'utf8')).toBe('');
      });
    });

    it('leaves a primary without replicas alone, and does nothing when off', () => {
      if (!hasPerl) return;
      const alone = workspace();
      contactedAt(alone, minutesAgo(5));
      // the only address of the headless Service is this pod's own
      check(alone, env(alone, { POD_IP: '127.0.0.1' }));
      expect(alone.calls()).toEqual([]);
      const off = workspace();
      contactedAt(off, minutesAgo(5));
      check(off, env(off, { CONTACT_TIMEOUT_SECONDS: '0' }));
      expect(off.calls()).toEqual([]);
    });
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
    const port = await freePort();
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
    // only authenticated requests count as contact (the isolation check's LAST_CONTACT_FILE)
    for (let i = 0; ; i++) {
      try {
        expect((await segmentRequest(port, 'wrong ISOLATION'))[0]).toMatch(/^ERR unauthorized/);
        break;
      } catch (err) {
        if (i > 50) throw err;
        await new Promise((r) => setTimeout(r, 100));
      }
    }
    expect(existsSync(join(base, 'last-contact'))).toBe(false);
    expect(await ask('ISOLATION')).toEqual(['OK online']);
    expect(Number(readFileSync(join(base, 'last-contact'), 'utf8'))).toBeGreaterThan(Date.now() / 1000 - 30);
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
