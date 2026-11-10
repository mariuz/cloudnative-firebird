import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { ChildProcess, spawn, spawnSync } from 'child_process';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'fs';
import { createServer, Server, Socket } from 'net';
import { tmpdir } from 'os';
import { join } from 'path';
import { REPLICATION_SCRIPTS } from '../src/utils/replication';
import { fakeSegmentServer } from './helpers/segment-auth';

/**
 * The replicas' segment puller has two implementations: segment-puller.pl and `fb-agent puller`
 * (docs/rust-agent-roadmap.md, Phase 2). The same scenarios run against both, with a fake primary:
 * what they fetch, the files and flags they leave, and their log lines must be the same.
 *
 * The Rust one needs the fb-agent binary (FB_AGENT_BIN, or a build in agent/target) and is skipped
 * without it; the "Rust agent" CI job runs this file with it.
 */
const agentBin = [
  process.env.FB_AGENT_BIN,
  ...['x86_64-unknown-linux-musl/release', 'aarch64-unknown-linux-musl/release', 'release', 'debug'].map((d) =>
    join(__dirname, '..', 'agent', 'target', d, 'fb-agent'),
  ),
].find((p) => p && existsSync(p));
const hasPerl = spawnSync('perl', ['-v']).status === 0;

const implementations = [
  { name: 'Perl (segment-puller.pl)', available: hasPerl, command: (dir: string) => ['perl', join(dir, 'segment-puller.pl')] },
  { name: 'Rust (fb-agent puller)', available: Boolean(agentBin), command: () => [agentBin!, 'puller'] },
];

describe.each(implementations)('segment puller: $name', ({ available, command }) => {
  let dir: string;
  let primary: Server;
  let port: number;
  let puller: ChildProcess | undefined;
  let out: string;
  /** the fake primary: its archived segments, its SYNCTO answer, and the requests it got */
  let segments: Record<string, Buffer>;
  let syncTo: string;
  let listReply: string | undefined;
  let requests: string[];
  const extra: Server[] = [];

  const source = () => join(dir, 'source');
  const read = (name: string) => (existsSync(join(dir, name)) ? readFileSync(join(dir, name), 'utf8') : undefined);
  const waitFor = async (check: () => boolean, what: string, ms = 8000) => {
    const end = Date.now() + ms;
    while (!check()) {
      if (Date.now() > end) throw new Error(`timed out waiting for ${what}; output: ${out}; requests: ${requests.join(' | ')}`);
      await new Promise((r) => setTimeout(r, 50));
    }
  };
  /** a number of further polls of the primary (LIST requests) */
  const polls = async (n: number) => {
    const from = requests.filter((r) => r === 'LIST').length;
    await waitFor(() => requests.filter((r) => r === 'LIST').length >= from + n, `${n} more polls`);
  };
  const start = (env: Record<string, string> = {}) => {
    const [cmd, ...args] = command(dir);
    puller = spawn(cmd, args, {
      env: {
        PATH: process.env.PATH ?? '',
        SOURCE_DIR: source(),
        STATE_FILE: join(dir, '.last-pulled'),
        PRIMARY_FILE: join(dir, 'primary'),
        REPLICATION_DIR: dir,
        POD_NAME: 'db-1',
        ISC_PASSWORD: 'tok',
        SEGMENT_PORT: String(port),
        POLL_SECONDS: '1',
        // nothing listens there: the "Firebird server" of the pod has stopped
        FIREBIRD_PORT: '1',
        ...env,
      },
    });
    puller.stdout!.on('data', (d) => (out += d.toString()));
    puller.stderr!.on('data', (d) => (out += d.toString()));
  };

  beforeEach(async () => {
    dir = mkdtempSync(join(tmpdir(), 'fb-puller-'));
    mkdirSync(source());
    writeFileSync(join(dir, 'segment-puller.pl'), REPLICATION_SCRIPTS['segment-puller.pl']);
    writeFileSync(join(dir, 'primary'), '127.0.0.1\n');
    out = '';
    requests = [];
    syncTo = 'none';
    listReply = undefined;
    segments = {
      'mydb.fdb.journal-000000007': Buffer.from('seven'),
      'mydb.fdb.journal-000000008': Buffer.alloc(200_000, 8),
    };
    primary = fakeSegmentServer((request: string, sock: Socket) => {
      requests.push(request);
      if (request === 'LIST') return void sock.end(listReply ?? `${Object.keys(segments).join('\n')}\nnot-a-segment\n.\n`);
      if (request === 'SYNCTO') return void sock.end(`OK ${syncTo}\n`);
      const name = /^GET (\S+)$/.exec(request)?.[1];
      if (name && segments[name]) {
        sock.write(`OK ${segments[name].length}\n`);
        return void sock.end(segments[name]);
      }
      sock.end('ERR bad request\n');
    }, 'tok');
    await new Promise<void>((r) => primary.listen(0, '127.0.0.1', r));
    port = (primary.address() as { port: number }).port;
  });
  afterEach(() => {
    puller?.kill('SIGKILL');
    puller = undefined;
    primary.close();
    for (const s of extra.splice(0)) s.close();
    rmSync(dir, { recursive: true, force: true });
  });

  it.skipIf(!available)('fetches the new segments in order, and records its position and the contact', async () => {
    start();
    await waitFor(() => read('.last-pulled') === 'mydb.fdb.journal-000000008\n', 'both segments');
    expect(readdirSync(source()).sort()).toEqual(['mydb.fdb.journal-000000007', 'mydb.fdb.journal-000000008']);
    expect(readFileSync(join(source(), 'mydb.fdb.journal-000000007'), 'utf8')).toBe('seven');
    expect(readFileSync(join(source(), 'mydb.fdb.journal-000000008')).equals(segments['mydb.fdb.journal-000000008'])).toBe(true);
    expect(requests.slice(0, 4)).toEqual(['LIST', 'SYNCTO', 'GET mydb.fdb.journal-000000007', 'GET mydb.fdb.journal-000000008']);
    expect(out).toContain(`segment puller: source=${source()} primary-file=${join(dir, 'primary')}\n`);
    expect(out).toContain('fetched mydb.fdb.journal-000000007 from 127.0.0.1\nfetched mydb.fdb.journal-000000008 from 127.0.0.1\n');
    const [at, host] = read('primary-seen')!.trim().split(' ');
    expect(Math.abs(Number(at) - Date.now() / 1000)).toBeLessThan(10);
    expect(host).toBe('127.0.0.1');
    // nothing is fetched twice; a new segment is fetched at the next poll
    await polls(2);
    expect(requests.filter((r) => r.startsWith('GET'))).toHaveLength(2);
    segments['mydb.fdb.journal-000000009'] = Buffer.from('nine');
    await waitFor(() => read('.last-pulled') === 'mydb.fdb.journal-000000009\n', 'the third segment');
    expect(requests.filter((r) => r.startsWith('GET'))).toEqual([
      'GET mydb.fdb.journal-000000007',
      'GET mydb.fdb.journal-000000008',
      'GET mydb.fdb.journal-000000009',
    ]);
  });

  it.skipIf(!available)('continues after its recorded position', async () => {
    writeFileSync(join(dir, '.last-pulled'), 'mydb.fdb.journal-000000007\n');
    start();
    await waitFor(() => read('.last-pulled') === 'mydb.fdb.journal-000000008\n', 'segment 8');
    expect(readdirSync(source())).toEqual(['mydb.fdb.journal-000000008']);
  });

  it.skipIf(!available)('idles on the primary itself, and once promoted in place', async () => {
    writeFileSync(join(dir, 'primary'), 'db-1.db-headless\n');
    start();
    await new Promise((r) => setTimeout(r, 1500));
    expect(requests).toEqual([]);
    // another instance is the primary, but this one was promoted in place
    writeFileSync(join(dir, 'promoted'), '1\n');
    writeFileSync(join(dir, 'primary'), '127.0.0.1\n');
    await new Promise((r) => setTimeout(r, 1500));
    expect(requests).toEqual([]);
    // a name that only begins like this pod's is another instance
    rmSync(join(dir, 'promoted'));
    writeFileSync(join(dir, 'primary'), 'db-10.invalid\n');
    await waitFor(() => /pull failed: connect db-10\.invalid:\d+: /.test(out), 'a failed connection to db-10');
  });

  it.skipIf(!available)('pauses while the local segment server takes a copy, and says so', async () => {
    writeFileSync(join(dir, '.pause-pull'), '');
    start();
    await waitFor(() => existsSync(join(dir, '.pull-paused')), 'the pause acknowledgement');
    expect(requests).toEqual([]);
    rmSync(join(dir, '.pause-pull'));
    await waitFor(() => read('.last-pulled') === 'mydb.fdb.journal-000000008\n', 'both segments');
    expect(existsSync(join(dir, '.pull-paused'))).toBe(false);
  });

  it.skipIf(!available)('a synchronous standby records the last archived segment instead of applying the journal', async () => {
    syncTo = 'db-0.db-headless,db-1.db-headless';
    writeFileSync(join(dir, 'sync-standby'), '');
    start();
    await waitFor(() => read('sync-seen') === '8\n', 'the last archived segment');
    segments['mydb.fdb.journal-000000012'] = Buffer.from('x');
    await waitFor(() => read('sync-seen') === '12\n', 'segment 12 seen');
    expect(requests.some((r) => r.startsWith('GET'))).toBe(false);
    expect(readdirSync(source())).toEqual([]);
    expect(read('.last-pulled')).toBeUndefined();
  });

  it.skipIf(!available)('does not apply the journal when the primary and its flag disagree about the standby', async () => {
    // named by the primary, not flagged here
    syncTo = 'db-1.db-headless';
    start();
    await waitFor(
      () => out.includes('the primary replicates to this replica synchronously but it is not the standby: not applying the journal\n'),
      'the message',
    );
    expect(requests.some((r) => r.startsWith('GET'))).toBe(false);
    // flagged here, no longer named: the record of the last segment goes, nothing is applied
    syncTo = 'none';
    writeFileSync(join(dir, 'sync-standby'), '');
    writeFileSync(join(dir, 'sync-seen'), '8\n');
    await waitFor(
      () => out.includes('synchronous standby no longer attached and not repositioned: not applying the journal (re-seed this replica)\n'),
      'the message',
    );
    expect(existsSync(join(dir, 'sync-seen'))).toBe(false);
    expect(requests.some((r) => r.startsWith('GET'))).toBe(false);
    // a primary whose answer names another instance only: journal shipping as usual
    rmSync(join(dir, 'sync-standby'));
    syncTo = 'db-10.db-headless';
    await waitFor(() => read('.last-pulled') === 'mydb.fdb.journal-000000008\n', 'both segments');
  });

  it.skipIf(!available)('reports a refused listing and a short segment, leaves no partial segment, and tries again', async () => {
    listReply = 'ERR not available without replication\n';
    start();
    await waitFor(() => out.includes('pull failed: server: ERR not available without replication\n'), 'the refusal');
    expect(existsSync(join(dir, 'primary-seen'))).toBe(false);
    // a segment that ends before its announced size
    listReply = undefined;
    const whole = segments['mydb.fdb.journal-000000007'];
    const short = createServer((sock) =>
      sock.once('data', (buf) => {
        const request = buf.toString().split('\n')[0].split(' ').slice(4).join(' ');
        if (request === 'LIST') return void sock.end('mydb.fdb.journal-000000007\n.\n');
        if (request === 'SYNCTO') return void sock.end('OK none\n');
        sock.end(`OK ${whole.length + 10}\nseven`);
      }),
    );
    extra.push(short);
    await new Promise<void>((r) => short.listen(0, '127.0.0.2', r));
    const shortPort = (short.address() as { port: number }).port;
    puller!.kill('SIGKILL');
    out = '';
    writeFileSync(join(dir, 'primary'), '127.0.0.2\n');
    start({ SEGMENT_PORT: String(shortPort) });
    await waitFor(() => out.includes('pull failed: short read for mydb.fdb.journal-000000007\n'), 'the short read');
    expect(readdirSync(source()).filter((f) => !f.startsWith('.'))).toEqual([]);
    expect(read('.last-pulled')).toBeUndefined();
    // again at the next poll
    await waitFor(() => (out.match(/pull failed: short read/g) ?? []).length >= 2, 'a second attempt');
  });

  it.skipIf(!available)('stops on SIGTERM once the Firebird server has stopped, not before', async () => {
    const firebird = createServer((sock) => sock.end());
    extra.push(firebird);
    await new Promise<void>((r) => firebird.listen(0, '127.0.0.1', r));
    start({ FIREBIRD_PORT: String((firebird.address() as { port: number }).port) });
    await waitFor(() => read('.last-pulled') === 'mydb.fdb.journal-000000008\n', 'both segments');
    let code: number | null | undefined;
    puller!.once('exit', (c) => (code = c));
    puller!.kill('SIGTERM');
    // still pulling while the server runs
    segments['mydb.fdb.journal-000000009'] = Buffer.from('nine');
    await waitFor(() => read('.last-pulled') === 'mydb.fdb.journal-000000009\n', 'a segment after SIGTERM');
    expect(code).toBeUndefined();
    await new Promise<void>((r) => firebird.close(() => r()));
    await waitFor(() => code !== undefined, 'the exit');
    expect(code).toBe(0);
    expect(out).toContain('stopping: the Firebird server has stopped\n');
  });
});
