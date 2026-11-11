import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { ChildProcess, spawn, spawnSync } from 'child_process';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'fs';
import { connect } from 'net';
import { tmpdir } from 'os';
import { join } from 'path';
import { REPLICATION_SCRIPTS } from '../src/utils/replication';
import { startSegmentServer } from './helpers/segment-server';
import { signLine } from './helpers/segment-auth';

/**
 * The segment server's front (`fb-agent segment-server`, docs/rust-agent-segment-server.md): it
 * answers PING, LIST, GET, WAIT and the requests about the instance's state itself and forwards
 * the other requests to segment-server.pl, which it runs behind it. The same requests go to the Perl server alone and to the front: the
 * replies must be the same, except for WAIT, which only the front knows.
 *
 * Needs the fb-agent binary (FB_AGENT_BIN, or a build in agent/target); skipped without it. The
 * "Rust agent" CI job runs this file with it.
 */
const agentBin = [
  process.env.FB_AGENT_BIN,
  ...['x86_64-unknown-linux-musl/release', 'aarch64-unknown-linux-musl/release', 'release', 'debug'].map((d) =>
    join(__dirname, '..', 'agent', 'target', d, 'fb-agent'),
  ),
].find((p) => p && existsSync(p));
const hasPerl = spawnSync('perl', ['-v']).status === 0;

/** a journal segment file: the 48-byte header with its length, and `size` bytes in all */
const segment = (sequence: number, length: number, size = length): Buffer => {
  const b = Buffer.alloc(Math.max(size, 48), sequence);
  b.write('FBCHANGELOG\0', 0, 'latin1');
  b.writeBigUInt64LE(BigInt(sequence), 32);
  b.writeBigUInt64LE(BigInt(length), 40);
  return b.subarray(0, size);
};

describe.skipIf(!agentBin || !hasPerl)('the segment server front (fb-agent) and segment-server.pl', () => {
  let root: string;
  let perl: { child: ChildProcess; port: number };
  let front: { child: ChildProcess; port: number; out: () => string };
  const children: ChildProcess[] = [];
  const archive = () => join(root, 'archive');
  const env = () => ({
    ARCHIVE_DIR: archive(),
    DATABASE_PATH: join(root, 'data', 'mydb.fdb'),
    SOURCE_DIR: join(root, 'source'),
    PRIMARY_FILE: join(root, 'primary'),
    POD_NAME: 'db-0',
    ISC_PASSWORD: 'tok',
    // nothing listens there: the pod's "Firebird server" has stopped
    FIREBIRD_PORT: '1',
  });

  /** the whole reply to one signed request, with what follows the request line (an upload) */
  const ask = (port: number, request: string, body?: Buffer, secret = 'tok'): Promise<Buffer> =>
    new Promise((resolve, reject) => {
      const sock = connect(port, '127.0.0.1');
      const chunks: Buffer[] = [];
      sock.once('error', reject);
      sock.on('data', (d: Buffer) => chunks.push(d));
      sock.once('end', () => resolve(Buffer.concat(chunks)));
      sock.once('connect', () => {
        sock.write(`${request.startsWith('SIG1 ') ? request : signLine(`${secret} ${request}`)}\n`);
        if (body) sock.write(body);
      });
    });
  const lines = async (port: number, request: string, secret?: string) =>
    (await ask(port, request, undefined, secret)).toString().split('\n').filter((l) => l !== '');
  /** the same request to the Perl server and to the front: the two replies */
  const both = async (request: string) => [await lines(perl.port, request), await lines(front.port, request)];
  const startFront = async (replDir: string): Promise<{ child: ChildProcess; port: number; out: () => string }> => {
    const child = spawn(agentBin!, ['segment-server', '--', 'perl', join(root, 'segment-server.pl')], {
      env: { ...process.env, ...env(), REPLICATION_DIR: replDir, SEGMENT_PORT: '0', SEGMENT_LISTEN: '', BACKEND_PORT: '0' },
    });
    children.push(child);
    let out = '';
    child.stderr.on('data', (d) => (out += d.toString()));
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`the front did not start: ${out}`)), 15000);
      child.stdout.on('data', (d) => {
        out += d.toString();
        const m = /\(fb-agent [^)]+\) listening on [^\s:]+:(\d+),/.exec(out);
        // ready once the Perl server behind it listens too
        if (m && /segment server listening on 127\.0\.0\.1:\d+/.test(out)) {
          clearTimeout(timer);
          resolve({ child, port: Number(m[1]), out: () => out });
        }
      });
      child.once('exit', (code) => reject(new Error(`the front exited ${code}: ${out}`)));
    });
  };

  beforeEach(async () => {
    root = mkdtempSync(join(tmpdir(), 'segfront-'));
    for (const d of ['archive', 'source', 'data', 'repl-perl', 'repl-front']) mkdirSync(join(root, d));
    writeFileSync(join(archive(), 'mydb.fdb.journal-000000007'), segment(7, 300_000));
    // still being copied into the archive: shorter than its header says
    writeFileSync(join(archive(), 'mydb.fdb.journal-000000008'), segment(8, 5000, 1200));
    // not a segment file: served as before
    writeFileSync(join(archive(), 'mydb.fdb.journal-000000009'), 'x');
    writeFileSync(join(root, 'data', 'nightly.nbk'), Buffer.alloc(70_000, 3));
    writeFileSync(join(root, 'primary'), 'db-0.db-headless\n');
    writeFileSync(join(root, 'segment-server.pl'), REPLICATION_SCRIPTS['segment-server.pl']);
    perl = await startSegmentServer(join(root, 'segment-server.pl'), { ...env(), REPLICATION_DIR: join(root, 'repl-perl') });
    children.push(perl.child);
    front = await startFront(join(root, 'repl-front'));
  });
  afterEach(() => {
    for (const c of children.splice(0)) c.kill('SIGKILL');
    spawnSync('pkill', ['-KILL', '-f', join(root, 'segment-server.pl')]);
    rmSync(root, { recursive: true, force: true });
  });

  it('lists and serves the archived segments alike, without one still being copied', async () => {
    const [p, f] = await both('LIST');
    expect(p).toEqual(['mydb.fdb.journal-000000007', 'mydb.fdb.journal-000000009', '.']);
    expect(f).toEqual(p);
    const whole = Buffer.concat([Buffer.from('OK 300000\n'), segment(7, 300_000)]);
    expect((await ask(perl.port, 'GET mydb.fdb.journal-000000007')).equals(whole)).toBe(true);
    expect((await ask(front.port, 'GET mydb.fdb.journal-000000007')).equals(whole)).toBe(true);
    for (const request of ['PING', 'GET mydb.fdb.journal-000000001', 'GET ../primary', 'GET', 'NOSUCH']) {
      const [a, b] = await both(request);
      expect(b, request).toEqual(a);
    }
    expect(await lines(front.port, 'GET mydb.fdb.journal-000000001')).toEqual(['ERR bad request']);
    // once the copy is complete it is listed
    writeFileSync(join(archive(), 'mydb.fdb.journal-000000008'), segment(8, 5000));
    expect((await both('LIST')).map((l) => l[1])).toEqual(['mydb.fdb.journal-000000008', 'mydb.fdb.journal-000000008']);
  });

  it('passes STATE to the Perl server when it cannot reach the Firebird server itself', async () => {
    // nothing listens on FIREBIRD_PORT: the front has no service attachment, the Perl server's fbsvcmgr answers
    const [p, f] = await both('STATE');
    expect(p).toEqual([`ERR cannot read the state of ${join(root, 'data', 'mydb.fdb')}`]);
    expect(f).toEqual(p);
  });

  it('answers the requests about the instance from its files, as the Perl server does', async () => {
    const inBoth = (name: string, content: string | Buffer | null) => {
      for (const dir of ['repl-perl', 'repl-front']) {
        if (content === null) rmSync(join(root, dir, name), { force: true });
        else writeFileSync(join(root, dir, name), content);
      }
    };
    const requests = [
      'HEADER', 'ISOLATION', 'POSITION', 'PRIMARYSEEN', 'FOLLOW', 'SYNCTO', 'POINTS 7', 'POINTS 8', 'LINEAGE', 'ARCHIVED',
      // refused arguments: the Perl server's words, through the front
      'POINTS x', 'POINTS', 'UPLOADED', 'UPLOADED x', 'RETAIN', 'RETAIN x',
    ];
    /** every request to both; the ages in two replies may be a second apart, so a difference is asked again */
    const same = async (stage: string) => {
      const replies: Record<string, string[]> = {};
      for (const request of requests) {
        let [a, b] = await both(request);
        for (let i = 0; i < 3 && JSON.stringify(a) !== JSON.stringify(b); i++) [a, b] = await both(request);
        expect(b, `${stage}: ${request}`).toEqual(a);
        expect(a.length, `${stage}: ${request}`).toBeGreaterThan(0);
        replies[request] = b;
      }
      return replies;
    };

    // the primary, with nothing recorded yet and no database file
    let r = await same('empty primary');
    expect(r['POSITION']).toEqual(['OK primary']);
    expect(r['ISOLATION']).toEqual(['OK online']);
    expect(r['HEADER'][0]).toMatch(/^ERR cannot read the header of /);

    // a database header (ODS 13) with the replication sequence clump after another one
    const page = Buffer.alloc(8192);
    page[0] = 1;
    page.writeUInt16LE(13 | 0x8000, 18);
    Buffer.from([5, 2, 9, 9, 11, 8]).copy(page, 128);
    page.writeBigUInt64LE(77n, 134);
    page.writeUInt16LE(143, 66);
    writeFileSync(join(root, 'data', 'mydb.fdb'), page);
    inBoth('self-fenced', '1700000000 lease\n');
    inBoth('primary-seen', `${Math.floor(Date.now() / 1000) - 60} db-1.db-headless\n`);
    inBoth('follow.state', 'applied 12\n');
    inBoth('sync.conf', 'sync_replica = db-1.db-headless:3050/x\n# sync_replica = no:1\nsync_replica=db-2.db-headless:1\n');
    for (const dir of ['repl-perl', 'repl-front']) mkdirSync(join(root, dir, 'points'), { recursive: true });
    inBoth('points/7', '100 48\nbad\n101 96\n102 1');
    inBoth('lineage', '3 1700000000\nx\n4 1700000100 \n');
    r = await same('primary with records');
    expect(r['HEADER']).toEqual(['OK 77']);
    expect(r['ISOLATION']).toEqual(['OK fenced 1700000000 lease']);
    expect(r['PRIMARYSEEN'][0]).toMatch(/^OK (59|6\d) db-1\.db-headless$/);
    expect(r['FOLLOW']).toEqual(['OK applied 12']);
    expect(r['SYNCTO']).toEqual(['OK db-1.db-headless,db-2.db-headless']);
    expect(r['POINTS 7']).toEqual(['100 48', '101 96', '.']);
    expect(r['POINTS 8']).toEqual(['.']);
    expect(r['LINEAGE']).toEqual(['mydb.fdb.lineage-3-1700000000', 'mydb.fdb.lineage-4-1700000100', '.']);
    // segments 7 and 8 (still being copied, but with its header); 9 has no segment header
    expect(r['ARCHIVED'].length).toBe(3);
    expect(r['ARCHIVED'][0]).toMatch(/^7 \d$/);

    // a replica: its control file, and the segments fetched after it
    writeFileSync(join(root, 'primary'), 'db-1.db-headless\n');
    expect((await same('replica without a control file'))['POSITION']).toEqual(['ERR no replica control file']);
    const control = Buffer.alloc(40);
    control.write('FBREPLCTL\0', 0, 'latin1');
    control.writeUInt16LE(1, 10);
    control.writeBigUInt64LE(41n, 16);
    control.writeUInt32LE(1234, 24);
    writeFileSync(join(root, 'source', '{6A0B3C1D-0000-4000-8000-000000000001}'), control);
    for (const n of [41, 42, 43]) writeFileSync(join(root, 'source', `mydb.fdb.journal-0000000${n}`), segment(n, 100));
    expect((await same('replica'))['POSITION']).toEqual(['OK 41 1234 2']);
    // a synchronous standby that has seen the end of the archive
    inBoth('sync-standby', '');
    inBoth('sync-seen', '57\n');
    expect((await same('synchronous standby'))['POSITION']).toEqual(['OK 57 0 0']);
    // promoted in place: the primary, whatever the file still says
    inBoth('promoted', '');
    expect((await same('promoted'))['POSITION']).toEqual(['OK primary']);

    // the numbers the pruning reads
    for (const [dir, port] of [['repl-perl', perl.port], ['repl-front', front.port]] as const) {
      expect(await lines(port, 'UPLOADED 12'), dir).toEqual(['OK']);
      expect(readFileSync(join(root, dir, 'uploaded-floor'), 'utf8'), dir).toBe('12\n');
      expect(await lines(port, 'RETAIN 9'), dir).toEqual(['OK']);
      expect(readFileSync(join(root, dir, 'retain-floor'), 'utf8'), dir).toBe('9\n');
      expect(await lines(port, 'RETAIN none'), dir).toEqual(['OK']);
      expect(existsSync(join(root, dir, 'retain-floor')), dir).toBe(false);
      // these too are contact for the isolation check
      rmSync(join(root, dir, 'last-contact'), { force: true });
      await lines(port, 'POSITION');
      expect(existsSync(join(root, dir, 'last-contact')), dir).toBe(true);
    }
    // the Perl server has no local Firebird to ask here, and neither has the front
    const [pv, fv] = await both('VERSION');
    expect(pv).toEqual(['ERR cannot read the engine version']);
    expect(fv).toEqual(pv);
  });

  it('refuses what the Perl server refuses, in its words', async () => {
    for (const port of [perl.port, front.port]) {
      expect(await lines(port, 'PING', 'wrong')).toEqual(['ERR unauthorized (signature)']);
      expect(await lines(port, 'tok PING'.replace(/^/, ''), '')).toEqual(['ERR unauthorized (signature)']);
      const once = signLine('tok LIST');
      expect((await lines(port, once)).at(-1)).toBe('.');
      expect(await lines(port, once)).toEqual(['ERR unauthorized (replay)']);
      expect(await lines(port, signLine('tok PING', Math.floor(Date.now() / 1000) - 1000))).toEqual(['ERR unauthorized (clock)']);
    }
  });

  it('forwards the other requests to the Perl server: the same replies, downloads and uploads', async () => {
    for (const request of ['ARCHIVED', 'PRIMARYSEEN', 'FILES', 'EXISTS', 'ISOLATION', 'SYNCTO', 'POINTS 7', 'RETAIN none', 'FOLLOW']) {
      const [a, b] = await both(request);
      expect(b, request).toEqual(a);
      expect(a.length, request).toBeGreaterThan(0);
    }
    expect(await lines(front.port, 'FILES')).toEqual(['nightly.nbk', '.']);
    // a download through the front
    const file = Buffer.concat([Buffer.from('OK 70000\n'), Buffer.alloc(70_000, 3)]);
    expect((await ask(front.port, 'FILE nightly.nbk')).equals(file)).toBe(true);
    // an upload through the front: the body follows the request line on the same connection
    const body = Buffer.alloc(250_000, 5);
    expect((await ask(front.port, `STORE restore-1.nbk ${body.length}`, body)).toString()).toBe('OK\n');
    expect(readFileSync(join(root, 'data', 'restore-1.nbk')).equals(body)).toBe(true);
    expect((await ask(front.port, 'REMOVE restore-1.nbk')).toString()).toBe('OK\n');
    expect(existsSync(join(root, 'data', 'restore-1.nbk'))).toBe(false);
    // the requests it answers itself count as contact for the isolation check, as the Perl server's do
    for (const [dir, port] of [['repl-perl', perl.port], ['repl-front', front.port]] as const) {
      rmSync(join(root, dir, 'last-contact'), { force: true });
      await lines(port, 'LIST');
      expect(Math.abs(Number(readFileSync(join(root, dir, 'last-contact'), 'utf8')) - Date.now() / 1000), dir).toBeLessThan(5);
    }
  });

  it('WAIT answers as soon as a complete segment is archived; the Perl server does not know it', async () => {
    expect(await lines(perl.port, 'WAIT 5 mydb.fdb.journal-000000009')).toEqual(['ERR bad request']);
    for (const bad of ['WAIT', 'WAIT 0', 'WAIT 61', 'WAIT x', 'WAIT 5 ../primary', 'WAIT 5 a b']) {
      expect(await lines(front.port, bad), bad).toEqual(['ERR bad request']);
    }
    // something after segment 7 is there already; nothing after 9 within a second
    expect(await lines(front.port, 'WAIT 5 mydb.fdb.journal-000000007')).toEqual(['OK new']);
    expect(await lines(front.port, 'WAIT 5')).toEqual(['OK new']);
    let start = Date.now();
    expect(await lines(front.port, 'WAIT 1 mydb.fdb.journal-000000009')).toEqual(['OK timeout']);
    expect(Date.now() - start).toBeGreaterThanOrEqual(900);
    // a segment being copied does not end the wait; the complete one does, at once
    start = Date.now();
    const waiting = lines(front.port, 'WAIT 20 mydb.fdb.journal-000000009');
    // other requests are answered meanwhile
    expect(await lines(front.port, 'PING')).toEqual(['OK']);
    await new Promise((r) => setTimeout(r, 300));
    writeFileSync(join(archive(), 'mydb.fdb.journal-000000010'), segment(10, 9000, 100));
    const early = await Promise.race([waiting, new Promise((r) => setTimeout(() => r('still waiting'), 700))]);
    expect(early).toBe('still waiting');
    writeFileSync(join(archive(), 'mydb.fdb.journal-000000010'), segment(10, 9000));
    expect(await waiting).toEqual(['OK new']);
    expect(Date.now() - start).toBeLessThan(3000);
  });

  it('a Rust puller waiting on the front fetches a new segment at once, not at its next poll', async () => {
    mkdirSync(join(root, 'replica'));
    mkdirSync(join(root, 'replica', 'source'));
    writeFileSync(join(root, 'replica', 'primary'), '127.0.0.1\n');
    let out = '';
    const puller = spawn(agentBin!, ['puller'], {
      env: {
        PATH: process.env.PATH ?? '',
        SOURCE_DIR: join(root, 'replica', 'source'),
        STATE_FILE: join(root, 'replica', '.last-pulled'),
        PRIMARY_FILE: join(root, 'replica', 'primary'),
        REPLICATION_DIR: join(root, 'replica'),
        POD_NAME: 'db-1',
        ISC_PASSWORD: 'tok',
        SEGMENT_PORT: String(front.port),
        // a poll that would make the test time out if the puller waited for it
        POLL_SECONDS: '20',
        FIREBIRD_PORT: '1',
      },
    });
    children.push(puller);
    puller.stdout.on('data', (d) => (out += d.toString()));
    const fetched = (name: string) => existsSync(join(root, 'replica', 'source', name));
    const waitFor = async (check: () => boolean, what: string, ms: number) => {
      const end = Date.now() + ms;
      while (!check()) {
        if (Date.now() > end) throw new Error(`timed out waiting for ${what}: ${out}`);
        await new Promise((r) => setTimeout(r, 20));
      }
    };
    await waitFor(() => fetched('mydb.fdb.journal-000000009'), 'the segments archived at the start', 5000);
    expect(readdirSync(join(root, 'replica', 'source')).sort()).toEqual(['mydb.fdb.journal-000000007', 'mydb.fdb.journal-000000009']);
    for (const n of [10, 11, 12]) {
      await new Promise((r) => setTimeout(r, 400));
      const name = `mydb.fdb.journal-0000000${n}`;
      const at = Date.now();
      writeFileSync(join(archive(), name), segment(n, 20_000));
      await waitFor(() => fetched(name), name, 3000);
      expect(Date.now() - at, name).toBeLessThan(1500);
    }
    expect(out).toContain('fetched mydb.fdb.journal-000000012 from 127.0.0.1\n');
  });

  it('stops with the Perl server behind it, and ends when that server ends', async () => {
    const exited = new Promise<number | null>((resolve) => front.child.once('exit', (code) => resolve(code)));
    front.child.kill('SIGTERM');
    // the Perl server exits once the Firebird server has stopped (nothing listens on FIREBIRD_PORT)
    expect(await exited).toBe(0);
    expect(front.out()).toContain('stopping: the Firebird server has stopped');
    // a backend that dies takes the front with it (the container restarts)
    const second = await startFront(join(root, 'repl-front'));
    const ended = new Promise<number | null>((resolve) => second.child.once('exit', (code) => resolve(code)));
    spawnSync('pkill', ['-KILL', '-f', join(root, 'segment-server.pl')]);
    expect(await ended).not.toBe(0);
  });
});
