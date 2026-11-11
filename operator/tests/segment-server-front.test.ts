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
 * answers PING, LIST, GET, WAIT, the requests about the instance's state, the backup files and the seeds itself and forwards
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
  const startFront = async (
    replDir: string,
    own: Record<string, string> = { ...env(), REPLICATION_DIR: replDir },
  ): Promise<{ child: ChildProcess; port: number; out: () => string }> => {
    const child = spawn(agentBin!, ['segment-server', '--', 'perl', join(root, 'segment-server.pl')], {
      env: { ...process.env, ...own, SEGMENT_PORT: '0', SEGMENT_LISTEN: '', BACKEND_PORT: '0' },
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
        if (m && /(?:segment|backup file) server listening on 127\.0\.0\.1:\d+/.test(out)) {
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

  it('tells where transactions begin in the archive, and refuses a seed, as the Perl server does', async () => {
    /** a segment with blocks of [transaction, flags, payload size] */
    const withBlocks = (sequence: number, blocks: Array<[number, number, number]>): Buffer => {
      const body = Buffer.concat(
        blocks.map(([transaction, flags, size]) => {
          const b = Buffer.alloc(16 + size);
          b.writeBigUInt64LE(BigInt(transaction), 0);
          b.writeUInt16LE(1, 8);
          b.writeUInt16LE(flags, 10);
          b.writeUInt32LE(size, 12);
          return b;
        }),
      );
      const header = Buffer.alloc(48);
      header.write('FBCHANGELOG\0', 0, 'latin1');
      header.writeBigUInt64LE(BigInt(sequence), 32);
      header.writeBigUInt64LE(BigInt(48 + body.length), 40);
      return Buffer.concat([header, body]);
    };
    writeFileSync(join(archive(), 'mydb.fdb.journal-000000010'), withBlocks(10, [[50, 1, 5], [51, 0, 0], [50, 0, 7]]));
    writeFileSync(join(archive(), 'mydb.fdb.journal-000000011'), withBlocks(11, [[51, 0, 3], [52, 1, 2], [0, 0, 4], [60, 1, 0]]));
    const requests = [
      'TXNS 11 50,52,53', 'TXNS 10 50,52', 'TXNS 11 ', 'PLAN 11 52 51', 'PLAN 11 99 ', 'PLAN 10 0 ',
      // refused arguments
      'TXNS', 'TXNS 11', 'TXNS x 1', 'TXNS 11 a', 'PLAN 11 1', 'PLAN 11 x 1',
      // no bootstrap seed, and a live copy of the primary is not allowed here
      'SEED',
    ];
    const replies: Record<string, string[]> = {};
    for (const request of requests) {
      const [a, b] = await both(request);
      expect(b, request).toEqual(a);
      expect(a.length, request).toBeGreaterThan(0);
      replies[request] = b;
    }
    expect(replies['TXNS 11 50,52,53']).toEqual(['50 10', '52 11', '.']);
    // (the other fixture segments are filled with one byte, which also reads as blocks of huge transaction numbers)
    expect(replies['PLAN 11 52 51'].filter((l) => !/^\d{6,} /.test(l))).toEqual(['51 10 0', '52 11 1', '60 11 1', '.']);
    expect(replies['SEED'][0]).toMatch(/^ERR no safe seed source on the primary/);
  });

  it('changes what the instance does (SYNC, STANDBY, FENCE, REJOIN, NBACKUP) as the Perl server does', async () => {
    const repl = (dir: string, name: string) => join(root, dir, name);
    const dirs = [['repl-perl', () => perl.port], ['repl-front', () => front.port]] as const;
    // refused arguments, and requests that need the local Firebird server, which is not there:
    // the front passes those on, and the Perl server's fbsvcmgr fails in its words
    const requests = [
      'SYNC', 'SYNC a,,b', 'SYNC -a', 'SYNC a b', 'STANDBY', 'STANDBY off', 'STANDBY off x', 'STANDBY maybe',
      'STANDBY on', 'REJOIN', 'FENCE lease lost', 'NBACKUP', 'NBACKUP 3 a.nbk', 'NBACKUP 0 a.fbk', 'NBACKUP 0 ../a.nbk', 'NBACKUP 0 a.nbk',
    ];
    const replies: Record<string, string[]> = {};
    for (const request of requests) {
      const [a, b] = await both(request);
      expect(b, request).toEqual(a);
      expect(a.length, request).toBeGreaterThan(0);
      replies[request] = b;
    }
    expect(replies['STANDBY on']).toEqual(['ERR this instance is the primary']);
    expect(replies['FENCE lease lost'][0]).toMatch(/^ERR cannot shut .* down$/);
    expect(replies['NBACKUP 0 a.nbk']).toEqual(['ERR nbackup failed']);
    // nothing is left of a fence or an nbackup that failed
    for (const [dir] of dirs) expect(existsSync(repl(dir, 'self-fenced')), dir).toBe(false);
    expect(readdirSync(join(root, 'data')).filter((n) => n.startsWith('a.nbk'))).toEqual([]);

    // synchronous replication: the same file from both
    for (const [dir, port] of dirs) {
      expect(await lines(port(), 'SYNC db-1.db-headless,db-2.db-headless'), dir).toEqual(['OK']);
      expect(await lines(port(), 'SYNCTO'), dir).toEqual(['OK db-1.db-headless,db-2.db-headless']);
    }
    expect(readFileSync(repl('repl-front', 'sync.conf'), 'utf8')).toBe(readFileSync(repl('repl-perl', 'sync.conf'), 'utf8'));
    expect(readFileSync(repl('repl-front', 'sync.conf'), 'utf8')).toContain(`sync_replica = db-1.db-headless:${join(root, 'data', 'mydb.fdb')}\n{\n`);
    for (const [dir, port] of dirs) {
      expect(await lines(port(), 'SYNC none'), dir).toEqual(['OK']);
      expect(readFileSync(repl(dir, 'sync.conf'), 'utf8'), dir).toBe('');
    }

    // a replica becomes the synchronous standby, and an asynchronous replica again after segment 57
    writeFileSync(join(root, 'primary'), 'db-1.db-headless\n');
    const controlPath = join(root, 'source', '{6A0B3C1D-0000-4000-8000-000000000001}');
    const control = () => {
      const b = Buffer.alloc(72, 7);
      b.write('FBREPLCTL\0', 0, 'latin1');
      b.writeUInt16LE(1, 10);
      b.writeUInt32LE(2, 12);
      b.writeBigUInt64LE(41n, 16);
      b.writeUInt32LE(1234, 24);
      b.writeBigUInt64LE(9n, 32);
      return b;
    };
    const after: Buffer[] = [];
    for (const [dir, port] of dirs) {
      expect(await lines(port(), 'STANDBY off 57'), `${dir}: not a standby`).toEqual(['OK']);
      expect(await lines(port(), 'STANDBY on'), dir).toEqual(['OK']);
      expect(readFileSync(repl(dir, 'sync-standby'), 'utf8'), dir).toMatch(/^\d+\n$/);
      expect(await lines(port(), 'STANDBY off 57'), `${dir}: no control file`).toEqual(['ERR no replica control file']);
      writeFileSync(controlPath, control());
      writeFileSync(repl(dir, '.last-pulled'), 'mydb.fdb.journal-000000041\n');
      writeFileSync(repl(dir, 'sync-seen'), '57\n');
      expect(await lines(port(), 'STANDBY off 57'), dir).toEqual(['OK']);
      expect(readFileSync(repl(dir, '.last-pulled'), 'utf8'), dir).toBe('mydb.fdb.journal-000000057\n');
      expect(existsSync(repl(dir, 'sync-standby')) || existsSync(repl(dir, 'sync-seen')), dir).toBe(false);
      expect(await lines(port(), 'POSITION'), dir).toEqual(['OK 57 0 0']);
      after.push(readFileSync(controlPath));
      rmSync(controlPath);
    }
    // the control file: at segment 57, no transactions, the database's own sequence kept
    expect(after[1].equals(after[0])).toBe(true);
    expect(after[1].length).toBe(40);
    expect(after[1].readBigUInt64LE(16)).toBe(57n);
    expect(after[1].readBigUInt64LE(32)).toBe(9n);
  }, 60_000);

  it('copies, lists and removes the backup files as the Perl server does', async () => {
    const data = (name: string) => join(root, 'data', name);
    mkdirSync(data('dir.nbk'));
    writeFileSync(data('mydb.fdb'), 'x');
    writeFileSync(data('empty.fbk'), '');
    writeFileSync(data('replica.nbk.ctl'), 'c');
    const requests = [
      'FILES', 'EXISTS mydb.fdb', 'EXISTS none.fdb', 'EXISTS dir.nbk', 'EXISTS', 'EXISTS ../primary', 'EXISTS .hidden',
      // refused, or not a file: the Perl server's words
      'FILE', 'FILE none.nbk', 'FILE mydb.fdb', 'FILE ../primary', 'FILE dir.nbk', 'STORE', 'STORE a.nbk', 'STORE a.nbk x',
      'STORE mydb.fdb 1', 'STORE ../a.nbk 1', 'REMOVE', 'REMOVE mydb.fdb', 'REMOVE ../primary', 'REMOVE none.nbk', 'REMOVE dir.nbk',
    ];
    for (const request of requests) {
      const [a, b] = await both(request);
      expect(b, request).toEqual(a);
      expect(a.length, request).toBeGreaterThan(0);
    }
    expect(await lines(front.port, 'FILES')).toEqual(['empty.fbk', 'nightly.nbk', 'replica.nbk.ctl', '.']);
    expect(existsSync(data('mydb.fdb'))).toBe(true);
    for (const name of ['nightly.nbk', 'empty.fbk']) {
      const [a, b] = [await ask(perl.port, `FILE ${name}`), await ask(front.port, `FILE ${name}`)];
      expect(b.equals(a), name).toBe(true);
    }
    // uploads: whole, empty, and one whose client stops early (nothing is left behind)
    const body = Buffer.alloc(300_000, 9);
    for (const [who, port] of [['perl', perl.port], ['front', front.port]] as const) {
      expect((await ask(port, `STORE ${who}.nbk ${body.length}`, body)).toString(), who).toBe('OK\n');
      expect(readFileSync(data(`${who}.nbk`)).equals(body), who).toBe(true);
      expect((await ask(port, `STORE ${who}-0.fbk 0`)).toString(), who).toBe('OK\n');
      expect(readFileSync(data(`${who}-0.fbk`)).length, who).toBe(0);
      // replaces a file of the same name
      expect((await ask(port, `STORE ${who}.nbk 3`, Buffer.from('abc'))).toString(), who).toBe('OK\n');
      expect(readFileSync(data(`${who}.nbk`), 'utf8'), who).toBe('abc');
      const short = await new Promise<string>((resolve, reject) => {
        const sock = connect(port, '127.0.0.1');
        let reply = '';
        sock.once('error', reject);
        sock.on('data', (d: Buffer) => (reply += d.toString()));
        sock.once('end', () => resolve(reply));
        // half-closed after ten of fifty bytes
        sock.once('connect', () => sock.end(Buffer.concat([Buffer.from(`${signLine(`tok STORE ${who}-short.nbk 50`)}\n`), Buffer.alloc(10, 1)])));
      });
      expect(short, who).toBe(`ERR short write for ${who}-short.nbk (10 of 50 bytes)\n`);
      expect(readdirSync(join(root, 'data')).filter((n) => n.includes('short')), who).toEqual([]);
      expect((await ask(port, `REMOVE ${who}.nbk`)).toString(), who).toBe('OK\n');
      expect(existsSync(data(`${who}.nbk`)), who).toBe(false);
    }
  });

  it('without replication (FILES_ONLY) serves the backup files and refuses the rest, as the Perl server does', async () => {
    const own = { DATABASE_PATH: join(root, 'data', 'mydb.fdb'), POD_NAME: 'db-0', ISC_PASSWORD: 'tok', FIREBIRD_PORT: '1', FILES_ONLY: 'true' };
    const perlFiles = await startSegmentServer(join(root, 'segment-server.pl'), own);
    children.push(perlFiles.child);
    const frontFiles = await startFront('', own);
    expect(frontFiles.out()).toMatch(/backup file server \(fb-agent [^)]+\) listening on /);
    const requests = ['PING', 'FILES', 'EXISTS nightly.nbk', 'LIST', 'GET mydb.fdb.journal-000000007', 'WAIT 1', 'STATE', 'POSITION', 'HEADER', 'VERSION', 'ARCHIVED', 'SEED', 'NOSUCH'];
    for (const request of requests) {
      const [a, b] = [await lines(perlFiles.port, request), await lines(frontFiles.port, request)];
      expect(b, request).toEqual(a);
      expect(a.length, request).toBeGreaterThan(0);
    }
    expect(await lines(frontFiles.port, 'LIST')).toEqual(['ERR not available without replication']);
    expect(await lines(frontFiles.port, 'FILES')).toEqual(['nightly.nbk', '.']);
    const file = Buffer.concat([Buffer.from('OK 70000\n'), Buffer.alloc(70_000, 3)]);
    expect((await ask(frontFiles.port, 'FILE nightly.nbk')).equals(file)).toBe(true);
    const body = Buffer.alloc(120_000, 4);
    expect((await ask(frontFiles.port, `STORE restore-2.nbk ${body.length}`, body)).toString()).toBe('OK\n');
    expect(readFileSync(join(root, 'data', 'restore-2.nbk')).equals(body)).toBe(true);
  });

  it('gives the same replies for requests of every kind, through the front or not', async () => {
    for (const request of ['ARCHIVED', 'PRIMARYSEEN', 'FILES', 'EXISTS', 'ISOLATION', 'SYNCTO', 'POINTS 7', 'RETAIN none', 'FOLLOW', 'NOSUCH 1']) {
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
