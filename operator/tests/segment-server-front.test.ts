import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { ChildProcess, spawn } from 'child_process';
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, utimesSync, writeFileSync } from 'fs';
import { connect } from 'net';
import { tmpdir } from 'os';
import { join } from 'path';
import { agentBin } from './helpers/implementations';
import { signLine } from './helpers/segment-auth';
import { createTlsCertificates } from '../src/utils/certificates';
import { connect as tlsConnect } from 'tls';

/**
 * The segment server (`fb-agent segment-server`, docs/rust-agent-segment-server.md): it answers
 * PING, LIST, GET, WAIT, the requests about the instance's state, the backup files, the seeds and
 * the changes of role itself, and refuses what it does not have in the words of the Perl server it
 * replaced (removed in v0.170.0), whose replies these tests compared it with until then.
 *
 * Needs the fb-agent binary (FB_AGENT_BIN, or a build in agent/target); skipped without it. The
 * "Rust agent" CI job runs this file with it.
 */

/** a journal segment file: the 48-byte header with its length, and `size` bytes in all */
const segment = (sequence: number, length: number, size = length): Buffer => {
  const b = Buffer.alloc(Math.max(size, 48), sequence);
  b.write('FBCHANGELOG\0', 0, 'latin1');
  b.writeBigUInt64LE(BigInt(sequence), 32);
  b.writeBigUInt64LE(BigInt(length), 40);
  return b.subarray(0, size);
};

describe.skipIf(!agentBin)('the segment server (fb-agent segment-server)', () => {
  let root: string;
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
  /** the reply of the server to a request */
  const reply = (request: string) => lines(front.port, request);
  /** replies, with the test's directory named ROOT, for the snapshots */
  const named = (replies: Record<string, string[]>) =>
    Object.fromEntries(Object.entries(replies).map(([k, v]) => [k, v.map((l) => l.split(root).join('ROOT'))]));
  const startFront = async (
    replDir: string,
    own: Record<string, string> = { ...env(), REPLICATION_DIR: replDir },
  ): Promise<{ child: ChildProcess; port: number; out: () => string }> => {
    const child = spawn(agentBin!, ['segment-server'], {
      env: { ...process.env, ...own, SEGMENT_PORT: '0', SEGMENT_LISTEN: '' },
    });
    children.push(child);
    let out = '';
    child.stderr.on('data', (d) => (out += d.toString()));
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`the front did not start: ${out}`)), 15000);
      child.stdout.on('data', (d) => {
        out += d.toString();
        const m = /\(fb-agent [^)]+\) listening on [^\s:]+:(\d+),/.exec(out);
        if (m) {
          clearTimeout(timer);
          resolve({ child, port: Number(m[1]), out: () => out });
        }
      });
      child.once('exit', (code) => reject(new Error(`the front exited ${code}: ${out}`)));
    });
  };

  beforeEach(async () => {
    root = mkdtempSync(join(tmpdir(), 'segfront-'));
    for (const d of ['archive', 'source', 'data', 'repl-front']) mkdirSync(join(root, d));
    writeFileSync(join(archive(), 'mydb.fdb.journal-000000007'), segment(7, 300_000));
    // still being copied into the archive: shorter than its header says
    writeFileSync(join(archive(), 'mydb.fdb.journal-000000008'), segment(8, 5000, 1200));
    // not a segment file: served as before
    writeFileSync(join(archive(), 'mydb.fdb.journal-000000009'), 'x');
    writeFileSync(join(root, 'data', 'nightly.nbk'), Buffer.alloc(70_000, 3));
    writeFileSync(join(root, 'primary'), 'db-0.db-headless\n');
    front = await startFront(join(root, 'repl-front'));
  });
  afterEach(() => {
    for (const c of children.splice(0)) c.kill('SIGKILL');
    rmSync(root, { recursive: true, force: true });
  });

  it('lists and serves the archived segments, without one still being copied', async () => {
    expect(await reply('LIST')).toEqual(['mydb.fdb.journal-000000007', 'mydb.fdb.journal-000000009', '.']);
    const whole = Buffer.concat([Buffer.from('OK 300000\n'), segment(7, 300_000)]);
    expect((await ask(front.port, 'GET mydb.fdb.journal-000000007')).equals(whole)).toBe(true);
    const replies: Record<string, string[]> = {};
    for (const request of ['PING', 'GET mydb.fdb.journal-000000001', 'GET ../primary', 'GET', 'NOSUCH']) replies[request] = await reply(request);
    expect(replies).toMatchInlineSnapshot(`
      {
        "GET": [
          "ERR bad request",
        ],
        "GET ../primary": [
          "ERR bad request",
        ],
        "GET mydb.fdb.journal-000000001": [
          "ERR bad request",
        ],
        "NOSUCH": [
          "ERR bad request",
        ],
        "PING": [
          "OK",
        ],
      }
    `);
    // once the copy is complete it is listed
    writeFileSync(join(archive(), 'mydb.fdb.journal-000000008'), segment(8, 5000));
    expect((await reply('LIST'))[1]).toBe('mydb.fdb.journal-000000008');
  });

  it('cannot read STATE without the Firebird server or its tools', async () => {
    // nothing listens on FIREBIRD_PORT, and there is no fbsvcmgr here
    expect(await reply('STATE')).toEqual([`ERR cannot read the state of ${join(root, 'data', 'mydb.fdb')}`]);
  });

  it('answers the requests about the instance from its files', async () => {
    const inBoth = (name: string, content: string | Buffer | null) => {
      if (content === null) rmSync(join(root, 'repl-front', name), { force: true });
      else writeFileSync(join(root, 'repl-front', name), content);
    };
    const requests = [
      'HEADER', 'ISOLATION', 'POSITION', 'PRIMARYSEEN', 'FOLLOW', 'SYNCTO', 'POINTS 7', 'POINTS 8', 'LINEAGE', 'ARCHIVED',
      // refused arguments
      'POINTS x', 'POINTS', 'UPLOADED', 'UPLOADED x', 'RETAIN', 'RETAIN x',
    ];
    /** every request; a snapshot of the replies but those that hold ages */
    const same = async (stage: string) => {
      const replies: Record<string, string[]> = {};
      for (const request of requests) {
        replies[request] = await reply(request);
        expect(replies[request].length, `${stage}: ${request}`).toBeGreaterThan(0);
      }
      return replies;
    };
    const steady = (replies: Record<string, string[]>) =>
      named(Object.fromEntries(Object.entries(replies).filter(([request]) => !['ARCHIVED', 'PRIMARYSEEN'].includes(request))));

    // the primary, with nothing recorded yet and no database file
    let r = await same('empty primary');
    expect(steady(r)).toMatchInlineSnapshot(`
      {
        "FOLLOW": [
          "OK none",
        ],
        "HEADER": [
          "ERR cannot read the header of ROOT/data/mydb.fdb",
        ],
        "ISOLATION": [
          "OK online",
        ],
        "LINEAGE": [
          ".",
        ],
        "POINTS": [
          "ERR bad request",
        ],
        "POINTS 7": [
          ".",
        ],
        "POINTS 8": [
          ".",
        ],
        "POINTS x": [
          "ERR bad request",
        ],
        "POSITION": [
          "OK primary",
        ],
        "RETAIN": [
          "ERR bad request",
        ],
        "RETAIN x": [
          "ERR bad request",
        ],
        "SYNCTO": [
          "OK none",
        ],
        "UPLOADED": [
          "ERR bad request",
        ],
        "UPLOADED x": [
          "ERR bad request",
        ],
      }
    `);
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
    mkdirSync(join(root, 'repl-front', 'points'), { recursive: true });
    inBoth('points/7', '100 48\nbad\n101 96\n102 1');
    inBoth('lineage', '3 1700000000\nx\n4 1700000100 \n');
    r = await same('primary with records');
    expect(steady(r)).toMatchInlineSnapshot(`
      {
        "FOLLOW": [
          "OK applied 12",
        ],
        "HEADER": [
          "OK 77",
        ],
        "ISOLATION": [
          "OK fenced 1700000000 lease",
        ],
        "LINEAGE": [
          "mydb.fdb.lineage-3-1700000000",
          "mydb.fdb.lineage-4-1700000100",
          ".",
        ],
        "POINTS": [
          "ERR bad request",
        ],
        "POINTS 7": [
          "100 48",
          "101 96",
          ".",
        ],
        "POINTS 8": [
          ".",
        ],
        "POINTS x": [
          "ERR bad request",
        ],
        "POSITION": [
          "OK primary",
        ],
        "RETAIN": [
          "ERR bad request",
        ],
        "RETAIN x": [
          "ERR bad request",
        ],
        "SYNCTO": [
          "OK db-1.db-headless,db-2.db-headless",
        ],
        "UPLOADED": [
          "ERR bad request",
        ],
        "UPLOADED x": [
          "ERR bad request",
        ],
      }
    `);
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
    r = await same('replica without a control file');
    expect(r['POSITION']).toEqual(['ERR no replica control file']);
    expect(steady(r)).toMatchInlineSnapshot(`
      {
        "FOLLOW": [
          "OK applied 12",
        ],
        "HEADER": [
          "OK 77",
        ],
        "ISOLATION": [
          "OK fenced 1700000000 lease",
        ],
        "LINEAGE": [
          "mydb.fdb.lineage-3-1700000000",
          "mydb.fdb.lineage-4-1700000100",
          ".",
        ],
        "POINTS": [
          "ERR bad request",
        ],
        "POINTS 7": [
          "100 48",
          "101 96",
          ".",
        ],
        "POINTS 8": [
          ".",
        ],
        "POINTS x": [
          "ERR bad request",
        ],
        "POSITION": [
          "ERR no replica control file",
        ],
        "RETAIN": [
          "ERR bad request",
        ],
        "RETAIN x": [
          "ERR bad request",
        ],
        "SYNCTO": [
          "OK db-1.db-headless,db-2.db-headless",
        ],
        "UPLOADED": [
          "ERR bad request",
        ],
        "UPLOADED x": [
          "ERR bad request",
        ],
      }
    `);
    const control = Buffer.alloc(40);
    control.write('FBREPLCTL\0', 0, 'latin1');
    control.writeUInt16LE(1, 10);
    control.writeBigUInt64LE(41n, 16);
    control.writeUInt32LE(1234, 24);
    writeFileSync(join(root, 'source', '{6A0B3C1D-0000-4000-8000-000000000001}'), control);
    for (const n of [41, 42, 43]) writeFileSync(join(root, 'source', `mydb.fdb.journal-0000000${n}`), segment(n, 100));
    r = await same('replica');
    expect(r['POSITION']).toEqual(['OK 41 1234 2']);
    expect(steady(r)).toMatchInlineSnapshot(`
      {
        "FOLLOW": [
          "OK applied 12",
        ],
        "HEADER": [
          "OK 77",
        ],
        "ISOLATION": [
          "OK fenced 1700000000 lease",
        ],
        "LINEAGE": [
          "mydb.fdb.lineage-3-1700000000",
          "mydb.fdb.lineage-4-1700000100",
          ".",
        ],
        "POINTS": [
          "ERR bad request",
        ],
        "POINTS 7": [
          "100 48",
          "101 96",
          ".",
        ],
        "POINTS 8": [
          ".",
        ],
        "POINTS x": [
          "ERR bad request",
        ],
        "POSITION": [
          "OK 41 1234 2",
        ],
        "RETAIN": [
          "ERR bad request",
        ],
        "RETAIN x": [
          "ERR bad request",
        ],
        "SYNCTO": [
          "OK db-1.db-headless,db-2.db-headless",
        ],
        "UPLOADED": [
          "ERR bad request",
        ],
        "UPLOADED x": [
          "ERR bad request",
        ],
      }
    `);
    // a synchronous standby that has seen the end of the archive
    inBoth('sync-standby', '');
    inBoth('sync-seen', '57\n');
    expect((await same('synchronous standby'))['POSITION']).toEqual(['OK 57 0 0']);
    // promoted in place: the primary, whatever the file still says
    inBoth('promoted', '');
    expect((await same('promoted'))['POSITION']).toEqual(['OK primary']);

    // the numbers the pruning reads
    for (const [dir, port] of [['repl-front', front.port]] as const) {
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
    // there is no local Firebird to ask here
    expect(await reply('VERSION')).toEqual(['ERR cannot read the engine version']);
  });

  it('refuses unsigned, wrongly signed, replayed and late requests', async () => {
    for (const port of [front.port]) {
      expect(await lines(port, 'PING', 'wrong')).toEqual(['ERR unauthorized (signature)']);
      expect(await lines(port, 'tok PING'.replace(/^/, ''), '')).toEqual(['ERR unauthorized (signature)']);
      const once = signLine('tok LIST');
      expect((await lines(port, once)).at(-1)).toBe('.');
      expect(await lines(port, once)).toEqual(['ERR unauthorized (replay)']);
      expect(await lines(port, signLine('tok PING', Math.floor(Date.now() / 1000) - 1000))).toEqual(['ERR unauthorized (clock)']);
    }
  });

  it('tells where transactions begin in the archive, and refuses a seed', async () => {
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
      replies[request] = await reply(request);
      expect(replies[request].length, request).toBeGreaterThan(0);
    }
    expect(named(replies)).toMatchInlineSnapshot(`
      {
        "PLAN 10 0 ": [
          "50 10 1",
          "51 10 0",
          "506381209866536711 7 1",
          "578721382704613384 8 0",
          ".",
        ],
        "PLAN 11 1": [
          "ERR bad request",
        ],
        "PLAN 11 52 51": [
          "51 10 0",
          "52 11 1",
          "60 11 1",
          "506381209866536711 7 1",
          "578721382704613384 8 0",
          ".",
        ],
        "PLAN 11 99 ": [
          "506381209866536711 7 1",
          "578721382704613384 8 0",
          ".",
        ],
        "PLAN 11 x 1": [
          "ERR bad request",
        ],
        "SEED": [
          "ERR no safe seed source on the primary; add a replica seed source, restart the primary cleanly for a fresh offline seed, or allow live seeds (allowLiveSeedFromPrimary)",
        ],
        "TXNS": [
          "ERR bad request",
        ],
        "TXNS 10 50,52": [
          "50 10",
          ".",
        ],
        "TXNS 11": [
          "ERR bad request",
        ],
        "TXNS 11 ": [
          ".",
        ],
        "TXNS 11 50,52,53": [
          "50 10",
          "52 11",
          ".",
        ],
        "TXNS 11 a": [
          "ERR bad request",
        ],
        "TXNS x 1": [
          "ERR bad request",
        ],
      }
    `);
    expect(replies['TXNS 11 50,52,53']).toEqual(['50 10', '52 11', '.']);
    // (the other fixture segments are filled with one byte, which also reads as blocks of huge transaction numbers)
    expect(replies['PLAN 11 52 51'].filter((l) => !/^\d{6,} /.test(l))).toEqual(['51 10 0', '52 11 1', '60 11 1', '.']);
    expect(replies['SEED'][0]).toMatch(/^ERR no safe seed source on the primary/);
  });

  it('changes what the instance does (SYNC, STANDBY, FENCE, REJOIN, NBACKUP, PROMOTE)', async () => {
    const repl = (dir: string, name: string) => join(root, dir, name);
    const dirs = [['repl-front', () => front.port]] as const;
    // refused arguments, and requests that need the local Firebird server, which is not there,
    // nor are its tools
    const requests = [
      'SYNC', 'SYNC a,,b', 'SYNC -a', 'SYNC a b', 'STANDBY', 'STANDBY off', 'STANDBY off x', 'STANDBY maybe',
      'STANDBY on', 'REJOIN', 'FENCE lease lost', 'NBACKUP', 'NBACKUP 3 a.nbk', 'NBACKUP 0 a.fbk', 'NBACKUP 0 ../a.nbk', 'NBACKUP 0 a.nbk',
      // the promotion: refused arguments, and without a local server the replica mode cannot be read
      'PROMOTE', 'PROMOTE x', 'PROMOTE 7 8', 'PROMOTE 7', 'PROMOTE none',
    ];
    const replies: Record<string, string[]> = {};
    for (const request of requests) {
      replies[request] = await reply(request);
      expect(replies[request].length, request).toBeGreaterThan(0);
    }
    expect(named(replies)).toMatchInlineSnapshot(`
      {
        "FENCE lease lost": [
          "ERR cannot shut ROOT/data/mydb.fdb down",
        ],
        "NBACKUP": [
          "ERR bad request",
        ],
        "NBACKUP 0 ../a.nbk": [
          "ERR bad request",
        ],
        "NBACKUP 0 a.fbk": [
          "ERR bad request",
        ],
        "NBACKUP 0 a.nbk": [
          "ERR nbackup failed",
        ],
        "NBACKUP 3 a.nbk": [
          "ERR bad request",
        ],
        "PROMOTE": [
          "ERR bad request",
        ],
        "PROMOTE 7": [
          "ERR cannot read the replica mode",
        ],
        "PROMOTE 7 8": [
          "ERR bad request",
        ],
        "PROMOTE none": [
          "ERR cannot read the replica mode",
        ],
        "PROMOTE x": [
          "ERR bad request",
        ],
        "REJOIN": [
          "OK",
        ],
        "STANDBY": [
          "ERR bad request",
        ],
        "STANDBY maybe": [
          "ERR bad request",
        ],
        "STANDBY off": [
          "ERR bad request",
        ],
        "STANDBY off x": [
          "ERR bad request",
        ],
        "STANDBY on": [
          "ERR this instance is the primary",
        ],
        "SYNC": [
          "ERR bad request",
        ],
        "SYNC -a": [
          "ERR bad request",
        ],
        "SYNC a b": [
          "ERR bad request",
        ],
        "SYNC a,,b": [
          "ERR bad request",
        ],
      }
    `);
    expect(replies['STANDBY on']).toEqual(['ERR this instance is the primary']);
    expect(replies['FENCE lease lost'][0]).toMatch(/^ERR cannot shut .* down$/);
    expect(replies['NBACKUP 0 a.nbk']).toEqual(['ERR nbackup failed']);
    expect(replies['PROMOTE 7']).toEqual(['ERR cannot read the replica mode']);
    for (const [dir] of dirs) expect(existsSync(repl(dir, '.pause-pull')) || existsSync(repl(dir, 'promoted')), dir).toBe(false);
    // nothing is left of a fence or an nbackup that failed
    for (const [dir] of dirs) expect(existsSync(repl(dir, 'self-fenced')), dir).toBe(false);
    expect(readdirSync(join(root, 'data')).filter((n) => n.startsWith('a.nbk'))).toEqual([]);

    // synchronous replication
    for (const [dir, port] of dirs) {
      expect(await lines(port(), 'SYNC db-1.db-headless,db-2.db-headless'), dir).toEqual(['OK']);
      expect(await lines(port(), 'SYNCTO'), dir).toEqual(['OK db-1.db-headless,db-2.db-headless']);
    }
    expect(readFileSync(repl('repl-front', 'sync.conf'), 'utf8').split(root).join('ROOT')).toMatchInlineSnapshot(`
      "sync_replica = db-1.db-headless:ROOT/data/mydb.fdb
      {
        username = SYSDBA
        password_env = ISC_PASSWORD
      }
      sync_replica = db-2.db-headless:ROOT/data/mydb.fdb
      {
        username = SYSDBA
        password_env = ISC_PASSWORD
      }
      "
    `);
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
    expect(after[0].length).toBe(40);
    expect(after[0].subarray(0, 10).toString('latin1')).toBe('FBREPLCTL\0');
    expect(after[0].readUInt32LE(12)).toBe(0);
    expect(after[0].readBigUInt64LE(16)).toBe(57n);
    expect(after[0].readUInt32LE(24)).toBe(0);
    expect(after[0].readBigUInt64LE(32)).toBe(9n);
  }, 60_000);

  it('prunes the archive and samples the recovery points itself', async () => {
    const now = Math.floor(Date.now() / 1000);
    /** an instance's directories: segments 1 to 6 aged 10 days, 10 days, 2 days, 2 days, an hour, an hour */
    const instance = (name: string, floor: string) => {
      const base = join(root, name);
      for (const d of ['archive', 'source', 'data', 'repl', 'journal']) mkdirSync(join(base, d), { recursive: true });
      [864_000, 864_000, 172_800, 172_800, 3600, 3600].forEach((age, i) => {
        const file = join(base, 'archive', `mydb.fdb.journal-00000000${i + 1}`);
        writeFileSync(file, segment(i + 1, 48));
        utimesSync(file, now - age, now - age);
      });
      writeFileSync(join(base, 'repl', 'retain-floor'), `${floor}\n`);
      // a segment being written in the journal
      writeFileSync(join(base, 'journal', 'mydb.fdb.journal-000000009'), segment(9, 100));
      return {
        base,
        env: {
          ARCHIVE_DIR: join(base, 'archive'),
          DATABASE_PATH: join(base, 'data', 'mydb.fdb'),
          SOURCE_DIR: join(base, 'source'),
          REPLICATION_DIR: join(base, 'repl'),
          JOURNAL_DIR: join(base, 'journal'),
          PRIMARY_FILE: join(root, 'primary'),
          POD_NAME: 'db-0',
          ISC_PASSWORD: 'tok',
          FIREBIRD_PORT: '1',
          RECOVERY_POINTS: 'true',
        },
      };
    };
    const archived = (base: string) => readdirSync(join(base, 'archive')).sort().map((n) => Number(n.slice(-1)));
    const points = (base: string) => {
      try {
        return readFileSync(join(base, 'repl', 'points', '9'), 'utf8').split('\n').filter(Boolean).map((l) => l.split(' ')[1]);
      } catch {
        return [];
      }
    };
    const until = async (what: () => boolean) => {
      for (let i = 0; i < 80 && !what(); i++) await new Promise((r) => setTimeout(r, 100));
    };
    const run = async (floor: string, extra: Record<string, string>) => {
      const fronted = instance(`front-${floor}`, floor);
      const agent = await startFront('', { ...fronted.env, ...extra });
      return { fronted: fronted.base, agent };
    };

    // a replica still needs segment 4 on: kept past the retention age, 3 is not, and none past the maximum
    const kept = await run('3', {});
    await until(() => archived(kept.fronted).length === 3);
    expect(archived(kept.fronted)).toEqual([4, 5, 6]);
    // the pruning may finish before its line is read off the agent's stdout (the archive was
    // already pruned on the first look, without the event loop running in between)
    await until(() => kept.agent.out().includes('the agent prunes the archive'));
    expect(kept.agent.out()).toContain('the agent prunes the archive and samples the recovery points\n');
    // each new length of the segment being written is a recovery point, once
    await until(() => points(kept.fronted).length === 1);
    writeFileSync(join(kept.fronted, 'journal', 'mydb.fdb.journal-000000009'), segment(9, 260));
    await until(() => points(kept.fronted).length === 2);
    await new Promise((r) => setTimeout(r, 1500));
    expect(points(kept.fronted)).toEqual(['100', '260']);

    // early pruning: what every replica applied goes at once, whatever its age
    const early = await run('6', { PRUNE_APPLIED: 'true' });
    await until(() => archived(early.fronted).length === 1);
    expect(archived(early.fronted)).toEqual([6]);
  }, 60_000);

  it('runs the isolation check itself, once: a primary cut off from everything is fenced', async () => {
    // a fake fbsvcmgr that records its calls and reports the database as shut down afterwards (the
    // agent's own attachment finds no server here and falls back to it)
    mkdirSync(join(root, 'bin'));
    writeFileSync(
      join(root, 'bin', 'fbsvcmgr'),
      `#!/bin/sh\necho "$*" >> "${root}/calls"\ncase "$*" in *sts_hdr_pages*) echo "Attributes force write, full shutdown";; esac\nexit 0\n`,
    );
    chmodSync(join(root, 'bin', 'fbsvcmgr'), 0o755);
    const repl = join(root, 'repl-isolated');
    mkdirSync(repl);
    const agent = await startFront('', {
      ...env(),
      REPLICATION_DIR: repl,
      PATH: `${join(root, 'bin')}:${process.env.PATH}`,
      // neither the API server nor a peer is reachable, for two seconds
      ISOLATION_TIMEOUT_SECONDS: '2',
      CHECK_INTERVAL: '1',
      CONNECT_TIMEOUT: '1',
      KUBERNETES_SERVICE_HOST: '127.0.0.1',
      KUBERNETES_SERVICE_PORT: '1',
    });
    // printed after the listening line the start waits for
    for (let i = 0; i < 50 && !agent.out().includes('the agent runs the isolation check\n'); i++) await new Promise((r) => setTimeout(r, 100));
    expect(agent.out()).toContain('the agent runs the isolation check\n');
    for (let i = 0; i < 100 && !existsSync(join(repl, 'self-fenced')); i++) await new Promise((r) => setTimeout(r, 100));
    expect(existsSync(join(repl, 'self-fenced'))).toBe(true);
    expect(agent.out()).toMatch(/isolated: neither the Kubernetes API server nor another instance reachable for \d+s; fencing the primary/);
    // one full shutdown
    await new Promise((r) => setTimeout(r, 2500));
    const calls = readFileSync(join(root, 'calls'), 'utf8').trim().split('\n');
    expect(calls.filter((c) => c.includes('prp_shutdown_mode'))).toEqual([
      `localhost:service_mgr action_properties dbname ${join(root, 'data', 'mydb.fdb')} prp_shutdown_mode prp_sm_full prp_force_shutdown 0`,
    ]);
    // the fence shows in its answers
    expect(await lines(agent.port, 'ISOLATION')).toEqual([expect.stringMatching(/^OK fenced \d+$/)]);
  }, 30_000);

  it('copies, lists and removes the backup files', async () => {
    const data = (name: string) => join(root, 'data', name);
    mkdirSync(data('dir.nbk'));
    writeFileSync(data('mydb.fdb'), 'x');
    writeFileSync(data('empty.fbk'), '');
    writeFileSync(data('replica.nbk.ctl'), 'c');
    const requests = [
      'FILES', 'EXISTS mydb.fdb', 'EXISTS none.fdb', 'EXISTS dir.nbk', 'EXISTS', 'EXISTS ../primary', 'EXISTS .hidden',
      // refused, or not a file
      'FILE', 'FILE none.nbk', 'FILE mydb.fdb', 'FILE ../primary', 'FILE dir.nbk', 'STORE', 'STORE a.nbk', 'STORE a.nbk x',
      'STORE mydb.fdb 1', 'STORE ../a.nbk 1', 'REMOVE', 'REMOVE mydb.fdb', 'REMOVE ../primary', 'REMOVE none.nbk', 'REMOVE dir.nbk',
    ];
    const replies: Record<string, string[]> = {};
    for (const request of requests) {
      replies[request] = await reply(request);
      expect(replies[request].length, request).toBeGreaterThan(0);
    }
    expect(named(replies)).toMatchInlineSnapshot(`
      {
        "EXISTS": [
          "ERR bad request",
        ],
        "EXISTS ../primary": [
          "ERR bad request",
        ],
        "EXISTS .hidden": [
          "ERR bad request",
        ],
        "EXISTS dir.nbk": [
          "OK yes",
        ],
        "EXISTS mydb.fdb": [
          "OK yes",
        ],
        "EXISTS none.fdb": [
          "OK no",
        ],
        "FILE": [
          "ERR bad request",
        ],
        "FILE ../primary": [
          "ERR bad request",
        ],
        "FILE dir.nbk": [
          "ERR bad request",
        ],
        "FILE mydb.fdb": [
          "ERR bad request",
        ],
        "FILE none.nbk": [
          "ERR bad request",
        ],
        "FILES": [
          "empty.fbk",
          "nightly.nbk",
          "replica.nbk.ctl",
          ".",
        ],
        "REMOVE": [
          "ERR bad request",
        ],
        "REMOVE ../primary": [
          "ERR bad request",
        ],
        "REMOVE dir.nbk": [
          "ERR remove dir.nbk: Is a directory",
        ],
        "REMOVE mydb.fdb": [
          "ERR bad request",
        ],
        "REMOVE none.nbk": [
          "OK",
        ],
        "STORE": [
          "ERR bad request",
        ],
        "STORE ../a.nbk 1": [
          "ERR bad request",
        ],
        "STORE a.nbk": [
          "ERR bad request",
        ],
        "STORE a.nbk x": [
          "ERR bad request",
        ],
        "STORE mydb.fdb 1": [
          "ERR bad request",
        ],
      }
    `);
    expect(await lines(front.port, 'FILES')).toEqual(['empty.fbk', 'nightly.nbk', 'replica.nbk.ctl', '.']);
    expect(existsSync(data('mydb.fdb'))).toBe(true);
    expect((await ask(front.port, 'FILE nightly.nbk')).equals(Buffer.concat([Buffer.from('OK 70000\n'), Buffer.alloc(70_000, 3)]))).toBe(true);
    expect((await ask(front.port, 'FILE empty.fbk')).toString()).toBe('OK 0\n');
    // uploads: whole, empty, and one whose client stops early (nothing is left behind)
    const body = Buffer.alloc(300_000, 9);
    for (const [who, port] of [['front', front.port]] as const) {
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

  it('without replication (FILES_ONLY) serves the backup files and refuses the rest', async () => {
    const own = { DATABASE_PATH: join(root, 'data', 'mydb.fdb'), POD_NAME: 'db-0', ISC_PASSWORD: 'tok', FIREBIRD_PORT: '1', FILES_ONLY: 'true' };
    const frontFiles = await startFront('', own);
    expect(frontFiles.out()).toMatch(/backup file server \(fb-agent [^)]+\) listening on /);
    const requests = ['PING', 'FILES', 'EXISTS nightly.nbk', 'LIST', 'GET mydb.fdb.journal-000000007', 'WAIT 1', 'STATE', 'POSITION', 'HEADER', 'VERSION', 'ARCHIVED', 'SEED', 'NOSUCH'];
    const replies: Record<string, string[]> = {};
    for (const request of requests) {
      replies[request] = await lines(frontFiles.port, request);
      expect(replies[request].length, request).toBeGreaterThan(0);
    }
    expect(named(replies)).toMatchInlineSnapshot(`
      {
        "ARCHIVED": [
          "ERR not available without replication",
        ],
        "EXISTS nightly.nbk": [
          "OK yes",
        ],
        "FILES": [
          "nightly.nbk",
          ".",
        ],
        "GET mydb.fdb.journal-000000007": [
          "ERR not available without replication",
        ],
        "HEADER": [
          "ERR not available without replication",
        ],
        "LIST": [
          "ERR not available without replication",
        ],
        "NOSUCH": [
          "ERR not available without replication",
        ],
        "PING": [
          "OK",
        ],
        "POSITION": [
          "ERR not available without replication",
        ],
        "SEED": [
          "ERR not available without replication",
        ],
        "STATE": [
          "ERR not available without replication",
        ],
        "VERSION": [
          "ERR not available without replication",
        ],
        "WAIT 1": [
          "ERR not available without replication",
        ],
      }
    `);
    expect(await lines(frontFiles.port, 'LIST')).toEqual(['ERR not available without replication']);
    expect(await lines(frontFiles.port, 'FILES')).toEqual(['nightly.nbk', '.']);
    const file = Buffer.concat([Buffer.from('OK 70000\n'), Buffer.alloc(70_000, 3)]);
    expect((await ask(frontFiles.port, 'FILE nightly.nbk')).equals(file)).toBe(true);
    const body = Buffer.alloc(120_000, 4);
    expect((await ask(frontFiles.port, `STORE restore-2.nbk ${body.length}`, body)).toString()).toBe('OK\n');
    expect(readFileSync(join(root, 'data', 'restore-2.nbk')).equals(body)).toBe(true);
  });

  it('answers requests of every kind', async () => {
    const replies: Record<string, string[]> = {};
    for (const request of ['PRIMARYSEEN', 'FILES', 'EXISTS', 'ISOLATION', 'SYNCTO', 'POINTS 7', 'RETAIN none', 'FOLLOW', 'NOSUCH 1']) {
      replies[request] = await reply(request);
      expect(replies[request].length, request).toBeGreaterThan(0);
    }
    expect(named(replies)).toMatchInlineSnapshot(`
      {
        "EXISTS": [
          "ERR bad request",
        ],
        "FILES": [
          "nightly.nbk",
          ".",
        ],
        "FOLLOW": [
          "OK none",
        ],
        "ISOLATION": [
          "OK online",
        ],
        "NOSUCH 1": [
          "ERR bad request",
        ],
        "POINTS 7": [
          ".",
        ],
        "PRIMARYSEEN": [
          "OK never",
        ],
        "RETAIN none": [
          "OK",
        ],
        "SYNCTO": [
          "OK none",
        ],
      }
    `);
    // segments 7 and 8 have a header (8 is still being copied), 9 not; their ages in seconds
    expect(await reply('ARCHIVED')).toEqual([expect.stringMatching(/^7 \d+$/), expect.stringMatching(/^8 \d+$/), '.']);
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
    // the requests count as contact for the isolation check
    for (const [dir, port] of [['repl-front', front.port]] as const) {
      rmSync(join(root, dir, 'last-contact'), { force: true });
      await lines(port, 'LIST');
      expect(Math.abs(Number(readFileSync(join(root, dir, 'last-contact'), 'utf8')) - Date.now() / 1000), dir).toBeLessThan(5);
    }
  });

  it('WAIT answers as soon as a complete segment is archived', async () => {
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
    // the file is in place before the line about it reaches this process
    await waitFor(() => out.includes('fetched mydb.fdb.journal-000000012 from 127.0.0.1\n'), 'the line about the last segment', 3000);
  });

  it('accepts mutual TLS itself, and its puller connects over TLS itself', async () => {
    const certs = (caName: string) => {
      const c = createTlsCertificates({ caName, dnsNames: ['*.db-headless'], clientAuth: true });
      return { ca: c.caCert, cert: c.tlsCert, key: c.tlsKey };
    };
    const files = certs('cluster db CA');
    const tlsDir = join(root, 'tls');
    const peersDir = join(root, 'tls-peers');
    for (const d of [tlsDir, peersDir, join(root, 'repl-tls')]) mkdirSync(d);
    writeFileSync(join(tlsDir, 'ca.crt'), files.ca);
    writeFileSync(join(tlsDir, 'tls.key'), files.key);
    writeFileSync(join(tlsDir, 'tls.crt'), files.cert);
    const direct = await startFront('', {
      ...env(),
      REPLICATION_DIR: join(root, 'repl-tls'),
      SEGMENT_TLS_LISTEN: '127.0.0.1:0',
      SEGMENT_TLS_DIR: tlsDir,
      SEGMENT_TLS_PEERS_DIR: peersDir,
    });
    for (let i = 0; i < 100 && !/accepts mutual TLS itself on/.test(direct.out()); i++) await new Promise((r) => setTimeout(r, 50));
    const tlsPort = Number(/accepts mutual TLS itself on 127\.0\.0\.1:(\d+)/.exec(direct.out())![1]);
    // a client of the cluster: mutual TLS with its certificates (the name is not checked, as
    // the agent's clients do not check it)
    const through = (client: { ca: string; cert: string; key: string }, request: string, body?: Buffer): Promise<Buffer> =>
      new Promise((resolve, reject) => {
        const sock = tlsConnect({ host: '127.0.0.1', port: tlsPort, ...client, minVersion: 'TLSv1.3', checkServerIdentity: () => undefined });
        const chunks: Buffer[] = [];
        sock.once('error', (err) => (chunks.length ? resolve(Buffer.concat(chunks)) : reject(err)));
        sock.on('data', (d: Buffer) => chunks.push(d));
        sock.once('close', () => resolve(Buffer.concat(chunks)));
        sock.once('secureConnect', () => {
          sock.write(`${signLine(`tok ${request}`)}\n`);
          if (body) sock.write(body);
        });
      });
    // the same replies as on the plain port
    for (const request of [
      'PING', 'LIST', 'GET mydb.fdb.journal-000000007', 'FILES', 'FILE nightly.nbk', 'POSITION', 'ISOLATION',
      'WAIT 1 mydb.fdb.journal-000000009', 'NOSUCH', 'GET ../primary', 'STORE bad/name 5', 'PROMOTE x',
    ]) {
      const [plain, encrypted] = [await ask(direct.port, request), await through(files, request)];
      expect(encrypted.equals(plain), `${request}: ${encrypted.subarray(0, 60).toString()} / ${plain.subarray(0, 60).toString()}`).toBe(true);
      expect(plain.length, request).toBeGreaterThan(0);
    }
    // an upload over TLS
    const body = Buffer.alloc(400_000, 6);
    expect((await through(files, `STORE tls-1.nbk ${body.length}`, body)).toString()).toBe('OK\n');
    expect(readFileSync(join(root, 'data', 'tls-1.nbk')).equals(body)).toBe(true);
    // a request over TLS counts as contact
    expect(existsSync(join(root, 'repl-tls', 'last-contact'))).toBe(true);

    // another cluster's client is refused before anything is read; so is plain text
    expect((await through(certs('cluster other CA'), 'PING').catch(() => Buffer.alloc(0))).toString()).toBe('');
    const plainly = (request: string) => ask(tlsPort, request).then((b) => b.toString()).catch(() => '');
    expect(await plainly('PING')).toBe('');
    // plain text while the operator allows it (the instances not switched yet)
    writeFileSync(join(peersDir, 'accept-plain-until'), String(Date.now() + 60_000));
    expect(await plainly('PING')).toBe('OK\n');
    writeFileSync(join(peersDir, 'accept-plain-until'), '0');
    expect(await plainly('PING')).toBe('');

    // the agent's puller: TLS itself, to the front (no proxy on either side)
    const pull = async (name: string, port: number, extra: Record<string, string> = {}) => {
      const dir = join(root, name);
      mkdirSync(join(dir, 'source'), { recursive: true });
      writeFileSync(join(dir, 'primary'), '127.0.0.1\n');
      let out = '';
      const puller = spawn(agentBin!, ['puller'], {
        env: {
          PATH: process.env.PATH ?? '',
          SOURCE_DIR: join(dir, 'source'),
          STATE_FILE: join(dir, '.last-pulled'),
          PRIMARY_FILE: join(dir, 'primary'),
          REPLICATION_DIR: dir,
          POD_NAME: 'db-1',
          ISC_PASSWORD: 'tok',
          SEGMENT_PORT: String(port),
          POLL_SECONDS: '20',
          FIREBIRD_PORT: '1',
          // segment TLS is on, and this process has the certificates
          SEGMENT_TLS: 'true',
          SEGMENT_TLS_DIR: tlsDir,
          SEGMENT_TLS_PEERS_DIR: peersDir,
          ...extra,
        },
      });
      children.push(puller);
      puller.stdout.on('data', (d) => (out += d.toString()));
      puller.stderr.on('data', (d) => (out += d.toString()));
      const got = () => readdirSync(join(dir, 'source')).filter((n) => n.includes('journal')).sort();
      for (let i = 0; i < 100 && got().length < 2; i++) await new Promise((r) => setTimeout(r, 50));
      return { got, out: () => out, dir };
    };
    const own = await pull('replica-tls', tlsPort);
    expect(own.got(), own.out()).toEqual(['mydb.fdb.journal-000000007', 'mydb.fdb.journal-000000009']);
    expect(readFileSync(join(own.dir, 'source', 'mydb.fdb.journal-000000007')).equals(segment(7, 300_000))).toBe(true);
    // and it waits on the front over TLS: a new segment is fetched at once
    const at = Date.now();
    writeFileSync(join(archive(), 'mydb.fdb.journal-000000010'), segment(10, 20_000));
    for (let i = 0; i < 100 && own.got().length < 3; i++) await new Promise((r) => setTimeout(r, 50));
    expect(own.got().length, own.out()).toBe(3);
    expect(Date.now() - at).toBeLessThan(2500);

    // an instance the operator lists as plain is reached in plain text ("127" names 127.0.0.1)
    writeFileSync(join(peersDir, 'plain-peers'), '127\n');
    const plainPeer = await pull('replica-plain', direct.port);
    expect(plainPeer.got().length, plainPeer.out()).toBeGreaterThanOrEqual(2);
    // without the certificates of the cluster nothing is fetched
    writeFileSync(join(peersDir, 'plain-peers'), '');
    const otherDir = join(root, 'tls-other');
    mkdirSync(otherDir);
    const other = certs('cluster other CA');
    writeFileSync(join(otherDir, 'ca.crt'), other.ca);
    writeFileSync(join(otherDir, 'tls.key'), other.key);
    writeFileSync(join(otherDir, 'tls.crt'), other.cert);
    const refused = await pull('replica-other', tlsPort, { SEGMENT_TLS_DIR: otherDir });
    expect(refused.got()).toEqual([]);
    expect(refused.out()).toMatch(/TLS handshake failed|not an instance of this cluster|connect|refused|alert/i);
  }, 60_000);

  it('stops once the Firebird server has stopped', async () => {
    // 'close': the output has been read when it fires, which 'exit' does not wait for
    const exited = new Promise<number | null>((resolve) => front.child.once('close', (code) => resolve(code)));
    front.child.kill('SIGTERM');
    // nothing listens on FIREBIRD_PORT: it stops at once
    expect(await exited).toBe(0);
    expect(front.out()).toContain('stopping: the Firebird server has stopped');
  });
});
