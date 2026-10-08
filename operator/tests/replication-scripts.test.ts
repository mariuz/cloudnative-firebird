import { describe, it, expect } from 'vitest';
import { execFileSync, spawn, spawnSync } from 'child_process';
import { existsSync, mkdtempSync, readdirSync, readFileSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { fakeSegmentServer } from './helpers/segment-auth';
import { JOB_SCRIPTS, REPLICATION_SCRIPTS } from '../src/utils/replication';

const SCRIPTS: Record<string, string> = { ...REPLICATION_SCRIPTS, ...JOB_SCRIPTS };

const hasPerl = spawnSync('perl', ['-v']).status === 0;
/** Modules of the Firebird images' perl-base that the scripts use (checked in the image) */
const PERL_BASE_MODULES = ['IO::Select', 'IO::Socket::INET', 'POSIX', 'Socket'];
const dir = mkdtempSync(join(tmpdir(), 'fb-repl-scripts-'));

describe('replication scripts shipped to instance pods', () => {
  it.each(Object.keys(SCRIPTS).filter((name) => name.endsWith('.pl')))(
    '%s compiles with core perl only',
    (name) => {
      if (!hasPerl) return;
      const file = join(dir, name);
      writeFileSync(file, SCRIPTS[name]);
      expect(() => execFileSync('perl', ['-c', file], { stdio: 'pipe' })).not.toThrow();
      // the Firebird image ships perl-base only (no HTTP::Tiny, File::Copy, Digest::*, Time::HiRes):
      // only modules checked in the image (perl -c in firebirdsql/firebird:5) are allowed
      const modules = [...SCRIPTS[name].matchAll(/^\s*(?:use|require)\s+([A-Z][\w:]*)/gm)].map((m) => m[1]);
      expect(modules.filter((m) => !PERL_BASE_MODULES.includes(m))).toEqual([]);
    },
  );

  it.each(Object.keys(SCRIPTS).filter((name) => name.endsWith('.sh')))('%s is valid POSIX sh', (name) => {
    const file = join(dir, name);
    writeFileSync(file, SCRIPTS[name]);
    expect(() => execFileSync('sh', ['-n', file], { stdio: 'pipe' })).not.toThrow();
  });

  it('init-instance.sh moves the database into place only after it is complete', () => {
    const lines = REPLICATION_SCRIPTS['init-instance.sh'].split('\n');
    const lastMove = lines.map((l, i) => (l.includes('mv "$work" "$DATABASE_PATH"') ? i : -1)).filter((i) => i >= 0);
    expect(lastMove).toHaveLength(2); // primary bootstrap and replica seed
    expect(lastMove[1]).toBeGreaterThan(lines.findIndex((l) => l.includes('gfix -replica read_only')));
    expect(lastMove[1]).toBeGreaterThan(lines.findIndex((l) => l.includes('mv "$SOURCE_DIR/.control.tmp"')));
  });

  it('init-instance.sh prefers replica seed sources over the primary', () => {
    expect(REPLICATION_SCRIPTS['init-instance.sh']).toContain('for source in $(cat "$SEED_SOURCES_FILE" 2>/dev/null || true) "$primary"');
  });

  it('replica-control.pl --adopt keeps the source position and rewrites db_sequence', () => {
    if (!hasPerl) return;
    const script = join(dir, 'replica-control.pl');
    writeFileSync(script, REPLICATION_SCRIPTS['replica-control.pl']);
    const source = join(dir, 'source.ctl');
    const header = Buffer.alloc(40);
    header.write('FBREPLCTL', 0, 'latin1');
    header.writeUInt16LE(1, 10);
    header.writeUInt32LE(1, 12);
    header.writeBigUInt64LE(42n, 16);
    header.writeBigUInt64LE(7n, 32);
    const txn = Buffer.alloc(16);
    txn.writeBigUInt64LE(900n, 0);
    txn.writeBigUInt64LE(40n, 8);
    writeFileSync(source, Buffer.concat([header, txn]));
    const target = join(dir, 'adopted.ctl');
    execFileSync('perl', [script, '--adopt', source, '9', target], { stdio: 'pipe' });
    const out = readFileSync(target);
    expect(out.length).toBe(56);
    expect(out.readBigUInt64LE(16)).toBe(42n); // position kept
    expect(out.readBigUInt64LE(32)).toBe(9n); // db_sequence rewritten
    expect(out.readBigUInt64LE(40)).toBe(900n); // active transaction kept
  });

  it('replica-control.pl writes ControlFile::DataV1 with no transactions when there are no candidates', () => {
    if (!hasPerl) return;
    const script = join(dir, 'replica-control.pl');
    writeFileSync(script, REPLICATION_SCRIPTS['replica-control.pl']);
    const target = join(dir, '{GUID}');
    execFileSync('perl', [script, 'unused-host', '15', '16', target], { stdio: 'pipe' });
    const out = readFileSync(target);
    expect(out.length).toBe(40);
    expect(out.subarray(0, 10).toString('latin1')).toBe('FBREPLCTL\0');
    expect(out.readUInt16LE(10)).toBe(1); // version
    expect(out.readUInt32LE(12)).toBe(0); // txn_count
    expect(out.readBigUInt64LE(16)).toBe(15n); // sequence: the copy contains segments <= S
    expect(out.readUInt32LE(24)).toBe(0); // offset
    expect(out.readBigUInt64LE(32)).toBe(16n); // db_sequence: the copy's own header value
  });

  it('replica-control.pl records journaled candidates as sorted active transactions', async () => {
    if (!hasPerl) return;
    const requests: string[] = [];
    // requests are recorded as "tok <request>" once their signature is checked
    const server = fakeSegmentServer((request, sock) => {
      requests.push(`tok ${request}\n`);
      sock.end('30 7\n12 5\n.\n'); // 12 starts in segment 5, 30 in 7; 99 is not journaled
    }, 'tok');
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const port = (server.address() as { port: number }).port;
    const script = join(dir, 'replica-control.pl');
    writeFileSync(script, REPLICATION_SCRIPTS['replica-control.pl']);
    const target = join(dir, '{GUID2}');
    const run = () =>
      new Promise<void>((resolve, reject) => {
        const child = spawn('perl', [script, '127.0.0.1', '9', '10', target, '30', '12', '99'], {
          env: { ...process.env, SEGMENT_PORT: String(port), ISC_PASSWORD: 'tok' },
        });
        child.on('exit', (code) => (code === 0 ? resolve() : reject(new Error(`exit ${code}`))));
      });
    await run();
    server.close();

    expect(requests).toEqual(['tok TXNS 9 30,12,99\n']);
    const out = readFileSync(target);
    expect(out.length).toBe(40 + 2 * 16);
    expect(out.readUInt32LE(12)).toBe(2); // txn_count
    expect(out.readBigUInt64LE(40)).toBe(12n); // sorted by tra_id
    expect(out.readBigUInt64LE(48)).toBe(5n);
    expect(out.readBigUInt64LE(56)).toBe(30n);
    expect(out.readBigUInt64LE(64)).toBe(7n);
  });

  describe('replica-control.pl --next (live seeds)', () => {
    /** Runs replica-control.pl against a stand-in segment server answering `reply(command)` */
    const plan = async (reply: (cmd: string) => string) => {
      const requests: string[] = [];
      const server = fakeSegmentServer((request, sock) => {
        const line = `tok ${request}`;
        requests.push(line);
        sock.end(reply(line.split(' ')[1]));
      }, 'tok');
      await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
      const port = (server.address() as { port: number }).port;
      const script = join(dir, 'replica-control.pl');
      writeFileSync(script, REPLICATION_SCRIPTS['replica-control.pl']);
      const target = join(dir, `{GUID-${requests.length}-${Math.random()}}`);
      const result = await new Promise<{ code: number | null; text: string }>((resolve) => {
        const child = spawn('perl', [script, '--next', '40', '127.0.0.1', '9', '9', target, '30', '12'], {
          env: { ...process.env, SEGMENT_PORT: String(port), ISC_PASSWORD: 'tok' },
        });
        let text = '';
        child.stdout.on('data', (d) => (text += d));
        child.stderr.on('data', (d) => (text += d));
        child.on('exit', (code) => resolve({ code, text }));
      });
      server.close();
      return { ...result, requests, target };
    };

    it('records the candidates and the transactions after the copy\'s next one from PLAN', async () => {
      if (!hasPerl) return;
      // 12 began in segment 2 (long before the lock), 41 started after the copy's next transaction
      const r = await plan(() => '12 2 1\n30 9 1\n41 9 1\n.\n');
      expect(r.code).toBe(0);
      expect(r.requests).toEqual(['tok PLAN 9 40 30,12']);
      const out = readFileSync(r.target);
      expect(out.readUInt32LE(12)).toBe(3);
      expect([40, 48, 56, 64, 72, 80].map((o) => out.readBigUInt64LE(o))).toEqual([12n, 2n, 30n, 9n, 41n, 9n]);
    });

    it('refuses a transaction whose first archived block does not begin it', async () => {
      if (!hasPerl) return;
      const r = await plan(() => '12 4 0\n.\n');
      expect(r.code).not.toBe(0);
      expect(r.text).toContain('transaction 12 began before the oldest archived segment');
    });

    it('falls back to TXNS on a segment server without PLAN', async () => {
      if (!hasPerl) return;
      const r = await plan((cmd) => (cmd === 'PLAN' ? 'ERR bad request\n' : '12 5\n.\n'));
      expect(r.code).toBe(0);
      expect(r.requests).toEqual(['tok PLAN 9 40 30,12', 'tok TXNS 9 30,12']);
      expect(readFileSync(r.target).readUInt32LE(12)).toBe(1);
    });
  });

  it('fetch-segments.pl fetches archived segments that are not uploaded yet', async () => {
    if (!hasPerl) return;
    const segments: Record<string, string> = {
      'mydb.fdb.journal-0000000001': 'one',
      'mydb.fdb.journal-0000000002': 'two',
      'mydb.fdb.journal-0000000003': 'three',
    };
    const requests: string[] = [];
    const server = fakeSegmentServer((request, sock) => {
      const line = `tok ${request}`;
      requests.push(line);
      const [, cmd, name] = line.split(' ');
      if (cmd === 'LIST') sock.end(Object.keys(segments).join('\n') + '\nnot-a-segment\n.\n');
      else if (cmd === 'ARCHIVED') sock.end('1 7200\n2 3600\n3 60\n.\n');
      else if (cmd === 'LINEAGE') sock.end('mydb.fdb.lineage-1-2\nmydb.fdb.lineage-0-1\nbad/name\n.\n');
      else if (cmd === 'POINTS') sock.end(name === '3' ? '1700000000 150\n1700000001 252\nbad\n.\n' : '.\n');
      else sock.end(`OK ${segments[name].length}\n${segments[name]}`);
    }, 'tok');
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const port = (server.address() as { port: number }).port;
    const script = join(dir, 'fetch-segments.pl');
    writeFileSync(script, JOB_SCRIPTS['fetch-segments.pl']);
    const out = mkdtempSync(join(tmpdir(), 'fb-segments-'));
    const skip = join(out, 'uploaded');
    writeFileSync(skip, 'mydb.fdb.journal-0000000001\nmydb.fdb.lineage-0-1\n');
    const segDir = join(out, 'segments');
    await new Promise<void>((resolve, reject) => {
      const child = spawn('perl', [script], {
        env: { ...process.env, FIREBIRD_HOST: '127.0.0.1', SEGMENT_PORT: String(port), ISC_PASSWORD: 'tok', OUT_DIR: segDir, SKIP_FILE: skip, RESULT_FILE: join(out, 'result') },
      });
      child.on('exit', (code) => (code === 0 ? resolve() : reject(new Error(`exit ${code}`))));
    });
    server.close();

    expect(requests).toEqual([
      'tok LIST',
      'tok LINEAGE',
      'tok ARCHIVED',
      'tok GET mydb.fdb.journal-0000000002',
      'tok POINTS 2',
      'tok GET mydb.fdb.journal-0000000003',
      'tok POINTS 3',
    ]);
    const files = readdirSync(segDir).sort();
    expect(files.filter((f) => !f.includes('.archived-'))).toEqual([
      'mydb.fdb.journal-0000000002',
      'mydb.fdb.journal-0000000003',
      'mydb.fdb.journal-0000000003.points',
      // a lineage marker not uploaded yet: an empty object for point-in-time recovery
      'mydb.fdb.lineage-1-2',
    ]);
    expect(readFileSync(join(segDir, 'mydb.fdb.lineage-1-2'), 'utf8')).toBe('');
    // recovery points of a segment that has any
    expect(readFileSync(join(segDir, 'mydb.fdb.journal-0000000003.points'), 'utf8')).toBe('1700000000 150\n1700000001 252\n');
    expect(existsSync(join(segDir, 'mydb.fdb.journal-0000000002.points'))).toBe(false);
    expect(readFileSync(join(segDir, 'mydb.fdb.journal-0000000003'), 'utf8')).toBe('three');
    // the highest sequence it may upload, for the operator
    expect(readFileSync(join(out, 'result'), 'utf8')).toBe('listed=3');
    // archive time markers (empty), for point-in-time recovery: now minus the age on the primary
    const markers = files.filter((f) => f.includes('.archived-'));
    expect(markers).toHaveLength(2);
    const at = (seq: string) => {
      const m = markers.find((f) => f.startsWith(`mydb.fdb.journal-000000000${seq}.archived-`))!;
      const [, y, mo, d, h, mi, sec] = /archived-(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z$/.exec(m)!;
      return Date.UTC(+y, +mo - 1, +d, +h, +mi, +sec);
    };
    expect(Math.abs(at('3') - (Date.now() - 60_000))).toBeLessThan(10_000);
    expect(Math.abs(at('2') - (Date.now() - 3_600_000))).toBeLessThan(10_000);
    expect(readFileSync(join(segDir, markers[0]), 'utf8')).toBe('');
  });
});

describe('pitr-plan.pl --describe', () => {
  it('reads a replica\'s position and the first segment its transactions in progress need', () => {
    if (!hasPerl) return;
    const ctlDir = mkdtempSync(join(tmpdir(), 'fb-ctl-'));
    const control = join(ctlDir, 'position.ctl');
    const b = Buffer.alloc(40 + 32);
    b.write('FBREPLCTL', 0, 'latin1');
    b.writeUInt16LE(1, 10);
    b.writeUInt32LE(2, 12);
    b.writeBigUInt64LE(21n, 16);
    b.writeBigUInt64LE(5n, 32);
    b.writeBigUInt64LE(700n, 40);
    b.writeBigUInt64LE(19n, 48);
    b.writeBigUInt64LE(701n, 56);
    b.writeBigUInt64LE(17n, 64);
    writeFileSync(control, b);
    const script = join(dir, 'pitr-plan.pl');
    writeFileSync(script, JOB_SCRIPTS['pitr-plan.pl']);
    expect(spawnSync('perl', [script, '--describe', control], { encoding: 'utf8' }).stdout).toBe('21 0 17\n');
    writeFileSync(control, b.subarray(0, 40));
    // no transaction in progress: the position itself (a truncated list is refused)
    b.writeUInt32LE(0, 12);
    writeFileSync(control, b.subarray(0, 40));
    expect(spawnSync('perl', [script, '--describe', control], { encoding: 'utf8' }).stdout).toBe('21 0 21\n');
    b.writeUInt32LE(3, 12);
    writeFileSync(control, b);
    expect(spawnSync('perl', [script, '--describe', control]).status).not.toBe(0);
  });
});

describe('pitr-plan.pl --reposition', () => {
  it('continues after the given segment with no active transaction, keeping db_sequence', () => {
    if (!hasPerl) return;
    const ctlDir = mkdtempSync(join(tmpdir(), 'fb-ctl-'));
    const control = join(ctlDir, '{GUID}');
    const b = Buffer.alloc(40 + 32);
    b.write('FBREPLCTL', 0, 'latin1');
    b.writeUInt16LE(1, 10);
    b.writeUInt32LE(2, 12); // two active transactions
    b.writeBigUInt64LE(21n, 16);
    b.writeUInt32LE(300, 24);
    b.writeBigUInt64LE(5n, 32);
    writeFileSync(control, b);
    const script = join(dir, 'pitr-plan.pl');
    writeFileSync(script, JOB_SCRIPTS['pitr-plan.pl']);
    const r = spawnSync('perl', [script, '--reposition', '41', control], { encoding: 'utf8' });
    expect(r.status).toBe(0);
    const ctl = readFileSync(control);
    expect(ctl.length).toBe(40);
    expect(ctl.subarray(0, 9).toString('latin1')).toBe('FBREPLCTL');
    expect(ctl.readUInt16LE(10)).toBe(1);
    expect(ctl.readUInt32LE(12)).toBe(0);
    expect(Number(ctl.readBigUInt64LE(16))).toBe(41);
    expect(ctl.readUInt32LE(24)).toBe(0);
    expect(Number(ctl.readBigUInt64LE(32))).toBe(5);
    expect(spawnSync('perl', [script, '--reposition', 'x', control]).status).not.toBe(0);
  });
});

describe('pitr-plan.pl', () => {
  const BEGIN = 1;
  const END = 2;
  /** A journal segment: [transaction, flags] blocks with a few payload bytes each */
  const segment = (seq: number, blocks: Array<[number, number]>): Buffer => {
    const body = Buffer.concat(
      blocks.map(([tra, flags]) => {
        const b = Buffer.alloc(16 + 3);
        b.writeBigUInt64LE(BigInt(tra), 0);
        b.writeUInt16LE(1, 8);
        b.writeUInt16LE(flags, 10);
        b.writeUInt32LE(3, 12);
        return b;
      }),
    );
    const h = Buffer.alloc(48);
    h.write('FBCHANGELOG', 0, 'latin1');
    h.writeUInt16LE(1, 12);
    h.writeUInt16LE(3, 14);
    h.writeBigUInt64LE(BigInt(seq), 32);
    h.writeBigUInt64LE(BigInt(48 + body.length), 40);
    return Buffer.concat([h, body]);
  };
  // backup at segment 5, transactions 100..109 in the copy; 100 and 105 not committed in it
  const SEGMENTS: Record<number, Array<[number, number]>> = {
    4: [[90, BEGIN | END], [100, BEGIN]],
    5: [[100, 0], [105, BEGIN], [101, BEGIN | END]],
    6: [[100, END], [112, BEGIN], [105, END], [112, END]],
  };
  const plan = (seqs: number[], extra: Record<number, Array<[number, number]>> = {}) => {
    const segDir = mkdtempSync(join(tmpdir(), 'fb-pitr-'));
    for (const seq of seqs) {
      writeFileSync(join(segDir, `db.fdb.journal-${String(seq).padStart(9, '0')}`), segment(seq, extra[seq] ?? SEGMENTS[seq]));
    }
    const candidates = join(segDir, 'candidates');
    writeFileSync(candidates, '100\n105\n');
    const control = join(segDir, '{GUID}');
    const script = join(dir, 'pitr-plan.pl');
    writeFileSync(script, JOB_SCRIPTS['pitr-plan.pl']);
    const r = spawnSync('perl', [script, segDir, '5', '100', '110', candidates, '6', control], { encoding: 'utf8' });
    return { status: r.status, out: r.stdout + r.stderr, control };
  };

  it('asks for earlier segments until every open transaction starts in the directory', () => {
    if (!hasPerl) return;
    const r = plan([5, 6]);
    expect(r.status).toBe(3);
    expect(r.out).toContain('need 4');
    expect(r.out).toContain('100 started before segment 5');
  });

  it('records the transactions open in the backup from their first segment', () => {
    if (!hasPerl) return;
    const r = plan([4, 5, 6]);
    expect(r.status).toBe(0);
    expect(r.out).toContain('replay: segments 6 to 6, and 2 transaction(s) open in the backup (100 from segment 4, 105 from segment 5)');
    const out = readFileSync(r.control);
    expect(out.subarray(0, 9).toString()).toBe('FBREPLCTL');
    expect(out.readUInt32LE(12)).toBe(2); // txn_count
    expect(out.readBigUInt64LE(16)).toBe(5n); // applied up to the backup's segment
    expect(out.readUInt32LE(24)).toBe(0);
    expect(out.readBigUInt64LE(32)).toBe(5n); // db_sequence: the restored header's
    expect([out.readBigUInt64LE(40), out.readBigUInt64LE(48), out.readBigUInt64LE(56), out.readBigUInt64LE(64)]).toEqual([100n, 4n, 105n, 5n]);
  });

  it('refuses a gap, and changes after the backup of a transaction complete in it', () => {
    if (!hasPerl) return;
    expect(plan([4, 6]).out).toContain('segment 5 is missing');
    const late = plan([4, 5, 6], { 6: [[101, END]] });
    expect(late.status).toBe(1);
    expect(late.out).toContain('transaction 101 is complete in the backup but has changes in segment 6');
  });
});

