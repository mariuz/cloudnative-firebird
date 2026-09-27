import { describe, it, expect } from 'vitest';
import { execFileSync, spawn, spawnSync } from 'child_process';
import { mkdtempSync, readdirSync, readFileSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { createServer } from 'net';
import { REPLICATION_SCRIPTS } from '../src/utils/replication';

const hasPerl = spawnSync('perl', ['-v']).status === 0;
const dir = mkdtempSync(join(tmpdir(), 'fb-repl-scripts-'));

describe('replication scripts shipped to instance pods', () => {
  it.each(Object.keys(REPLICATION_SCRIPTS).filter((name) => name.endsWith('.pl')))(
    '%s compiles with core perl only',
    (name) => {
      if (!hasPerl) return;
      const file = join(dir, name);
      writeFileSync(file, REPLICATION_SCRIPTS[name]);
      expect(() => execFileSync('perl', ['-c', file], { stdio: 'pipe' })).not.toThrow();
      // the Firebird image ships perl-base only; HTTP::Tiny, File::Copy, Digest::* are absent
      expect(REPLICATION_SCRIPTS[name]).not.toMatch(/^use (HTTP::|File::Copy|Digest::|LWP)/m);
    },
  );

  it('init-instance.sh is valid POSIX sh', () => {
    const file = join(dir, 'init-instance.sh');
    writeFileSync(file, REPLICATION_SCRIPTS['init-instance.sh']);
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
    const { createServer } = await import('net');
    const requests: string[] = [];
    const server = createServer((sock) => {
      sock.once('data', (buf) => {
        requests.push(buf.toString());
        sock.end('30 7\n12 5\n.\n'); // 12 starts in segment 5, 30 in 7; 99 is not journaled
      });
    });
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

  it('fetch-segments.pl fetches archived segments that are not uploaded yet', async () => {
    if (!hasPerl) return;
    const segments: Record<string, string> = {
      'mydb.fdb.journal-0000000001': 'one',
      'mydb.fdb.journal-0000000002': 'two',
      'mydb.fdb.journal-0000000003': 'three',
    };
    const requests: string[] = [];
    const server = createServer((sock) => {
      sock.once('data', (buf) => {
        const line = buf.toString().trim();
        requests.push(line);
        const [, cmd, name] = line.split(' ');
        if (cmd === 'LIST') sock.end(Object.keys(segments).join('\n') + '\nnot-a-segment\n.\n');
        else sock.end(`OK ${segments[name].length}\n${segments[name]}`);
      });
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const port = (server.address() as { port: number }).port;
    const script = join(dir, 'fetch-segments.pl');
    writeFileSync(script, REPLICATION_SCRIPTS['fetch-segments.pl']);
    const out = mkdtempSync(join(tmpdir(), 'fb-segments-'));
    const skip = join(out, 'uploaded');
    writeFileSync(skip, 'mydb.fdb.journal-0000000001\n');
    const segDir = join(out, 'segments');
    await new Promise<void>((resolve, reject) => {
      const child = spawn('perl', [script], {
        env: { ...process.env, FIREBIRD_HOST: '127.0.0.1', SEGMENT_PORT: String(port), ISC_PASSWORD: 'tok', OUT_DIR: segDir, SKIP_FILE: skip },
      });
      child.on('exit', (code) => (code === 0 ? resolve() : reject(new Error(`exit ${code}`))));
    });
    server.close();

    expect(requests).toEqual(['tok LIST', 'tok GET mydb.fdb.journal-0000000002', 'tok GET mydb.fdb.journal-0000000003']);
    expect(readdirSync(segDir).sort()).toEqual(['mydb.fdb.journal-0000000002', 'mydb.fdb.journal-0000000003']);
    expect(readFileSync(join(segDir, 'mydb.fdb.journal-0000000003'), 'utf8')).toBe('three');
  });
});

