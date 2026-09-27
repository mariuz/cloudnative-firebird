import { describe, it, expect } from 'vitest';
import { execFileSync, spawn, spawnSync } from 'child_process';
import { mkdtempSync, readFileSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
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

  it('seed-replica.sh is valid POSIX sh', () => {
    const file = join(dir, 'seed-replica.sh');
    writeFileSync(file, REPLICATION_SCRIPTS['seed-replica.sh']);
    expect(() => execFileSync('sh', ['-n', file], { stdio: 'pipe' })).not.toThrow();
  });

  it('seed-replica.sh moves the database into place only after it is a complete replica', () => {
    const script = REPLICATION_SCRIPTS['seed-replica.sh'];
    const lines = script.split('\n');
    const moveLine = lines.findIndex((l) => l.startsWith('mv "$work" "$DATABASE_PATH"'));
    expect(moveLine).toBeGreaterThan(lines.findIndex((l) => l.startsWith('gfix -replica read_only')));
    expect(moveLine).toBeGreaterThan(lines.findIndex((l) => l.includes('replica-control.pl')));
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
});
