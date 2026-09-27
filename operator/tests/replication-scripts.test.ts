import { describe, it, expect } from 'vitest';
import { execFileSync, spawnSync } from 'child_process';
import { mkdtempSync, writeFileSync } from 'fs';
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
    expect(moveLine).toBeGreaterThan(lines.findIndex((l) => l.includes('FBREPLCTL')));
  });

  it('writes a replica control file matching Firebird ControlFile::DataV1 (40 bytes)', () => {
    if (!hasPerl) return;
    const out = execFileSync('perl', [
      '-e',
      'print pack("a10 v V Q< V x4 Q<", "FBREPLCTL", 1, 0, $ARGV[0] - 1, 0, $ARGV[1])',
      '15',
      '16',
    ]);
    expect(out.length).toBe(40);
    expect(out.subarray(0, 10).toString('latin1')).toBe('FBREPLCTL\0');
    expect(out.readUInt16LE(10)).toBe(1); // version
    expect(out.readUInt32LE(12)).toBe(0); // txn_count
    expect(out.readBigUInt64LE(16)).toBe(14n); // sequence: applied through S-1
    expect(out.readUInt32LE(24)).toBe(0); // offset
    expect(out.readBigUInt64LE(32)).toBe(16n); // db_sequence
  });
});
