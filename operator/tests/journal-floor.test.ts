import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { spawnSync } from 'child_process';
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync, existsSync, readdirSync, rmSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { REPLICATION_SCRIPTS } from '../src/utils/replication';

/**
 * The journal floor: both promotions record the segment the new primary's journal continues
 * after, and the init container raises a database header that fell behind it (a journal
 * restarting at segment 1 would be skipped by every replica). The block runs here as it is in
 * init-instance.sh, with the Firebird tools stubbed.
 */
const init = REPLICATION_SCRIPTS['init-instance.sh'];
const start = init.indexOf('floor_file="$REPLICATION_DIR/journal-floor"');
const end = init.indexOf('# The offline bootstrap seed serves new replicas');
const block = init.slice(start, end);

let dir: string;
beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), 'journal-floor-'));
  for (const d of ['repl', 'journal', 'archive', 'scripts']) mkdirSync(join(dir, d));
  writeFileSync(join(dir, 'db.fdb'), '');
  // records the sequence it was asked to set
  writeFileSync(join(dir, 'scripts', 'set-repl-seq.pl'), 'open(my $f, ">", "$ARGV[0].seq") or die; print $f "$ARGV[1]\\n";');
});
afterEach(() => rmSync(dir, { recursive: true, force: true }));

/** a journal segment file: u64 sequence at offset 32 */
const segment = (name: string, sequence: number) => {
  const b = Buffer.alloc(64);
  b.writeBigUInt64LE(BigInt(sequence), 32);
  writeFileSync(join(dir, 'journal', name), b);
};

const run = (opts: { floor?: string; header: number; primary?: boolean; replica?: boolean }) => {
  if (opts.floor !== undefined) writeFileSync(join(dir, 'repl', 'journal-floor'), `${opts.floor}\n`);
  const r = spawnSync(
    'sh',
    [
      '-c',
      `set -eu
       seq_of() { echo ${opts.header}; }
       is_primary() { ${opts.primary === false ? 'false' : 'true'}; }
       is_replica_db() { ${opts.replica ? 'true' : 'false'}; }
       own() { :; }
       ${block}`,
    ],
    {
      encoding: 'utf8',
      env: {
        ...process.env,
        REPLICATION_DIR: join(dir, 'repl'),
        DATABASE_PATH: join(dir, 'db.fdb'),
        JOURNAL_DIR: join(dir, 'journal'),
        ARCHIVE_DIR: join(dir, 'archive'),
        SCRIPT_DIR: join(dir, 'scripts'),
      },
    },
  );
  const set = join(dir, 'db.fdb.seq');
  return { ...r, set: existsSync(set) ? readFileSync(set, 'utf8').trim() : undefined, floorKept: existsSync(join(dir, 'repl', 'journal-floor')) };
};

describe('the journal floor (init-instance.sh)', () => {
  it('is the script block, before the seed refresh and the server start', () => {
    expect(start).toBeGreaterThan(0);
    expect(end).toBeGreaterThan(start);
    expect(init.indexOf('echo "database exists, nothing to initialise"')).toBeGreaterThan(end);
  });

  it('raises a header behind the floor, and keeps the floor until the header reached it', () => {
    const r = run({ floor: '62', header: 0 });
    expect(r.status).toBe(0);
    expect(r.set).toBe('62');
    expect(r.stdout).toContain('replication sequence 0 of the database is behind the journal floor 62');
    expect(r.stdout).not.toContain('re-seed them');
    expect(r.floorKept).toBe(true);
  });

  it('removes the journal segments up to the floor, and tells which archived ones the replicas skipped', () => {
    segment('db.fdb.journal-000000001', 1);
    segment('db.fdb.journal-000000063', 63);
    writeFileSync(join(dir, 'archive', 'db.fdb.journal-000000001'), '');
    writeFileSync(join(dir, 'archive', 'db.fdb.journal-000000002'), '');
    const r = run({ floor: '62', header: 2 });
    expect(r.set).toBe('62');
    expect(r.stdout).toContain('2 segments were journaled below the floor since the promotion');
    expect(readdirSync(join(dir, 'journal'))).toEqual(['db.fdb.journal-000000063']);
  });

  it('goes once the header reached it, and changes nothing', () => {
    segment('db.fdb.journal-000000063', 63);
    for (const header of [62, 70]) {
      const r = run({ floor: '62', header });
      expect(r.status).toBe(0);
      expect(r.set).toBeUndefined();
      expect(r.floorKept).toBe(false);
    }
    expect(readdirSync(join(dir, 'journal'))).toEqual(['db.fdb.journal-000000063']);
  });

  it('applies only to the primary, and only with a floor', () => {
    expect(run({ floor: '62', header: 0, primary: false }).set).toBeUndefined();
    expect(run({ floor: '62', header: 0, replica: true }).set).toBeUndefined();
    rmSync(join(dir, 'repl', 'journal-floor'));
    expect(run({ header: 0 }).set).toBeUndefined();
  });

  it('ignores an invalid floor', () => {
    const r = run({ floor: 'x', header: 0 });
    expect(r.status).toBe(0);
    expect(r.set).toBeUndefined();
    expect(r.stdout).toContain('ignoring an invalid journal floor');
    expect(r.floorKept).toBe(false);
  });

  it('is written by the offline promotion and cleared with the replication state', () => {
    const promote = init.slice(init.indexOf('promoting this replica to primary'), init.indexOf('echo "promoted to primary"'));
    // after wipe_replication_state, which removes it
    expect(promote.indexOf('echo "$seq" > "$REPLICATION_DIR/journal-floor"')).toBeGreaterThan(promote.indexOf('wipe_replication_state'));
    expect(init).toContain('rm -f "$STATE_FILE" "$REPLICATION_DIR/sync-standby" "$REPLICATION_DIR/sync-seen" "$REPLICATION_DIR/journal-floor"\n}');
    expect(init).toContain('rm -f "$DATABASE_PATH" "$STATE_FILE" "$REPLICATION_DIR/sync-standby" "$REPLICATION_DIR/sync-seen" "$REPLICATION_DIR/journal-floor"');
  });
});

describe('the in-place promotion (segment-server.pl promote_here)', () => {
  const script = readFileSync(join(__dirname, '..', 'src', 'replication', 'segment-server.pl'), 'utf8');
  const promote = script.slice(script.indexOf('sub promote_here'), script.indexOf('sub seed_from_primary'));

  it('writes the journal floor with the header, and removes it when the promotion is refused', () => {
    expect(script).toContain('my $journal_floor = "$base/journal-floor";');
    const setSeq = promote.indexOf('set-repl-seq.pl');
    expect(promote.indexOf('write_file($journal_floor, "$seq\\n")')).toBeGreaterThan(setSeq);
    expect(promote).toContain('unlink "$bootstrap_seed.tmp", $journal_floor;');
  });

  it('checks the sequence the server journals after before it reports the promotion', () => {
    const check = promote.indexOf('die "the journal would continue after segment $live, not $seq\\n" if $live < $seq;');
    expect(check).toBeGreaterThan(promote.indexOf('enable-publication.sql'));
    expect(check).toBeLessThan(promote.indexOf('print $client "OK $seq\\n";\n    print "promoted in place'));
    expect(check).toBeLessThan(promote.indexOf('write_file($promoted_flag, time . "\\n");\n    if (defined $lineage'));
  });
});
