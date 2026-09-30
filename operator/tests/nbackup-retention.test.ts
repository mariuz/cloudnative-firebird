import { describe, it, expect } from 'vitest';
import { execFileSync } from 'child_process';
import { mkdtempSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { NBACKUP_EXPIRED_AWK } from '../src/utils/backup';

// the chain of the Firebird 5 experiment: a level N backup is based on the latest earlier level
// N-1 backup, of any schedule (C2 below builds on A1, not on the newer B0)
const A0 = 'nbackup-l0-weekly-20260901T000000Z.nbk';
const A1 = 'nbackup-l1-daily-20260902T000000Z.nbk';
const A2 = 'nbackup-l2-hourly-20260903T000000Z.nbk';
const B0 = 'nbackup-l0-weekly-20260908T000000Z.nbk';
const C2 = 'nbackup-l2-hourly-20260909T000000Z.nbk';
const B1 = 'nbackup-l1-daily-20260910T000000Z.nbk';
const B2 = 'nbackup-l2-hourly-20260911T000000Z.nbk';
const HISTORY: Array<[number, string]> = [[0, A0], [1, A1], [2, A2], [0, B0], [2, C2], [1, B1], [2, B2]];

/** Runs the awk program like the retention script does; returns the files it would delete */
function expired(options: {
  series: string;
  cutoff: string;
  all: string[];
  history?: Array<[number, string]>;
  f?: string;
}): string[] {
  const dir = mkdtempSync(join(tmpdir(), 'nbk-'));
  const history = options.history ?? HISTORY;
  const series = options.all
    .filter((x) => new RegExp(`^nbackup-l[0-2]-${options.series}-[0-9]{8}T[0-9]{6}Z[.]nbk$`).test(x))
    .sort();
  writeFileSync(join(dir, 'history'), history.map(([l, n]) => `${l} ${n}\n`).join(''));
  writeFileSync(join(dir, 'all'), options.all.map((x) => `${x}\n`).join(''));
  writeFileSync(join(dir, 'series'), series.map((x) => `${x}\n`).join(''));
  const newest = series[series.length - 1] ?? '';
  const out = execFileSync(
    'awk',
    ['-v', `c=${options.cutoff}`, '-v', `n=${newest}`, '-v', `f=${options.f ?? newest}`, NBACKUP_EXPIRED_AWK,
      join(dir, 'history'), join(dir, 'all'), join(dir, 'series')],
    { encoding: 'utf8' },
  );
  return out.split('\n').filter(Boolean).sort();
}

const everything = HISTORY.map(([, n]) => n);

describe('nbackup retention (chains from RDB$BACKUP_HISTORY)', () => {
  it('keeps a base that increments of other schedules still need', () => {
    expect(expired({ series: 'weekly', cutoff: '20260905T000000Z', all: everything })).toEqual([]);
  });

  it('deletes expired increments nothing depends on, never the newest', () => {
    expect(expired({ series: 'hourly', cutoff: '20260910T000000Z', all: everything })).toEqual([A2, C2].sort());
  });

  it('frees a base once the increments built on it are gone', () => {
    const withoutHourly = everything.filter((n) => n !== A2 && n !== C2);
    expect(expired({ series: 'daily', cutoff: '20260909T120000Z', all: withoutHourly })).toEqual([A1]);
    // while C2 still exists, A1 is part of its chain
    expect(expired({ series: 'daily', cutoff: '20260909T120000Z', all: everything.filter((n) => n !== A2) })).toEqual([]);
    const withoutA1 = withoutHourly.filter((n) => n !== A1);
    expect(expired({ series: 'weekly', cutoff: '20260905T000000Z', all: withoutA1 })).toEqual([A0]);
  });

  it('keeps bases needed by schedules stored in another location', () => {
    // no daily file here: its backups live elsewhere, so A1 (and B1) count as kept
    const noDaily = everything.filter((n) => !n.includes('-daily-'));
    expect(expired({ series: 'weekly', cutoff: '20260905T000000Z', all: noDaily })).toEqual([]);
  });

  it('never deletes files missing from the history, and keeps chains of on-demand backups', () => {
    const stranger = 'nbackup-l0-weekly-20260801T000000Z.nbk';
    expect(expired({ series: 'weekly', cutoff: '20260905T000000Z', all: [stranger, B0], history: [[0, B0]] })).toEqual([]);
    const manual = 'nbackup-l1-before-upgrade.nbk';
    expect(
      expired({ series: 'weekly', cutoff: '20260905T000000Z', all: [A0, B0], history: [[0, A0], [1, manual], [0, B0]] }),
    ).toEqual([]);
    expect(expired({ series: 'weekly', cutoff: '20260905T000000Z', all: [A0, B0], history: [[0, A0], [0, B0]] })).toEqual([A0]);
  });

  it('keeps everything with an empty history (the query failed)', () => {
    expect(expired({ series: 'hourly', cutoff: '20260910T000000Z', all: everything, history: [] })).toEqual([]);
  });
});
