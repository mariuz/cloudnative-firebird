import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { ChildProcess, spawnSync } from 'child_process';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { JOB_SCRIPTS, REPLICATION_SCRIPTS } from '../src/utils/replication';
import { startSegmentServer } from './helpers/segment-server';

/**
 * The replication init container's helpers (docs/rust-agent-roadmap.md): on clusters that run
 * the agent, init-instance.sh calls `fb-agent fetch-seed | replica-control | set-repl-seq |
 * reposition | request` where it called a Perl script. Each pair gets the same arguments here:
 * the files they write, what they print and whether they succeed must be the same.
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

/** a replica control file: position, db_sequence and active transactions [id, first segment] */
const control = (sequence: number, dbSequence: number, active: Array<[number, number]> = []): Buffer => {
  const b = Buffer.alloc(40 + 16 * active.length);
  b.write('FBREPLCTL\0', 0, 'latin1');
  b.writeUInt16LE(1, 10);
  b.writeUInt32LE(active.length, 12);
  b.writeBigUInt64LE(BigInt(sequence), 16);
  b.writeBigUInt64LE(BigInt(dbSequence), 32);
  active.forEach(([id, first], i) => {
    b.writeBigUInt64LE(BigInt(id), 40 + 16 * i);
    b.writeBigUInt64LE(BigInt(first), 48 + 16 * i);
  });
  return b;
};

/** a header page as set-repl-seq.pl expects it: ODS 13 (clumps from 128) or 14 (from 148) */
const headerPage = (ods: 13 | 14, clumps: number[] = []): Buffer => {
  const page = Buffer.alloc(8192);
  page[0] = 1;
  page.writeUInt16LE(8192, 16);
  page.writeUInt16LE(ods | 0x8000, 18);
  const [endAt, start] = ods === 13 ? [66, 128] : [36, 148];
  Buffer.from(clumps).copy(page, start);
  page.writeUInt16LE(start + clumps.length, endAt);
  return Buffer.concat([page, Buffer.alloc(8192, 7)]);
};

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

describe.skipIf(!agentBin || !hasPerl)('the replication init helpers: fb-agent and the Perl scripts', () => {
  let root: string;
  const children: ChildProcess[] = [];
  /** the two working directories, with the same files */
  const dirs = () => ({ perl: join(root, 'perl'), agent: join(root, 'agent') });
  const script = (name: string) => join(root, name);
  const run = (command: string, args: string[], env: Record<string, string> = {}) => {
    const done = spawnSync(command, args, { env: { ...process.env, ISC_PASSWORD: 'tok', SEGMENT_PROXY: '', ...env } });
    return { status: done.status, out: done.stdout.toString(), err: done.stderr.toString() };
  };
  /** the directory's files and their bytes */
  const files = (dir: string) => Object.fromEntries(readdirSync(dir).sort().map((name) => [name, readFileSync(join(dir, name)).toString('hex')]));
  /**
   * runs the Perl helper and the agent's command, each in its directory (`{}` in an argument),
   * and expects the same status, output (with the directory's name taken out) and files
   */
  const both = (perlScript: string, perlArgs: string[], agentArgs: string[], env: Record<string, string> = {}) => {
    const { perl, agent } = dirs();
    const at = (dir: string, args: string[]) => args.map((a) => a.split('{}').join(dir));
    const p = run('perl', [script(perlScript), ...at(perl, perlArgs)], env);
    const a = run(agentBin!, at(agent, agentArgs), env);
    const what = agentArgs.join(' ');
    expect(a.status === 0, `${what}: ${a.err}${p.err}`).toBe(p.status === 0);
    expect(a.out.split(agent).join('DIR'), what).toBe(p.out.split(perl).join('DIR'));
    expect(files(agent), what).toEqual(files(perl));
    return { perl: p, agent: a };
  };
  const put = (name: string, content: Buffer | string) => {
    for (const dir of Object.values(dirs())) writeFileSync(join(dir, name), content);
  };

  beforeEach(() => {
    root = mkdtempSync(join(tmpdir(), 'inithelpers-'));
    for (const dir of Object.values(dirs())) mkdirSync(dir);
    for (const name of ['fetch-seed.pl', 'replica-control.pl', 'set-repl-seq.pl', 'segment-request.pl', 'segment-server.pl']) {
      writeFileSync(script(name), REPLICATION_SCRIPTS[name]);
    }
    writeFileSync(script('pitr-plan.pl'), JOB_SCRIPTS['pitr-plan.pl']);
  });
  afterEach(() => {
    for (const c of children.splice(0)) c.kill('SIGKILL');
    spawnSync('pkill', ['-KILL', '-f', join(root, 'segment-server.pl')]);
    rmSync(root, { recursive: true, force: true });
  });

  it('set the replication sequence in the header of a database file', () => {
    // no clump yet (a promoted replica), on both on-disk structures
    put('db13.fdb', headerPage(13));
    put('db14.fdb', headerPage(14));
    // a clump already there, behind another one
    put('with.fdb', headerPage(14, [5, 2, 9, 9, 11, 8, 1, 0, 0, 0, 0, 0, 0, 0, 0]));
    for (const name of ['db13.fdb', 'db14.fdb', 'with.fdb']) {
      const { agent } = both('set-repl-seq.pl', [`{}/${name}`, '4660'], ['set-repl-seq', `{}/${name}`, '4660']);
      expect(agent.status).toBe(0);
      expect(agent.out).toContain('set to 4660');
    }
    const written = readFileSync(join(dirs().agent, 'db14.fdb'));
    expect([...written.subarray(148, 159)]).toEqual([11, 8, 0x34, 0x12, 0, 0, 0, 0, 0, 0, 0]);
    expect(written.readUInt16LE(36)).toBe(158);
    // the pages after the header are not touched
    expect(written.subarray(8192).equals(Buffer.alloc(8192, 7))).toBe(true);
    // refused alike: not a database, an unknown structure, not a number
    put('junk.fdb', Buffer.alloc(100));
    const unknown = headerPage(13);
    unknown.writeUInt16LE(12 | 0x8000, 18);
    put('old.fdb', unknown);
    for (const [name, sequence] of [['junk.fdb', '5'], ['old.fdb', '5'], ['db13.fdb', 'x'], ['missing.fdb', '5']]) {
      const { agent } = both('set-repl-seq.pl', [`{}/${name}`, sequence], ['set-repl-seq', `{}/${name}`, sequence]);
      expect(agent.status, name).not.toBe(0);
    }
  });

  it('adopt the control file of a replica seed, and reposition one', () => {
    put('from.ctl', control(7, 3, [[40, 6], [44, 7]]));
    const adopted = both('replica-control.pl', ['--adopt', '{}/from.ctl', '9', '{}/target'], ['replica-control', '--adopt', '{}/from.ctl', '9', '{}/target']);
    expect(adopted.agent.out).toBe('replica control: adopted position after segment 7 with 2 active transaction(s)\n');
    expect(readFileSync(join(dirs().agent, 'target')).equals(control(7, 9, [[40, 6], [44, 7]]))).toBe(true);
    const moved = both('pitr-plan.pl', ['--reposition', '12', '{}/target'], ['reposition', '12', '{}/target']);
    expect(moved.agent.out).toBe('the replay continues after segment 12\n');
    expect(readFileSync(join(dirs().agent, 'target')).equals(control(12, 9))).toBe(true);
    // refused alike
    put('junk.ctl', 'short');
    expect(both('replica-control.pl', ['--adopt', '{}/junk.ctl', '9', '{}/other'], ['replica-control', '--adopt', '{}/junk.ctl', '9', '{}/other']).agent.status).not.toBe(0);
    expect(both('pitr-plan.pl', ['--reposition', '12', '{}/junk.ctl'], ['reposition', '12', '{}/junk.ctl']).agent.status).not.toBe(0);
    expect(both('pitr-plan.pl', ['--reposition', 'x', '{}/target'], ['reposition', 'x', '{}/target']).agent.status).not.toBe(0);
  });

  it('fetch a seed, ask a segment server, and write the control file of a seed from the primary', async () => {
    const repl = join(root, 'repl');
    for (const d of ['archive', 'source', 'data', 'repl']) mkdirSync(join(root, d));
    writeFileSync(join(root, 'primary'), 'db-0.db-headless\n');
    writeFileSync(join(root, 'archive', 'mydb.fdb.journal-000000010'), withBlocks(10, [[50, 1, 5], [51, 0, 0], [50, 0, 7]]));
    writeFileSync(join(root, 'archive', 'mydb.fdb.journal-000000011'), withBlocks(11, [[51, 0, 3], [52, 1, 2], [0, 0, 4], [60, 1, 0]]));
    // the primary's offline bootstrap seed: served as it is
    const seed = Buffer.alloc(300_000, 5);
    writeFileSync(join(repl, 'bootstrap-seed.fdb'), seed);
    const server = await startSegmentServer(script('segment-server.pl'), {
      ARCHIVE_DIR: join(root, 'archive'),
      DATABASE_PATH: join(root, 'data', 'mydb.fdb'),
      SOURCE_DIR: join(root, 'source'),
      REPLICATION_DIR: repl,
      PRIMARY_FILE: join(root, 'primary'),
      POD_NAME: 'db-0',
      ISC_PASSWORD: 'tok',
      FIREBIRD_PORT: '1',
    });
    children.push(server.child);
    const env = { SEGMENT_PORT: String(server.port) };

    const fetched = both('fetch-seed.pl', ['127.0.0.1', '{}/work.fdb'], ['fetch-seed', '127.0.0.1', '{}/work.fdb'], env);
    expect(fetched.agent.out).toBe('fetched offline seed copy (300000 bytes) from 127.0.0.1\n');
    expect(readFileSync(join(dirs().agent, 'work.fdb')).equals(seed)).toBe(true);
    expect(readFileSync(join(dirs().agent, 'work.fdb.kind'), 'utf8')).toBe('offline\n');
    expect(readdirSync(dirs().agent).sort()).toEqual(['work.fdb', 'work.fdb.kind']);

    // a request, for the script's SYNCTO
    for (const request of [['SYNCTO'], ['POSITION'], ['NOSUCH']]) {
      both('segment-request.pl', ['127.0.0.1', ...request], ['request', '127.0.0.1', ...request], env);
    }

    // an offline seed: no candidates, nothing asked
    const offline = both('replica-control.pl', ['127.0.0.1', '11', '11', '{}/offline.ctl'], ['replica-control', '127.0.0.1', '11', '11', '{}/offline.ctl'], env);
    expect(offline.agent.out).toBe('replica control: continue after segment 11, replay 0 transaction(s)\n');
    // candidates without --next: TXNS tells where those in the journal begin (53 is not there)
    const txns = both('replica-control.pl', ['127.0.0.1', '11', '4', '{}/txns.ctl', '52', '50', '53'], ['replica-control', '127.0.0.1', '11', '4', '{}/txns.ctl', '52', '50', '53'], env);
    expect(txns.agent.out).toBe('replica control: continue after segment 11, replay 2 transaction(s) (50 from segment 10, 52 from segment 11)\n');
    expect(readFileSync(join(dirs().agent, 'txns.ctl')).equals(control(11, 4, [[50, 10], [52, 11]]))).toBe(true);
    // a live seed (--next): PLAN, with the transactions numbered from <next> on that began in the archive
    const plan = both('replica-control.pl', ['--next', '55', '127.0.0.1', '11', '4', '{}/plan.ctl', '52'], ['replica-control', '--next', '55', '127.0.0.1', '11', '4', '{}/plan.ctl', '52'], env);
    expect(plan.agent.status).toBe(0);
    expect(plan.agent.out).toContain('52 from segment 11');
    expect(plan.agent.out).toContain('60 from segment 11');
    // a transaction that began before the oldest archived segment: no control file, alike
    const partial = both('replica-control.pl', ['--next', '55', '127.0.0.1', '11', '4', '{}/partial.ctl', '51'], ['replica-control', '--next', '55', '127.0.0.1', '11', '4', '{}/partial.ctl', '51'], env);
    expect(partial.agent.status).not.toBe(0);
    expect(partial.agent.err).toContain('transaction 51 began before the oldest archived segment of 127.0.0.1 (10 is its first); cannot seed from this copy');
    expect(partial.perl.err).toContain('transaction 51 began before the oldest archived segment of 127.0.0.1 (10 is its first); cannot seed from this copy');
    expect(existsSync(join(dirs().agent, 'partial.ctl'))).toBe(false);
    // nobody listening, a wrong password: both fail, and nothing is written
    expect(both('fetch-seed.pl', ['127.0.0.1', '{}/none.fdb'], ['fetch-seed', '127.0.0.1', '{}/none.fdb'], { SEGMENT_PORT: '1' }).agent.status).not.toBe(0);
    expect(both('fetch-seed.pl', ['127.0.0.1', '{}/none.fdb'], ['fetch-seed', '127.0.0.1', '{}/none.fdb'], { ...env, ISC_PASSWORD: 'wrong' }).agent.status).not.toBe(0);
    expect(existsSync(join(dirs().agent, 'none.fdb'))).toBe(false);
  });
});
