import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { ChildProcess, spawnSync } from 'child_process';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { startSegmentServer } from './helpers/segment-server';
import { agentBin } from './helpers/implementations';

/**
 * The replication init container's helpers (docs/rust-agent-roadmap.md): init-instance.sh calls
 * `fb-agent fetch-seed | replica-control | set-repl-seq | reposition | request`, where it called
 * Perl scripts before v0.170.0: the files they write, what they print and whether they succeed.
 *
 * Needs the fb-agent binary (FB_AGENT_BIN, or a build in agent/target); skipped without it. The
 * "Rust agent" CI job runs this file with it.
 */

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

/** a header page as set-repl-seq expects it: ODS 13 (clumps from 128) or 14 (from 148) */
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

describe.skipIf(!agentBin)('the replication init helpers of fb-agent', () => {
  let root: string;
  const children: ChildProcess[] = [];
  /** the working directory */
  const dirs = () => ({ agent: join(root, 'agent') });
  const run = (command: string, args: string[], env: Record<string, string> = {}) => {
    const done = spawnSync(command, args, { env: { ...process.env, ISC_PASSWORD: 'tok', SEGMENT_TLS: '', ...env } });
    return { status: done.status, out: done.stdout.toString(), err: done.stderr.toString() };
  };
  /** runs the agent's command in its directory (`{}` in an argument) */
  const both = (agentArgs: string[], env: Record<string, string> = {}) => {
    const { agent } = dirs();
    const a = run(agentBin!, agentArgs.map((arg) => arg.split('{}').join(agent)), env);
    return { agent: { ...a, out: a.out.split(agent).join('DIR') } };
  };
  const put = (name: string, content: Buffer | string) => writeFileSync(join(dirs().agent, name), content);

  beforeEach(() => {
    root = mkdtempSync(join(tmpdir(), 'inithelpers-'));
    mkdirSync(dirs().agent);
  });
  afterEach(() => {
    for (const c of children.splice(0)) c.kill('SIGKILL');
    rmSync(root, { recursive: true, force: true });
  });

  it('set the replication sequence in the header of a database file', () => {
    // no clump yet (a promoted replica), on both on-disk structures
    put('db13.fdb', headerPage(13));
    put('db14.fdb', headerPage(14));
    // a clump already there, behind another one
    put('with.fdb', headerPage(14, [5, 2, 9, 9, 11, 8, 1, 0, 0, 0, 0, 0, 0, 0, 0]));
    for (const name of ['db13.fdb', 'db14.fdb', 'with.fdb']) {
      const { agent } = both(['set-repl-seq', `{}/${name}`, '4660']);
      expect(agent.status).toBe(0);
      expect(agent.out).toContain('set to 4660');
    }
    const written = readFileSync(join(dirs().agent, 'db14.fdb'));
    expect([...written.subarray(148, 159)]).toEqual([11, 8, 0x34, 0x12, 0, 0, 0, 0, 0, 0, 0]);
    expect(written.readUInt16LE(36)).toBe(158);
    // the pages after the header are not touched
    expect(written.subarray(8192).equals(Buffer.alloc(8192, 7))).toBe(true);
    // refused: not a database, an unknown structure, not a number
    put('junk.fdb', Buffer.alloc(100));
    const unknown = headerPage(13);
    unknown.writeUInt16LE(12 | 0x8000, 18);
    put('old.fdb', unknown);
    for (const [name, sequence] of [['junk.fdb', '5'], ['old.fdb', '5'], ['db13.fdb', 'x'], ['missing.fdb', '5']]) {
      const { agent } = both(['set-repl-seq', `{}/${name}`, sequence]);
      expect(agent.status, name).not.toBe(0);
    }
  });

  it('adopt the control file of a replica seed, and reposition one', () => {
    put('from.ctl', control(7, 3, [[40, 6], [44, 7]]));
    const adopted = both(['replica-control', '--adopt', '{}/from.ctl', '9', '{}/target']);
    expect(adopted.agent.out).toBe('replica control: adopted position after segment 7 with 2 active transaction(s)\n');
    expect(readFileSync(join(dirs().agent, 'target')).equals(control(7, 9, [[40, 6], [44, 7]]))).toBe(true);
    const moved = both(['reposition', '12', '{}/target']);
    expect(moved.agent.out).toBe('the replay continues after segment 12\n');
    expect(readFileSync(join(dirs().agent, 'target')).equals(control(12, 9))).toBe(true);
    // refused
    put('junk.ctl', 'short');
    expect(both(['replica-control', '--adopt', '{}/junk.ctl', '9', '{}/other']).agent.status).not.toBe(0);
    expect(both(['reposition', '12', '{}/junk.ctl']).agent.status).not.toBe(0);
    expect(both(['reposition', 'x', '{}/target']).agent.status).not.toBe(0);
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
    // the image's isql, which the server asks where the local server does not answer: the
    // replication sequence of a primary that has not journalled since its bootstrap seed
    const bin = join(root, 'bin');
    mkdirSync(bin);
    writeFileSync(join(bin, 'isql'), '#!/bin/sh\ncat >/dev/null\necho "V                               0"\n', { mode: 0o755 });
    const server = await startSegmentServer({
      PATH: `${bin}:${process.env.PATH}`,
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

    const fetched = both(['fetch-seed', '127.0.0.1', '{}/work.fdb'], env);
    expect(fetched.agent.out).toBe('fetched offline seed copy (300000 bytes) from 127.0.0.1\n');
    expect(readFileSync(join(dirs().agent, 'work.fdb')).equals(seed)).toBe(true);
    expect(readFileSync(join(dirs().agent, 'work.fdb.kind'), 'utf8')).toBe('offline\n');
    expect(readdirSync(dirs().agent).sort()).toEqual(['work.fdb', 'work.fdb.kind']);

    // a request, for the script's SYNCTO
    for (const [request, reply] of [
      ['SYNCTO', 'OK none\n'],
      ['POSITION', 'OK primary\n'],
      ['NOSUCH', 'ERR bad request\n'],
    ]) {
      expect(both(['request', '127.0.0.1', request], env).agent, request).toMatchObject({ status: 0, out: reply });
    }

    // an offline seed: no candidates, nothing asked
    const offline = both(['replica-control', '127.0.0.1', '11', '11', '{}/offline.ctl'], env);
    expect(offline.agent.out).toBe('replica control: continue after segment 11, replay 0 transaction(s)\n');
    // candidates without --next: TXNS tells where those in the journal begin (53 is not there)
    const txns = both(['replica-control', '127.0.0.1', '11', '4', '{}/txns.ctl', '52', '50', '53'], env);
    expect(txns.agent.out).toBe('replica control: continue after segment 11, replay 2 transaction(s) (50 from segment 10, 52 from segment 11)\n');
    expect(readFileSync(join(dirs().agent, 'txns.ctl')).equals(control(11, 4, [[50, 10], [52, 11]]))).toBe(true);
    // a live seed (--next): PLAN, with the transactions numbered from <next> on that began in the archive
    const plan = both(['replica-control', '--next', '55', '127.0.0.1', '11', '4', '{}/plan.ctl', '52'], env);
    expect(plan.agent.status).toBe(0);
    expect(plan.agent.out).toContain('52 from segment 11');
    expect(plan.agent.out).toContain('60 from segment 11');
    // a transaction that began before the oldest archived segment: no control file
    const partial = both(['replica-control', '--next', '55', '127.0.0.1', '11', '4', '{}/partial.ctl', '51'], env);
    expect(partial.agent.status).not.toBe(0);
    expect(partial.agent.err).toContain('transaction 51 began before the oldest archived segment of 127.0.0.1 (10 is its first); cannot seed from this copy');
    expect(existsSync(join(dirs().agent, 'partial.ctl'))).toBe(false);
    // nobody listening, a wrong password: it fails, and nothing is written
    expect(both(['fetch-seed', '127.0.0.1', '{}/none.fdb'], { SEGMENT_PORT: '1' }).agent.status).not.toBe(0);
    expect(both(['fetch-seed', '127.0.0.1', '{}/none.fdb'], { ...env, ISC_PASSWORD: 'wrong' }).agent.status).not.toBe(0);
    expect(existsSync(join(dirs().agent, 'none.fdb'))).toBe(false);
  });
});
