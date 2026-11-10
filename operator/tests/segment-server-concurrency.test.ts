import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { ChildProcess, spawnSync } from 'child_process';
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'fs';
import { connect, Socket } from 'net';
import { tmpdir } from 'os';
import { join } from 'path';
import { REPLICATION_SCRIPTS } from '../src/utils/replication';
import { startSegmentServer } from './helpers/segment-server';
import { signLine } from './helpers/segment-auth';

const hasPerl = spawnSync('perl', ['-v']).status === 0;

/**
 * The segment server answers its long requests (GET, SEED, TXNS, PLAN, like FILE, STORE, PROMOTE
 * and NBACKUP before) from child processes. In its accept loop, a seed downloaded slowly held
 * every other request: the Lease holder's STATE, the pullers, the operator. The Lease was not
 * renewed and the isolation check fenced the primary (hack/bench/segment-server.sh). The copies
 * (a seed, an nbackup, the in-place promotion) still run one at a time, under a lock.
 */
describe.skipIf(!hasPerl)('segment-server.pl answers other requests during a long one', () => {
  const MB = 1024 * 1024;
  // more than the socket buffers hold: the server blocks writing to a client that does not read
  const SIZE = 64 * MB;
  let root: string;
  let child: ChildProcess | undefined;
  let port: number;
  const sockets: Socket[] = [];

  beforeEach(async () => {
    root = mkdtempSync(join(tmpdir(), 'segsrv-conc-'));
    for (const d of ['archive', 'source', 'repl', 'bin']) mkdirSync(join(root, d));
    writeFileSync(join(root, 'archive', 'mydb.fdb.journal-000000001'), Buffer.alloc(SIZE, 1));
    writeFileSync(join(root, 'mydb.fdb'), Buffer.alloc(SIZE, 2));
    writeFileSync(join(root, 'repl', 'primary'), 'db-0.db-headless\n');
    // the local server's isql: records the statements it is given (the backup lock of a live seed)
    writeFileSync(join(root, 'bin', 'isql'), `#!/bin/sh\ncat >> ${join(root, 'isql.log')}\n`);
    chmodSync(join(root, 'bin', 'isql'), 0o755);
    const script = join(root, 'segment-server.pl');
    writeFileSync(script, REPLICATION_SCRIPTS['segment-server.pl']);
    ({ child, port } = await startSegmentServer(script, {
      ARCHIVE_DIR: join(root, 'archive'),
      DATABASE_PATH: join(root, 'mydb.fdb'),
      SOURCE_DIR: join(root, 'source'),
      REPLICATION_DIR: join(root, 'repl'),
      PRIMARY_FILE: join(root, 'repl', 'primary'),
      POD_NAME: 'db-0',
      ISC_PASSWORD: 'tok',
      ALLOW_LIVE_SEED: 'true',
      PATH: `${join(root, 'bin')}:${process.env.PATH}`,
    }));
  });
  afterEach(() => {
    for (const s of sockets.splice(0)) s.destroy();
    child?.kill();
    rmSync(root, { recursive: true, force: true });
  });

  /** A request whose reply is not read until drain(): the server's writes block meanwhile */
  const stalled = async (request: string) => {
    const sock = connect(port, '127.0.0.1');
    sockets.push(sock);
    sock.pause();
    await new Promise<void>((resolve, reject) => sock.once('connect', resolve).once('error', reject));
    sock.write(`${signLine(`tok ${request}`)}\n`);
    return {
      /** reads the whole reply: its first line and the number of bytes after it */
      drain: () =>
        new Promise<{ head: string; bytes: number }>((resolve, reject) => {
          let head: string | undefined;
          let pending = Buffer.alloc(0);
          let bytes = 0;
          sock.on('data', (d: Buffer) => {
            if (head !== undefined) return void (bytes += d.length);
            pending = Buffer.concat([pending, d]);
            const nl = pending.indexOf(10);
            if (nl < 0) return;
            head = pending.subarray(0, nl).toString();
            bytes = pending.length - nl - 1;
          });
          sock.once('end', () => resolve({ head: head ?? '', bytes }));
          sock.once('error', reject);
          sock.resume();
        }),
    };
  };
  // the real client (the unit tests' setup replaces it)
  const ask = async (request: string) => {
    const { segmentRequest } = await vi.importActual<typeof import('../src/utils/replication-lag')>('../src/utils/replication-lag');
    return segmentRequest('127.0.0.1', port, `tok ${request}`, 5000);
  };
  const backupLocks = () => (existsSync(join(root, 'isql.log')) ? readFileSync(join(root, 'isql.log'), 'utf8') : '').match(/BEGIN BACKUP/g)?.length ?? 0;
  const waitFor = async (check: () => boolean, ms = 10000) => {
    const end = Date.now() + ms;
    while (!check()) {
      if (Date.now() > end) throw new Error('timed out waiting');
      await new Promise((r) => setTimeout(r, 50));
    }
  };

  it('a segment being downloaded does not hold PING and LIST', async () => {
    const download = await stalled('GET mydb.fdb.journal-000000001');
    await new Promise((r) => setTimeout(r, 300));
    expect(await ask('PING')).toEqual(['OK']);
    expect(await ask('LIST')).toEqual(['mydb.fdb.journal-000000001']);
    // a second download of it at the same time
    const second = await stalled('GET mydb.fdb.journal-000000001');
    expect(await ask('PING')).toEqual(['OK']);
    expect(await download.drain()).toEqual({ head: `OK ${SIZE}`, bytes: SIZE });
    expect(await second.drain()).toEqual({ head: `OK ${SIZE}`, bytes: SIZE });
  }, 30000);

  it('a live seed being downloaded does not hold the others, and seeds run one at a time', async () => {
    const first = await stalled('SEED');
    await waitFor(() => backupLocks() === 1);
    // served while the first seed is still being downloaded
    expect(await ask('PING')).toEqual(['OK']);
    expect(await ask('ISOLATION')).toEqual(['OK online']);
    // a second seed waits for the first: one copy file, one backup lock at a time
    const second = await stalled('SEED');
    await new Promise((r) => setTimeout(r, 1500));
    expect(backupLocks()).toBe(1);
    expect(await ask('PING')).toEqual(['OK']);
    expect(await first.drain()).toEqual({ head: `OK ${SIZE} 0 live`, bytes: SIZE });
    await waitFor(() => backupLocks() === 2);
    expect(await second.drain()).toEqual({ head: `OK ${SIZE} 0 live`, bytes: SIZE });
    expect(readFileSync(join(root, 'isql.log'), 'utf8').match(/(BEGIN|END) BACKUP/g)).toEqual([
      'BEGIN BACKUP',
      'END BACKUP',
      'BEGIN BACKUP',
      'END BACKUP',
    ]);
    // the copy is removed after each seed
    await waitFor(() => !existsSync(join(root, 'repl', 'seed.copy')));
  }, 40000);
});
