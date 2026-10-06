import { describe, it, expect, afterAll } from 'vitest';
import { spawn, spawnSync, ChildProcess } from 'child_process';
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { createServer, Server, Socket } from 'net';
import { JOB_SCRIPTS, REPLICATION_SCRIPTS } from '../src/utils/replication';

const hasPerl = spawnSync('perl', ['-v']).status === 0;

/** One request to a segment server (the shared client is mocked in unit tests) */
const request = (port: number, line: string): Promise<string[]> =>
  new Promise((resolve, reject) => {
    const socket = new Socket();
    let data = '';
    socket.once('error', reject);
    socket.on('data', (chunk) => (data += chunk.toString()));
    socket.once('end', () => resolve(data.split('\n').filter((l) => l !== '')));
    socket.connect(port, '127.0.0.1', () => socket.write(`${line}\n`));
  });

/** A directory with the scripts and a fake fbsvcmgr logging its arguments ("<dir>/fail" makes it fail) */
function workspace() {
  const dir = mkdtempSync(join(tmpdir(), 'fb-sync-'));
  for (const name of ['segment-server.pl']) writeFileSync(join(dir, name), REPLICATION_SCRIPTS[name]);
  writeFileSync(join(dir, 'sync-standby.pl'), JOB_SCRIPTS['sync-standby.pl']);
  mkdirSync(join(dir, 'bin'));
  writeFileSync(
    join(dir, 'bin', 'fbsvcmgr'),
    `#!/bin/sh\necho "$*" >> "${dir}/calls"\n[ -f "${dir}/fail" ] && exit 1\ncase "$*" in *sts_hdr_pages*) echo "Attributes force write";; esac\nexit 0\n`,
  );
  chmodSync(join(dir, 'bin', 'fbsvcmgr'), 0o755);
  const calls = () => (existsSync(join(dir, 'calls')) ? readFileSync(join(dir, 'calls'), 'utf8').trim().split('\n') : []);
  return { dir, calls };
}

describe('segment server: synchronous replication commands', () => {
  const servers: ChildProcess[] = [];
  afterAll(() => servers.forEach((s) => s.kill()));

  async function start(pod: string, primary: string, database = '/var/lib/firebird/data/mydb.fdb', extraEnv: Record<string, string> = {}) {
    const ws = workspace();
    const base = join(ws.dir, 'repl');
    for (const d of ['archive', 'source']) mkdirSync(join(base, d), { recursive: true });
    writeFileSync(join(ws.dir, 'primary'), `${primary}.db-headless\n`);
    const port = 43000 + Math.floor(Math.random() * 2000);
    const server = spawn('perl', [join(ws.dir, 'segment-server.pl')], {
      env: {
        PATH: `${join(ws.dir, 'bin')}:${process.env.PATH}`,
        ARCHIVE_DIR: join(base, 'archive'),
        SOURCE_DIR: join(base, 'source'),
        REPLICATION_DIR: base,
        STATE_FILE: join(base, '.last-pulled'),
        DATABASE_PATH: database,
        PRIMARY_FILE: join(ws.dir, 'primary'),
        POD_NAME: pod,
        ISC_USER: 'SYSDBA',
        ISC_PASSWORD: 'tok',
        SEGMENT_PORT: String(port),
        SCRIPT_DIR: ws.dir,
        ...extraEnv,
      },
    });
    servers.push(server);
    const ask = async (line: string) => {
      for (let i = 0; ; i++) {
        try {
          return await request(port, `tok ${line}`);
        } catch (err) {
          if (i > 50) throw err;
          await new Promise((r) => setTimeout(r, 100));
        }
      }
    };
    return { ws, base, ask };
  }

  it('takes an nbackup of a replica at a known position, with its control file (NBACKUP)', async () => {
    if (!hasPerl) return;
    const data = mkdtempSync(join(tmpdir(), 'fb-data-'));
    const { ws, base, ask } = await start('db-1', 'db-0', join(data, 'mydb.fdb'));
    const guid = '{11111111-2222-3333-4444-555555555555}';
    const ctl = Buffer.alloc(56);
    ctl.write('FBREPLCTL', 0, 'latin1');
    ctl.writeUInt16LE(1, 10);
    ctl.writeUInt32LE(1, 12);
    ctl.writeBigUInt64LE(9n, 16);
    ctl.writeBigUInt64LE(3n, 32);
    ctl.writeBigUInt64LE(500n, 40);
    ctl.writeBigUInt64LE(8n, 48);
    writeFileSync(join(base, 'source', guid), ctl);
    // the segment puller acknowledges the pause
    const ack = setInterval(() => {
      if (existsSync(join(base, '.pause-pull'))) writeFileSync(join(base, '.pull-paused'), '');
    }, 50);
    try {
      expect(await ask('NBACKUP 1 nbackup-l1-x.nbk')).toEqual(['OK replica 9']);
    } finally {
      clearInterval(ack);
    }
    expect(readFileSync(join(data, 'nbackup-l1-x.nbk.ctl')).equals(ctl)).toBe(true);
    expect(existsSync(join(base, '.pause-pull'))).toBe(false);
    expect(ws.calls().at(-1)).toContain(`action_nbak dbname ${join(data, 'mydb.fdb')} nbk_file ${join(data, 'nbackup-l1-x.nbk')} nbk_level 1`);
    // served and removed like the backups
    expect((await ask('FILES'))).toContain('nbackup-l1-x.nbk.ctl');
    expect(await ask('REMOVE nbackup-l1-x.nbk.ctl')).toEqual(['OK']);
    expect((await ask('NBACKUP 3 x.nbk'))[0]).toBe('ERR bad request');
    expect((await ask('NBACKUP 0 ../x.nbk'))[0]).toBe('ERR bad request');
  });

  it('takes a plain nbackup on the primary (NBACKUP)', async () => {
    if (!hasPerl) return;
    const data = mkdtempSync(join(tmpdir(), 'fb-data-'));
    const { ws, ask } = await start('db-0', 'db-0', join(data, 'mydb.fdb'));
    expect(await ask('NBACKUP 0 nbackup-l0-x.nbk')).toEqual(['OK']);
    expect(existsSync(join(data, 'nbackup-l0-x.nbk.ctl'))).toBe(false);
    expect(ws.calls().at(-1)).toContain('nbk_level 0');
  });

  it('samples the primary\'s journal segments as recovery points (POINTS)', async () => {
    if (!hasPerl) return;
    const journal = mkdtempSync(join(tmpdir(), 'fb-journal-'));
    const segment = (seq: number, length: number) => {
      const h = Buffer.alloc(length);
      h.write('FBCHANGELOG', 0, 'latin1');
      h.writeBigUInt64LE(BigInt(seq), 32);
      h.writeBigUInt64LE(BigInt(length), 40);
      writeFileSync(join(journal, `mydb.fdb.journal-${String(seq).padStart(9, '0')}`), h);
    };
    segment(7, 150);
    const { base, ask } = await start('db-0', 'db-0', undefined, { RECOVERY_POINTS: 'true', JOURNAL_DIR: journal });
    const lengths = async () => (await ask('POINTS 7')).filter((l) => l !== '.').map((l) => Number(l.split(' ')[1]));
    for (let i = 0; i < 40 && (await lengths()).length < 1; i++) await new Promise((r) => setTimeout(r, 100));
    segment(7, 252);
    for (let i = 0; i < 40 && (await lengths()).length < 2; i++) await new Promise((r) => setTimeout(r, 100));
    expect(await lengths()).toEqual([150, 252]);
    const [line] = await ask('POINTS 7');
    expect(Math.abs(Number(line.split(' ')[0]) - Date.now() / 1000)).toBeLessThan(10);
    expect(existsSync(join(base, 'points', '7'))).toBe(true);
    // a segment without points, and a bad request
    expect(await ask('POINTS 8')).toEqual(['.']);
    expect((await ask('POINTS x'))[0]).toBe('ERR bad request');
  }, 15_000);

  it('samples nothing on a replica', async () => {
    if (!hasPerl) return;
    const journal = mkdtempSync(join(tmpdir(), 'fb-journal-'));
    const h = Buffer.alloc(150);
    h.write('FBCHANGELOG', 0, 'latin1');
    h.writeBigUInt64LE(3n, 32);
    h.writeBigUInt64LE(150n, 40);
    writeFileSync(join(journal, 'mydb.fdb.journal-000000003'), h);
    const { ask } = await start('db-1', 'db-0', undefined, { RECOVERY_POINTS: 'true', JOURNAL_DIR: journal });
    await new Promise((r) => setTimeout(r, 1500));
    expect(await ask('POINTS 3')).toEqual(['.']);
  });

  it('reports the engine version of the local server (VERSION)', async () => {
    if (!hasPerl) return;
    const { ws, ask } = await start('db-0', 'db-0');
    expect((await ask('VERSION'))[0]).toMatch(/^ERR/);
    writeFileSync(join(ws.dir, 'bin', 'isql'), '#!/bin/sh\necho "V                               4.0.7"\n');
    chmodSync(join(ws.dir, 'bin', 'isql'), 0o755);
    expect(await ask('VERSION')).toEqual(['OK 4.0.7']);
  });

  it('reports the lineage switches recorded at promotion (LINEAGE)', async () => {
    if (!hasPerl) return;
    const { base, ask } = await start('db-1', 'db-1');
    expect(await ask('LINEAGE')).toEqual(['.']);
    writeFileSync(join(base, 'lineage'), '21 41\n60 75\nbad line\n');
    expect(await ask('LINEAGE')).toEqual(['mydb.fdb.lineage-21-41', 'mydb.fdb.lineage-60-75', '.']);
  });

  it('writes the sync_replica entry on the primary and reports it', async () => {
    if (!hasPerl) return;
    const { base, ask } = await start('db-0', 'db-0');
    expect(await ask('SYNCTO')).toEqual(['OK none']);
    expect(await ask('SYNC db-1.db-headless')).toEqual(['OK']);
    expect(readFileSync(join(base, 'sync.conf'), 'utf8')).toBe(
      'sync_replica = db-1.db-headless:/var/lib/firebird/data/mydb.fdb\n{\n  username = SYSDBA\n  password_env = ISC_PASSWORD\n}\n',
    );
    expect(await ask('SYNCTO')).toEqual(['OK db-1.db-headless']);
    expect((await ask('SYNC db-1;rm -rf'))[0]).toBe('ERR bad request');
    expect(await ask('SYNC none')).toEqual(['OK']);
    expect(readFileSync(join(base, 'sync.conf'), 'utf8')).toBe('');
    expect(await ask('SYNCTO')).toEqual(['OK none']);
    // several standbys: an entry each, every commit waits for all of them
    expect(await ask('SYNC db-1.db-headless,db-2.db-headless')).toEqual(['OK']);
    expect(readFileSync(join(base, 'sync.conf'), 'utf8').match(/^sync_replica = /gm)).toHaveLength(2);
    expect(readFileSync(join(base, 'sync.conf'), 'utf8')).toContain('sync_replica = db-2.db-headless:/var/lib/firebird/data/mydb.fdb\n');
    expect(await ask('SYNCTO')).toEqual(['OK db-1.db-headless,db-2.db-headless']);
    expect((await ask('SYNC db-1,,db-2'))[0]).toBe('ERR bad request');
    expect(await ask('SYNC none')).toEqual(['OK']);
    // the primary is never a standby
    expect((await ask('STANDBY on'))[0]).toMatch(/^ERR this instance is the primary/);
  });

  it('turns a replica into the standby and back, repositioned after the primary\'s last segment', async () => {
    if (!hasPerl) return;
    const { base, ask } = await start('db-1', 'db-0');
    const guid = '{11111111-2222-3333-4444-555555555555}';
    const control = (seq: number, dbseq: number) => {
      const b = Buffer.alloc(40);
      b.write('FBREPLCTL', 0, 'latin1');
      b.writeUInt16LE(1, 10);
      b.writeBigUInt64LE(BigInt(seq), 16);
      b.writeBigUInt64LE(BigInt(dbseq), 32);
      return b;
    };
    writeFileSync(join(base, 'source', guid), control(7, 3));
    writeFileSync(join(base, '.last-pulled'), 'mydb.fdb.journal-0000000007\n');
    expect(await ask('POSITION')).toEqual(['OK 7 0 0']);

    expect(await ask('STANDBY on')).toEqual(['OK']);
    expect(existsSync(join(base, 'sync-standby'))).toBe(true);
    // until the puller has confirmed that the primary names it, the control file position counts
    expect(await ask('POSITION')).toEqual(['OK 7 0 0']);
    writeFileSync(join(base, 'sync-seen'), '12\n');
    expect(await ask('POSITION')).toEqual(['OK 12 0 0']);

    expect(await ask('STANDBY off 15')).toEqual(['OK']);
    const ctl = readFileSync(join(base, 'source', guid));
    expect(ctl.subarray(0, 9).toString('latin1')).toBe('FBREPLCTL');
    expect(ctl.readUInt32LE(12)).toBe(0); // no transaction in progress
    expect(Number(ctl.readBigUInt64LE(16))).toBe(15);
    expect(ctl.readUInt32LE(24)).toBe(0);
    expect(Number(ctl.readBigUInt64LE(32))).toBe(3); // the database's own sequence, kept
    expect(readFileSync(join(base, '.last-pulled'), 'utf8')).toBe('mydb.fdb.journal-0000000015\n');
    expect(existsSync(join(base, 'sync-standby'))).toBe(false);
    expect(existsSync(join(base, 'sync-seen'))).toBe(false);
    expect(await ask('POSITION')).toEqual(['OK 15 0 0']);
    // not a standby: nothing to reposition
    expect(await ask('STANDBY off 20')).toEqual(['OK']);
    expect(Number(readFileSync(join(base, 'source', guid)).readBigUInt64LE(16))).toBe(15);
  });
});

describe('sync-standby.pl', () => {
  /**
   * Runs the Job script against fake segment servers for the primary (127.0.0.1) and the standby
   * (127.0.0.2, same port; none with __down): answers per command prefix, requests logged
   */
  async function run(
    action: string,
    answers: { primary: Record<string, string>; standby: Record<string, string | null> },
    extraEnv: Record<string, string> = {},
  ) {
    const ws = workspace();
    const log: string[] = [];
    const make = (who: 'primary' | 'standby', host: string, port: number) =>
      new Promise<Server>((resolve, reject) => {
        const server = createServer((sock) =>
          sock.once('data', (buf) => {
            const line = buf.toString().trim().split(' ').slice(1).join(' ');
            log.push(`${who} ${line}`);
            const table = answers[who] as Record<string, string | null>;
            const key = Object.keys(table).find((k) => line.startsWith(k));
            const reply = key === undefined ? 'ERR bad request' : table[key];
            if (reply === null) sock.destroy();
            else sock.end(`${reply}\n`);
          }),
        );
        server.once('error', reject);
        server.listen(port, host, () => resolve(server));
      });
    // both on the same port (the script has one SEGMENT_PORT): the primary's is chosen by the
    // system, and taken again if it is in use on 127.0.0.2
    let servers: Server[] = [];
    let port = 0;
    for (let attempt = 0; servers.length === 0; attempt++) {
      const primary = await make('primary', '127.0.0.1', 0);
      port = (primary.address() as { port: number }).port;
      if (answers.standby.__down !== undefined) {
        servers = [primary];
        break;
      }
      try {
        servers = [primary, await make('standby', '127.0.0.2', port)];
      } catch (err) {
        primary.close();
        if (attempt >= 10) throw err;
      }
    }
    const child = spawn('perl', [join(ws.dir, 'sync-standby.pl')], {
      env: {
        PATH: `${join(ws.dir, 'bin')}:${process.env.PATH}`,
        ACTION: action,
        PRIMARY: '127.0.0.1',
        STANDBY: '127.0.0.2',
        DATABASE_PATH: '/data/mydb.fdb',
        ISC_PASSWORD: 'tok',
        SEGMENT_PORT: String(port),
        TIMEOUT_SECONDS: '4',
        RESULT_FILE: join(ws.dir, 'result'),
        ...extraEnv,
      },
    });
    let out = '';
    child.stdout.on('data', (d) => (out += d));
    child.stderr.on('data', (d) => (out += d));
    const code = await new Promise<number>((resolve) => child.on('exit', (c) => resolve(c ?? -1)));
    servers.forEach((s) => s.close());
    const result = existsSync(join(ws.dir, 'result')) ? readFileSync(join(ws.dir, 'result'), 'utf8') : undefined;
    return { code, out, log, result, calls: ws.calls() };
  }

  it('attaches: primary shut down, standby caught up, SYNC before STANDBY on, primary back online', async () => {
    if (!hasPerl) return;
    const r = await run('attach', {
      primary: { HEADER: 'OK 9', LIST: 'mydb.fdb.journal-0000000009\n.', SYNC: 'OK' },
      standby: { POSITION: 'OK 9 0 0', 'STANDBY on': 'OK' },
    });
    expect(r.code).toBe(0);
    expect(r.result).toBe('attached');
    expect(r.log).toEqual(['primary HEADER', 'standby POSITION', 'primary SYNC 127.0.0.2', 'standby STANDBY on']);
    expect(r.calls[1]).toContain('prp_shutdown_mode prp_sm_full prp_force_shutdown 0');
    expect(r.calls[r.calls.length - 1]).toContain('prp_online_mode prp_sm_normal');
  });

  it('attaches to a primary just promoted and not written to (no segment at its sequence)', async () => {
    if (!hasPerl) return;
    // its journal starts after S: nothing archived; the standby, seeded at S, has applied S
    const r = await run('attach', {
      primary: { HEADER: 'OK 12', LIST: '.', SYNC: 'OK' },
      standby: { POSITION: 'OK 12 0 0', 'STANDBY on': 'OK' },
    });
    expect(r.code).toBe(0);
    expect(r.result).toBe('attached');
  });

  it('keeps the other standbys in every SYNC (OTHERS)', async () => {
    if (!hasPerl) return;
    const attach = await run(
      'attach',
      { primary: { HEADER: 'OK 9', SYNC: 'OK' }, standby: { POSITION: 'OK 9 0 0', 'STANDBY on': 'OK' } },
      { OTHERS: 'db-1.db-headless' },
    );
    expect(attach.result).toBe('attached');
    expect(attach.log).toContain('primary SYNC db-1.db-headless,127.0.0.2');
    const detach = await run('detach', { primary: { HEADER: 'OK 31', SYNC: 'OK' }, standby: { 'STANDBY off': 'OK' } }, { OTHERS: 'db-1.db-headless' });
    expect(detach.result).toBe('detached');
    expect(detach.log).toContain('primary SYNC db-1.db-headless');
  });

  it('undoes a half-done attach and brings the primary back online', async () => {
    if (!hasPerl) return;
    const r = await run('attach', {
      primary: { HEADER: 'OK 9', LIST: 'mydb.fdb.journal-0000000009\n.', SYNC: 'OK' },
      standby: { POSITION: 'OK 9 0 0', 'STANDBY on': 'ERR no', 'STANDBY off': 'OK' },
    });
    expect(r.code).not.toBe(0);
    expect(r.out).toContain('refused to become the synchronous standby');
    expect(r.log.slice(-3)).toEqual(['standby STANDBY on', 'primary SYNC none', 'standby STANDBY off 9']);
    expect(r.calls[r.calls.length - 1]).toContain('prp_online_mode prp_sm_normal');
    expect(r.result).toBe('failed clean');
  });

  it('reports a standby it could not reposition, for the operator to re-seed it', async () => {
    if (!hasPerl) return;
    const r = await run('attach', {
      primary: { HEADER: 'OK 9', LIST: 'mydb.fdb.journal-0000000009\n.', SYNC: 'OK' },
      standby: { POSITION: 'OK 9 0 0', 'STANDBY on': 'ERR no', 'STANDBY off': 'ERR no' },
    });
    expect(r.code).not.toBe(0);
    expect(r.result).toBe('failed');
  });

  it('gives up on a standby that does not catch up, and leaves synchronous replication off', async () => {
    if (!hasPerl) return;
    const r = await run('attach', {
      primary: { HEADER: 'OK 9', LIST: 'mydb.fdb.journal-0000000009\n.', SYNC: 'OK' },
      standby: { POSITION: 'OK 8 0 1' },
    });
    expect(r.code).not.toBe(0);
    expect(r.out).toContain('timed out waiting for 127.0.0.2 to apply segment 9');
    expect(r.log).not.toContain('primary SYNC 127.0.0.2');
    expect(r.log).toContain('primary SYNC none');
    expect(r.calls[r.calls.length - 1]).toContain('prp_online_mode');
  }, 20_000); // waits TIMEOUT_SECONDS (4) for the standby

  it('detaches: standby repositioned first, then synchronous replication off', async () => {
    if (!hasPerl) return;
    const r = await run('detach', {
      primary: { HEADER: 'OK 31', SYNC: 'OK' },
      standby: { 'STANDBY off': 'OK' },
    });
    expect(r.code).toBe(0);
    expect(r.result).toBe('detached');
    expect(r.log).toEqual(['primary HEADER', 'standby STANDBY off 31', 'primary SYNC none']);
    expect(r.calls[r.calls.length - 1]).toContain('prp_online_mode prp_sm_normal');
  });

  it('keeps the standby attached when synchronous replication cannot be turned off', async () => {
    if (!hasPerl) return;
    const r = await run('detach', {
      primary: { HEADER: 'OK 31', SYNC: 'ERR disk full' },
      standby: { 'STANDBY off': 'OK', 'STANDBY on': 'OK' },
    });
    expect(r.code).not.toBe(0);
    expect(r.log).toEqual(['primary HEADER', 'standby STANDBY off 31', 'primary SYNC none', 'standby STANDBY on']);
    expect(r.calls[r.calls.length - 1]).toContain('prp_online_mode');
  });

  it('detaches a standby that cannot be reached, for the operator to re-seed it', async () => {
    if (!hasPerl) return;
    const r = await run('detach', { primary: { HEADER: 'OK 31', SYNC: 'OK' }, standby: { __down: null } });
    expect(r.code).toBe(0);
    expect(r.result).toBe('detached unreachable');
    expect(r.log).toEqual(['primary HEADER', 'primary SYNC none']);
  });
});
