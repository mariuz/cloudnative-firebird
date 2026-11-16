import { describe, it, expect, afterEach } from 'vitest';
import { spawn, spawnSync } from 'child_process';
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'fs';
import { Server, Socket } from 'net';
import { tmpdir } from 'os';
import { join } from 'path';
import { JOB_SCRIPTS, REPLICATION_SCRIPTS } from '../src/utils/replication';
import { fakeSegmentServer } from './helpers/segment-auth';
import { agentBin, hasPerl, implementations } from './helpers/implementations';

/**
 * The Jobs' programs that had no test running them: `fb-agent failover`, `backup-file` and
 * `cut-segment` against failover.pl, backup-file.pl and pitr-restore.sh's Perl one-liner. Each
 * pair gets the same environment and the same segment servers: what they print, the files they
 * leave and their exit status must be the same.
 *
 * The other programs (switchover, sync-standby, fetch-segments, pitr-plan, set-repl-seq) run in
 * the tests of their scripts, once per implementation (tests/helpers/implementations.ts).
 */
describe.skipIf(!agentBin || !hasPerl)('the Jobs\' programs: fb-agent and the Perl scripts', { timeout: 60_000 }, () => {
  const servers: Server[] = [];
  let dir = '';
  afterEach(() => {
    for (const s of servers.splice(0)) s.close();
    if (dir) rmSync(dir, { recursive: true, force: true });
  });

  /** a segment server on host:port; the requests it got */
  const serve = async (host: string, port: number, handler: (request: string, sock: Socket, raw: string) => void) => {
    const server = fakeSegmentServer(handler, 'tok');
    await new Promise<void>((resolve, reject) => {
      server.once('error', reject);
      server.listen(port, host, () => resolve());
    });
    servers.push(server);
    return (server.address() as { port: number }).port;
  };
  const run = (command: [string, string[]], env: Record<string, string>, cwd: string) =>
    new Promise<{ code: number | null; out: string; err: string }>((resolve) => {
      const child = spawn(command[0], command[1], { env: { PATH: process.env.PATH ?? '', ISC_PASSWORD: 'tok', ...env }, cwd });
      let out = '';
      let err = '';
      child.stdout.on('data', (d) => (out += d));
      child.stderr.on('data', (d) => (err += d));
      child.once('close', (code) => resolve({ code, out, err }));
    });
  const files = (at: string) =>
    Object.fromEntries(readdirSync(at).sort().map((name) => [name, readFileSync(join(at, name)).toString('hex')]));
  /** the same arguments and environment to both, each in a directory of its own ({} in them) */
  const both = async (script: string, args: string[], env: Record<string, string>, setup?: (at: string) => void) => {
    dir = mkdtempSync(join(tmpdir(), 'jobs-conformance-'));
    const results = [];
    for (const impl of implementations(script)) {
      const at = join(dir, impl.name);
      mkdirSync(at);
      setup?.(at);
      const path = join(dir, script);
      writeFileSync(path, { ...REPLICATION_SCRIPTS, ...JOB_SCRIPTS }[script]);
      const fill = (v: string) => v.split('{}').join(at);
      const r = await run(impl.command(path, args.map(fill)), Object.fromEntries(Object.entries(env).map(([k, v]) => [k, fill(v)])), at);
      results.push({ ...r, out: r.out.split(at).join('DIR'), err: r.err.split(at).join('DIR'), files: files(at) });
    }
    const [perl, agent] = results;
    const what = `${script} ${args.join(' ')}`;
    expect(agent.code === 0, `${what}: ${agent.err} / ${perl.err}`).toBe(perl.code === 0);
    expect(agent.out, what).toBe(perl.out);
    expect(agent.files, what).toEqual(perl.files);
    return { perl, agent };
  };

  it('failover: picks the candidate furthest ahead, ties in the listed order, alike', async () => {
    // three replicas on 127.0.0.1-3, one port: positions as the replicas answer them
    const answers: Record<string, string | null> = { '127.0.0.1': 'OK 40 0 0', '127.0.0.2': 'OK 42 7 0', '127.0.0.3': 'OK 42 0 0' };
    const port = await serve('127.0.0.1', 0, (r, sock) => sock.end(r === 'POSITION' ? `${answers['127.0.0.1']}\n` : 'ERR\n'));
    for (const host of ['127.0.0.2', '127.0.0.3']) {
      await serve(host, port, (r, sock) => {
        const answer = answers[host];
        if (answer === null) sock.destroy();
        else sock.end(r === 'POSITION' ? `${answer}\n` : 'ERR\n');
      });
    }
    const env = { CANDIDATES: '127.0.0.1 127.0.0.2  127.0.0.3', SEGMENT_PORT: String(port), SETTLE_SECONDS: '3', RESULT_FILE: '{}/result' };
    const all = await both('failover.pl', [], env);
    expect(all.agent.out).toBe(
      '127.0.0.1: applied up to segment 40\n127.0.0.2: applied up to segment 42\n127.0.0.3: applied up to segment 42\n' +
        'target=127.0.0.2 sequence=42 positions=127.0.0.1:40,127.0.0.2:42,127.0.0.3:42\n',
    );
    expect(Buffer.from(all.agent.files.result, 'hex').toString()).toBe('target=127.0.0.2 sequence=42 positions=127.0.0.1:40,127.0.0.2:42,127.0.0.3:42');
    // still applying when the settle time runs out; one not answering; one that is not a replica
    answers['127.0.0.1'] = 'OK 43 0 2';
    answers['127.0.0.2'] = null;
    answers['127.0.0.3'] = 'OK primary';
    const pending = await both('failover.pl', [], env);
    expect(pending.agent.out).toContain('127.0.0.1: applied up to segment 43 (2 segment(s) still pending)\n');
    expect(pending.agent.out).toContain('127.0.0.2: no position (unreachable or not a replica), not a candidate\n');
    expect(pending.agent.out).toContain('127.0.0.3: no position (unreachable or not a replica), not a candidate\n');
    // none: refused, with no result
    answers['127.0.0.1'] = 'ERR no replica control file';
    const none = await both('failover.pl', [], { ...env, SETTLE_SECONDS: '1' });
    expect(none.agent.code).not.toBe(0);
    expect(none.agent.err).toBe(none.perl.err);
    expect(none.agent.err).toBe('no candidate reported a position\n');
    expect(none.agent.files).toEqual({});
    const empty = await both('failover.pl', [], { ...env, CANDIDATES: ' ' });
    expect(empty.agent.err).toBe('no candidate replicas\n');
  });

  it('backup-file: copies, lists, removes and takes nbackups through the segment server, alike', async () => {
    const stored: Record<string, Buffer> = {};
    const requests: string[] = [];
    const files: Record<string, Buffer> = { 'a.nbk': Buffer.alloc(150_000, 7), 'empty.fbk': Buffer.alloc(0) };
    let listing = 'a.nbk\nempty.fbk\n.\n';
    const port = await serve('127.0.0.1', 0, (request, sock, raw) => {
      requests.push(request);
      const [command, name, size] = request.split(' ');
      if (command === 'FILE') {
        if (name === 'short.nbk') return void sock.end('OK 100\nonly some');
        if (!files[name]) return void sock.end(`ERR cannot read ${name}: No such file or directory\n`);
        sock.write(`OK ${files[name].length}\n`);
        return void sock.end(files[name]);
      }
      if (command === 'STORE') {
        if (name === 'refused.nbk') return void sock.end('ERR cannot write refused.nbk: Permission denied\n');
        // what came with the request line, then the rest
        const chunks = [Buffer.from(raw.slice(raw.indexOf('\n') + 1), 'latin1')];
        const want = Number(size);
        const done = () => {
          stored[name] = Buffer.concat(chunks).subarray(0, want);
          sock.end('OK\n');
        };
        if (Buffer.concat(chunks).length >= want) return done();
        sock.on('data', (d: Buffer) => {
          chunks.push(d);
          if (Buffer.concat(chunks).length >= want) done();
        });
        return;
      }
      if (command === 'REMOVE') return void sock.end(name === 'locked.nbk' ? 'ERR remove locked.nbk: Is a directory\n' : 'OK\n');
      if (command === 'FILES') return void sock.end(listing);
      if (command === 'NBACKUP') {
        // NBACKUP <level> <name>
        const file = size;
        if (file === 'replica.nbk') return void sock.end('OK replica 41\n');
        if (file === 'bad.nbk') return void sock.end('ERR nbackup failed\n');
        if (file === 'silent.nbk') return void sock.destroy();
        return void sock.end('OK\n');
      }
      sock.end('ERR bad request\n');
    });
    const env = { FIREBIRD_HOST: '127.0.0.1', SEGMENT_PORT: String(port) };
    const get = await both('backup-file.pl', ['get', 'a.nbk', '{}/copy.nbk'], env);
    expect(get.agent.out).toBe('copied a.nbk (150000 bytes) from 127.0.0.1\n');
    expect(Buffer.from(get.agent.files['copy.nbk'], 'hex').equals(files['a.nbk'])).toBe(true);
    await both('backup-file.pl', ['get', 'empty.fbk', '{}/empty.fbk'], env);
    for (const [args, message] of [
      [['get', 'none.nbk', '{}/x'], 'server: ERR cannot read none.nbk: No such file or directory\n'],
      [['get', 'short.nbk', '{}/x'], 'short read for short.nbk (9 of 100 bytes)\n'],
      [['put', '{}/missing', 'x.nbk'], 'cannot read DIR/missing\n'],
      [['remove', 'ok.nbk', 'locked.nbk', 'after.nbk'], 'server: ERR remove locked.nbk: Is a directory\n'],
      [['nbackup', '0', 'bad.nbk'], 'server: ERR nbackup failed\n'],
      [['nbackup', '0', 'silent.nbk'], 'server: no reply\n'],
      [['list', 'extra'], 'usage: backup-file.pl get <name> <file> | put <file> <name> | remove <name>... | list | nbackup <level> <name>\n'],
      [[], 'usage: backup-file.pl get <name> <file> | put <file> <name> | remove <name>... | list | nbackup <level> <name>\n'],
    ] as const) {
      const r = await both('backup-file.pl', [...args], env);
      expect(r.agent.code, args.join(' ')).not.toBe(0);
      expect(r.agent.err, args.join(' ')).toBe(message);
      expect(r.perl.err, args.join(' ')).toBe(message);
    }
    // the names before the refused one were removed, the ones after it not asked for, by both
    expect(requests.filter((r) => r.startsWith('REMOVE '))).toEqual(['REMOVE ok.nbk', 'REMOVE locked.nbk', 'REMOVE ok.nbk', 'REMOVE locked.nbk']);
    const upload = Buffer.alloc(300_001, 3);
    const put = await both('backup-file.pl', ['put', '{}/up.nbk', 'up.nbk'], env, (at) => writeFileSync(join(at, 'up.nbk'), upload));
    expect(put.agent.out).toBe('copied DIR/up.nbk (300001 bytes) to up.nbk on 127.0.0.1\n');
    expect(stored['up.nbk'].equals(upload)).toBe(true);
    const refused = await both('backup-file.pl', ['put', '{}/up.nbk', 'refused.nbk'], env, (at) => writeFileSync(join(at, 'up.nbk'), 'x'));
    expect(refused.agent.err).toBe('server: ERR cannot write refused.nbk: Permission denied\n');
    const list = await both('backup-file.pl', ['list'], env);
    expect(list.agent.out).toBe('a.nbk\nempty.fbk\n');
    listing = 'a.nbk\nERR cannot list\n.\n';
    expect((await both('backup-file.pl', ['list'], env)).agent.err).toBe('server: ERR cannot list\n');
    listing = 'a.nbk\n';
    expect((await both('backup-file.pl', ['list'], env)).agent.err).toBe('server: incomplete listing\n');
    expect((await both('backup-file.pl', ['remove', 'a.nbk', 'b.nbk'], env)).agent.out).toBe('removed a.nbk on 127.0.0.1\nremoved b.nbk on 127.0.0.1\n');
    expect((await both('backup-file.pl', ['nbackup', '1', 'primary.nbk'], env)).agent.out).toBe('nbackup primary.nbk written on 127.0.0.1\n');
    expect((await both('backup-file.pl', ['nbackup', '0', 'replica.nbk'], env)).agent.out).toBe(
      'nbackup replica.nbk written on 127.0.0.1 at replica position 41\nreplica position 41\n',
    );
    // without a host: refused before anything
    const nohost = await both('backup-file.pl', ['list'], { SEGMENT_PORT: String(port) });
    expect(nohost.agent.err).toBe('FIREBIRD_HOST is required\n');
    // nobody there
    const down = await both('backup-file.pl', ['list'], { FIREBIRD_HOST: '127.0.0.1', SEGMENT_PORT: '1' });
    expect(down.agent.code).not.toBe(0);
  });

  it('cut-segment: the segment\'s length in its header, and the file cut there, as the one-liner', () => {
    dir = mkdtempSync(join(tmpdir(), 'jobs-conformance-'));
    const segment = Buffer.alloc(1000, 9);
    segment.write('FBCHANGELOG\0', 0, 'latin1');
    segment.writeBigUInt64LE(1000n, 40);
    const [perl, agent] = [join(dir, 'perl'), join(dir, 'agent')];
    writeFileSync(perl, segment);
    writeFileSync(agent, segment);
    const oneLiner =
      'open(my $f, "+<:raw", $ARGV[0]) or die "$ARGV[0]: $!\\n"; seek($f, 40, 0); print $f pack("Q<", $ARGV[1]); close $f or die; truncate($ARGV[0], $ARGV[1]) or die "$ARGV[0]: $!\\n"';
    // the one-liner is the one in pitr-restore.sh
    expect(JOB_SCRIPTS['pitr-restore.sh']).toContain(oneLiner.replace(/\\\\n/g, '\\n'));
    expect(spawnSync('perl', ['-e', oneLiner, perl, '300']).status).toBe(0);
    expect(spawnSync(agentBin!, ['cut-segment', agent, '300']).status).toBe(0);
    expect(readFileSync(agent)).toEqual(readFileSync(perl));
    expect(readFileSync(agent).length).toBe(300);
    expect(readFileSync(agent).readBigUInt64LE(40)).toBe(300n);
    // a file that is not there, or a length that is not a number: refused
    expect(spawnSync(agentBin!, ['cut-segment', join(dir, 'none'), '3']).status).not.toBe(0);
    expect(spawnSync(agentBin!, ['cut-segment', agent, 'x']).status).not.toBe(0);
    expect(existsSync(join(dir, 'none'))).toBe(false);
  });
});
