import { describe, it, expect, beforeAll, afterAll, vi } from 'vitest';
import { ChildProcess, spawn, spawnSync } from 'child_process';
import { existsSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'fs';
import { createServer, Server, Socket } from 'net';
import { tmpdir } from 'os';
import { join } from 'path';
import { REPLICATION_SCRIPTS } from '../src/utils/replication';
import { startSegmentServer } from './helpers/segment-server';
import { signLine } from './helpers/segment-auth';

/**
 * The signed request protocol (SIG1) has three implementations: Perl (segment-auth.pl and
 * segment-server.pl), TypeScript (replication-lag.ts) and Rust (agent/sig1, the fb-agent binary:
 * docs/rust-agent-roadmap.md, Phase 1). Each client is run against each server, and the servers
 * are given the same lines and must give the same replies.
 *
 * Needs the fb-agent binary: FB_AGENT_BIN, or a build in agent/target (`cargo build` in
 * operator/agent). Skipped without one; the "Rust agent" CI job builds it and runs this file.
 */
const agentBin = [
  process.env.FB_AGENT_BIN,
  ...['x86_64-unknown-linux-musl/release', 'aarch64-unknown-linux-musl/release', 'release', 'debug'].map((d) =>
    join(__dirname, '..', 'agent', 'target', d, 'fb-agent'),
  ),
].find((p) => p && existsSync(p));
const hasPerl = spawnSync('perl', ['-v']).status === 0;

/** The replies of a server to one raw line */
const raw = (port: number, line: string): Promise<string[]> =>
  new Promise((resolve, reject) => {
    const socket = new Socket();
    let data = '';
    socket.once('error', reject);
    socket.on('data', (chunk) => (data += chunk.toString()));
    socket.once('end', () => resolve(data.split('\n').filter((l) => l !== '')));
    socket.connect(port, '127.0.0.1', () => socket.write(`${line}\n`));
  });

describe.skipIf(!agentBin || !hasPerl)('SIG1 conformance: Perl, TypeScript and Rust', () => {
  let root: string;
  let perl: { child: ChildProcess; port: number };
  let rust: { child: ChildProcess; port: number };
  const servers: Server[] = [];
  /** stdout lines and the exit status of a client program */
  const run = (command: string, args: string[], env: Record<string, string>) => {
    const r = spawnSync(command, args, { encoding: 'utf8', env: { ...process.env, ...env } });
    return { status: r.status, lines: r.stdout.split('\n').filter((l) => l !== ''), stderr: r.stderr };
  };
  const agent = (port: number, secret: string, ...request: string[]) =>
    run(agentBin!, ['request', '127.0.0.1', ...request], { ISC_PASSWORD: secret, SEGMENT_PORT: String(port), SEGMENT_PROXY: '' });
  const perlClient = (port: number, secret: string, ...request: string[]) =>
    run('perl', [join(root, 'segment-request.pl'), '127.0.0.1', ...request], { ISC_PASSWORD: secret, SEGMENT_PORT: String(port), SEGMENT_PROXY: '' });
  const nodeClient = async (port: number, secret: string, request: string) => {
    // the real client (the unit tests' setup replaces it)
    const { segmentRequest } = await vi.importActual<typeof import('../src/utils/replication-lag')>('../src/utils/replication-lag');
    return segmentRequest('127.0.0.1', port, `${secret} ${request}`, 5000);
  };

  beforeAll(async () => {
    root = mkdtempSync(join(tmpdir(), 'sig1-'));
    for (const d of ['archive', 'source', 'repl']) mkdirSync(join(root, d));
    writeFileSync(join(root, 'archive', 'mydb.fdb.journal-000000007'), 'x');
    writeFileSync(join(root, 'repl', 'primary'), 'db-0.db-headless\n');
    for (const name of ['segment-server.pl', 'segment-request.pl']) writeFileSync(join(root, name), REPLICATION_SCRIPTS[name]);
    perl = await startSegmentServer(join(root, 'segment-server.pl'), {
      ARCHIVE_DIR: join(root, 'archive'),
      DATABASE_PATH: join(root, 'mydb.fdb'),
      SOURCE_DIR: join(root, 'source'),
      REPLICATION_DIR: join(root, 'repl'),
      PRIMARY_FILE: join(root, 'repl', 'primary'),
      POD_NAME: 'db-0',
      ISC_PASSWORD: 'tok',
    });
    // fb-agent serve: SEGMENT_PORT=0 takes a free port, reported as the Perl server reports it
    const child = spawn(agentBin!, ['serve'], { env: { ...process.env, ISC_PASSWORD: 'tok', SEGMENT_PORT: '0', SEGMENT_LISTEN: '' } });
    rust = await new Promise((resolve, reject) => {
      let out = '';
      const timer = setTimeout(() => reject(new Error(`fb-agent did not start: ${out}`)), 15000);
      child.stdout.on('data', (d) => {
        out += d.toString();
        const m = /listening on [^\s:]+:(\d+)/.exec(out);
        if (m) {
          clearTimeout(timer);
          resolve({ child, port: Number(m[1]) });
        }
      });
      child.once('exit', (code) => reject(new Error(`fb-agent exited ${code}: ${out}`)));
    });
  });
  afterAll(() => {
    perl?.child.kill();
    rust?.child.kill();
    for (const s of servers) s.close();
    if (root) rmSync(root, { recursive: true, force: true });
  });

  it('the Rust client is answered by the Perl server', () => {
    expect(agent(perl.port, 'tok', 'PING')).toMatchObject({ status: 0, lines: ['OK'] });
    // a request with an argument, and a reply of several lines
    expect(agent(perl.port, 'tok', 'LIST').lines).toEqual(['mydb.fdb.journal-000000007', '.']);
    expect(agent(perl.port, 'tok', 'GET', 'mydb.fdb.journal-000000007').lines).toEqual(['OK 1', 'x']);
    expect(agent(perl.port, 'wrong', 'PING').lines).toEqual(['ERR unauthorized (signature)']);
  });

  it('the Perl and TypeScript clients are answered by the Rust server', async () => {
    expect(perlClient(rust.port, 'tok', 'PING')).toMatchObject({ status: 0, lines: ['OK'] });
    expect(perlClient(rust.port, 'wrong', 'PING').lines).toEqual(['ERR unauthorized (signature)']);
    expect(await nodeClient(rust.port, 'tok', 'PING')).toEqual(['OK']);
    expect(await nodeClient(rust.port, 'wrong', 'PING')).toEqual(['ERR unauthorized (signature)']);
    expect(agent(rust.port, 'tok', 'PING').lines).toEqual(['OK']);
    // nothing but PING yet
    expect(await nodeClient(rust.port, 'tok', 'LIST')).toEqual(['ERR not available in fb-agent yet']);
  });

  it('both servers refuse the same lines with the same replies', async () => {
    const now = Math.floor(Date.now() / 1000);
    const replayed = signLine('tok PING');
    const cases: Array<[string, string[]]> = [
      ['tok PING', ['ERR unauthorized (unsigned)']],
      ['PING', ['ERR unauthorized (unsigned)']],
      ['SIG1 x y z PING', ['ERR unauthorized (unsigned)']],
      [signLine('other PING'), ['ERR unauthorized (signature)']],
      [signLine('tok PING').replace(/PING$/, 'LIST'), ['ERR unauthorized (signature)']],
      // outside the 300 s window, either way
      [signLine('tok PING', now - 1000), ['ERR unauthorized (clock)']],
      [signLine('tok PING', now + 1000), ['ERR unauthorized (clock)']],
      [signLine('tok PING', now - 200), ['OK']],
      [replayed, ['OK']],
      [replayed, ['ERR unauthorized (replay)']],
    ];
    for (const [line, expected] of cases) {
      expect(await raw(perl.port, line), `Perl: ${line}`).toEqual(expected);
      expect(await raw(rust.port, line), `Rust: ${line}`).toEqual(expected);
    }
  });

  it('the Rust client goes through the segment TLS proxy as the Perl client does (CONNECT <host> <port>)', async () => {
    const firstLines: string[] = [];
    const proxy = createServer((client) => {
      let head = Buffer.alloc(0);
      const onData = (d: Buffer) => {
        head = Buffer.concat([head, d]);
        const nl = head.indexOf(10);
        if (nl < 0) return;
        client.off('data', onData);
        firstLines.push(head.subarray(0, nl).toString());
        const upstream = new Socket();
        upstream.connect(perl.port, '127.0.0.1', () => {
          upstream.write(head.subarray(nl + 1));
          client.pipe(upstream);
          upstream.pipe(client);
        });
      };
      client.on('data', onData);
    });
    servers.push(proxy);
    const proxyPort = await new Promise<number>((resolve) => proxy.listen(0, '127.0.0.1', () => resolve((proxy.address() as { port: number }).port)));
    const env = { ISC_PASSWORD: 'tok', SEGMENT_PORT: '3051', SEGMENT_PROXY: `127.0.0.1:${proxyPort}` };
    // not spawnSync: the proxy runs in this process
    const lines = (command: string, args: string[]) =>
      new Promise<string[]>((resolve, reject) => {
        const child = spawn(command, args, { env: { ...process.env, ...env } });
        let out = '';
        child.stdout.on('data', (d) => (out += d.toString()));
        child.once('error', reject);
        child.once('exit', () => resolve(out.split('\n').filter((l) => l !== '')));
      });
    expect(await lines(agentBin!, ['request', 'db-1.db-headless', 'PING'])).toEqual(['OK']);
    expect(await lines('perl', [join(root, 'segment-request.pl'), 'db-1.db-headless', 'PING'])).toEqual(['OK']);
    expect(firstLines).toEqual(['CONNECT db-1.db-headless 3051', 'CONNECT db-1.db-headless 3051']);
  });
});
