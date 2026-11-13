import { describe, it, expect, afterEach } from 'vitest';
import { ChildProcess, spawn } from 'child_process';
import { existsSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'fs';
import { createServer, Server, Socket } from 'net';
import { tmpdir } from 'os';
import { join } from 'path';
import { createTlsCertificates } from '../src/utils/certificates';
import { parsePeers, SegmentTlsFiles, SegmentTlsPeers, startClient, startServer } from '../src/segment-tls';

/**
 * The segment TLS proxy has two implementations: segment-tls.ts (Node, from the operator image)
 * and `fb-agent segment-tls` (docs/rust-agent-segment-server.md, step 6). While a cluster is
 * switched to the agent its instances restart one by one, so each must work with the other: the
 * scenarios run for every pair of a server side and a client side.
 *
 * The Rust one needs the fb-agent binary (FB_AGENT_BIN, or a build in agent/target) and is skipped
 * without it; the "Rust agent" CI job runs this file with it.
 */
const agentBin = [
  process.env.FB_AGENT_BIN,
  ...['x86_64-unknown-linux-musl/release', 'aarch64-unknown-linux-musl/release', 'release', 'debug'].map((d) =>
    join(__dirname, '..', 'agent', 'target', d, 'fb-agent'),
  ),
].find((p) => p && existsSync(p));

type Kind = 'Node' | 'Rust';
const kinds: Kind[] = ['Node', 'Rust'];
const pairs = kinds.flatMap((server) => kinds.map((client) => ({ server, client, name: `${server} server side, ${client} client side` })));

const certs = (caName: string): SegmentTlsFiles => {
  const c = createTlsCertificates({ caName, dnsNames: ['*.db-headless'], clientAuth: true });
  return { ca: c.caCert, cert: c.tlsCert, key: c.tlsKey };
};

/** One exchange as a client does it: bytes out, everything until the other end closes back */
const exchange = (port: number, data: string | Buffer): Promise<Buffer> =>
  new Promise((resolve, reject) => {
    const socket = new Socket();
    const chunks: Buffer[] = [];
    socket.on('data', (c: Buffer) => chunks.push(c));
    socket.once('error', reject);
    socket.once('close', () => resolve(Buffer.concat(chunks)));
    socket.connect(port, '127.0.0.1', () => socket.write(data));
  });
const text = async (port: number, data: string) => (await exchange(port, data).catch(() => Buffer.alloc(0))).toString();

describe.each(pairs)('segment TLS proxies together: $name', ({ server, client }) => {
  const available = Boolean(agentBin) || (server === 'Node' && client === 'Node');
  const servers: Server[] = [];
  const children: ChildProcess[] = [];
  const dirs: string[] = [];
  afterEach(() => {
    servers.splice(0).forEach((s) => s.close());
    children.splice(0).forEach((c) => c.kill('SIGKILL'));
    dirs.splice(0).forEach((d) => rmSync(d, { recursive: true, force: true }));
  });

  /** A segment server stand-in: answers "OK <first line>" once it has the line, then `reply` */
  async function segmentServer(reply?: Buffer): Promise<{ port: number; seen: Buffer[] }> {
    const seen: Buffer[] = [];
    const target = createServer({ allowHalfOpen: true }, (s) => {
      const chunks: Buffer[] = [];
      let answered = false;
      s.on('data', (c: Buffer) => {
        chunks.push(c);
        const all = Buffer.concat(chunks);
        if (!answered && all.includes(0x0a)) {
          answered = true;
          s.write(`OK ${all.subarray(0, all.indexOf(0x0a)).toString()}\n`);
          if (reply) s.write(reply);
          // a request without a body is answered and closed, as the segment server does
          if (!all.toString().startsWith('STORE ')) s.end();
        }
      });
      s.on('end', () => {
        seen.push(Buffer.concat(chunks));
        s.end();
      });
      s.on('error', () => undefined);
    });
    servers.push(target);
    await new Promise<void>((resolve) => target.listen(0, '127.0.0.1', resolve));
    return { port: (target.address() as { port: number }).port, seen };
  }

  /** The files of a proxy: its certificates, and what the operator publishes for a switch */
  function directory(files: SegmentTlsFiles, peers?: { plain?: string; until?: number }) {
    const dir = mkdtempSync(join(tmpdir(), 'segtls-'));
    dirs.push(dir);
    mkdirSync(join(dir, 'tls'));
    mkdirSync(join(dir, 'peers'));
    const write = (f: SegmentTlsFiles) => {
      writeFileSync(join(dir, 'tls', 'ca.crt'), f.ca);
      writeFileSync(join(dir, 'tls', 'tls.key'), f.key);
      writeFileSync(join(dir, 'tls', 'tls.crt'), f.cert);
    };
    write(files);
    if (peers?.plain !== undefined) writeFileSync(join(dir, 'peers', 'plain-peers'), peers.plain);
    if (peers?.until !== undefined) writeFileSync(join(dir, 'peers', 'accept-plain-until'), String(peers.until));
    return { dir, write };
  }

  /** `fb-agent segment-tls` with the environment of the sidecar; the ports it reports */
  function rust(dir: string, env: Record<string, string>): Promise<{ serverPort?: number; clientPort?: number }> {
    const child = spawn(agentBin!, ['segment-tls'], {
      env: { ...process.env, SEGMENT_TLS_DIR: join(dir, 'tls'), SEGMENT_TLS_PEERS_DIR: join(dir, 'peers'), SEGMENT_TLS_RELOAD_SECONDS: '1', ...env },
    });
    children.push(child);
    let out = '';
    child.stderr.on('data', (d) => (out += d.toString()));
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`fb-agent segment-tls did not start: ${out}`)), 10_000);
      child.stdout.on('data', (d) => {
        out += d.toString();
        const accepting = /accepting on [^\s:]+:(\d+),/.exec(out);
        const proxying = /client proxy on [^\s:]+:(\d+)/.exec(out);
        if ((!env.SERVER_LISTEN || accepting) && (!env.CLIENT_LISTEN || proxying)) {
          clearTimeout(timer);
          resolve({ serverPort: accepting ? Number(accepting[1]) : undefined, clientPort: proxying ? Number(proxying[1]) : undefined });
        }
      });
      child.once('exit', (code) => reject(new Error(`fb-agent segment-tls exited ${code}: ${out}`)));
    });
  }

  const peersOf = (peers?: { plain?: string; until?: number }): SegmentTlsPeers =>
    parsePeers(peers?.plain, peers?.until === undefined ? undefined : String(peers.until));

  /** A server side of the kind under test in front of `target`: its port */
  async function serverSide(files: SegmentTlsFiles, target: number, peers?: { plain?: string; until?: number }) {
    const d = directory(files, peers);
    if (server === 'Node') {
      const s = await startServer(() => files, 0, { host: '127.0.0.1', port: target }, '127.0.0.1', () => peersOf(peers));
      servers.push(s);
      return { port: (s.address() as { port: number }).port, write: d.write };
    }
    const started = await rust(d.dir, { SERVER_LISTEN: '127.0.0.1:0', SERVER_TARGET: `127.0.0.1:${target}` });
    return { port: started.serverPort!, write: d.write };
  }

  /** A client side of the kind under test: its port */
  async function clientSide(files: SegmentTlsFiles, peers?: { plain?: string; until?: number }) {
    const d = directory(files, peers);
    if (client === 'Node') {
      const c = await startClient(() => files, 0, '127.0.0.1', () => peersOf(peers));
      servers.push(c);
      return { port: (c.address() as { port: number }).port, write: d.write };
    }
    const started = await rust(d.dir, { CLIENT_LISTEN: '127.0.0.1:0' });
    return { port: started.clientPort!, write: d.write };
  }

  it('relays a request over mutual TLS, without the CONNECT line', async () => {
    if (!available) return;
    const files = certs('cluster db CA');
    const target = await segmentServer();
    const s = await serverSide(files, target.port);
    const c = await clientSide(files);
    expect(await text(c.port, `CONNECT 127.0.0.1 ${s.port}\nSIG1 1 2 3 PING\n`)).toBe('OK SIG1 1 2 3 PING\n');
    expect(await text(c.port, `CONNECT 127.0.0.1 ${s.port}\nLIST\n`)).toBe('OK LIST\n');
    await new Promise((r) => setTimeout(r, 100));
    expect(target.seen.map((b) => b.toString())).toEqual(['SIG1 1 2 3 PING\n', 'LIST\n']);
  });

  it('carries a large upload and a large download unchanged', { timeout: 30_000 }, async () => {
    if (!available) return;
    const files = certs('cluster db CA');
    const download = Buffer.alloc(3_000_000);
    for (let i = 0; i < download.length; i++) download[i] = (i * 31) & 0xff;
    const target = await segmentServer(download);
    const s = await serverSide(files, target.port);
    const c = await clientSide(files);
    // a download: the reply line, then three megabytes
    const got = await exchange(c.port, `CONNECT 127.0.0.1 ${s.port}\nFILE big.nbk\n`);
    expect(got.subarray(0, 16).toString()).toBe('OK FILE big.nbk\n');
    expect(got.subarray(16).equals(download)).toBe(true);
    // an upload: the body follows the request line on the same connection, then the client is done
    const upload = Buffer.alloc(2_500_000);
    for (let i = 0; i < upload.length; i++) upload[i] = (i * 17) & 0xff;
    const reply = await new Promise<Buffer>((resolve, reject) => {
      const socket = new Socket({ allowHalfOpen: true });
      const chunks: Buffer[] = [];
      socket.on('data', (d: Buffer) => chunks.push(d));
      socket.once('error', reject);
      socket.once('close', () => resolve(Buffer.concat(chunks)));
      socket.connect(c.port, '127.0.0.1', () => {
        socket.write(`CONNECT 127.0.0.1 ${s.port}\nSTORE up.nbk ${upload.length}\n`);
        socket.end(upload);
      });
    });
    expect(reply.subarray(0, 24).toString()).toBe(`OK STORE up.nbk ${upload.length}\n`);
    await new Promise((r) => setTimeout(r, 200));
    const stored = target.seen.at(-1)!;
    expect(stored.subarray(stored.indexOf(0x0a) + 1).equals(upload)).toBe(true);
  });

  it('refuses a client of another cluster, and a plain client once the switch is over', async () => {
    if (!available) return;
    const files = certs('cluster db CA');
    const target = await segmentServer();
    const s = await serverSide(files, target.port);
    // a client whose certificate another CA signed
    const stranger = await clientSide(certs('cluster other CA'));
    expect(await text(stranger.port, `CONNECT 127.0.0.1 ${s.port}\nPING\n`)).toBe('');
    // a plain client on the TLS port never gets the segment server's answer
    expect(await text(s.port, 'PING\n')).not.toMatch(/OK/);
    await new Promise((r) => setTimeout(r, 100));
    expect(target.seen).toEqual([]);
    // and a server of another cluster is not trusted either
    const other = await serverSide(certs('cluster other CA'), target.port);
    const c = await clientSide(files);
    expect(await text(c.port, `CONNECT 127.0.0.1 ${other.port}\nPING\n`)).toBe('');
    expect(target.seen).toEqual([]);
  });

  it('accepts plain connections while the operator allows them, beside TLS', async () => {
    if (!available) return;
    const files = certs('cluster db CA');
    const target = await segmentServer();
    const switching = await serverSide(files, target.port, { until: Date.now() + 60_000 });
    // an instance or a Job without the proxy yet: plain text, forwarded as it is
    expect(await text(switching.port, 'SIG1 1 2 3 PING\n')).toBe('OK SIG1 1 2 3 PING\n');
    const c = await clientSide(files);
    expect(await text(c.port, `CONNECT 127.0.0.1 ${switching.port}\nLIST\n`)).toBe('OK LIST\n');
    // the switch is over: TLS only
    const strict = await serverSide(files, target.port, { until: Date.now() - 1000 });
    expect(await text(strict.port, 'SIG1 1 2 3 PING\n')).toBe('');
    expect(await text(c.port, `CONNECT 127.0.0.1 ${strict.port}\nLIST\n`)).toBe('OK LIST\n');
  });

  it('connects in plain text to the instances the operator lists as plain, and only to those', async () => {
    if (!available) return;
    const files = certs('cluster db CA');
    const target = await segmentServer();
    const c = await clientSide(files, { plain: 'localhost\n../x\n' });
    // "localhost" is listed: plain, straight to the segment server
    expect(await text(c.port, `CONNECT localhost ${target.port}\nPING\n`)).toBe('OK PING\n');
    // 127.0.0.1 is not: TLS, which a plain segment server does not answer with OK
    expect(await text(c.port, `CONNECT 127.0.0.1 ${target.port}\nPING\n`)).not.toMatch(/^OK PING/);
  });

  it('answers a malformed first line without connecting anywhere', async () => {
    if (!available) return;
    const c = await clientSide(certs('cluster db CA'));
    for (const line of ['PING', 'CONNECT host', 'CONNECT host port', 'CONNECT a b 1']) {
      expect(await text(c.port, `${line}\n`), line).toBe('ERR expected CONNECT <host> <port>\n');
    }
  });
});

describe.skipIf(!agentBin)('fb-agent segment-tls', () => {
  const children: ChildProcess[] = [];
  const servers: Server[] = [];
  const dirs: string[] = [];
  afterEach(() => {
    children.splice(0).forEach((c) => c.kill('SIGKILL'));
    servers.splice(0).forEach((s) => s.close());
    dirs.splice(0).forEach((d) => rmSync(d, { recursive: true, force: true }));
  });

  it('reads renewed certificates and a changed peers file without a restart', { timeout: 30_000 }, async () => {
    const target = createServer((s) => s.on('data', () => s.end('OK\n')));
    servers.push(target);
    await new Promise<void>((resolve) => target.listen(0, '127.0.0.1', resolve));
    const targetPort = (target.address() as { port: number }).port;
    const dir = mkdtempSync(join(tmpdir(), 'segtls-'));
    dirs.push(dir);
    for (const d of ['tls', 'peers']) mkdirSync(join(dir, d));
    const write = (f: SegmentTlsFiles) => {
      writeFileSync(join(dir, 'tls', 'ca.crt'), f.ca);
      writeFileSync(join(dir, 'tls', 'tls.key'), f.key);
      writeFileSync(join(dir, 'tls', 'tls.crt'), f.cert);
    };
    const first = certs('cluster db CA');
    write(first);
    const child = spawn(agentBin!, ['segment-tls'], {
      env: {
        ...process.env,
        SEGMENT_TLS_DIR: join(dir, 'tls'),
        SEGMENT_TLS_PEERS_DIR: join(dir, 'peers'),
        SEGMENT_TLS_RELOAD_SECONDS: '1',
        SERVER_LISTEN: '127.0.0.1:0',
        SERVER_TARGET: `127.0.0.1:${targetPort}`,
        CLIENT_LISTEN: '127.0.0.1:0',
      },
    });
    children.push(child);
    let out = '';
    child.stdout.on('data', (d) => (out += d.toString()));
    for (let i = 0; i < 100 && !/client proxy on/.test(out); i++) await new Promise((r) => setTimeout(r, 50));
    const serverPort = Number(/accepting on [^\s:]+:(\d+),/.exec(out)![1]);
    const clientPort = Number(/client proxy on [^\s:]+:(\d+)/.exec(out)![1]);
    const through = (port: number) => text(port, `CONNECT 127.0.0.1 ${serverPort}\nPING\n`);
    // a Node client side with the first certificates, and one with those of a renewed CA
    const oldClient = await startClient(() => first, 0);
    servers.push(oldClient);
    const renewed = certs('cluster db CA, renewed');
    const newClient = await startClient(() => renewed, 0);
    servers.push(newClient);
    const port = (s: Server) => (s.address() as { port: number }).port;
    expect(await through(port(oldClient))).toBe('OK\n');
    expect(await through(port(newClient))).toBe('');
    expect(await through(clientPort)).toBe('OK\n');
    // the Secret is renewed: the proxy reads the files again
    await new Promise((r) => setTimeout(r, 1100));
    write(renewed);
    for (let i = 0; i < 60 && !/certificates were read again/.test(out); i++) await new Promise((r) => setTimeout(r, 100));
    expect(out).toMatch(/segment TLS: the certificates were read again/);
    expect(await through(port(newClient))).toBe('OK\n');
    expect(await through(port(oldClient))).toBe('');
    expect(await through(clientPort)).toBe('OK\n');
    // plain connections are accepted once the operator publishes a time, within five seconds
    expect(await text(serverPort, 'PING\n')).toBe('');
    writeFileSync(join(dir, 'peers', 'accept-plain-until'), String(Date.now() + 60_000));
    let plain = '';
    for (let i = 0; i < 80 && plain !== 'OK\n'; i++) {
      await new Promise((r) => setTimeout(r, 100));
      plain = await text(serverPort, 'PING\n');
    }
    expect(plain).toBe('OK\n');
  });

  it('stops once the Firebird server has stopped, and refuses to start without anything to do', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'segtls-'));
    dirs.push(dir);
    mkdirSync(join(dir, 'tls'));
    const files = certs('cluster db CA');
    writeFileSync(join(dir, 'tls', 'ca.crt'), files.ca);
    writeFileSync(join(dir, 'tls', 'tls.key'), files.key);
    writeFileSync(join(dir, 'tls', 'tls.crt'), files.cert);
    const run = (env: Record<string, string>) => {
      const child = spawn(agentBin!, ['segment-tls'], { env: { ...process.env, SEGMENT_TLS_DIR: join(dir, 'tls'), ...env } });
      children.push(child);
      let out = '';
      child.stdout.on('data', (d) => (out += d.toString()));
      child.stderr.on('data', (d) => (out += d.toString()));
      // 'close': once its output has been read too
      const exited = new Promise<number | null>((resolve) => child.once('close', resolve));
      return { child, out: () => out, exited };
    };
    const idle = run({});
    expect(await idle.exited).toBe(1);
    expect(idle.out()).toMatch(/neither SERVER_LISTEN\/SERVER_TARGET nor CLIENT_LISTEN is set/);
    // a "Firebird server" that stops after the signal: the proxy goes on until then
    const firebird = createServer();
    await new Promise<void>((resolve) => firebird.listen(0, '127.0.0.1', resolve));
    const proxy = run({ CLIENT_LISTEN: '127.0.0.1:0', FIREBIRD_PORT: String((firebird.address() as { port: number }).port) });
    for (let i = 0; i < 100 && !/client proxy on/.test(proxy.out()); i++) await new Promise((r) => setTimeout(r, 50));
    proxy.child.kill('SIGTERM');
    for (let i = 0; i < 60 && !/stopping once the Firebird server has stopped/.test(proxy.out()); i++) await new Promise((r) => setTimeout(r, 50));
    expect(proxy.out()).toMatch(/segment TLS: stopping once the Firebird server has stopped/);
    expect(proxy.child.exitCode).toBeNull();
    await new Promise<void>((resolve) => firebird.close(() => resolve()));
    expect(await proxy.exited).toBe(0);
    expect(proxy.out()).toMatch(/segment TLS: the Firebird server has stopped, exiting/);
  });
});
