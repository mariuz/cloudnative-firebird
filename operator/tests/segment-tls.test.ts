import { describe, it, expect, afterEach } from 'vitest';
import { spawn, spawnSync, ChildProcess } from 'child_process';
import { mkdtempSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { REPLICATION_SCRIPTS } from '../src/utils/replication';
import { createServer, Server, Socket } from 'net';
import { createTlsCertificates } from '../src/utils/certificates';
import { parsePeers, peerName, SegmentTlsFiles, SegmentTlsPeers, startClient, startServer } from '../src/segment-tls';

const certs = (caName: string): SegmentTlsFiles => {
  const c = createTlsCertificates({ caName, dnsNames: ['*.db-headless'], clientAuth: true });
  return { ca: c.caCert, cert: c.tlsCert, key: c.tlsKey };
};

const listen = (server: Server): Promise<number> =>
  new Promise((resolve) => server.listen(0, '127.0.0.1', () => resolve((server.address() as { port: number }).port)));

/** One exchange as a Perl client does it: lines out, everything until the server closes back */
const exchange = (port: number, text: string): Promise<string> =>
  new Promise((resolve, reject) => {
    const socket = new Socket();
    let data = '';
    socket.on('data', (c) => (data += c.toString()));
    socket.once('error', reject);
    socket.once('close', () => resolve(data));
    socket.connect(port, '127.0.0.1', () => socket.write(text));
  });

describe('segment TLS proxy', () => {
  const servers: Server[] = [];
  afterEach(() => servers.splice(0).forEach((s) => s.close()));

  /** A segment server stand-in: answers "OK <first line>" (and what followed it) and closes */
  async function segmentServer(): Promise<{ port: number; seen: string[] }> {
    const seen: string[] = [];
    const server = createServer((s) => {
      let data = '';
      s.on('data', (c) => {
        data += c.toString();
        if (data.includes('\n')) {
          seen.push(data);
          s.end(`OK ${data.split('\n')[0]}\n`);
        }
      });
    });
    servers.push(server);
    return { port: await listen(server), seen };
  }

  async function pair(serverFiles: SegmentTlsFiles, clientFiles: SegmentTlsFiles) {
    const target = await segmentServer();
    const tlsServer = await startServer(() => serverFiles, 0, { host: '127.0.0.1', port: target.port }, '127.0.0.1');
    servers.push(tlsServer);
    const client = await startClient(() => clientFiles, 0);
    servers.push(client);
    return {
      target,
      tlsPort: (tlsServer.address() as { port: number }).port,
      clientPort: (client.address() as { port: number }).port,
    };
  }

  it('relays a request over mutual TLS between the client and server sides', async () => {
    const files = certs('cluster db CA');
    const p = await pair(files, files);
    const reply = await exchange(p.clientPort, `CONNECT 127.0.0.1 ${p.tlsPort}\nSIG1 1 2 3 PING\n`);
    expect(reply).toBe('OK SIG1 1 2 3 PING\n');
    // the segment server saw the request as sent, without the CONNECT line
    expect(p.target.seen).toEqual(['SIG1 1 2 3 PING\n']);
  });

  it('carries data sent after the request line (STORE) and a second request', async () => {
    const files = certs('cluster db CA');
    const p = await pair(files, files);
    expect(await exchange(p.clientPort, `CONNECT 127.0.0.1 ${p.tlsPort}\nSTORE x.nbk 5\nhello`)).toBe('OK STORE x.nbk 5\n');
    expect(p.target.seen[0]).toMatch(/^STORE x\.nbk 5\n/);
    expect(await exchange(p.clientPort, `CONNECT 127.0.0.1 ${p.tlsPort}\nLIST\n`)).toBe('OK LIST\n');
  });

  it('accepts plain connections only while the operator allows them (switching segment TLS)', async () => {
    const files = certs('cluster db CA');
    const target = await segmentServer();
    let peers: SegmentTlsPeers = { plain: new Set(), acceptPlainUntil: 2_000 };
    let clock = 1_000;
    const tlsServer = await startServer(() => files, 0, { host: '127.0.0.1', port: target.port }, '127.0.0.1', () => peers, () => clock);
    servers.push(tlsServer);
    const tlsPort = (tlsServer.address() as { port: number }).port;
    // an instance or Job without the proxy yet: plain text, forwarded as it is
    expect(await exchange(tlsPort, 'SIG1 1 2 3 PING\n')).toBe('OK SIG1 1 2 3 PING\n');
    // TLS on the same port meanwhile
    const client = await startClient(() => files, 0);
    servers.push(client);
    const clientPort = (client.address() as { port: number }).port;
    expect(await exchange(clientPort, `CONNECT 127.0.0.1 ${tlsPort}\nLIST\n`)).toBe('OK LIST\n');
    // the switch is over: TLS only
    clock = 2_000;
    expect(await exchange(tlsPort, 'SIG1 1 2 3 PING\n').catch(() => '')).toBe('');
    peers = { plain: new Set(), acceptPlainUntil: 0 };
    expect(await exchange(clientPort, `CONNECT 127.0.0.1 ${tlsPort}\nLIST\n`)).toBe('OK LIST\n');
    expect(target.seen).toEqual(['SIG1 1 2 3 PING\n', 'LIST\n', 'LIST\n']);
    // every connection was closed on both ends
    await new Promise((r) => setTimeout(r, 50));
    const open = await new Promise<number>((r) => tlsServer.getConnections((_e, n) => r(n)));
    expect(open).toBe(0);
  });

  it('connects in plain text to the instances the operator lists as plain, and only to those', async () => {
    const files = certs('cluster db CA');
    const target = await segmentServer();
    const peers: SegmentTlsPeers = { plain: new Set(['localhost']), acceptPlainUntil: 0 };
    const client = await startClient(() => files, 0, '127.0.0.1', () => peers);
    servers.push(client);
    const clientPort = (client.address() as { port: number }).port;
    // "localhost" is listed: plain, straight to the segment server
    expect(await exchange(clientPort, `CONNECT localhost ${target.port}\nPING\n`)).toBe('OK PING\n');
    // 127.0.0.1 is not: TLS, which a plain segment server does not answer with OK
    expect(await exchange(clientPort, `CONNECT 127.0.0.1 ${target.port}\nPING\n`).catch(() => '')).not.toMatch(/^OK PING/);
  });

  it('reads the peers the operator publishes, ignoring anything that is not a pod name', () => {
    expect(parsePeers('db-0\ndb-2\n', '1700000000000')).toEqual({ plain: new Set(['db-0', 'db-2']), acceptPlainUntil: 1700000000000 });
    expect(parsePeers(undefined, undefined)).toEqual({ plain: new Set(), acceptPlainUntil: 0 });
    expect(parsePeers('db-0 ../x DB-1', 'soon')).toEqual({ plain: new Set(['db-0']), acceptPlainUntil: 0 });
    expect(peerName('db-1.db-headless.prod.svc')).toBe('db-1');
  });

  it('refuses plain connections and peers of another cluster', async () => {
    const files = certs('cluster db CA');
    const p = await pair(files, files);
    // a plain client on the TLS port gets a TLS alert at most, never the segment server's answer
    expect(await exchange(p.tlsPort, 'PING\n').catch(() => '')).not.toMatch(/OK/);
    expect(p.target.seen).toEqual([]);
    // a client whose certificate another CA signed
    const other = certs('cluster other CA');
    const stranger = await startClient(() => other, 0);
    servers.push(stranger);
    const port = (stranger.address() as { port: number }).port;
    expect(await exchange(port, `CONNECT 127.0.0.1 ${p.tlsPort}\nPING\n`).catch(() => '')).toBe('');
    expect(p.target.seen).toEqual([]);
  });

  it('answers a malformed first line without connecting anywhere', async () => {
    const files = certs('cluster db CA');
    const client = await startClient(() => files, 0);
    servers.push(client);
    expect(await exchange((client.address() as { port: number }).port, 'PING\n')).toBe('ERR expected CONNECT <host> <port>\n');
  });
});

const hasPerl = spawnSync('perl', ['-v']).status === 0;

/** A port no other server listens on */
const freePort = async (): Promise<number> => {
  const probe = createServer();
  const port = await listen(probe);
  await new Promise((resolve) => probe.close(resolve));
  return port;
};

describe('segment TLS with the Perl segment server and clients', () => {
  const children: ChildProcess[] = [];
  const servers: Server[] = [];
  afterEach(() => {
    children.splice(0).forEach((c) => c.kill());
    servers.splice(0).forEach((s) => s.close());
  });

  it('serves signed requests through both proxies, the server on localhost only', { timeout: 20_000 }, async () => {
    if (!hasPerl) return;
    const dir = mkdtempSync(join(tmpdir(), 'fb-segment-tls-'));
    for (const name of ['segment-server.pl', 'segment-request.pl']) writeFileSync(join(dir, name), REPLICATION_SCRIPTS[name]);
    const data = mkdtempSync(join(tmpdir(), 'fb-data-'));
    writeFileSync(join(data, 'old.fdb'), 'x');
    const [inner, outer] = [await freePort(), await freePort()];
    const server = spawn('perl', [join(dir, 'segment-server.pl')], {
      env: {
        PATH: process.env.PATH,
        FILES_ONLY: 'true',
        DATABASE_PATH: join(data, 'mydb.fdb'),
        ISC_PASSWORD: 'tok',
        SEGMENT_PORT: String(outer),
        SEGMENT_LISTEN: `127.0.0.1:${inner}`,
      },
    });
    children.push(server);
    const files = certs('cluster db CA');
    servers.push(await startServer(() => files, outer, { host: '127.0.0.1', port: inner }, '127.0.0.1'));
    const client = await startClient(() => files, 0);
    servers.push(client);
    const proxy = `127.0.0.1:${(client.address() as { port: number }).port}`;
    const request = (...args: string[]) =>
      new Promise<string>((resolve) => {
        const c = spawn('perl', [join(dir, 'segment-request.pl'), '127.0.0.1', ...args], {
          env: { PATH: process.env.PATH, ISC_PASSWORD: 'tok', SEGMENT_PORT: String(outer), SEGMENT_PROXY: proxy },
        });
        let out = '';
        c.stdout.on('data', (d) => (out += d));
        c.stderr.on('data', (d) => (out += d));
        c.on('close', () => resolve(out.trim()));
      });
    let reply = '';
    for (let i = 0; i < 50 && !reply.startsWith('OK'); i++) {
      reply = await request('EXISTS', 'old.fdb');
      if (!reply.startsWith('OK')) await new Promise((r) => setTimeout(r, 100));
    }
    // signed (the PING probe and the request both went through the proxies)
    expect(reply).toBe('OK yes');
    expect(await request('EXISTS', 'new.fdb')).toBe('OK no');
    // only the TLS proxy listens on the segment port: the server itself is on localhost:inner
    expect(await exchange(outer, 'tok EXISTS old.fdb\n').catch(() => '')).not.toMatch(/OK/);
  });
});
