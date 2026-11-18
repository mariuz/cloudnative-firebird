import { describe, it, expect, afterEach } from 'vitest';
import { spawn, ChildProcess } from 'child_process';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { createServer } from 'net';
import { createTlsCertificates } from '../src/utils/certificates';
import { agentBin } from './helpers/implementations';

/**
 * Segment TLS between the agent's processes (spec.segmentTLS, agent/fb-agent/src/tls.rs): the
 * segment server accepts mutual TLS on its port (SEGMENT_TLS_LISTEN) and plain text on localhost
 * (SEGMENT_LISTEN), and a client (`fb-agent request`, as the puller, the replication init and the
 * Jobs' programs connect) makes TLS itself with SEGMENT_TLS=true. Since v0.171.0 there is no proxy
 * between them.
 *
 * Needs the fb-agent binary (FB_AGENT_BIN, or a build in agent/target); the "Rust agent" CI job
 * runs this file with it.
 */

/** The cluster's certificates in a directory, as the Secret is mounted (SEGMENT_TLS_DIR) */
const certificates = (root: string, name: string, caName: string): string => {
  const c = createTlsCertificates({ caName, dnsNames: ['*.db-headless'], clientAuth: true });
  const dir = join(root, name);
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, 'ca.crt'), c.caCert);
  writeFileSync(join(dir, 'tls.key'), c.tlsKey);
  // last: the files are read again when tls.crt changes
  writeFileSync(join(dir, 'tls.crt'), c.tlsCert);
  return dir;
};

/** What the operator publishes while a cluster switches (SEGMENT_TLS_PEERS_DIR) */
const peers = (root: string, name: string, data: { plainPeers?: string; acceptPlainUntil?: number }): string => {
  const dir = join(root, name);
  mkdirSync(dir, { recursive: true });
  if (data.plainPeers !== undefined) writeFileSync(join(dir, 'plain-peers'), data.plainPeers);
  if (data.acceptPlainUntil !== undefined) writeFileSync(join(dir, 'accept-plain-until'), String(data.acceptPlainUntil));
  return dir;
};

const freePort = (): Promise<number> =>
  new Promise((resolve) => {
    const probe = createServer();
    probe.listen(0, '127.0.0.1', () => {
      const { port } = probe.address() as { port: number };
      probe.close(() => resolve(port));
    });
  });

describe.runIf(agentBin)('segment TLS between the agent\'s segment server and its clients', { timeout: 30_000 }, () => {
  const children: ChildProcess[] = [];
  const dirs: string[] = [];
  afterEach(() => {
    children.splice(0).forEach((c) => c.kill('SIGKILL'));
    dirs.splice(0).forEach((d) => rmSync(d, { recursive: true, force: true }));
  });
  const root = () => {
    const dir = mkdtempSync(join(tmpdir(), 'segment-tls-'));
    dirs.push(dir);
    return dir;
  };

  /** A files-only segment server: TLS on `tls`, plain on localhost `plain` */
  async function server(at: string, tlsDir: string, peersDir?: string) {
    const data = join(at, 'data');
    mkdirSync(data, { recursive: true });
    writeFileSync(join(data, 'old.fdb'), 'x');
    const [plain, tls] = [await freePort(), await freePort()];
    const child = spawn(agentBin!, ['segment-server'], {
      env: {
        PATH: process.env.PATH,
        FILES_ONLY: 'true',
        DATABASE_PATH: join(data, 'mydb.fdb'),
        ISC_PASSWORD: 'tok',
        SEGMENT_LISTEN: `127.0.0.1:${plain}`,
        SEGMENT_TLS_LISTEN: `127.0.0.1:${tls}`,
        SEGMENT_TLS_DIR: tlsDir,
        ...(peersDir ? { SEGMENT_TLS_PEERS_DIR: peersDir } : {}),
      },
    });
    children.push(child);
    let out = '';
    child.stdout!.on('data', (d) => (out += d));
    child.stderr!.on('data', (d) => (out += d));
    for (let i = 0; i < 100 && !out.includes('accepts mutual TLS itself'); i++) await new Promise((r) => setTimeout(r, 50));
    expect(out).toContain(`segment TLS: the segment server accepts mutual TLS itself on 127.0.0.1:${tls}`);
    return { plain, tls };
  }

  /** `fb-agent request <host> <words>`: what it prints (stdout and stderr), and its exit status */
  const request = (host: string, port: number, env: Record<string, string>, ...words: string[]) =>
    new Promise<{ out: string; code: number | null }>((resolve) => {
      const c = spawn(agentBin!, ['request', host, ...words], {
        env: { PATH: process.env.PATH, ISC_PASSWORD: 'tok', SEGMENT_PORT: String(port), ...env },
      });
      let out = '';
      c.stdout.on('data', (d) => (out += d));
      c.stderr.on('data', (d) => (out += d));
      c.on('close', (code) => resolve({ out: out.trim(), code }));
    });

  it('answers a signed request over mutual TLS, plain text on localhost only', async () => {
    const at = root();
    const files = certificates(at, 'tls', 'cluster db CA');
    const { plain, tls } = await server(at, files);
    const client = { SEGMENT_TLS: 'true', SEGMENT_TLS_DIR: files };
    expect(await request('127.0.0.1', tls, client, 'EXISTS', 'old.fdb')).toEqual({ out: 'OK yes', code: 0 });
    expect((await request('127.0.0.1', tls, client, 'EXISTS', 'new.fdb')).out).toBe('OK no');
    // the pod's own clients (the Lease holder) on the plain listener
    expect((await request('127.0.0.1', plain, {}, 'EXISTS', 'old.fdb')).out).toBe('OK yes');
    // plain text on the TLS port: refused (no switch in progress)
    expect((await request('127.0.0.1', tls, {}, 'EXISTS', 'old.fdb')).out).not.toMatch(/^OK/);
    // TLS to the plain listener: no handshake, no reply
    expect((await request('127.0.0.1', plain, client, 'EXISTS', 'old.fdb')).code).not.toBe(0);
  });

  it('carries a large upload and a large download unchanged (the backup files)', async () => {
    const at = root();
    const files = certificates(at, 'tls', 'cluster db CA');
    const { tls } = await server(at, files);
    const work = join(at, 'work');
    mkdirSync(work);
    const big = Buffer.alloc(5 * 1024 * 1024 + 7);
    for (let i = 0; i < big.length; i++) big[i] = (i * 31 + 7) & 0xff;
    writeFileSync(join(work, 'up.nbk'), big);
    const backupFile = (...args: string[]) =>
      new Promise<{ out: string; code: number | null }>((resolve) => {
        const c = spawn(agentBin!, ['backup-file', ...args], {
          env: { PATH: process.env.PATH, ISC_PASSWORD: 'tok', FIREBIRD_HOST: '127.0.0.1', SEGMENT_PORT: String(tls), SEGMENT_TLS: 'true', SEGMENT_TLS_DIR: files },
        });
        let out = '';
        c.stdout.on('data', (d) => (out += d));
        c.stderr.on('data', (d) => (out += d));
        c.on('close', (code) => resolve({ out: out.trim(), code }));
      });
    expect(await backupFile('put', join(work, 'up.nbk'), 'up.nbk')).toEqual({ out: `copied ${join(work, 'up.nbk')} (${big.length} bytes) to up.nbk on 127.0.0.1`, code: 0 });
    expect(readFileSync(join(at, 'data', 'up.nbk')).equals(big)).toBe(true);
    expect(await backupFile('get', 'up.nbk', join(work, 'down.nbk'))).toEqual({ out: `copied up.nbk (${big.length} bytes) from 127.0.0.1`, code: 0 });
    expect(readFileSync(join(work, 'down.nbk')).equals(big)).toBe(true);
  });

  it('accepts plain connections on its TLS port only until the operator\'s accept-plain-until', async () => {
    const at = root();
    const files = certificates(at, 'tls', 'cluster db CA');
    const switching = peers(at, 'peers', { plainPeers: '', acceptPlainUntil: Date.now() + 60_000 });
    const { tls } = await server(at, files, switching);
    expect((await request('127.0.0.1', tls, {}, 'EXISTS', 'old.fdb')).out).toBe('OK yes');
    // the grace period is over: read again for every connection
    writeFileSync(join(switching, 'accept-plain-until'), String(Date.now() - 1));
    expect((await request('127.0.0.1', tls, {}, 'EXISTS', 'old.fdb')).out).not.toMatch(/^OK/);
    // TLS all along
    expect((await request('127.0.0.1', tls, { SEGMENT_TLS: 'true', SEGMENT_TLS_DIR: files }, 'EXISTS', 'old.fdb')).out).toBe('OK yes');
  });

  it('connects in plain text to the instances the operator lists as plain, and only to those', async () => {
    const at = root();
    const files = certificates(at, 'tls', 'cluster db CA');
    const { plain } = await server(at, files);
    // "localhost" stands for an instance not restarted with TLS yet
    const listed = peers(at, 'listed', { plainPeers: 'localhost\n' });
    const env = { SEGMENT_TLS: 'true', SEGMENT_TLS_DIR: files, SEGMENT_TLS_PEERS_DIR: listed };
    expect((await request('localhost', plain, env, 'EXISTS', 'old.fdb')).out).toBe('OK yes');
    // not listed: TLS, which a plain server does not answer
    const others = peers(at, 'others', { plainPeers: 'db-0\n' });
    const tlsOnly = await request('localhost', plain, { ...env, SEGMENT_TLS_PEERS_DIR: others }, 'EXISTS', 'old.fdb');
    expect(tlsOnly.code).not.toBe(0);
    expect(tlsOnly.out).not.toMatch(/^OK/);
  });

  it('refuses a peer of another cluster, both ways, and never falls back to plain text', async () => {
    const at = root();
    const files = certificates(at, 'tls', 'cluster db CA');
    const stranger = certificates(at, 'stranger', 'cluster other CA');
    const { tls } = await server(at, files);
    const refused = await request('127.0.0.1', tls, { SEGMENT_TLS: 'true', SEGMENT_TLS_DIR: stranger }, 'EXISTS', 'old.fdb');
    expect(refused.code).not.toBe(0);
    expect(refused.out).toContain('is not an instance of this cluster (the TLS handshake failed)');
    // without its certificates a client sends nothing
    const none = await request('127.0.0.1', tls, { SEGMENT_TLS: 'true', SEGMENT_TLS_DIR: '' }, 'EXISTS', 'old.fdb');
    expect(none.code).not.toBe(0);
    expect(none.out).toContain('segment TLS: no certificates in SEGMENT_TLS_DIR');
  });

  it('takes renewed certificates without a restart', async () => {
    const at = root();
    const files = certificates(at, 'tls', 'cluster db CA');
    const renewed = certificates(at, 'renewed', 'cluster db CA, renewed');
    const { tls } = await server(at, files);
    const client = { SEGMENT_TLS: 'true', SEGMENT_TLS_DIR: renewed };
    expect((await request('127.0.0.1', tls, client, 'EXISTS', 'old.fdb')).code).not.toBe(0);
    // the Secret's new files reach the mounted directory
    await new Promise((r) => setTimeout(r, 20));
    for (const name of ['ca.crt', 'tls.key', 'tls.crt']) writeFileSync(join(files, name), readFileSync(join(renewed, name)));
    expect((await request('127.0.0.1', tls, client, 'EXISTS', 'old.fdb')).out).toBe('OK yes');
  });
});
