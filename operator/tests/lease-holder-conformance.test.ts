import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { ChildProcess, spawn } from 'child_process';
import { existsSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'fs';
import { createServer, IncomingMessage, Server as HttpsServer, ServerResponse } from 'https';
import { Server, Socket } from 'net';
import { tmpdir } from 'os';
import { join } from 'path';
import { kubernetesLeaseApi } from '../src/lease-holder';
import { createTlsCertificates } from '../src/utils/certificates';
import { fakeSegmentServer } from './helpers/segment-auth';

/**
 * The primary Lease holder of the Rust agent (`fb-agent lease-holder`, agent/fb-agent/src/lease.rs)
 * as a process, against a Lease API and a segment server played here: what it asks them, and
 * that its renewal is the one the operator's lease-holder.ts sends. Its decisions are the unit
 * tests of lease.rs, case by case those of lease-holder.ts (tests/primary-lease.test.ts).
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

const PATH = '/apis/coordination.k8s.io/v1/namespaces/prod/leases/db-lease';

/** a Lease as the API serves it and a renewal writes it back */
interface LeaseObject {
  apiVersion?: string;
  kind?: string;
  metadata: { resourceVersion: string; [key: string]: unknown };
  spec: { holderIdentity?: string; leaseDurationSeconds?: number; renewTime?: string; [key: string]: unknown };
}

describe.skipIf(!agentBin)('the lease holder of the agent (fb-agent lease-holder)', () => {
  let dir: string;
  let api: HttpsServer;
  let segments: Server;
  let child: ChildProcess | undefined;
  let out = '';
  /** the Lease the API serves; undefined: 404 */
  let lease: LeaseObject | undefined;
  /** what the API does with a request: answer, refuse a renewal, or keep silent */
  let apiMode: 'answer' | 'conflict' | 'silent' | 'chunked' = 'answer';
  let calls: Array<{ method: string; url: string; token: string; body?: LeaseObject; agent: string }> = [];
  let database = 'OK online';
  let asked: string[] = [];

  const waitFor = async (check: () => boolean, what: string, ms = 12_000) => {
    const end = Date.now() + ms;
    while (!check()) {
      if (Date.now() > end) throw new Error(`timed out waiting for ${what}; output: ${out}; segment server: ${asked.join(' | ')}; API: ${calls.map((c) => c.method).join(' ')}`);
      await new Promise((r) => setTimeout(r, 50));
    }
  };
  const environment = (extra: Record<string, string> = {}) => ({
    PATH: process.env.PATH ?? '',
    POD_NAME: 'db-0',
    POD_NAMESPACE: 'prod',
    LEASE_NAME: 'db-lease',
    // renewed every second, fenced after 2 s without a renewal
    LEASE_DURATION_SECONDS: '3',
    PRIMARY_FILE: join(dir, 'primary'),
    REPLICATION_DIR: join(dir, 'repl'),
    TOKEN_DIR: join(dir, 'token'),
    KUBERNETES_SERVICE_HOST: 'localhost',
    KUBERNETES_SERVICE_PORT: String((api.address() as { port: number }).port),
    SEGMENT_SERVER: `127.0.0.1:${(segments.address() as { port: number }).port}`,
    ISC_PASSWORD: 'tok',
    // nothing listens there: the pod's "Firebird server" has stopped
    FIREBIRD_PORT: '1',
    ...extra,
  });
  const start = (extra: Record<string, string> = {}) => {
    child = spawn(agentBin!, ['lease-holder'], { env: environment(extra) });
    child.stdout!.on('data', (d) => (out += d.toString()));
    child.stderr!.on('data', (d) => (out += d.toString()));
  };

  beforeEach(async () => {
    dir = mkdtempSync(join(tmpdir(), 'lease-holder-'));
    mkdirSync(join(dir, 'token'));
    mkdirSync(join(dir, 'repl'));
    writeFileSync(join(dir, 'primary'), 'db-0.db-headless\n');
    writeFileSync(join(dir, 'token', 'token'), 'first-token\n');
    const certificates = createTlsCertificates({ caName: 'test API CA', dnsNames: ['localhost'] });
    writeFileSync(join(dir, 'token', 'ca.crt'), certificates.caCert);
    out = '';
    calls = [];
    asked = [];
    apiMode = 'answer';
    database = 'OK online';
    lease = {
      apiVersion: 'coordination.k8s.io/v1',
      kind: 'Lease',
      metadata: { name: 'db-lease', namespace: 'prod', resourceVersion: '41', labels: { cluster: 'db' }, ownerReferences: [{ uid: 'u-1' }] },
      spec: { holderIdentity: 'db-0', leaseDurationSeconds: 15, leaseTransitions: 2 },
    };
    // this test's own records: a request of the previous test's holder, still finishing, lands
    // in that test's arrays
    const seen = calls;
    const requests = () => asked;
    api = createServer({ cert: certificates.tlsCert, key: certificates.tlsKey }, (request: IncomingMessage, response: ServerResponse) => {
      const chunks: Buffer[] = [];
      request.on('data', (d: Buffer) => chunks.push(d));
      request.on('end', () => {
        const text = Buffer.concat(chunks).toString();
        seen.push({
          method: request.method ?? '',
          url: request.url ?? '',
          token: request.headers.authorization ?? '',
          agent: request.headers['user-agent'] ?? '',
          ...(text ? { body: JSON.parse(text) } : {}),
        });
        if (apiMode === 'silent') return;
        const send = (status: number, body: object) => {
          const json = JSON.stringify(body);
          // chunked: no Content-Length, written in pieces
          if (apiMode === 'chunked') {
            response.writeHead(status, { 'Content-Type': 'application/json' });
            response.write(json.slice(0, 10));
            response.end(json.slice(10));
          } else {
            response.writeHead(status, { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(json) });
            response.end(json);
          }
        };
        if (request.url !== PATH || !lease) return send(404, { kind: 'Status', message: 'leases "db-lease" not found' });
        if (request.method === 'GET') return send(200, lease);
        if (apiMode === 'conflict') return send(409, { kind: 'Status', message: 'the object has been modified' });
        // a replace with the version that was read
        const body = JSON.parse(text);
        if (body.metadata.resourceVersion !== lease.metadata.resourceVersion) return send(409, { kind: 'Status', message: 'the object has been modified' });
        lease = { ...body, metadata: { ...body.metadata, resourceVersion: String(Number(lease.metadata.resourceVersion) + 1) } };
        send(200, lease);
      });
    });
    await new Promise<void>((r) => api.listen(0, '127.0.0.1', r));
    segments = fakeSegmentServer((request: string, sock: Socket) => {
      requests().push(request);
      if (request === 'STATE') return void sock.end(`${database}\n`);
      if (request.startsWith('FENCE')) {
        database = 'OK fenced lease';
        return void sock.end('OK\n');
      }
      if (request === 'REJOIN') {
        database = 'OK online';
        return void sock.end('OK\n');
      }
      sock.end('ERR bad request\n');
    }, 'tok');
    await new Promise<void>((r) => segments.listen(0, '127.0.0.1', r));
  });
  afterEach(async () => {
    // gone before the next test's servers start
    if (child && child.exitCode === null && child.signalCode === null) {
      const gone = new Promise((resolve) => child!.once('close', resolve));
      child.kill('SIGKILL');
      await gone;
    }
    child = undefined;
    api.closeAllConnections();
    api.close();
    segments.close();
    rmSync(dir, { recursive: true, force: true });
  });

  it('renews the Lease as the operator\'s holder does, with the token of the moment', async () => {
    start();
    await waitFor(() => calls.filter((c) => c.method === 'PUT').length >= 2, 'two renewals');
    expect(out).toContain('lease holder: db-0 watching Lease prod/db-lease (duration 3s, renew deadline 2s)\n');
    expect(out).toContain('lease holder: holding the Lease as the primary, renewed every 1s\n');
    // logged once, however many renewals
    expect(out.match(/holding the Lease/g)).toHaveLength(1);
    const [read, renewal] = [calls.find((c) => c.method === 'GET')!, calls.find((c) => c.method === 'PUT')!];
    expect(read.url).toBe(PATH);
    expect(renewal.url).toBe(PATH);
    expect(read.token).toBe('Bearer first-token');
    expect(read.agent).toMatch(/^fb-agent\/\d+\.\d+\.\d+$/);
    // the Lease that was read, whole, with the renewal
    expect(renewal.body.metadata).toEqual({ name: 'db-lease', namespace: 'prod', resourceVersion: '41', labels: { cluster: 'db' }, ownerReferences: [{ uid: 'u-1' }] });
    expect(renewal.body.spec).toEqual({ holderIdentity: 'db-0', leaseDurationSeconds: 3, leaseTransitions: 2, renewTime: renewal.body.spec.renewTime });
    expect(renewal.body.spec.renewTime).toMatch(/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z$/);
    expect(Math.abs(Date.parse(renewal.body.spec.renewTime) - Date.now())).toBeLessThan(15_000);
    // each renewal carries the version of its own read
    const renewals = calls.filter((c) => c.method === 'PUT');
    expect(Number(renewals[1].body.metadata.resourceVersion)).toBe(Number(renewals[0].body.metadata.resourceVersion) + 1);
    // the state is asked before each read, and nothing else of the segment server
    expect(new Set(asked)).toEqual(new Set(['STATE']));

    // the same renewal from lease-holder.ts, through the same API
    const before = calls.length;
    const operators = kubernetesLeaseApi(
      { TOKEN_DIR: join(dir, 'token'), KUBERNETES_SERVICE_HOST: 'localhost', KUBERNETES_SERVICE_PORT: String((api.address() as { port: number }).port) },
      'db-lease',
      'prod',
    );
    child!.kill('SIGKILL');
    const current = await operators.read();
    await operators.renew(current!, 'db-0', 3);
    const theirs = calls.slice(before).find((c) => c.method === 'PUT')!;
    expect(theirs.url).toBe(renewal.url);
    expect(Object.keys(theirs.body.spec).sort()).toEqual(Object.keys(renewal.body.spec).sort());
    expect(theirs.body.spec.renewTime).toMatch(/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z$/);
    expect({ ...theirs.body.spec, renewTime: '' }).toEqual({ ...renewal.body.spec, renewTime: '' });
    expect({ ...theirs.body.metadata, resourceVersion: '' }).toEqual({ ...renewal.body.metadata, resourceVersion: '' });
  });

  it('reads the token again for every request, and a chunked answer', async () => {
    apiMode = 'chunked';
    start();
    await waitFor(() => calls.some((c) => c.method === 'PUT'), 'a renewal');
    writeFileSync(join(dir, 'token', 'token'), 'second-token\n');
    await waitFor(() => calls.some((c) => c.token === 'Bearer second-token' && c.method === 'PUT'), 'a renewal with the new token');
    expect(out).toContain('holding the Lease as the primary');
    expect(out).not.toContain('cannot');
  });

  it('fences when the operator moved the Lease, and comes back once it holds it again', async () => {
    start();
    await waitFor(() => calls.some((c) => c.method === 'PUT'), 'a renewal');
    lease = { ...lease!, spec: { ...lease!.spec, holderIdentity: 'db-2' } };
    await waitFor(() => asked.some((a) => a.startsWith('FENCE')), 'the fence');
    expect(asked.find((a) => a.startsWith('FENCE'))).toBe('FENCE the Lease is held by db-2');
    await waitFor(() => out.includes('lease holder: fenced; the Lease is held by db-2\n'), 'the fenced state');
    const renewals = calls.filter((c) => c.method === 'PUT').length;
    await new Promise((r) => setTimeout(r, 1500));
    // nothing written while another instance holds it
    expect(calls.filter((c) => c.method === 'PUT').length).toBe(renewals);
    expect(asked).not.toContain('REJOIN');
    // the Lease is this pod's again (the failover was called off): re-acquired, then the database
    lease = { ...lease!, spec: { ...lease!.spec, holderIdentity: 'db-0' } };
    await waitFor(() => asked.includes('REJOIN'), 'the rejoin');
    expect(calls.filter((c) => c.method === 'PUT').length).toBe(renewals + 1);
    await waitFor(() => out.includes('lease holder: re-acquired the Lease: database back online\n'), 'the log line');
    await waitFor(() => out.split('holding the Lease').length === 3, 'holding it again');
  });

  it('fences at the renew deadline when the API server stops answering, or refuses its renewals', async () => {
    start();
    await waitFor(() => calls.some((c) => c.method === 'PUT'), 'a renewal');
    const cut = Date.now();
    apiMode = 'silent';
    await waitFor(() => asked.some((a) => a.startsWith('FENCE')), 'the fence');
    // the deadline is 2 s after the last renewal; the check runs every second
    expect(Date.now() - cut).toBeLessThan(4500);
    expect(asked.find((a) => a.startsWith('FENCE'))).toMatch(/^FENCE cannot renew the Lease: (no renewal succeeded for \d+s \(renew deadline\)|the API server has not answered for \d+s)$/);
    expect(out).toContain('lease holder: fenced: the Lease could not be renewed');
    // the API answers again: the Lease is still this pod's, and the database comes back
    apiMode = 'answer';
    await waitFor(() => asked.includes('REJOIN'), 'the rejoin', 15_000);

    // renewals refused (the operator's write landed first): said with the API's message, then fenced
    asked = [];
    out = '';
    apiMode = 'conflict';
    await waitFor(() => asked.some((a) => a.startsWith('FENCE')), 'the second fence', 15_000);
    expect(out).toMatch(/lease holder: cannot renew the Lease \(HTTP 409: the object has been modified\): fencing in \ds\n/);
  });

  it('does nothing as a replica, with a database down, or without a Lease', async () => {
    writeFileSync(join(dir, 'primary'), 'db-1.db-headless\n');
    start();
    await waitFor(() => out.includes('lease holder: not the primary: not holding the Lease\n'), 'the replica');
    await new Promise((r) => setTimeout(r, 1200));
    expect(calls).toEqual([]);
    expect(asked).toEqual([]);
    // promoted in place: the marker counts before the ConfigMap names it; its database is down
    database = 'OK shutdown';
    writeFileSync(join(dir, 'repl', 'promoted'), '1\n');
    await waitFor(() => out.includes('lease holder: database shutdown: not renewing the Lease\n'), 'the database down');
    database = 'OK fenced';
    await waitFor(() => out.includes('lease holder: database isolated: not renewing the Lease\n'), 'the isolation fence');
    expect(calls).toEqual([]);
    expect(asked).not.toContain('REJOIN');
    // online, and no Lease yet: the operator creates it
    lease = undefined;
    database = 'OK online';
    await waitFor(() => out.includes('lease holder: the Lease does not exist yet\n'), 'no Lease');
    expect(calls.every((c) => c.method === 'GET')).toBe(true);
  });

  it('trusts only the API server\'s CA, and stops once the Firebird server has stopped', async () => {
    writeFileSync(join(dir, 'token', 'ca.crt'), createTlsCertificates({ caName: 'another CA', dnsNames: ['localhost'] }).caCert);
    start();
    await waitFor(() => /lease holder: cannot read the Lease \(.*\): fencing in \ds\n/.test(out), 'the refused API server');
    // nothing was sent to a server it does not trust: not the token either
    expect(calls).toEqual([]);
    const exited = new Promise<number | null>((resolve) => child!.once('close', (code) => resolve(code)));
    child!.kill('SIGTERM');
    expect(await exited).toBe(0);
    expect(out).toContain('lease holder: stopping once the Firebird server has stopped\n');
    expect(out).toContain('lease holder: the Firebird server has stopped, exiting\n');
  });

  it('needs its pod and its Lease named', async () => {
    start({ LEASE_NAME: '' });
    const code = await new Promise<number | null>((resolve) => child!.once('close', (c) => resolve(c)));
    expect(code).not.toBe(0);
    expect(out).toContain('lease holder: POD_NAME and LEASE_NAME are required');
  });
});
