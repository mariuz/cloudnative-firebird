/**
 * TLS for the segment servers (spec.segmentTLS), run from the operator image as a
 * sidecar beside the instances and in the Jobs that talk to them:
 *
 * - server side (SERVER_LISTEN, SERVER_TARGET): accepts mutual TLS on the segment port and
 *   forwards each connection to the segment server, which then listens on localhost only;
 * - client side (CLIENT_LISTEN, on localhost): the Perl clients connect here and send one line,
 *   "CONNECT <host> <port>", followed by their request as before; the proxy opens mutual TLS to
 *   that host's segment port and relays both ways.
 *
 * The certificates come from the cluster's <cluster>-segment-tls Secret, mounted in
 * SEGMENT_TLS_DIR (ca.crt, tls.crt, tls.key): one CA per cluster, which signs the certificate all
 * of its instances present. A valid chain to the cluster's CA is what authenticates a peer, so
 * host names are not checked (the Perl clients address instances by short names that a wildcard
 * certificate cannot cover in Node). The files are read again when they change (rotation).
 */
import { readFileSync, statSync } from 'fs';
import { createServer, connect as tcpConnect, Server, Socket } from 'net';
import { connect as tlsConnect, createServer as createTlsServer, SecureContextOptions, TLSSocket } from 'tls';
import { join } from 'path';

export const CONNECT_LINE_MAX = 512;

export interface SegmentTlsFiles {
  ca: string;
  cert: string;
  key: string;
}

export function readTlsFiles(dir: string): SegmentTlsFiles {
  const read = (name: string) => readFileSync(join(dir, name), 'utf8');
  return { ca: read('ca.crt'), cert: read('tls.crt'), key: read('tls.key') };
}

/** TLS options shared by both sides: mutual TLS, TLS 1.3, the cluster's CA only */
export function tlsOptions(files: SegmentTlsFiles): SecureContextOptions & { minVersion: 'TLSv1.3' } {
  return { ca: files.ca, cert: files.cert, key: files.key, minVersion: 'TLSv1.3' };
}

/** Relays two sockets both ways; either side ending or failing ends the other */
function relay(a: Socket, b: Socket): void {
  a.pipe(b);
  b.pipe(a);
  const close = () => {
    a.destroy();
    b.destroy();
  };
  a.on('error', close);
  b.on('error', close);
  a.on('close', () => b.end());
  b.on('close', () => a.end());
}

/** Server side: mutual TLS on `port`, each connection forwarded to target (the segment server) */
export function startServer(files: () => SegmentTlsFiles, port: number, target: { host: string; port: number }, host?: string): Promise<Server> {
  const server = createTlsServer({ ...tlsOptions(files()), requestCert: true, rejectUnauthorized: true, allowHalfOpen: true }, (tls: TLSSocket) => {
    const upstream = tcpConnect({ host: target.host, port: target.port, allowHalfOpen: true });
    relay(tls, upstream);
  });
  // a peer that is not a client of this cluster (or the isolation check's bare TCP probe)
  server.on('tlsClientError', () => undefined);
  return new Promise((resolve) => server.listen(port, host, () => resolve(server)));
}

/** Client side: plain connections on host:port, "CONNECT <host> <port>" first, relayed over mutual TLS */
export function startClient(files: () => SegmentTlsFiles, port: number, host = '127.0.0.1'): Promise<Server> {
  const server = createServer({ allowHalfOpen: true }, (local: Socket) => {
    let buffered = Buffer.alloc(0);
    const onData = (chunk: Buffer) => {
      buffered = Buffer.concat([buffered, chunk]);
      const newline = buffered.indexOf(0x0a);
      if (newline < 0) {
        if (buffered.length > CONNECT_LINE_MAX) local.destroy();
        return;
      }
      local.off('data', onData);
      local.pause();
      const line = buffered.subarray(0, newline).toString('utf8').replace(/\r$/, '');
      const rest = buffered.subarray(newline + 1);
      const m = /^CONNECT (\S+) (\d+)$/.exec(line);
      if (!m) {
        local.end('ERR expected CONNECT <host> <port>\n');
        return;
      }
      const remote = tlsConnect({
        ...tlsOptions(files()),
        host: m[1],
        port: Number(m[2]),
        // the chain to the cluster's CA authenticates the peer (see above)
        checkServerIdentity: () => undefined,
      });
      remote.once('secureConnect', () => {
        if (rest.length) remote.write(rest);
        relay(local, remote);
        local.resume();
      });
      remote.once('error', () => local.destroy());
    };
    local.on('data', onData);
    local.on('error', () => local.destroy());
  });
  return new Promise((resolve) => server.listen(port, host, () => resolve(server)));
}

/** The certificate files, read again once they changed (Secret volume updates) */
function watchedFiles(dir: string, onChange: (files: SegmentTlsFiles) => void): () => SegmentTlsFiles {
  let current = readTlsFiles(dir);
  let stamp = statSync(join(dir, 'tls.crt')).mtimeMs;
  setInterval(() => {
    try {
      const now = statSync(join(dir, 'tls.crt')).mtimeMs;
      if (now === stamp) return;
      stamp = now;
      current = readTlsFiles(dir);
      onChange(current);
    } catch {
      // being replaced: next time
    }
  }, 30_000).unref();
  return () => current;
}

const hostPort = (value: string, defaultHost: string) => {
  const i = value.lastIndexOf(':');
  return i < 0 ? { host: defaultHost, port: Number(value) } : { host: value.slice(0, i), port: Number(value.slice(i + 1)) };
};

export async function main(env = process.env): Promise<Server[]> {
  const dir = env.SEGMENT_TLS_DIR ?? '/etc/segment-tls';
  const servers: Server[] = [];
  let tlsServer: Server | undefined;
  const files = watchedFiles(dir, (f) => (tlsServer as unknown as { setSecureContext?: (o: object) => void })?.setSecureContext?.(tlsOptions(f)));
  if (env.SERVER_LISTEN && env.SERVER_TARGET) {
    const listen = hostPort(env.SERVER_LISTEN, '0.0.0.0');
    tlsServer = await startServer(files, listen.port, hostPort(env.SERVER_TARGET, '127.0.0.1'), listen.host);
    servers.push(tlsServer);
    console.log(`segment TLS: accepting on ${env.SERVER_LISTEN}, forwarding to ${env.SERVER_TARGET}`);
  }
  if (env.CLIENT_LISTEN) {
    const listen = hostPort(env.CLIENT_LISTEN, '127.0.0.1');
    servers.push(await startClient(files, listen.port, listen.host));
    console.log(`segment TLS: client proxy on ${env.CLIENT_LISTEN}`);
  }
  if (servers.length === 0) throw new Error('segment TLS: neither SERVER_LISTEN/SERVER_TARGET nor CLIENT_LISTEN is set');
  return servers;
}

if (require.main === module) {
  main().catch((err) => {
    console.error((err as Error).message);
    process.exit(1);
  });
}
