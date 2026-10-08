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
 *
 * While segment TLS is switched on or off, the instances restart one by one into the new mode. So
 * that they keep replicating meanwhile, the operator publishes the instances that still serve in
 * plain text in the <cluster>-segment-tls-peers ConfigMap (SEGMENT_TLS_PEERS_DIR):
 *
 * - `plain-peers`: the client side connects to these instances in plain text, not over TLS;
 * - `accept-plain-until` (epoch milliseconds): until then the server side also accepts plain
 *   connections (from instances and Jobs not running the proxy yet, or no longer), told apart
 *   from TLS by the first byte (a TLS handshake record starts with 0x16).
 *
 * Without the ConfigMap, or once the switch is over, both are empty: TLS only. The operator
 * decides which instances are plain; a failed TLS handshake never falls back to plain text.
 */
import { readFileSync, statSync } from 'fs';
import { createServer, connect as tcpConnect, Server, Socket } from 'net';
import { connect as tlsConnect, createServer as createTlsServer, SecureContextOptions, TLSSocket } from 'tls';
import { join } from 'path';
import { Duplex } from 'stream';

export const CONNECT_LINE_MAX = 512;
/** First byte of a TLS handshake record */
const TLS_HANDSHAKE = 0x16;
/** How long the server side waits for a connection's first byte */
const FIRST_BYTE_TIMEOUT_MS = 30_000;

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

/** What the operator allows during a switch of segment TLS (see above) */
export interface SegmentTlsPeers {
  /** Instance pod names reached in plain text */
  plain: Set<string>;
  /** Until when (epoch ms) plain connections are accepted */
  acceptPlainUntil: number;
}

export const STRICT: SegmentTlsPeers = { plain: new Set(), acceptPlainUntil: 0 };

/** Parses the ConfigMap files; anything missing or malformed means TLS only */
export function parsePeers(plainPeers: string | undefined, acceptPlainUntil: string | undefined): SegmentTlsPeers {
  const until = Number((acceptPlainUntil ?? '').trim());
  return {
    plain: new Set((plainPeers ?? '').split(/\s+/).filter((p) => /^[a-z0-9]([-a-z0-9]*[a-z0-9])?$/.test(p))),
    acceptPlainUntil: Number.isFinite(until) ? until : 0,
  };
}

/** The peers files, read again every few seconds (ConfigMap volume updates) */
export function readPeers(dir: string): SegmentTlsPeers {
  const read = (name: string) => {
    try {
      return readFileSync(join(dir, name), 'utf8');
    } catch {
      return undefined;
    }
  };
  return parsePeers(read('plain-peers'), read('accept-plain-until'));
}

/** The instance a segment server address names: its first label (<pod>.<cluster>-headless...) */
export const peerName = (host: string) => host.split('.')[0];

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

/**
 * Server side: mutual TLS on `port`, each connection forwarded to target (the segment server);
 * plain connections too while the operator allows them (peers().acceptPlainUntil)
 */
export function startServer(
  files: () => SegmentTlsFiles,
  port: number,
  target: { host: string; port: number },
  host?: string,
  peers: () => SegmentTlsPeers = () => STRICT,
  now = () => Date.now(),
): Promise<Server> {
  let current = files();
  const upstream = () => tcpConnect({ host: target.host, port: target.port, allowHalfOpen: true });
  const tlsServer = createTlsServer({ ...tlsOptions(current), requestCert: true, rejectUnauthorized: true, allowHalfOpen: true }, (tls: TLSSocket) =>
    relay(tls, upstream()),
  );
  // a peer that is not a client of this cluster
  tlsServer.on('tlsClientError', () => undefined);
  const server = createServer({ allowHalfOpen: true }, (socket: Socket) => {
    socket.on('error', () => socket.destroy());
    // nothing sent: the isolation check's bare TCP probe, or a client that gave up
    socket.setTimeout(FIRST_BYTE_TIMEOUT_MS, () => socket.destroy());
    socket.once('data', (first: Buffer) => {
      socket.setTimeout(0);
      socket.pause();
      socket.unshift(first);
      if (first[0] === TLS_HANDSHAKE) {
        const latest = files();
        if (latest !== current) {
          current = latest;
          tlsServer.setSecureContext(tlsOptions(current));
        }
        // through a JS stream: TLS on the socket itself would read from its handle and miss the
        // byte read here
        tlsServer.emit('connection', Duplex.from({ readable: socket, writable: socket }));
        socket.resume();
      } else if (now() < peers().acceptPlainUntil) {
        relay(socket, upstream());
        socket.resume();
      } else {
        socket.destroy();
      }
    });
  });
  return new Promise((resolve) => server.listen(port, host, () => resolve(server)));
}

/**
 * Client side: plain connections on host:port, "CONNECT <host> <port>" first, relayed over mutual
 * TLS, or in plain text to the instances the operator lists as plain (peers().plain)
 */
export function startClient(files: () => SegmentTlsFiles, port: number, host = '127.0.0.1', peers: () => SegmentTlsPeers = () => STRICT): Promise<Server> {
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
      const plain = peers().plain.has(peerName(m[1]));
      const remote = plain
        ? tcpConnect({ host: m[1], port: Number(m[2]), allowHalfOpen: true })
        : tlsConnect({
            ...tlsOptions(files()),
            host: m[1],
            port: Number(m[2]),
            // the chain to the cluster's CA authenticates the peer (see above)
            checkServerIdentity: () => undefined,
          });
      remote.once(plain ? 'connect' : 'secureConnect', () => {
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

/** The peers files, read again every 5 seconds */
function watchedPeers(dir: string | undefined): () => SegmentTlsPeers {
  if (!dir) return () => STRICT;
  let current = readPeers(dir);
  setInterval(() => (current = readPeers(dir)), 5_000).unref();
  return () => current;
}

/** The certificate files, read again once they changed (Secret volume updates) */
function watchedFiles(dir: string): () => SegmentTlsFiles {
  let current = readTlsFiles(dir);
  let stamp = statSync(join(dir, 'tls.crt')).mtimeMs;
  setInterval(() => {
    try {
      const now = statSync(join(dir, 'tls.crt')).mtimeMs;
      if (now === stamp) return;
      stamp = now;
      current = readTlsFiles(dir);
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
  const files = watchedFiles(dir);
  const peers = watchedPeers(env.SEGMENT_TLS_PEERS_DIR);
  if (env.SERVER_LISTEN && env.SERVER_TARGET) {
    const listen = hostPort(env.SERVER_LISTEN, '0.0.0.0');
    servers.push(await startServer(files, listen.port, hostPort(env.SERVER_TARGET, '127.0.0.1'), listen.host, peers));
    console.log(`segment TLS: accepting on ${env.SERVER_LISTEN}, forwarding to ${env.SERVER_TARGET}`);
  }
  if (env.CLIENT_LISTEN) {
    const listen = hostPort(env.CLIENT_LISTEN, '127.0.0.1');
    servers.push(await startClient(files, listen.port, listen.host, peers));
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
