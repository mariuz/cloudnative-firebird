import { createHmac } from 'crypto';
import { createServer, Server, Socket } from 'net';

/**
 * The request of a segment server line: "<request>" from a signed line ("SIG1 <epoch> <nonce>
 * <mac> <request>", checked against the secret when given) or a legacy "<token> <request>" one.
 */
export function segmentRequestOf(raw: string, secret?: string): { request: string; signed: boolean } {
  const line = raw.replace(/\r?\n$/, '');
  const m = /^SIG1 (\d+) ([0-9a-f]+) ([0-9a-f]{64}) (.+)$/.exec(line);
  if (m) {
    if (secret !== undefined) {
      const mac = createHmac('sha256', secret).update(`${m[1]} ${m[2]} ${m[4]}`).digest('hex');
      if (mac !== m[3]) throw new Error(`bad signature: ${line}`);
    }
    return { request: m[4], signed: true };
  }
  return { request: line.slice(line.indexOf(' ') + 1), signed: false };
}

/**
 * A fake segment server: answers the clients' signed PING probe with "OK" and passes every
 * other request to the handler (one request per connection, as the real server).
 */
export function fakeSegmentServer(handler: (request: string, sock: Socket, raw: string) => void, secret?: string): Server {
  return createServer((sock) =>
    sock.once('data', (buf) => {
      const raw = buf.toString();
      const { request } = segmentRequestOf(raw.split('\n')[0], secret);
      if (request === 'PING') sock.end('OK\n');
      else handler(request, sock, raw);
    }),
  );
}
