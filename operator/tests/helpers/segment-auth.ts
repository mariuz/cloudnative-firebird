import { createHmac, randomBytes } from 'crypto';
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

/**
 * A "<token> <request>" line signed with its token, as every client sends it since v0.83.0 (lines
 * already signed are left as they are)
 */
export function signLine(line: string, at = Math.floor(Date.now() / 1000)): string {
  if (line.startsWith('SIG1 ')) return line;
  const space = line.indexOf(' ');
  const [secret, request] = space < 0 ? ['', line] : [line.slice(0, space), line.slice(space + 1)];
  const nonce = randomBytes(16).toString('hex');
  return `SIG1 ${at} ${nonce} ${createHmac('sha256', secret).update(`${at} ${nonce} ${request}`).digest('hex')} ${request}`;
}
