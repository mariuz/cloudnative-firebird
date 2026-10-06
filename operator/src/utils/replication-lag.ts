import { createHmac, randomBytes } from 'crypto';
import { Socket } from 'net';
import { ReplicaLagStatus, SegmentRetentionStatus } from '../types';

/**
 * Replication lag, measured by the operator from the segment servers:
 *
 * - the primary answers ARCHIVED with the sequence and age of every archived journal segment;
 * - each replica answers POSITION with the segment its replica control file has applied.
 *
 * A replica's lag is the number of archived segments after its position and the age of the
 * oldest of them (0 when it applied everything the primary archived). Transactions still in the
 * primary's active segment are not counted; archiveTimeoutSeconds bounds how long they wait.
 */

/** Sends one request line to a segment server and returns the reply lines (without the "." terminator) */
export type SegmentClient = (host: string, port: number, line: string, timeoutMs?: number) => Promise<string[]>;

export const SEGMENT_REQUEST_TIMEOUT_MS = 3000;

/** One request line to a segment server, as it is sent; returns the reply lines */
function rawSegmentRequest(host: string, port: number, line: string, timeoutMs: number): Promise<string[]> {
  return new Promise((resolve, reject) => {
    const socket = new Socket();
    let data = '';
    const done = (err?: Error) => {
      socket.destroy();
      if (err) reject(err);
      else resolve(data.split('\n').filter((l) => l !== '' && l !== '.'));
    };
    socket.setTimeout(timeoutMs, () => done(new Error(`segment server ${host}:${port} timed out`)));
    socket.once('error', (err) => done(err));
    socket.on('data', (chunk) => {
      data += chunk.toString('utf8');
    });
    socket.once('end', () => done());
    socket.connect(port, host, () => socket.write(`${line}\n`));
  });
}

/**
 * A request signed with the SYSDBA password (segment-server.pl): "SIG1 <epoch> <nonce> <mac>
 * <request>", mac = hex HMAC-SHA256 of "<epoch> <nonce> <request>". The password never crosses
 * the network.
 */
export function signSegmentRequest(secret: string, request: string, now = Date.now()): string {
  const at = Math.floor(now / 1000);
  const nonce = randomBytes(16).toString('hex');
  const mac = createHmac('sha256', secret).update(`${at} ${nonce} ${request}`).digest('hex');
  return `SIG1 ${at} ${nonce} ${mac} ${request}`;
}

/** Segment servers of an earlier version (legacy plain password), by "host:port", with when they said so */
const legacySegmentServers = new Map<string, number>();
const signedSegmentServers = new Set<string>();
const LEGACY_RECHECK_MS = 60_000;

/**
 * The line is "<password> <request>". It is sent signed to segment servers that support it; a
 * server of an earlier version (during a rolling update) answers the signed PING probe with
 * "ERR unauthorized" and gets the line as it is.
 */
export const segmentRequest: SegmentClient = async (host, port, line, timeoutMs = SEGMENT_REQUEST_TIMEOUT_MS) => {
  const space = line.indexOf(' ');
  const [secret, request] = space < 0 ? ['', line] : [line.slice(0, space), line.slice(space + 1)];
  const key = `${host}:${port}`;
  if (!signedSegmentServers.has(key)) {
    const legacySince = legacySegmentServers.get(key);
    if (legacySince === undefined || Date.now() - legacySince > LEGACY_RECHECK_MS) {
      const [reply] = await rawSegmentRequest(host, port, signSegmentRequest(secret, 'PING'), timeoutMs);
      if (reply === undefined) throw new Error(`segment server ${host}:${port}: no answer`);
      if (reply === 'ERR unauthorized') legacySegmentServers.set(key, Date.now());
      else {
        legacySegmentServers.delete(key);
        signedSegmentServers.add(key);
      }
    }
  }
  return rawSegmentRequest(host, port, signedSegmentServers.has(key) ? signSegmentRequest(secret, request) : line, timeoutMs);
};

export interface ArchivedSegment {
  sequence: number;
  ageSeconds: number;
}

/** Parses an ARCHIVED reply */
export function parseArchived(lines: string[]): ArchivedSegment[] {
  if (lines[0]?.startsWith('ERR')) throw new Error(lines[0]);
  return lines
    .map((l) => l.trim().split(/\s+/).map(Number))
    .filter(([seq, age]) => Number.isInteger(seq) && Number.isFinite(age))
    .map(([sequence, ageSeconds]) => ({ sequence, ageSeconds: Math.max(0, ageSeconds) }))
    .sort((a, b) => a.sequence - b.sequence);
}

export interface ReplicaPosition {
  sequence: number;
  pending: number;
}

/** Parses a replica's POSITION reply ("OK <sequence> <offset> <pending>") */
export function parsePosition(lines: string[]): ReplicaPosition {
  const m = /^OK (\d+) (\d+) (\d+)$/.exec(lines[0] ?? '');
  if (!m) throw new Error(lines[0] ? `unexpected POSITION reply: ${lines[0]}` : 'empty POSITION reply');
  return { sequence: Number(m[1]), pending: Number(m[3]) };
}

/** Lag of a replica positioned at `applied` against the primary's archived segments */
export function computeLag(archived: ArchivedSegment[], applied: number): { lagSegments: number; lagSeconds: number } {
  const behind = archived.filter((s) => s.sequence > applied);
  return {
    lagSegments: behind.length,
    lagSeconds: behind.length ? Math.max(...behind.map((s) => s.ageSeconds)) : 0,
  };
}

/**
 * Segment retention floor: the primary keeps every archived segment after the lowest segment
 * applied by a replica (up to maxSegmentRetentionHours), so a slow or stopped replica can catch up
 * instead of being re-seeded. A replica that cannot be measured now (not ready, stopped) keeps its
 * last known position; replicas that no longer exist (scaled away, now the primary) are dropped,
 * and replicas never measured yet (being seeded) do not hold the floor back.
 */
export function segmentRetention(options: {
  /** Replicas the cluster has: instance pods other than the primary, below spec.instances */
  replicaNames: string[];
  measured: ReplicaLagStatus[];
  previous?: SegmentRetentionStatus;
}): SegmentRetentionStatus {
  const { replicaNames, measured, previous } = options;
  const replicas = replicaNames
    .map((name) => {
      const appliedSequence =
        measured.find((r) => r.name === name)?.appliedSequence ??
        previous?.replicas?.find((r) => r.name === name)?.appliedSequence;
      return appliedSequence === undefined ? undefined : { name, appliedSequence };
    })
    .filter((r): r is { name: string; appliedSequence: number } => r !== undefined);
  return replicas.length
    ? { floorSequence: Math.min(...replicas.map((r) => r.appliedSequence)), replicas }
    : {};
}
