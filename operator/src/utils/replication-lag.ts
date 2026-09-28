import { Socket } from 'net';

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

export const segmentRequest: SegmentClient = (host, port, line, timeoutMs = SEGMENT_REQUEST_TIMEOUT_MS) =>
  new Promise((resolve, reject) => {
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
