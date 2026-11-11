import { describe, it, expect, vi } from 'vitest';
import { spawnSync } from 'child_process';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { REPLICATION_SCRIPTS } from '../src/utils/replication';
import { startSegmentServer } from './helpers/segment-server';

const hasPerl = spawnSync('perl', ['-v']).status === 0;

/**
 * Firebird archives a journal segment with a plain copy, and its replica skips a segment file
 * shorter than its header says. A segment listed while it is being copied would be fetched short
 * and recorded as fetched: the replica would wait for it for ever. LIST therefore names only the
 * segments that hold their whole length (the fb-agent front does the same:
 * segment-server-front.test.ts).
 */
describe.skipIf(!hasPerl)('segment-server.pl LIST', () => {
  it('leaves out a segment that is still being copied into the archive', async () => {
    const root = mkdtempSync(join(tmpdir(), 'seglist-'));
    for (const d of ['archive', 'source', 'repl']) mkdirSync(join(root, d));
    const segment = (sequence: number, length: number, size = length) => {
      const b = Buffer.alloc(Math.max(size, 48));
      b.write('FBCHANGELOG\0', 0, 'latin1');
      b.writeBigUInt64LE(BigInt(sequence), 32);
      b.writeBigUInt64LE(BigInt(length), 40);
      writeFileSync(join(root, 'archive', `mydb.fdb.journal-00000000${sequence}`), b.subarray(0, size));
    };
    segment(1, 4096);
    segment(2, 4096, 1000); // being copied
    segment(3, 100, 4096); // longer than its length: complete
    writeFileSync(join(root, 'archive', 'mydb.fdb.journal-000000004'), 'x'); // no segment header: as before
    writeFileSync(join(root, 'archive', 'mydb.fdb.journal-000000005'), Buffer.alloc(20)); // shorter than a header: as before
    writeFileSync(join(root, 'repl', 'primary'), 'db-0\n');
    writeFileSync(join(root, 'segment-server.pl'), REPLICATION_SCRIPTS['segment-server.pl']);
    const { child, port } = await startSegmentServer(join(root, 'segment-server.pl'), {
      ARCHIVE_DIR: join(root, 'archive'),
      DATABASE_PATH: join(root, 'mydb.fdb'),
      SOURCE_DIR: join(root, 'source'),
      REPLICATION_DIR: join(root, 'repl'),
      PRIMARY_FILE: join(root, 'repl', 'primary'),
      POD_NAME: 'db-0',
      ISC_PASSWORD: 'tok',
    });
    try {
      const { segmentRequest } = await vi.importActual<typeof import('../src/utils/replication-lag')>('../src/utils/replication-lag');
      const list = () => segmentRequest('127.0.0.1', port, 'tok LIST');
      expect(await list()).toEqual([1, 3, 4, 5].map((n) => `mydb.fdb.journal-00000000${n}`));
      // the copy ends: listed
      segment(2, 4096);
      expect(await list()).toEqual([1, 2, 3, 4, 5].map((n) => `mydb.fdb.journal-00000000${n}`));
      // the other commands still see every file (the lag and the pruning count them)
      expect((await segmentRequest('127.0.0.1', port, 'tok ARCHIVED')).map((l) => l.split(' ')[0])).toEqual(['1', '2', '3']);
    } finally {
      child.kill();
      rmSync(root, { recursive: true, force: true });
    }
  });
});
