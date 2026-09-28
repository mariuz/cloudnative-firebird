import { vi } from 'vitest';

// Controllers default to a TCP client for the segment servers (replication lag); unit tests inject
// their own client, and everything else fails fast instead of resolving cluster DNS names.
vi.mock('../../src/utils/replication-lag', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../src/utils/replication-lag')>()),
  segmentRequest: vi.fn().mockRejectedValue(new Error('no segment servers in unit tests')),
}));
