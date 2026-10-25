import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    include: ['tests/**/*.test.ts'],
    // unit tests never reach real segment servers
    setupFiles: ['tests/helpers/no-network.ts'],
    // several tests run the Perl and shell scripts for real, with sleeps of a second or two
    // (smart shutdown, stop-with-server, PITR, the segment server's commands): the default 5 s
    // left them a slim margin on a loaded runner, where the whole suite runs in parallel. A hung
    // test still fails, later; nothing is retried, so a flaky one stays visible.
    testTimeout: 30_000,
    hookTimeout: 30_000,
    retry: 0,
    coverage: {
      provider: 'v8',
      reporter: ['text', 'json', 'html'],
      include: ['src/**/*.ts'],
      exclude: ['src/index.ts'],
    },
  },
});
