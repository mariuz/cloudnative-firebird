import { existsSync } from 'fs';
import { join } from 'path';

/**
 * The agent's binary the tests run (FB_AGENT_BIN, or a build in agent/target): the programs of
 * the instance pods and the Jobs (docs/rust-agent-roadmap.md). The tests that need it are left
 * out without it; the "Rust agent" and "Unit Tests" CI jobs build it.
 */
export const agentBin = [
  process.env.FB_AGENT_BIN,
  ...['x86_64-unknown-linux-musl/release', 'aarch64-unknown-linux-musl/release', 'release', 'debug'].map((d) =>
    join(__dirname, '..', '..', 'agent', 'target', d, 'fb-agent'),
  ),
].find((p) => p && existsSync(p));

/** The agent's command line: `fb-agent <command> <args...>` */
export function agent(command: string[], args: string[] = []): [string, string[]] {
  return [agentBin ?? 'fb-agent', [...command, ...args]];
}
