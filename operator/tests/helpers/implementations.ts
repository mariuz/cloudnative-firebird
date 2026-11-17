import { spawnSync } from 'child_process';
import { existsSync } from 'fs';
import { join } from 'path';

/**
 * The two implementations of a program the Jobs run: the Perl script, and the Rust agent's
 * command that replaces it (agent/fb-agent/src/jobs.rs, docs/rust-agent-roadmap.md). The tests
 * of a script run against both; the agent's are left out without its binary (FB_AGENT_BIN, or a
 * build in agent/target), as the Perl ones without perl. The "Rust agent" CI job has both.
 */
export const agentBin = [
  process.env.FB_AGENT_BIN,
  ...['x86_64-unknown-linux-musl/release', 'aarch64-unknown-linux-musl/release', 'release', 'debug'].map((d) =>
    join(__dirname, '..', '..', 'agent', 'target', d, 'fb-agent'),
  ),
].find((p) => p && existsSync(p));
export const hasPerl = spawnSync('perl', ['-v']).status === 0;

/** The agent's command for each script */
const AGENT_COMMANDS: Record<string, string[]> = {
  'backup-file.pl': ['backup-file'],
  'fetch-segments.pl': ['fetch-segments'],
  'pitr-plan.pl': ['pitr-plan'],
  'switchover.pl': ['switchover'],
  'failover.pl': ['failover'],
  'sync-standby.pl': ['sync-standby'],
  'set-repl-seq.pl': ['set-repl-seq'],
  'segment-request.pl': ['request'],
};

export interface Implementation {
  name: 'perl' | 'agent';
  /** Whether it can run here */
  available: boolean;
  /** The command line: the script's path is the Perl script (the agent's command ignores it) */
  command(script: string, args?: string[]): [string, string[]];
}

export function implementations(script: string): Implementation[] {
  const agent = AGENT_COMMANDS[script];
  if (!agent) throw new Error(`no agent command for ${script}`);
  return [
    { name: 'perl', available: hasPerl, command: (path, args = []) => ['perl', [path, ...args]] },
    { name: 'agent', available: !!agentBin, command: (_path, args = []) => [agentBin ?? 'fb-agent', [...agent, ...args]] },
  ];
}
