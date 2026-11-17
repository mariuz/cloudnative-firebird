import { ChildProcess, spawn } from 'child_process';
import { agentBin } from './implementations';

/**
 * Starts the real segment server (`fb-agent segment-server`) on a free port: SEGMENT_PORT=0 makes
 * it take one from the OS and report it in its "listening on" line, so test files running in
 * parallel never pick the same port. Resolves once it listens; rejects if it exits first or does
 * not start within 15 s. The caller kills the child.
 */
export function startSegmentServer(env: Record<string, string>): Promise<{ child: ChildProcess; port: number }> {
  if (!agentBin) return Promise.reject(new Error('no fb-agent binary: build operator/agent or set FB_AGENT_BIN'));
  const child = spawn(agentBin, ['segment-server'], { env: { ...process.env, ...env, SEGMENT_PORT: '0' } });
  return new Promise((resolve, reject) => {
    let out = '';
    const timer = setTimeout(() => reject(new Error(`segment server did not start within 15s: ${out}`)), 15000);
    child.stdout.on('data', (d) => {
      out += d.toString();
      const m = /listening on (?:[^\s:]+:)?(\d+)/.exec(out);
      if (m) {
        clearTimeout(timer);
        resolve({ child, port: Number(m[1]) });
      }
    });
    child.stderr?.on('data', (d) => {
      out += d.toString();
    });
    child.once('exit', (code) => {
      clearTimeout(timer);
      reject(new Error(`segment server exited ${code}: ${out}`));
    });
  });
}
