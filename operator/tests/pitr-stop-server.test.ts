import { describe, it, expect } from 'vitest';
import { spawnSync } from 'child_process';
import { readFileSync } from 'fs';
import { join } from 'path';

/**
 * The point-in-time recovery Job stops the Firebird server it replayed the journal with by SIGTERM,
 * then by SIGKILL when it has not exited in time (a Firebird 4 server sometimes hung on SIGTERM,
 * and the Job with it). The script's stop function runs here against stand-in servers.
 */
const script = readFileSync(join(__dirname, '..', 'src', 'replication', 'pitr-restore.sh'), 'utf8');
const start = script.indexOf('stop_server() {');
const fn = script.slice(start, script.indexOf('\n}\n', start) + 3);

// server: the stand-in server's shell code; prints the function's output, then whether the server is gone
const run = (server: string, timeout = 2) =>
  spawnSync(
    'sh',
    [
      '-c',
      `set -eu
${fn}
sh -c '${server}' &
server=$!
sleep 0.3
begin=$(date +%s)
stop_server "$server"
end=$(date +%s)
if kill -0 "$server" 2>/dev/null; then echo running; else echo stopped; fi
echo "took=$((end - begin))"`,
    ],
    { encoding: 'utf8', env: { ...process.env, SERVER_STOP_TIMEOUT: String(timeout) }, timeout: 20000 },
  );

describe('the recovery Job stopping its server', { timeout: 30000 }, () => {
  it('is the script function, used after every replay', () => {
    expect(start).toBeGreaterThan(-1);
    expect(fn).toMatch(/^stop_server\(\) \{[\s\S]*kill -9 "\$1"[\s\S]*\n\}\n$/);
    expect(script).toMatch(/grep -E 'is \(replayed\|replicated\)' "\$W\/fb\/replication\.log" \|\| true\n {2}stop_server "\$server"\n\}/);
    expect(script).not.toMatch(/wait "\$server"/);
  });

  it('stops a server that exits on SIGTERM without waiting for the timeout', () => {
    const r = run('trap "exit 0" TERM; while :; do sleep 0.1; done', 10);
    expect(r.status).toBe(0);
    expect(r.stdout).not.toContain('killed');
    expect(r.stdout).toContain('stopped');
    expect(Number(r.stdout.match(/took=(\d+)/)![1])).toBeLessThan(5);
  });

  it('kills a server that ignores SIGTERM once the timeout passed', () => {
    const r = run('trap "" TERM; while :; do sleep 0.1; done', 2);
    expect(r.status).toBe(0);
    expect(r.stdout).toContain('the Firebird server did not stop within 2s of SIGTERM: killed');
    expect(r.stdout).toContain('stopped');
    const took = Number(r.stdout.match(/took=(\d+)/)![1]);
    expect(took).toBeGreaterThanOrEqual(1);
    expect(took).toBeLessThan(6);
  });

  it('returns for a server that already exited', () => {
    const r = run('exit 0', 10);
    expect(r.status).toBe(0);
    expect(r.stdout).toContain('stopped');
    expect(r.stdout).not.toContain('killed');
  });
});
