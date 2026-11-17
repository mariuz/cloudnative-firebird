import { describe, it, expect } from 'vitest';
import { spawn } from 'child_process';
import { mkdtempSync } from 'fs';
import { AddressInfo, createServer, Server } from 'net';
import { tmpdir } from 'os';
import { join } from 'path';
import { agentBin } from './helpers/implementations';

const listen = (): Promise<Server> => new Promise((resolve) => { const s = createServer((c) => c.destroy()); s.listen(0, '127.0.0.1', () => resolve(s)); });
const portOf = (s: Server) => (s.address() as AddressInfo).port;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * The sidecars stop with the Firebird server: the container's SIGTERM is taken as "stop once the
 * pod's Firebird server has stopped", so a primary's segments are served to the end. Every
 * sidecar is the agent's since v0.171.0 (the Node ones are deleted); the Lease holder's and the
 * puller's stopping are tested with them.
 */
describe('sidecars stop with the Firebird server', () => {
  it.runIf(agentBin)('the agent\'s segment server keeps serving after SIGTERM until the Firebird server has stopped', async () => {
    const server = await listen();
    const dir = mkdtempSync(join(tmpdir(), 'stop-agent-'));
    const child = spawn(agentBin!, ['segment-server'], {
      env: { ...process.env, FIREBIRD_PORT: String(portOf(server)), ARCHIVE_DIR: dir, SEGMENT_PORT: '0', ISC_PASSWORD: 'x' },
    });
    let out = '';
    child.stdout.on('data', (d) => (out += d));
    const exited = new Promise<number | null>((r) => child.on('exit', (code) => r(code)));
    for (let i = 0; i < 50 && !out.includes('listening on'); i++) await sleep(50);
    expect(out).toContain('listening on');
    child.kill('SIGTERM');
    await sleep(1500);
    expect(child.exitCode).toBeNull(); // still running: the server is up
    await new Promise((r) => server.close(r));
    expect(await exited).toBe(0);
    expect(out).toContain('the Firebird server has stopped');
  }, 10000);
});
