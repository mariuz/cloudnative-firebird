import { describe, it, expect } from 'vitest';
import { spawn } from 'child_process';
import { mkdtempSync } from 'fs';
import { AddressInfo, createServer, Server } from 'net';
import { tmpdir } from 'os';
import { join } from 'path';
import { serverStopped, stopWithServer } from '../src/stop-with-server';
import { agentBin } from './helpers/implementations';

const listen = (): Promise<Server> => new Promise((resolve) => { const s = createServer((c) => c.destroy()); s.listen(0, '127.0.0.1', () => resolve(s)); });
const portOf = (s: Server) => (s.address() as AddressInfo).port;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe('sidecars stop with the Firebird server', () => {
  it('sees whether the server port is open', async () => {
    const server = await listen();
    const port = portOf(server);
    expect(await serverStopped(port)).toBe(false);
    await new Promise((r) => server.close(r));
    expect(await serverStopped(port)).toBe(true);
  });

  it('Node sidecars exit after SIGTERM once the server has stopped', async () => {
    const server = await listen();
    const exits: number[] = [];
    const lines: string[] = [];
    const before = process.listeners('SIGTERM').length;
    stopWithServer('test sidecar', { port: portOf(server), pollMs: 50, exit: (code) => exits.push(code), log: (l) => lines.push(l) });
    const handler = process.listeners('SIGTERM').slice(before)[0] as () => void;
    try {
      handler();
      await sleep(200);
      expect(exits).toEqual([]);
      expect(lines).toEqual(['test sidecar: stopping once the Firebird server has stopped']);
      await new Promise((r) => server.close(r));
      await sleep(200);
      expect(exits).toEqual([0]);
      expect(lines.at(-1)).toContain('has stopped, exiting');
    } finally {
      process.removeListener('SIGTERM', handler);
      for (const l of process.listeners('SIGINT').slice(-1)) process.removeListener('SIGINT', l as () => void);
    }
  });

  it.runIf(agentBin)('the agent\'s segment server does the same', async () => {
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
