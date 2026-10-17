import { describe, it, expect } from 'vitest';
import { spawn } from 'child_process';
import { readFileSync, writeFileSync, mkdtempSync } from 'fs';
import { AddressInfo, createServer, Server } from 'net';
import { tmpdir } from 'os';
import { join } from 'path';
import { serverStopped, stopWithServer } from '../src/stop-with-server';

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

  it('Perl sidecars (the segment server and puller include) do the same', async () => {
    const server = await listen();
    const dir = mkdtempSync(join(tmpdir(), 'stop-perl-'));
    const script = join(dir, 'sidecar.pl');
    writeFileSync(
      script,
      'use strict; use warnings; use IO::Socket::INET; $| = 1;\n' +
        readFileSync(join(__dirname, '..', 'src', 'replication', 'stop-with-server.pl'), 'utf8') +
        '\nprint "ready\\n"; while (1) { exit_when_stopped(); sleep 1; }\n',
    );
    const child = spawn('perl', [script], { env: { ...process.env, FIREBIRD_PORT: String(portOf(server)) } });
    let out = '';
    child.stdout.on('data', (d) => (out += d));
    const exited = new Promise<number | null>((r) => child.on('exit', (code) => r(code)));
    for (let i = 0; i < 50 && !out.includes('ready'); i++) await sleep(50);
    child.kill('SIGTERM');
    await sleep(1500);
    expect(child.exitCode).toBeNull(); // still running: the server is up
    await new Promise((r) => server.close(r));
    expect(await exited).toBe(0);
    expect(out).toContain('the Firebird server has stopped');
  }, 10000);
});
