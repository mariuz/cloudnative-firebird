import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { HealthServer } from '../src/utils/health';
import http from 'http';

describe('HealthServer', () => {
  let server: HealthServer;
  const port = 9876;

  beforeAll(() => {
    server = new HealthServer(port);
    server.start();
  });

  afterAll(() => {
    server.stop();
  });

  const get = (path: string): Promise<{ statusCode: number; body: string }> => {
    return new Promise((resolve, reject) => {
      http
        .get(`http://127.0.0.1:${port}${path}`, (res) => {
          let data = '';
          res.on('data', (chunk) => (data += chunk));
          res.on('end', () => resolve({ statusCode: res.statusCode || 500, body: data }));
        })
        .on('error', reject);
    });
  };

  it('returns 200 ok for /healthz', async () => {
    const res = await get('/healthz');
    expect(res.statusCode).toBe(200);
    expect(res.body).toBe('ok');
  });

  it('returns 503 not ready for /readyz when not set ready', async () => {
    const res = await get('/readyz');
    expect(res.statusCode).toBe(503);
    expect(res.body).toBe('not ready');
  });

  it('returns 200 ok for /readyz after setReady(true)', async () => {
    server.setReady(true);
    const res = await get('/readyz');
    expect(res.statusCode).toBe(200);
    expect(res.body).toBe('ok');
  });

  it('returns 404 for unknown endpoints', async () => {
    const res = await get('/unknown');
    expect(res.statusCode).toBe(404);
    expect(res.body).toBe('not found');
  });
});
