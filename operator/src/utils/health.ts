import { createServer, IncomingMessage, ServerResponse } from 'http';
import { logger } from './logger';

/**
 * A minimal HTTP server that serves Kubernetes liveness (/healthz)
 * and readiness (/readyz) probe endpoints.
 */
export class HealthServer {
  private server: ReturnType<typeof createServer>;
  private ready = false;

  constructor(private readonly port = 8080) {
    this.server = createServer(this.handleRequest.bind(this));
  }

  /** Mark the operator as ready to serve traffic */
  setReady(ready: boolean): void {
    this.ready = ready;
  }

  /** Start the health server */
  start(): void {
    this.server.listen(this.port, () => {
      logger.info({ port: this.port }, 'Health server listening');
    });
  }

  /** Stop the health server */
  stop(): void {
    this.server.close();
  }

  private handleRequest(req: IncomingMessage, res: ServerResponse): void {
    if (req.url === '/healthz') {
      // Liveness: the process is running
      res.writeHead(200, { 'Content-Type': 'text/plain' });
      res.end('ok');
      return;
    }

    if (req.url === '/readyz') {
      // Readiness: the operator is fully initialized
      if (this.ready) {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('ok');
      } else {
        res.writeHead(503, { 'Content-Type': 'text/plain' });
        res.end('not ready');
      }
      return;
    }

    res.writeHead(404, { 'Content-Type': 'text/plain' });
    res.end('not found');
  }
}
