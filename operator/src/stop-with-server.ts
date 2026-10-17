import { connect } from 'net';

/**
 * SIGTERM for a sidecar that runs as its container's PID 1, which ignores signals it has no
 * handler for (Kubernetes then kills it only at the end of the grace period): the sidecar keeps
 * working until the pod's Firebird server has stopped (the smart shutdown waits for clients first,
 * utils/lifecycle.ts), then exits. Where no server runs (a Job's pod) that is at once.
 */
export const FIREBIRD_PORT = 3050;
export const STOP_POLL_MS = 1000;

/** Whether nothing listens on the local Firebird port */
export function serverStopped(port = FIREBIRD_PORT, host = '127.0.0.1'): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = connect(port, host);
    socket.setTimeout(2000, () => {
      socket.destroy();
      resolve(false);
    });
    socket.once('connect', () => {
      socket.destroy();
      resolve(false);
    });
    socket.once('error', () => resolve(true));
  });
}

/** Installs the SIGTERM / SIGINT handlers; exit is called once the server has stopped */
export function stopWithServer(
  name: string,
  options: { port?: number; pollMs?: number; exit?: (code: number) => void; log?: (line: string) => void } = {},
): void {
  const exit = options.exit ?? ((code: number) => process.exit(code));
  const log = options.log ?? ((line: string) => console.log(line));
  let stopping = false;
  const onSignal = () => {
    if (stopping) return;
    stopping = true;
    log(`${name}: stopping once the Firebird server has stopped`);
    const check = async () => {
      if (await serverStopped(options.port)) {
        log(`${name}: the Firebird server has stopped, exiting`);
        exit(0);
        return;
      }
      setTimeout(() => void check(), options.pollMs ?? STOP_POLL_MS);
    };
    void check();
  };
  process.on('SIGTERM', onSignal);
  process.on('SIGINT', onSignal);
}
