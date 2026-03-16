import { KubeConfig } from '@kubernetes/client-node';
import { logger } from './utils/logger';
import { Operator } from './operator';

async function main(): Promise<void> {
  const kubeConfig = new KubeConfig();

  // Load config from in-cluster service account when running inside a pod,
  // or fall back to local ~/.kube/config for development.
  if (process.env.KUBERNETES_SERVICE_HOST) {
    kubeConfig.loadFromCluster();
    logger.info('Loaded Kubernetes config from cluster');
  } else {
    kubeConfig.loadFromDefault();
    logger.info('Loaded Kubernetes config from default (local)');
  }

  const operator = new Operator(kubeConfig);

  // Graceful shutdown handlers
  const shutdown = (signal: string) => {
    logger.info({ signal }, 'Received shutdown signal');
    operator.stop();
    process.exit(0);
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));

  process.on('uncaughtException', (err) => {
    logger.fatal({ err }, 'Uncaught exception');
    process.exit(1);
  });

  process.on('unhandledRejection', (reason) => {
    logger.fatal({ reason }, 'Unhandled promise rejection');
    process.exit(1);
  });

  await operator.start();
}

main().catch((err) => {
  logger.fatal({ err }, 'Fatal error during startup');
  process.exit(1);
});
