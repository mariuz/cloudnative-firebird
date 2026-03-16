import pino, { Logger } from 'pino';

const isDevelopment = process.env.NODE_ENV !== 'production';

export const logger: Logger = pino(
  {
    level: process.env.LOG_LEVEL ?? 'info',
    base: {
      operator: 'cloudnative-firebird',
    },
  },
  isDevelopment
    ? pino.transport({ target: 'pino-pretty', options: { colorize: true } })
    : undefined,
);
