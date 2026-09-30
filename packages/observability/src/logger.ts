import pino from 'pino';
import { getConfig } from '@iot/config';

let logger: pino.Logger | undefined;

export function getLogger(): pino.Logger {
  if (logger) return logger;

  const config = getConfig();
  logger = pino({
    level: config.LOG_LEVEL,
    base: {
      service: config.OTEL_SERVICE_NAME,
      env: config.NODE_ENV,
    },
    timestamp: pino.stdTimeFunctions.isoTime,
  });
  return logger;
}