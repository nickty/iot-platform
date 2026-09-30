import { config as loadDotenv } from 'dotenv';
import { z } from 'zod';

// Load .env from the process's current directory
loadDotenv();

const ConfigSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  LOG_LEVEL: z.enum(['trace', 'debug', 'info', 'warn', 'error', 'fatal']).default('info'),

  // Kafka
  KAFKA_BROKERS: z.string().default('localhost:19092'),
  KAFKA_CLIENT_ID: z.string().default('iot-platform'),

  // TimescaleDB
  POSTGRES_HOST: z.string().default('localhost'),
  POSTGRES_PORT: z.coerce.number().default(5432),
  POSTGRES_USER: z.string().default('iot'),
  POSTGRES_PASSWORD: z.string().default('iot_dev_password'),
  POSTGRES_DB: z.string().default('iot'),

  // Redis
  REDIS_HOST: z.string().default('localhost'),
  REDIS_PORT: z.coerce.number().default(6379),

  // S3 / LocalStack
  S3_ENDPOINT: z.string().default('http://localhost:4566'),
  S3_REGION: z.string().default('us-east-1'),
  S3_ACCESS_KEY: z.string().default('test'),
  S3_SECRET_KEY: z.string().default('test'),
  S3_BUCKET_RAW: z.string().default('raw'),
  S3_BUCKET_CURATED: z.string().default('curated'),
  S3_FORCE_PATH_STYLE: z.coerce.boolean().default(true),

  // OpenTelemetry
  OTEL_EXPORTER_OTLP_ENDPOINT: z.string().default('http://localhost:4320'),
  OTEL_SERVICE_NAME: z.string().default('iot-platform'),
  OTEL_ENABLED: z.coerce.boolean().default(false),

  // API
  API_PORT: z.coerce.number().default(3000),
});

export type AppConfig = z.infer<typeof ConfigSchema>;

let cached: AppConfig | undefined;

/**
 * Load and validate config. Caches on first call.
 * Throws with a clear message if anything is invalid.
 */
export function getConfig(): AppConfig {
  if (cached) return cached;

  const parsed = ConfigSchema.safeParse(process.env);
  if (!parsed.success) {
    console.error('❌ Invalid environment configuration:');
    console.error(parsed.error.format());
    throw new Error('Invalid environment configuration');
  }
  console.log('✅ Environment configuration loaded successfully');
  cached = parsed.data;
  return cached;
}