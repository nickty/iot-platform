import postgres from 'postgres';
import { getConfig } from '@iot/config';
import { getLogger } from '@iot/observability';

let sql: ReturnType<typeof postgres> | undefined;

export function getDb(): ReturnType<typeof postgres> {
  if (sql) return sql;

  const config = getConfig();
  const logger = getLogger();

  sql = postgres({
    host: config.POSTGRES_HOST,
    port: config.POSTGRES_PORT,
    database: config.POSTGRES_DB,
    username: config.POSTGRES_USER,
    password: config.POSTGRES_PASSWORD,
    max: 10,                    // connection pool size
    idle_timeout: 30,           // close idle connections after 30s
    connect_timeout: 10,
    onnotice: () => {},         // suppress NOTICE logs
  });

  logger.info('PostgreSQL client initialized');
  return sql;
}

export async function closeDb(): Promise<void> {
  if (sql) {
    await sql.end();
    sql = undefined;
  }
}