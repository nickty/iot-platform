import postgres from 'postgres';
import { getConfig } from '@iot/config';

let sql: ReturnType<typeof postgres> | undefined;

export function getDb(): ReturnType<typeof postgres> {
  if (sql) return sql;
  const config = getConfig();
  sql = postgres({
    host: config.POSTGRES_HOST,
    port: config.POSTGRES_PORT,
    database: config.POSTGRES_DB,
    username: config.POSTGRES_USER,
    password: config.POSTGRES_PASSWORD,
    max: 5,
    idle_timeout: 30,
    onnotice: () => {},
  });
  return sql;
}

export async function closeDb(): Promise<void> {
  if (sql) {
    await sql.end();
    sql = undefined;
  }
}