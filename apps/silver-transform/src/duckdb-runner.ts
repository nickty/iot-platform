import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { getConfig } from '@iot/config';
import { getLogger } from '@iot/observability';

const execFileAsync = promisify(execFile);

export interface DuckDbResult<T> {
  rows: T[];
  durationMs: number;
}

/**
 * Run a DuckDB SQL statement and return JSON rows.
 * DuckDB CLI supports `-json` output, which we parse.
 */
export async function runDuckDb<T = Record<string, unknown>>(
  sql: string,
): Promise<DuckDbResult<T>> {
  const config = getConfig();
  const logger = getLogger();
  const startTime = performance.now();

  try {
    const { stdout, stderr } = await execFileAsync(
      config.DUCKDB_PATH,
      ['-json', '-c', sql],
      { maxBuffer: 512 * 1024 * 1024 },      // 512 MB output buffer
    );

    if (stderr && stderr.trim()) {
      logger.warn({ stderr }, 'DuckDB warning');
    }

    const rows = stdout.trim() ? (JSON.parse(stdout) as T[]) : [];
    const durationMs = performance.now() - startTime;

    logger.debug({ rows: rows.length, durationMs }, 'DuckDB query complete');
    return { rows, durationMs };
  } catch (err) {
    logger.error({ err, sql: sql.slice(0, 200) }, 'DuckDB query failed');
    throw err;
  }
}

/**
 * Run a DuckDB SQL statement that doesn't return rows (CREATE, INSERT, COPY).
 */
export async function runDuckDbNoResult(sql: string): Promise<void> {
  const config = getConfig();
  const logger = getLogger();
  const startTime = performance.now();

  try {
    const { stderr } = await execFileAsync(
      config.DUCKDB_PATH,
      ['-c', sql],
      { maxBuffer: 512 * 1024 * 1024 },
    );

    if (stderr && stderr.trim()) {
      logger.warn({ stderr }, 'DuckDB warning');
    }

    logger.debug(
      { durationMs: (performance.now() - startTime).toFixed(1) },
      'DuckDB statement complete',
    );
  } catch (err) {
    logger.error({ err, sql: sql.slice(0, 200) }, 'DuckDB statement failed');
    throw err;
  }
}