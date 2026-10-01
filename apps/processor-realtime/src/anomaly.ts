import type Redis from 'ioredis';

const WINDOW_SIZE = 100;           // rolling window of samples
const Z_THRESHOLD = 3.0;           // |z| > 3 is an anomaly
const MIN_SAMPLES = 20;            // need at least this many to detect

/**
 * Track rolling statistics for a device/metric combination.
 * Uses Redis sorted sets for the rolling window (score = timestamp).
 */
export async function detectAnomaly(
  redis: Redis,
  deviceId: string,
  metric: string,
  value: number,
): Promise<{ isAnomaly: boolean; zScore: number; mean: number; stdDev: number } | null> {
  const key = `stats:${deviceId}:${metric}`;
  const now = Date.now();

  // Add current value to rolling window
  await redis.zadd(key, now, `${now}:${value}`);

  // Trim to keep only the last WINDOW_SIZE samples
  const count = await redis.zcard(key);
  if (count > WINDOW_SIZE) {
    await redis.zremrangebyrank(key, 0, count - WINDOW_SIZE - 1);
  }

  // Get all values in the window
  const entries = await redis.zrange(key, 0, -1);
  if (entries.length < MIN_SAMPLES) {
    return null; // not enough data yet
  }

  // Extract numeric values from "timestamp:value" format
  const values = entries.map((e) => {
    const parts = e.split(':');
    return parseFloat(parts[1] ?? '0');
  });

  // Compute mean
  const mean = values.reduce((a, b) => a + b, 0) / values.length;

  // Compute standard deviation
  const variance = values.reduce((sum, v) => sum + (v - mean) ** 2, 0) / values.length;
  const stdDev = Math.sqrt(variance);

  // Avoid division by zero (constant signal)
  if (stdDev === 0) {
    return { isAnomaly: false, zScore: 0, mean, stdDev: 0 };
  }

  const zScore = Math.abs((value - mean) / stdDev);

  return {
    isAnomaly: zScore > Z_THRESHOLD,
    zScore,
    mean,
    stdDev,
  };
}