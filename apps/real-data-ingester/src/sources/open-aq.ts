import { getLogger } from '@iot/observability';
import type { TelemetryEvent } from '@iot/contracts';
import { v7 as uuidv7 } from 'uuid';

export interface AirQualityLocation {
  locationId: string;
  city: string;
  country: string;
  lat: number;
  lon: number;
  deviceId: string;
}

// Major cities with known OpenAQ coverage
export const AIR_QUALITY_LOCATIONS: AirQualityLocation[] = [
  { locationId: '2178', city: 'Los Angeles', country: 'US', lat: 34.0522, lon: -118.2437, deviceId: 'aq-la-001' },
  { locationId: '8118', city: 'London', country: 'GB', lat: 51.5074, lon: -0.1278, deviceId: 'aq-lon-001' },
  { locationId: '155', city: 'Delhi', country: 'IN', lat: 28.6139, lon: 77.2090, deviceId: 'aq-del-001' },
  { locationId: '2473', city: 'Beijing', country: 'CN', lat: 39.9042, lon: 116.4074, deviceId: 'aq-bei-001' },
];

/**
 * Fetch latest air quality measurements.
 * OpenAQ v3 API — public endpoints for location discovery.
 * Requires API key for higher usage; basic access works without.
 */
export async function fetchAirQuality(
  location: AirQualityLocation,
  apiKey?: string,
): Promise<TelemetryEvent | null> {
  const logger = getLogger();

  const url = new URL(`https://api.openaq.org/v3/locations/${location.locationId}`);
  
  const headers: Record<string, string> = {
    'User-Agent': 'iot-platform-practice/1.0',
  };

  if (apiKey) {
    headers['X-API-Key'] = apiKey;
  }

  try {
    const res = await fetch(url.toString(), { headers });

    if (!res.ok) {
      logger.warn({ location: location.city, status: res.status }, 'OpenAQ request failed');
      return null;
    }

    const data = (await res.json()) as {
      results?: Array<{
        sensors?: Array<{
          parameter?: { name?: string };
          latest?: { value?: number; datetime?: string };
        }>;
      }>;
    };

    const result = data.results?.[0];
    if (!result?.sensors) {
      logger.warn({ location: location.city }, 'No sensors in response');
      return null;
    }

    const metrics: Record<string, number> = {};
    let latestTime: string | undefined;

    for (const sensor of result.sensors) {
      const paramName = sensor.parameter?.name;
      const value = sensor.latest?.value;
      if (paramName && value !== undefined) {
        metrics[paramName] = value;
        if (sensor.latest?.datetime) {
          latestTime = sensor.latest.datetime;
        }
      }
    }

    if (Object.keys(metrics).length === 0) {
      logger.warn({ location: location.city }, 'No metrics extracted');
      return null;
    }

    const now = new Date().toISOString();

    return {
      eventId: uuidv7(),
      deviceId: location.deviceId,
      timestamp: latestTime ? new Date(latestTime).toISOString() : now,
      ingestedAt: now,
      schemaVersion: '1.0',
      metrics,
      tags: {
        region: 'unknown',
        source: 'openaq',
        city: location.city,
        country: location.country,
        lat: String(location.lat),
        lon: String(location.lon),
      },
      source: 'http',
    };
  } catch (err) {
    logger.error({ err, city: location.city }, 'OpenAQ fetch error');
    return null;
  }
}