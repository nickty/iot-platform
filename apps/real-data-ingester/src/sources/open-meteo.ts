import { getLogger } from '@iot/observability';
import type { TelemetryEvent } from '@iot/contracts';
import { v7 as uuidv7 } from 'uuid';

/**
 * Cities to poll for weather data.
 * Each city becomes a "virtual device" in our pipeline.
 */
export interface City {
  name: string;
  deviceId: string;
  lat: number;
  lon: number;
  region: string;
}

export const CITIES: City[] = [
  { name: 'San Francisco', deviceId: 'weather-sf-001', lat: 37.7749, lon: -122.4194, region: 'us-west' },
  { name: 'New York', deviceId: 'weather-ny-001', lat: 40.7128, lon: -74.0060, region: 'us-east' },
  { name: 'London', deviceId: 'weather-lon-001', lat: 51.5074, lon: -0.1278, region: 'eu-west' },
  { name: 'Tokyo', deviceId: 'weather-tyo-001', lat: 35.6762, lon: 139.6503, region: 'ap-northeast' },
  { name: 'Sydney', deviceId: 'weather-syd-001', lat: -33.8688, lon: 151.2093, region: 'ap-southeast' },
  { name: 'Dubai', deviceId: 'weather-dxb-001', lat: 25.2048, lon: 55.2708, region: 'me-south' },
  { name: 'Singapore', deviceId: 'weather-sin-001', lat: 1.3521, lon: 103.8198, region: 'ap-southeast' },
  { name: 'Berlin', deviceId: 'weather-ber-001', lat: 52.5200, lon: 13.4050, region: 'eu-central' },
];

/**
 * Fetch current weather for a city.
 * Open-Meteo requires no API key — just call the endpoint.
 */
export async function fetchWeather(city: City): Promise<TelemetryEvent | null> {
  const logger = getLogger();

  const url = new URL('https://api.open-meteo.com/v1/forecast');
  url.searchParams.set('latitude', String(city.lat));
  url.searchParams.set('longitude', String(city.lon));
  url.searchParams.set('current', 'temperature_2m,wind_speed_10m,relative_humidity_2m,weather_code');
  url.searchParams.set('timezone', 'UTC');

  try {
    const res = await fetch(url.toString(), {
      headers: { 'User-Agent': 'iot-platform-practice/1.0' },
    });

    if (!res.ok) {
      logger.warn({ city: city.name, status: res.status }, 'Open-Meteo request failed');
      return null;
    }

    const data = (await res.json()) as {
      current?: {
        time?: string;
        temperature_2m?: number;
        wind_speed_10m?: number;
        relative_humidity_2m?: number;
        weather_code?: number;
      };
    };

    const current = data.current;
    if (!current) {
      logger.warn({ city: city.name }, 'No current data in response');
      return null;
    }

    const now = new Date().toISOString();

    // Map weather code to human-readable condition
    const weatherConditions: Record<number, string> = {
      0: 'Clear', 1: 'Mainly Clear', 2: 'Partly Cloudy', 3: 'Overcast',
      45: 'Fog', 48: 'Rime Fog', 51: 'Light Drizzle', 53: 'Drizzle',
      55: 'Heavy Drizzle', 61: 'Light Rain', 63: 'Rain', 65: 'Heavy Rain',
      71: 'Light Snow', 73: 'Snow', 75: 'Heavy Snow', 80: 'Rain Showers',
      81: 'Heavy Showers', 82: 'Violent Showers', 95: 'Thunderstorm',
      96: 'Thunderstorm Hail', 99: 'Severe Thunderstorm',
    };

    return {
      eventId: uuidv7(),
      deviceId: city.deviceId,
      timestamp: current.time ? new Date(current.time).toISOString() : now,
      ingestedAt: now,
      schemaVersion: '1.0',
      metrics: {
        temperature: current.temperature_2m ?? 0,
        humidity: current.relative_humidity_2m ?? 0,
        wind_speed: current.wind_speed_10m ?? 0,
        weather_code: current.weather_code ?? 0,
      },
      tags: {
        region: city.region,
        source: 'open-meteo',
        city: city.name,
        condition: weatherConditions[current.weather_code ?? 0] ?? 'Unknown',
        lat: String(city.lat),
        lon: String(city.lon),
      },
      source: 'http',
    };
  } catch (err) {
    logger.error({ err, city: city.name }, 'Open-Meteo fetch error');
    return null;
  }
}