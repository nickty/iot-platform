import { v4 as uuidv4 } from 'uuid';
import type { TelemetryEvent } from '@iot/contracts';

export interface SimulatedDevice {
  deviceId: string;
  name: string;
  region: string;
  model: string;
  baseTemperature: number;
  baseHumidity: number;
}

/**
 * Create a device with a distinct baseline so charts look realistic.
 */
export function createDevice(index: number): SimulatedDevice {
  return {
    deviceId: `dev-${String(index).padStart(4, '0')}`,
    name: `Sensor ${index}`,
    region: ['us-east', 'us-west', 'eu-west'][index % 3]!,
    model: ['DHT22', 'BME280', 'SHT31'][index % 3]!,
    baseTemperature: 20 + Math.random() * 5,
    baseHumidity: 40 + Math.random() * 20,
  };
}

/**
 * Generate a realistic telemetry event with:
 * - Sinusoidal drift (day/night cycle)
 * - Gaussian noise (sensor jitter)
 * - Occasional spike (interference)
 */
export function generateTelemetry(device: SimulatedDevice): TelemetryEvent {
  const t = Date.now() / 1000;
  const sinusoid = Math.sin(t / 60) * 2;
  const noise = (Math.random() - 0.5) * 0.5;
  const spike = Math.random() < 0.02 ? (Math.random() - 0.5) * 10 : 0;

  const temperature = device.baseTemperature + sinusoid + noise + spike;
  const humidity = device.baseHumidity + Math.sin(t / 90) * 5 + (Math.random() - 0.5) * 2;
  const pressure = 1013 + Math.sin(t / 300) * 5 + (Math.random() - 0.5) * 0.5;

  return {
    eventId: uuidv4(),
    deviceId: device.deviceId,
    timestamp: new Date().toISOString(),
    ingestedAt: new Date().toISOString(),
    schemaVersion: '1.0',
    metrics: {
      temperature: Number(temperature.toFixed(2)),
      humidity: Number(humidity.toFixed(2)),
      pressure: Number(pressure.toFixed(2)),
    },
    tags: {
      region: device.region,
      model: device.model,
    },
    source: 'replay',
  };
}