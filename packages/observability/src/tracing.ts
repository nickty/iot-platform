import { NodeSDK } from '@opentelemetry/sdk-node';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { getNodeAutoInstrumentations } from '@opentelemetry/auto-instrumentations-node';
import { Resource } from '@opentelemetry/resources';
import { ATTR_SERVICE_NAME, ATTR_SERVICE_VERSION } from '@opentelemetry/semantic-conventions';
import { getConfig } from '@iot/config';

let sdk: NodeSDK | undefined;

export function startTracing(): void {
  const config = getConfig();
  if (!config.OTEL_ENABLED || sdk) return;

  sdk = new NodeSDK({
    resource: new Resource({
      [ATTR_SERVICE_NAME]: config.OTEL_SERVICE_NAME,
      [ATTR_SERVICE_VERSION]: '0.1.0',
    }),
    traceExporter: new OTLPTraceExporter({
      url: `${config.OTEL_EXPORTER_OTLP_ENDPOINT}/v1/traces`,
    }),
    instrumentations: [
      getNodeAutoInstrumentations({
        '@opentelemetry/instrumentation-fs': { enabled: false },
      }),
    ],
  });

  sdk.start();
  process.on('SIGTERM', () => {
    sdk?.shutdown().catch((err) => console.error('OTel shutdown error', err));
  });
}