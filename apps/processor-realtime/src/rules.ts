import type { AlertRule } from '@iot/contracts';

/**
 * Alert rules — hardcoded for dev.
 * In production, these come from a DB and can be hot-reloaded.
 */
export const ALERT_RULES: AlertRule[] = [
  {
    ruleId: 'temp-high',
    deviceIdPattern: '*',
    metric: 'temperature',
    operator: '>',
    threshold: 30,
    severity: 'warning',
    message: 'Temperature above 30°C',
  },
  {
    ruleId: 'temp-critical',
    deviceIdPattern: '*',
    metric: 'temperature',
    operator: '>',
    threshold: 40,
    severity: 'critical',
    message: 'Temperature above 40°C — CRITICAL',
  },
  {
    ruleId: 'humidity-low',
    deviceIdPattern: '*',
    metric: 'humidity',
    operator: '<',
    threshold: 20,
    severity: 'info',
    message: 'Humidity below 20%',
  },
  {
    ruleId: 'pressure-drop',
    deviceIdPattern: '*',
    metric: 'pressure',
    operator: '<',
    threshold: 1000,
    severity: 'warning',
    message: 'Pressure below 1000 hPa',
  },
];

/**
 * Check which rules are breached by a given event.
 */
export function evaluateRules(
  deviceId: string,
  metrics: Record<string, number>,
): Array<{ rule: AlertRule; value: number }> {
  const breaches: Array<{ rule: AlertRule; value: number }> = [];

  for (const rule of ALERT_RULES) {
    // Skip rules that don't apply to this device
    if (rule.deviceIdPattern !== '*' && rule.deviceIdPattern !== deviceId) continue;

    const value = metrics[rule.metric];
    if (value === undefined) continue;

    let breached = false;
    switch (rule.operator) {
      case '>':  breached = value >  rule.threshold; break;
      case '<':  breached = value <  rule.threshold; break;
      case '>=': breached = value >= rule.threshold; break;
      case '<=': breached = value <= rule.threshold; break;
    }

    if (breached) breaches.push({ rule, value });
  }

  return breaches;
}