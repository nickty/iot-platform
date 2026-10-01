import { Kafka, type Consumer, type Admin } from 'kafkajs';
import { getConfig } from '@iot/config';
import { getLogger, kafkaConsumerLag } from '@iot/observability';

/**
 * Periodically query Kafka for consumer lag and expose it as a Prometheus gauge.
 * Consumer lag = (latest offset in partition) - (committed offset by the group)
 */
export class LagMonitor {
  private admin: Admin | undefined;
  private timer: NodeJS.Timeout | undefined;
  private running = false;

  constructor(
    private groupId: string,
    private topics: string[],
    private intervalMs = 15000,
  ) {}

  async start(): Promise<void> {
    const config = getConfig();
    const logger = getLogger();

    const kafka = new Kafka({
      clientId: `${config.KAFKA_CLIENT_ID}-lag-monitor`,
      brokers: config.KAFKA_BROKERS.split(','),
    });
    this.admin = kafka.admin();
    await this.admin.connect();

    this.timer = setInterval(() => void this.check(), this.intervalMs);
    logger.info({ groupId: this.groupId, topics: this.topics }, 'Lag monitor started');
  }

  private async check(): Promise<void> {
    if (this.running || !this.admin) return;
    this.running = true;

    try {
      const logger = getLogger();

      const [groupOffsets, topicOffsets] = await Promise.all([
        this.admin.fetchOffsets({ groupId: this.groupId, topics: this.topics }),
        this.admin.fetchTopicOffsets('telemetry.raw'),
      ]);

      // Build map of topic-partition → latest offset
      const latestByPartition = new Map<string, number>();
      for (const t of topicOffsets) {
        latestByPartition.set(`${t.partition}`, Number(t.offset));
      }

      // Compare against committed offsets
      for (const t of groupOffsets) {
        for (const p of t.partitions) {
          const latest = latestByPartition.get(`${p.partition}`) ?? 0;
          const committed = Number(p.offset);
          const lag = Math.max(0, latest - committed);

          kafkaConsumerLag.set(
            { group: this.groupId, topic: t.topic, partition: `${p.partition}` },
            lag,
          );
        }
      }

      logger.debug('Lag monitor updated');
    } catch (err) {
      const logger = getLogger();
      logger.warn({ err }, 'Lag monitor check failed');
    } finally {
      this.running = false;
    }
  }

  async stop(): Promise<void> {
    if (this.timer) clearInterval(this.timer);
    if (this.admin) await this.admin.disconnect();
    this.admin = undefined;
  }
}