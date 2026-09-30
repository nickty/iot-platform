import { z } from 'zod';

export const DeviceMetadataSchema = z.object({
  deviceId: z.string().min(1).max(128),
  name: z.string().min(1).max(128),
  model: z.string().max(64).optional(),
  region: z.string().max(64).optional(),
  tags: z.record(z.string(), z.string()).optional(),
});

export type DeviceMetadata = z.infer<typeof DeviceMetadataSchema>;