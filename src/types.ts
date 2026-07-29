import type { Queue } from './queue'

/** Decoded job payload. Serialized to an opaque `data` string; step data lives in the `:steps` hash. */
export interface WorkflowJobPayloadInternal {
  input: unknown
  tracingHeaders: unknown
}

export type WorkflowQueueInternal = Queue
