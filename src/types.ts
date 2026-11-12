import type { Queue } from './queue'

export interface WorkflowJobPayloadInternal {
  input: unknown
  tracingHeaders: unknown
}

export type WorkflowQueueInternal = Queue
