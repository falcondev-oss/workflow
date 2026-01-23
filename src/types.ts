import type { Job, Queue } from 'bullmq'
import type { Serialized } from './serializer'
import type { WorkflowStepData } from './step'

export type WorkflowJobInternal<Input, Output> = Job<
  Serialized<{
    input: Input
    stepData: Record<string, WorkflowStepData>
    tracingHeaders: unknown
  }>,
  Serialized<Output>,
  string
>

export type WorkflowQueueInternal<Input, Output> = Queue<WorkflowJobInternal<Input, Output>>
