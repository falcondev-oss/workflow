import type { Job, Queue } from 'groupmq'
import type { Serialized } from './serializer'
import type { WorkflowStepData } from './step'

export type WorkflowJobPayloadInternal<Input> = Serialized<{
  input: Input | undefined
  stepData: Record<string, WorkflowStepData>
  tracingHeaders: unknown
}>
export type WorkflowJobInternal<Input> = Job<WorkflowJobPayloadInternal<Input>>
export type WorkflowQueueInternal<Input> = Queue<WorkflowJobPayloadInternal<Input>>
