import type { StandardSchemaV1 } from '@standard-schema/spec'
import type { Queue } from './queue'
import type { QueueEvent } from './queue/types'
import { deserialize } from './serializer'

export type WorkflowEvent<Output, Progress = never> =
  | { type: 'started'; attempt: number }
  | ([Progress] extends [never] ? never : { type: 'progress'; data: Progress })
  | { type: 'completed'; output: Output }
  | { type: 'failed'; error: Error }

export class WorkflowJob<Output, Progress = never> {
  private queue
  private jobId
  private progressSchema
  private readonly watchBeforeEnqueue
  id

  constructor(opts: {
    queue: Queue
    jobId: string
    progressSchema?: StandardSchemaV1<unknown, Progress>
    watchBeforeEnqueue?: boolean
  }) {
    this.queue = opts.queue
    this.jobId = opts.jobId
    this.progressSchema = opts.progressSchema
    this.watchBeforeEnqueue = opts.watchBeforeEnqueue
    this.id = opts.jobId
  }

  /** Stream events published after this watcher attaches. */
  async watch(opts?: {
    signal?: AbortSignal
  }): Promise<ReadableStream<WorkflowEvent<Output, Progress>>> {
    const events = await this.queue.watch(this.jobId, {
      ...opts,
      allowMissing: this.watchBeforeEnqueue,
    })
    const progressSchema = this.progressSchema
    const jobId = this.jobId
    return events.pipeThrough(
      new TransformStream<QueueEvent, WorkflowEvent<Output, Progress>>({
        async transform(event, controller) {
          if (event.type === 'completed') {
            controller.enqueue({ type: 'completed', output: deserialize<Output>(event.output) })
          } else if (event.type === 'progress') {
            if (!progressSchema) throw new Error(`Job ${jobId} emitted undeclared progress`)
            const parsed = await progressSchema['~standard'].validate(deserialize(event.data))
            if (parsed.issues)
              throw new Error(`Invalid workflow progress for job ${jobId}`, {
                cause: parsed.issues,
              })
            controller.enqueue({ type: 'progress', data: parsed.value } as WorkflowEvent<
              Output,
              Progress
            >)
          } else {
            controller.enqueue(event)
          }
        },
      }),
    )
  }

  /**
   * Block until the job finishes and return its output. Propagates the workflow's own failure,
   * `TimeoutError` (if `timeoutMs` elapses), and `ResultExpiredError` (past the result window).
   */
  async wait(timeoutMs?: number): Promise<Output> {
    const raw = await this.queue.wait(this.jobId, { timeoutMs })
    return deserialize<Output>(raw)
  }
}
