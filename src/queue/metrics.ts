import type { Meter } from '@opentelemetry/api'

export interface MetricsOptions {
  meter: Meter
  prefix: string
  /** Record `<prefix>_workflow_step_duration_ms` per `step.do`. Off by default: step names can be high-cardinality. */
  stepDuration?: boolean
}

/** What one handler attempt ended as, once Redis committed it. */
export type AttemptOutcome = 'completed' | 'retried' | 'failed' | 'rate_limited'

// The SDK default stops at 10s, too short for a backlog or a long handler.
const MS_BUCKETS = [
  5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10_000, 30_000, 60_000, 300_000, 900_000,
  3_600_000,
]

/** The worker's OTel instruments for one workflow, every point tagged with `workflow_id`. */
export function createWorkerMetrics(
  { meter, prefix, stepDuration }: MetricsOptions,
  workflowId: string,
) {
  const attributes = { workflow_id: workflowId }
  const ms = (name: string, description: string) =>
    meter.createHistogram(`${prefix}_${name}`, {
      description,
      unit: 'ms',
      advice: { explicitBucketBoundaries: MS_BUCKETS },
    })

  const queueWait = ms(
    'workflow_queue_wait_ms',
    'Time a job waited in the queue before a worker claimed it',
  )
  const duration = ms('workflow_duration_ms', 'Time a handler attempt ran')
  const jobs = meter.createCounter(`${prefix}_workflow_jobs`, {
    description: 'Handler attempts by outcome',
  })
  const attempts = meter.createHistogram(`${prefix}_workflow_attempts`, {
    description: 'Attempts a job used before it completed or was dead-lettered',
    advice: { explicitBucketBoundaries: [1, 2, 3, 5, 10, 25] },
  })
  const endToEnd = ms(
    'workflow_end_to_end_ms',
    'Time from enqueue to completion or dead-letter, including retry backoff',
  )
  const stalled = meter.createCounter(`${prefix}_workflow_stalled`, {
    description: 'Jobs recovered from a lost lock, by whether they were requeued or dead-lettered',
  })
  const fireDelay = ms(
    'schedule_fire_delay_ms',
    'How late a cron occurrence was enqueued after its due time',
  )
  const step = stepDuration
    ? ms('workflow_step_duration_ms', 'Time a step.do ran, cached replays excluded')
    : undefined

  return {
    claimed(queueWaitMs: number) {
      queueWait.record(queueWaitMs, attributes)
    },
    /** `attemptsUsed` and `endToEndMs` only matter for terminal outcomes. */
    attempt(outcome: AttemptOutcome, durationMs: number, attemptsUsed: number, endToEndMs: number) {
      const outcomeAttributes = { ...attributes, outcome }
      duration.record(durationMs, outcomeAttributes)
      jobs.add(1, outcomeAttributes)
      if (outcome === 'completed' || outcome === 'failed') {
        attempts.record(attemptsUsed, outcomeAttributes)
        endToEnd.record(endToEndMs, outcomeAttributes)
      }
    },
    stalled(requeued: number, deadLettered: [attempts: number, ageMs: number][]) {
      if (requeued > 0) stalled.add(requeued, { ...attributes, result: 'requeued' })
      if (deadLettered.length === 0) return
      stalled.add(deadLettered.length, { ...attributes, result: 'failed' })
      const failed = { ...attributes, outcome: 'failed' }
      for (const [attemptsUsed, ageMs] of deadLettered) {
        attempts.record(attemptsUsed, failed)
        endToEnd.record(ageMs, failed)
      }
    },
    fired(scheduleId: string, delayMs: number) {
      fireDelay.record(delayMs, { ...attributes, schedule_id: scheduleId })
    },
    recordStep:
      step &&
      ((name: string, durationMs: number) =>
        step.record(durationMs, { ...attributes, step: name })),
  }
}

export type WorkerMetrics = ReturnType<typeof createWorkerMetrics>
