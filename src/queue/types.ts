import type Redis from 'ioredis'

export type WorkflowLogger = {
  info?: (...data: any[]) => void
  success?: (...data: any[]) => void
  error?: (...data: any[]) => void
  debug?: (...data: any[]) => void
  warn?: (...data: any[]) => void
}

export interface NamespaceOptions {
  /** Namespace id — scopes the cross-workflow concurrency cap. */
  id: string
  /** Namespace concurrency ceiling across all its workflows. Default: unlimited. */
  concurrency?: number
  /** Shared redis connection, owned by the namespace. */
  redis: Redis
  /** Global key prefix. Default: `wf`. */
  prefix?: string
  /** Logger inherited by every queue and worker of this namespace. Default: no logging. */
  logger?: WorkflowLogger
}

export interface QueueOptions {
  /** Workflow id. */
  id: string
  /** Per-workflow concurrency ceiling. Default: unlimited. */
  concurrency?: number
  /** Per-group concurrency cap. Default: 1. */
  groupConcurrency?: number
  /** TTL (seconds) of the `wait()` result record. Default: 300. */
  resultTtl?: number
}

interface AddOptionsBase {
  /** Group id. Default: a random UUID (every add is its own group). */
  groupId?: string
  /** Numeric priority, 0…2^21-1, higher runs first. Default: 0. */
  priority?: number
  /** Per-job override of the worker's `maxAttempts` default. */
  maxAttempts?: number
  /** Explicit job id. Default: a random UUID. */
  jobId?: string
}

/** Options for {@link Queue.add}. `runAt` and `runIn` are mutually exclusive. */
export type AddOptions = AddOptionsBase &
  (
    | {
        /** Absolute epoch-ms timestamp to run the job at. Mutually exclusive with `runIn`. */
        runAt?: number
        runIn?: never
      }
    | {
        /** Delay in ms before the job runs. Mutually exclusive with `runAt`. */
        runIn?: number
        runAt?: never
      }
  )

export interface WorkerOptions {
  /** Per-process (per-worker) concurrency cap. Default: 1. */
  concurrency?: number
  /** Lock/heartbeat TTL (ms) held while a job runs. Default: 30_000. */
  lockMs?: number
  /** Times a job may be recovered from a stall before being dead-lettered. Default: 1. */
  maxStalledCount?: number
  /** Min interval (ms) between stalled-recovery scans across all workers. Default: 30_000. */
  stalledInterval?: number
  /** Backstop `BRPOP` re-poll timeout (seconds). Default: 5. */
  safetyTimeout?: number
  /** Max due delayed jobs promoted per `reserve` call; drains larger backlogs in chunks. Default: 500. */
  promoteBatchSize?: number
  /** Default `maxAttempts` for jobs of this workflow. Default: 1. */
  maxAttempts?: number
  /** Retry backoff — ms before the given (1-based) attempt. Per-workflow only. Default: `expBackoff()`. */
  backoff?: (attempt: number) => number
  /** Max retained failed jobs (count-trimmed by `finishedOn`). Default: 100. */
  keepFailed?: number
  /** Called with a worker-internal/unexpected error (best-effort). Defaults to the namespace logger. */
  onError?: (error: unknown) => void
  /** Called with the job + error each time a handler throws and the job fails (best-effort). Defaults to the namespace logger. */
  onFailed?: (job: ReservedJob, error: unknown) => void
}

export interface ScheduleOptions {
  /** Cron pattern (Croner syntax). No `every` interval — pattern only. */
  pattern: string
  /** Opaque data string enqueued for every occurrence. */
  data: string
  /** IANA timezone. Default: the queuer's local zone captured at register time. */
  tz?: string
  /** Numeric priority for occurrences, 0…2^21-1. Default: 0. */
  priority?: number
  /** Group id for occurrences. Default: the `scheduleId` (serializes occurrences via group cap). */
  groupId?: string
  /** Skip an occurrence while the previous one is still running. Default: true. */
  skipIfRunning?: boolean
}

export interface ScheduleInfo {
  scheduleId: string
  pattern: string
  tz: string
  /** Next-fire epoch ms (the `schedules:due` score). */
  nextRun: number
  /** Epoch ms the last occurrence was enqueued; `null` if it has never fired. */
  lastFireAt: number | null
  /** Job id of the last enqueued occurrence; `null` if it has never fired. */
  lastJobId: string | null
}

/** Point-in-time queue-depth gauges, read on demand — never maintained by counters. */
export interface QueueMetrics {
  /** Jobs currently claimed and running (`ZCARD wf:active`). */
  active: number
  /** Jobs waiting to run, summed over the active-groups set (`Σ ZCARD wf:g:*:jobs`). */
  waiting: number
  /** Jobs parked for a future `runAt` (`ZCARD wf:delayed`). */
  delayed: number
}

export interface WaitOptions {
  /** Reject with `TimeoutError` after this many ms. No default (waits forever). */
  timeoutMs?: number
}

/** A job handed to a worker handler — a frozen value, not a class. */
export interface ReservedJob {
  id: string
  groupId: string
  data: string
  attemptsMade: number
  priority: number
  /**
   * The job's persisted step data, read atomically with the claim. Empty on a first attempt;
   * on a replay it is the complete memo, so resolving a cached step costs no round-trip.
   */
  steps: Map<string, string>
}

export interface JobContext {
  signal: AbortSignal
}
