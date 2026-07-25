import type Redis from 'ioredis'

export interface NamespaceOptions {
  /** Namespace id — scopes the cross-workflow concurrency cap. */
  id: string
  /** Namespace concurrency ceiling across all its workflows. Default: unlimited. */
  concurrency?: number
  /** Shared redis connection, owned by the namespace. */
  redis: Redis
  /** Global key prefix. Default: `wf`. */
  prefix?: string
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

export interface AddOptions {
  /** Group id. Default: a random UUID (every add is its own group). */
  groupId?: string
  /** Numeric priority, 0…2^21-1, higher runs first. Default: 0. */
  priority?: number
  /** Absolute epoch-ms instant to run at → delayed ZSET. Mutually exclusive with `runIn`. */
  runAt?: number
  /** Delay in ms before running (`runAt = redisNow + runIn`). Mutually exclusive with `runAt`. */
  runIn?: number
  /** Per-job override of the worker's `maxAttempts` default. */
  maxAttempts?: number
  /** Explicit job id. Default: a random UUID. */
  jobId?: string
}

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
  /** Called with a worker-internal error (best-effort). Defaults to `Settings.logger`. */
  onError?: (error: unknown) => void
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
}

export interface JobContext {
  signal: AbortSignal
}
