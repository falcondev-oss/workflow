import type Redis from 'ioredis'

export type WorkflowLogger = {
  info?: (...data: any[]) => void
  success?: (...data: any[]) => void
  error?: (...data: any[]) => void
  debug?: (...data: any[]) => void
  warn?: (...data: any[]) => void
}

export interface NamespaceOptions {
  id: string
  concurrency?: number
  redis: Redis
  prefix?: string
  logger?: WorkflowLogger
  rateLimiters?: Record<string, RateLimiterBudget>
}

export interface RateLimiterMetrics {
  starts: number
  pausedMs: number
}

export interface RateLimiterBudget {
  limit: number
  window: number
}

export interface QueueOptions {
  id: string
  concurrency?: number
  groupConcurrency?: number
  resultTtl?: number
  rateLimiters?: readonly string[]
}

interface AddOptionsBase {
  groupId?: string
  priority?: number
  maxAttempts?: number
  jobId?: string
}

export type AddOptions = AddOptionsBase &
  (
    | {
        runAt?: number
        runIn?: never
      }
    | {
        runIn?: number
        runAt?: never
      }
  )

export interface WorkerOptions {
  concurrency?: number
  lockMs?: number
  maxStalledCount?: number
  stalledInterval?: number
  safetyTimeout?: number
  promoteBatchSize?: number
  maxAttempts?: number
  backoff?: (attempt: number) => number
  keepFailed?: number
  onError?: (error: unknown) => void
  onFailed?: (job: ReservedJob, error: unknown) => void
}

export interface ScheduleOptions {
  pattern: string
  data: string
  tz?: string
  priority?: number
  groupId?: string
  skipIfRunning?: boolean
}

export interface ScheduleInfo {
  scheduleId: string
  pattern: string
  tz: string
  nextRun: number
  lastFireAt: number | null
  lastJobId: string | null
}

export interface QueueMetrics {
  active: number
  waiting: number
  delayed: number
  failed: number
}

export interface WaitOptions {
  timeoutMs?: number
}

export interface WatchOptions {
  signal?: AbortSignal
  allowMissing?: boolean
}

export type QueueEvent =
  | { type: 'started'; attempt: number }
  | { type: 'progress'; data: string }
  | { type: 'completed'; output: string }
  | { type: 'failed'; error: Error }

export interface ReservedJob {
  id: string
  groupId: string
  data: string
  attemptsMade: number
  priority: number
  steps: Map<string, string>
  queueWaitMs: number
}

export interface JobContext {
  signal: AbortSignal
}
