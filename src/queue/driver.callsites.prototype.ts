/**
 * PROTOTYPE — throwaway. The compile-proof for `driver.prototype.ts`: every place `Queue` and
 * `Worker` touch Redis today, rewritten against the seam. If a call site cannot be expressed here,
 * the interface is wrong. Nothing is executed — `tsc --noEmit` is the whole test.
 *
 * Twenty call sites in v0.13.1, by file:
 *   queue.ts   — enqueue, get(result), hset(step), multi(upsert), multi(remove), zrange+hmget,
 *                zcard×2 + smembers + pipeline(zcard), addWaiter, removeWaiter
 *   worker.ts  — reserve, hmget(schedule pattern/tz), fireSchedule, recoverStalled, complete,
 *                heartbeat, fail, brpop, lpush+ltrim, duplicate()
 *   namespace.ts — duplicate()+subscribe/unsubscribe, disconnect×2
 */

import type { ClaimedJob, WorkflowDriver } from './driver.prototype'
import type { AddOptions, ReservedJob, ScheduleOptions, WaitOptions } from './types'
import { randomUUID } from 'node:crypto'
import { JobAlreadyExistsError, NonRecoverableError, ResultExpiredError } from './errors'
import { localTimeZone, nextRunMs } from './schedule'

declare const driver: WorkflowDriver
declare const wfId: string
declare const concurrency: number
declare const groupConcurrency: number
declare const resultTtlMs: number

/* ───────────────────────────────────── Queue ───────────────────────────────────── */

async function add(data: string, opts?: AddOptions): Promise<{ id: string; groupId: string }> {
  const id = opts?.jobId ?? randomUUID()
  const groupId = opts?.groupId ?? randomUUID()
  // The `(runAt, runIn)` pair collapses to one absolute epoch-ms here, in JS.
  const runAt = opts?.runAt ?? (opts?.runIn === undefined ? null : Date.now() + opts.runIn)
  const outcome = await driver.enqueue({
    wfId,
    jobId: id,
    data,
    groupId,
    priority: opts?.priority ?? 0,
    maxAttempts: opts?.maxAttempts ?? 1,
    groupConcurrency,
    runAt,
  })
  // The string match on `err.message` is gone; the error class stays owned by the shared layer.
  if (outcome === 'duplicate') throw new JobAlreadyExistsError(id)
  return { id, groupId }
}

async function wait(jobId: string, opts?: WaitOptions): Promise<string> {
  const waiter = await driver.resultWaiter({ wfId, jobId })
  try {
    const first = await driver.getResult({ wfId, jobId })
    if (first !== null) return parseResult(first)

    const published =
      opts?.timeoutMs === undefined
        ? await waiter.published
        : await Promise.race([waiter.published, timeout(opts.timeoutMs)])
    if (published === null) throw new Error('TimeoutError')
    // Unchanged, and driver-agnostic: a non-record payload is a bare wake-up, so re-read.
    if (published.startsWith('{')) return parseResult(published)
    const stored = await driver.getResult({ wfId, jobId })
    if (stored === null) throw new ResultExpiredError(jobId)
    return parseResult(stored)
  } finally {
    void waiter.close()
  }
}

async function setStepData(jobId: string, stepName: string, value: string): Promise<void> {
  await driver.setStepData({ wfId, jobId, stepName, value })
}

async function upsertSchedule(scheduleId: string, opts: ScheduleOptions): Promise<void> {
  const tz = opts.tz ?? localTimeZone()
  const nextRun = nextRunMs(opts.pattern, tz)
  if (nextRun === null) throw new Error(`cron pattern has no next occurrence: ${opts.pattern}`)
  await driver.upsertSchedule({
    wfId,
    scheduleId,
    pattern: opts.pattern,
    tz,
    data: opts.data,
    priority: opts.priority ?? 0,
    groupId: opts.groupId ?? scheduleId,
    skipIfRunning: opts.skipIfRunning ?? true,
    nextRun,
  })
}

async function removeSchedule(scheduleId: string): Promise<void> {
  await driver.removeSchedule({ wfId, scheduleId })
}

// Both collapse to one op each: the N+1 (`zrange` then `hmget` per id) and the fan-out
// (`zcard`×2 + `smembers` + a `zcard` pipeline) were Redis access patterns, not contracts.
const getSchedules = async () => driver.getSchedules({ wfId })
const getMetrics = async () => driver.getMetrics({ wfId })

/* ───────────────────────────────────── Worker ───────────────────────────────────── */

async function loop(opts: {
  lockMs: number
  promoteBatchSize: number
  wakePollIntervalMs: number
}): Promise<void> {
  const waiter = driver.wakeWaiter({ wfId })
  let inFlight = 0
  let closing = false

  while (!closing) {
    // The cap is the driver's, not a constant in this file — 64 for Redis, 256 for Postgres.
    const want = Math.min(concurrency - inFlight, driver.maxReserveBatch)
    const res = await driver.reserve({
      wfId,
      concurrency,
      groupConcurrency,
      lockMs: opts.lockMs,
      promoteBatchSize: opts.promoteBatchSize,
      want,
    })
    // The public value type is minted here, once, not in each driver.
    for (const { job, token } of res.claims)
      void process_({ job: toReservedJob(job), token }, opts.lockMs)
    if (closing) break

    if (res.dueSchedules.length > 0) {
      await tickSchedules(res.dueSchedules)
      continue
    }
    const saturated = inFlight >= concurrency
    if (!saturated && res.claims.length === want) continue
    // `msToNext === 0` — a `null` here can no longer be confused with "due right now", which is
    // exactly what the `-1`/`0` sentinel pair made possible.
    if (!saturated && res.msToNext === 0 && !res.maxed) continue

    const nearest = [res.msToNext, res.msToSchedule].filter((m) => m !== null && m > 0) as number[]
    const timeoutMs =
      nearest.length === 0
        ? opts.wakePollIntervalMs
        : Math.min(Math.min(...nearest), opts.wakePollIntervalMs)
    if (saturated) await waitForSlot(timeoutMs)
    else await waiter.wait({ timeoutMs })
    if (!closing) void sweep(opts.promoteBatchSize)
  }
  closing = true
  // `waiter.close()` is the kick that unblocks this loop — there is no `wake()` op to call.
  await waiter.close()
}

async function tickSchedules(due: Awaited<ReturnType<WorkflowDriver['reserve']>>['dueSchedules']) {
  for (const { scheduleId, nextRun: expectedRun, pattern, tz } of due) {
    // #19: clamp the cron origin to `max(now, expectedRun)` so clock skew cannot re-fire the same
    // occurrence. `pattern`/`tz` came back with the claim, so the per-schedule read is gone.
    const origin = new Date(Math.max(Date.now(), expectedRun))
    const nextRun = nextRunMs(pattern, tz, origin)
    if (nextRun === null) continue
    const outcome = await driver.fireSchedule({
      wfId,
      scheduleId,
      expectedRun,
      nextRun,
      jobId: randomUUID(),
      groupConcurrency,
    })
    // Exhaustive: every expected outcome is a value, so a new one is a compile error here.
    switch (outcome) {
      case 'fired':
      case 'skipped':
      case 'stale':
        break
      default:
        outcome satisfies never
    }
  }
}

async function sweep(promoteBatchSize: number): Promise<void> {
  await driver.sweep({
    wfId,
    groupConcurrency,
    maxStalledCount: 1,
    stalledIntervalMs: 30_000,
    promoteBatchSize,
    resultTtlMs,
    keepFailed: 100,
  })
}

async function process_(
  claim: { job: ReservedJob; token: string },
  lockMs: number,
): Promise<void> {
  const controller = new AbortController()
  const stopHeartbeat = startHeartbeat(claim, controller, lockMs)
  try {
    const result = await handler(claim.job, { signal: controller.signal })
    if (controller.signal.aborted) return
    await driver.complete({
      wfId,
      jobId: claim.job.id,
      token: claim.token,
      record: JSON.stringify({ state: 'completed', value: result }),
      resultTtlMs,
      groupConcurrency,
    })
  } catch (err) {
    if (controller.signal.aborted) return
    const error = err instanceof Error ? err : new Error(String(err))
    const outcome = await driver.fail({
      wfId,
      jobId: claim.job.id,
      token: claim.token,
      reason: error.message,
      stack: error.stack ?? '',
      retryAt: Date.now() + backoff(claim.job.attemptsMade + 1),
      resultTtlMs,
      groupConcurrency,
      nonRecoverable: err instanceof NonRecoverableError,
    })
    outcome satisfies 'requeued' | 'dead-lettered' | 'stale-token'
  } finally {
    stopHeartbeat()
  }
}

function startHeartbeat(
  claim: { job: ReservedJob; token: string },
  controller: AbortController,
  lockMs: number,
): () => void {
  const timer = setInterval(() => {
    void (async () => {
      // `=== 'stale-token'` where it used to be `ok === 0`.
      const outcome = await driver.heartbeat({
        wfId,
        jobId: claim.job.id,
        token: claim.token,
        lockMs,
      })
      if (outcome === 'stale-token') {
        controller.abort()
        clearInterval(timer)
      }
    })()
  }, Math.min(lockMs / 3, 10_000))
  return () => clearInterval(timer)
}

/* ─────────────────────────────────── Namespace ─────────────────────────────────── */

async function namespaceClose(queues: { close: () => Promise<void> }[]): Promise<void> {
  // Drain first: no op is in flight by the time the driver's connections go.
  await Promise.all(queues.map(async (q) => q.close()))
  await driver.close()
}

/* ──────────────────────────────── unrelated stubs ──────────────────────────────── */

/** Replaces `flatToMap` — the shaping every driver would otherwise have copied. */
function toReservedJob(row: ClaimedJob): ReservedJob {
  return Object.freeze({ ...row, steps: new Map(Object.entries(row.steps)) })
}

declare function parseResult(raw: string): string
declare function timeout(ms: number): Promise<null>
declare function waitForSlot(ms: number): Promise<void>
declare function backoff(attempt: number): number
declare function handler(job: ReservedJob, ctx: { signal: AbortSignal }): Promise<string>

export {
  add,
  getMetrics,
  getSchedules,
  loop,
  namespaceClose,
  removeSchedule,
  setStepData,
  upsertSchedule,
  wait,
}
