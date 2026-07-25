import type Redis from 'ioredis'

/**
 * Purpose-built queue Lua. Every atomic op is exactly one script, registered via
 * `defineCommand` (free NOSCRIPT/EVAL fallback). Single-instance only (§2): keys are
 * built inside the scripts from the prefix + ids, so `numberOfKeys` is 0 and everything
 * is passed as ARGV. The `releaseActive` helper is defined once and prepended to every
 * script that frees a claim, so reserve/release can never drift.
 */

/** Max priority (2^21-1) — keeps the packed score exact in a ZSET double. */
export const PMAX = 2 ** 21 - 1
/** Sentinel for an unlimited cap. */
export const UNLIMITED = Number.MAX_SAFE_INTEGER

/** `now` in ms from Redis `TIME` — the single clock authority (§2). */
const NOW = `
local __t = redis.call("TIME")
local now = tonumber(__t[1]) * 1000 + math.floor(tonumber(__t[2]) / 1000)
`

/**
 * The single source of truth for a group's derived membership: the `ready` ZSET-of-groups
 * (runnable = has waiting jobs AND under the group cap) and the lightweight `groups` metrics
 * SET (§11 — member exactly while the group has waiting jobs). Both are keyed off the one
 * `ZCARD groupJobs` read, so the metrics SET cannot drift from the `ready` set or over-count:
 * every path that adds/pops a waiting job (`addWaiting`, `reserve`'s pop, `releaseActive`) funnels
 * through here. It is a MEMBERSHIP set, never an INCR/DECR count. Assumes nothing else in scope.
 */
const MAINTAIN_GROUP = `
local function maintainGroup(wf, groupId, groupCap)
  local groupJobs = wf .. ":g:" .. groupId .. ":jobs"
  local readyKey = wf .. ":ready"
  local groupsKey = wf .. ":groups"
  if redis.call("ZCARD", groupJobs) > 0 then
    redis.call("SADD", groupsKey, groupId)
    if redis.call("SCARD", wf .. ":g:" .. groupId .. ":active") < groupCap then
      local head = redis.call("ZRANGE", groupJobs, 0, 0, "WITHSCORES")
      redis.call("ZADD", readyKey, head[2], groupId)
    else
      redis.call("ZREM", readyKey, groupId)
    end
  else
    redis.call("SREM", groupsKey, groupId)
    redis.call("ZREM", readyKey, groupId)
  end
end
`

/**
 * The single source of truth for freeing a claim. Reads `groupId`+`nsId` off the job
 * hash and mirrors reserve's writes: removes the job from the three active structures,
 * deletes the lock, re-evaluates the group's `ready`/`groups` membership, and kicks both wake
 * lists. Shared (included, not copied) by every finalize path. Assumes `maintainGroup` is in scope.
 */
const RELEASE_ACTIVE = `
local function releaseActive(prefix, wfId, jobId, groupCap)
  local wf = prefix .. ":" .. wfId
  local jobKey = wf .. ":j:" .. jobId
  local groupId = redis.call("HGET", jobKey, "groupId")
  local nsId = redis.call("HGET", jobKey, "nsId")

  redis.call("ZREM", wf .. ":active", jobId)
  redis.call("SREM", wf .. ":g:" .. groupId .. ":active", jobId)
  redis.call("SREM", prefix .. ":ns:" .. nsId .. ":active", wfId .. ":" .. jobId)
  redis.call("DEL", jobKey .. ":lock")

  maintainGroup(wf, groupId, groupCap)

  local wfWake = wf .. ":wake"
  redis.call("LPUSH", wfWake, "1")
  redis.call("LTRIM", wfWake, 0, 0)
  local nsWake = prefix .. ":ns:" .. nsId .. ":wake"
  redis.call("LPUSH", nsWake, "1")
  redis.call("LTRIM", nsWake, 0, 0)
end
`

/**
 * Terminal-failure finalize, shared (included, not copied) by `fail`'s exhausted branch and
 * `moveToFailed` so a dead-letter entry is only ever written in one place. Sets `state=failed`
 * ahead of `releaseActive` (fences the fail-vs-recover race, mirroring `complete`), frees the
 * claim, deletes step data, appends to the `failed` retention ZSET scored by `finishedOn` and
 * count-trims it (`DEL`ing evicted job hashes), writes the TTL result record, and publishes
 * `done`. The failed job hash is kept (not `DEL`'d) for debug history until trimmed out.
 * Assumes `releaseActive` is already in scope.
 */
const FINALIZE_FAILED = `
local function finalizeFailed(prefix, wfId, jobId, reason, stack, resultTtl, groupCap, keepFailed)
  local wf = prefix .. ":" .. wfId
  local jobKey = wf .. ":j:" .. jobId
  local __t = redis.call("TIME")
  local now = tonumber(__t[1]) * 1000 + math.floor(tonumber(__t[2]) / 1000)

  redis.call("HSET", jobKey, "state", "failed", "failedReason", reason, "stacktrace", stack, "finishedOn", now)
  releaseActive(prefix, wfId, jobId, groupCap)
  redis.call("DEL", jobKey .. ":steps")

  local failedKey = wf .. ":failed"
  redis.call("ZADD", failedKey, now, jobId)
  local excess = redis.call("ZCARD", failedKey) - keepFailed
  if excess > 0 then
    local evicted = redis.call("ZRANGE", failedKey, 0, excess - 1)
    for i = 1, #evicted do redis.call("DEL", wf .. ":j:" .. evicted[i]) end
    redis.call("ZREMRANGEBYRANK", failedKey, 0, excess - 1)
  end

  redis.call("SET", wf .. ":result:" .. jobId,
    cjson.encode({ state = "failed", reason = reason, stack = stack }), "EX", resultTtl)
  redis.call("PUBLISH", wf .. ":done:" .. jobId, "1")
end
`

/**
 * Move a job into the waiting structure: stamp the FIFO tiebreak counter (`INCR pc` at
 * promotion time, §6), pack the priority score, `ZADD` the group ZSET, and re-evaluate
 * `ready`. The single source of truth for "a job becomes runnable" — shared (included, not
 * copied) by `enqueue`'s immediate path and `reserve`'s delayed-promotion, so the ready
 * logic can never drift between them.
 */
const ADD_WAITING = `
local function addWaiting(wf, jobKey, jobId, groupId, priority, groupCap, scoreArg)
  -- Recovery requeues at the STORED packed score (front of its band); the enqueue/promotion
  -- paths pass no score and stamp a fresh FIFO counter (§6). Either way the ready-set
  -- maintenance below is the single shared source of truth, so recovery can't drift.
  local score = scoreArg
  if not score then
    local counter = redis.call("INCR", wf .. ":pc") % 4294967296
    score = (${PMAX} - priority) * 4294967296 + counter
  end
  redis.call("HSET", jobKey, "state", "waiting")
  redis.call("ZADD", wf .. ":g:" .. groupId .. ":jobs", score, jobId)
  maintainGroup(wf, groupId, groupCap)
end
`

/**
 * Write a fresh job hash and drop it straight into waiting via the shared `addWaiting`, then
 * kick `wake`. The single source of truth for "enqueue an immediately-runnable job" — shared
 * (included, not copied) by `enqueue`'s immediate branch and `fireSchedule`'s occurrence, so a
 * cron occurrence is enqueued through the *exact* same path as a plain `add`, never a parallel
 * one. Assumes `addWaiting` is already in scope.
 */
const ENQUEUE_NOW = `
local function enqueueNow(prefix, wf, jobKey, jobId, data, groupId, priority, maxAttempts, nsId, groupCap, now)
  redis.call("HSET", jobKey,
    "data", data, "attempts", 0,
    "maxAttempts", maxAttempts, "stalledCount", 0, "priority", priority,
    "groupId", groupId, "nsId", nsId, "createdAt", now)
  addWaiting(wf, jobKey, jobId, groupId, priority, groupCap)
  local wake = wf .. ":wake"
  redis.call("LPUSH", wake, "1")
  redis.call("LTRIM", wake, 0, 0)
end
`

/**
 * Add a job. `EXISTS` guard on the job hash (in-script, no TOCTOU) → `JobAlreadyExists`.
 * The effective `runAt` is resolved against Redis `TIME` (§2): `runIn` → `now + runIn`,
 * absolute `runAt` verbatim. `runAt > now` lands in the `delayed` ZSET scored by `runAt`
 * (kicking `wake` so an idle worker re-computes to the nearer due time); otherwise
 * (`runAt <= now`, or neither given) the job goes straight into waiting via the shared
 * `enqueueNow`.
 *
 * ARGV: prefix, wfId, nsId, jobId, data, groupId, priority, maxAttempts, groupCap, runAt, runIn
 * (`runAt`/`runIn` use `-1` as the "unset" sentinel; they are mutually exclusive.)
 */
const ENQUEUE = `
${MAINTAIN_GROUP}
${ADD_WAITING}
${ENQUEUE_NOW}
local prefix = ARGV[1]
local wfId = ARGV[2]
local nsId = ARGV[3]
local jobId = ARGV[4]
local data = ARGV[5]
local groupId = ARGV[6]
local priority = tonumber(ARGV[7])
local maxAttempts = ARGV[8]
local groupCap = tonumber(ARGV[9])
local runAtArg = tonumber(ARGV[10])
local runInArg = tonumber(ARGV[11])

local wf = prefix .. ":" .. wfId
local jobKey = wf .. ":j:" .. jobId

if redis.call("EXISTS", jobKey) == 1 then
  return redis.error_reply("JobAlreadyExists")
end
${NOW}

local runAt = 0
if runInArg >= 0 then
  runAt = now + runInArg
elseif runAtArg >= 0 then
  runAt = runAtArg
end

if runAt > now then
  redis.call("HSET", jobKey,
    "data", data, "state", "delayed", "attempts", 0,
    "maxAttempts", maxAttempts, "stalledCount", 0, "priority", priority,
    "groupId", groupId, "nsId", nsId, "createdAt", now, "runAt", runAt)
  redis.call("ZADD", wf .. ":delayed", runAt, jobId)
  local wake = wf .. ":wake"
  redis.call("LPUSH", wake, "1")
  redis.call("LTRIM", wake, 0, 0)
else
  enqueueNow(prefix, wf, jobKey, jobId, data, groupId, priority, maxAttempts, nsId, groupCap, now)
end
return 1
`

/**
 * The hot path (ported from `prototypes/reserve.lua`). Delayed-job promotion is embedded at
 * the top (§7): due jobs (`ZRANGEBYSCORE delayed -inf now`, batch-capped) are `ZREM`'d and
 * moved into waiting via the shared `addWaiting` — atomic under Lua's single thread ⇒
 * exactly-once across concurrent workers, and promoting in `runAt` order stamps `pc`
 * ascending-by-due-time (FIFO-by-ready-time, §6). Then O(1) top-gates → pop head group of
 * `ready` → `ZPOPMIN` its head job → all-or-nothing claim into the three active structures
 * + lock + `state=active` → ready-set maintenance.
 *
 * Returns `{"maxed"}`, `{"empty", msToNext}`, or
 * `{"job", jobId, groupId, data, attempts, priority}`. `msToNext` is the ms until the next
 * due delayed job (`-1` if none, `0` if due work remains past the promote cap) — an idle
 * worker uses it as its `BRPOP wake` timeout, so the block itself is the delayed-job timer.
 *
 * ARGV: prefix, wfId, nsId, nsCap, wfCap, groupCap, lockMs, token, promoteCap
 */
const RESERVE = `
${MAINTAIN_GROUP}
${ADD_WAITING}
local prefix = ARGV[1]
local wfId = ARGV[2]
local nsId = ARGV[3]
local nsCap = tonumber(ARGV[4])
local wfCap = tonumber(ARGV[5])
local groupCap = tonumber(ARGV[6])
local lockMs = tonumber(ARGV[7])
local token = ARGV[8]
local promoteCap = tonumber(ARGV[9])

local wf = prefix .. ":" .. wfId
local nsActive = prefix .. ":ns:" .. nsId .. ":active"
local wfActive = wf .. ":active"
local readyKey = wf .. ":ready"
local delayedKey = wf .. ":delayed"
${NOW}

-- Promote due delayed jobs (batch-capped). ZRANGEBYSCORE ascending ⇒ promote in runAt order.
local due = redis.call("ZRANGEBYSCORE", delayedKey, "-inf", now, "LIMIT", 0, promoteCap)
for i = 1, #due do
  local dj = due[i]
  redis.call("ZREM", delayedKey, dj)
  local djKey = wf .. ":j:" .. dj
  local meta = redis.call("HMGET", djKey, "groupId", "priority")
  addWaiting(wf, djKey, dj, meta[1], tonumber(meta[2]), groupCap)
end

-- ms until the next delayed job is due (for an idle worker BRPOP timeout). Returns 0 when
-- work is already due but was left behind by the promote cap, so the worker re-reserves now.
local function msToNext()
  local next = redis.call("ZRANGE", delayedKey, 0, 0, "WITHSCORES")
  if #next == 0 then return -1 end
  local d = tonumber(next[2]) - now
  if d < 0 then return 0 end
  return d
end

if redis.call("SCARD", nsActive) >= nsCap then return { "maxed" } end
if redis.call("ZCARD", wfActive) >= wfCap then return { "maxed" } end

local head = redis.call("ZRANGE", readyKey, 0, 0)
if #head == 0 then return { "empty", msToNext() } end
local gid = head[1]

local groupJobs = wf .. ":g:" .. gid .. ":jobs"
local groupActive = wf .. ":g:" .. gid .. ":active"

local popped = redis.call("ZPOPMIN", groupJobs)
if #popped == 0 then
  maintainGroup(wf, gid, groupCap)
  return { "empty", msToNext() }
end
local jobId = popped[1]

local jobKey = wf .. ":j:" .. jobId
local deadline = now + lockMs

redis.call("ZADD", wfActive, deadline, jobId)
redis.call("SADD", groupActive, jobId)
redis.call("SADD", nsActive, wfId .. ":" .. jobId)
redis.call("SET", jobKey .. ":lock", token, "PX", lockMs)
-- Store the popped packed score so stalled-recovery can requeue at the front of its band.
redis.call("HSET", jobKey, "state", "active", "deadlineAt", deadline, "score", popped[2])

maintainGroup(wf, gid, groupCap)

local vals = redis.call("HMGET", jobKey, "data", "priority", "attempts")
return { "job", jobId, gid, vals[1], vals[3], vals[2] }
`

/**
 * Token-guarded finalize: no-op if `lock != myToken` (recovery took the claim). Else set
 * `state=completing` *before* `releaseActive` (fences the complete-vs-recover race), free
 * the claim, delete the (now reusable) job hash + step data, `SET` the TTL result record,
 * and `PUBLISH done`.
 *
 * ARGV: prefix, wfId, jobId, token, result, resultTtl, groupCap
 */
const COMPLETE = `
${MAINTAIN_GROUP}
${RELEASE_ACTIVE}
local prefix = ARGV[1]
local wfId = ARGV[2]
local jobId = ARGV[3]
local token = ARGV[4]
local result = ARGV[5]
local resultTtl = tonumber(ARGV[6])
local groupCap = tonumber(ARGV[7])

local wf = prefix .. ":" .. wfId
local jobKey = wf .. ":j:" .. jobId

if redis.call("GET", jobKey .. ":lock") ~= token then
  return 0
end

redis.call("HSET", jobKey, "state", "completing")
releaseActive(prefix, wfId, jobId, groupCap)

redis.call("DEL", jobKey)
redis.call("DEL", jobKey .. ":steps")
redis.call("SET", wf .. ":result:" .. jobId, result, "EX", resultTtl)
redis.call("PUBLISH", wf .. ":done:" .. jobId, "1")
return 1
`

/**
 * Token-guarded finalize on a handler throw: no-op if `lock != myToken` (recovery took the
 * claim). Else `HINCRBY attempts 1` (the single attempt counter, never in JS; `stalledCount`
 * is untouched). Retryable (`attempts < maxAttempts`) → set `state=delayed`, `releaseActive`
 * (frees ALL concurrency slots + kicks both wake lists during backoff), and `ZADD delayed` at
 * the JS-computed `runAt` score so `reserve` promotes it when due. Exhausted → `finalizeFailed`.
 *
 * Returns 0 (stale token no-op), 1 (terminal dead-letter), or 2 (requeued for retry).
 * ARGV: prefix, wfId, jobId, token, reason, stack, runAt, resultTtl, groupCap, keepFailed
 */
const FAIL = `
${MAINTAIN_GROUP}
${RELEASE_ACTIVE}
${FINALIZE_FAILED}
local prefix = ARGV[1]
local wfId = ARGV[2]
local jobId = ARGV[3]
local token = ARGV[4]
local reason = ARGV[5]
local stack = ARGV[6]
local runAt = tonumber(ARGV[7])
local resultTtl = tonumber(ARGV[8])
local groupCap = tonumber(ARGV[9])
local keepFailed = tonumber(ARGV[10])

local wf = prefix .. ":" .. wfId
local jobKey = wf .. ":j:" .. jobId

if redis.call("GET", jobKey .. ":lock") ~= token then
  return 0
end

local attempts = redis.call("HINCRBY", jobKey, "attempts", 1)
local maxAttempts = tonumber(redis.call("HGET", jobKey, "maxAttempts"))

if attempts < maxAttempts then
  redis.call("HSET", jobKey, "state", "delayed", "runAt", runAt)
  releaseActive(prefix, wfId, jobId, groupCap)
  redis.call("ZADD", wf .. ":delayed", runAt, jobId)
  return 2
end

finalizeFailed(prefix, wfId, jobId, reason, stack, resultTtl, groupCap, keepFailed)
return 1
`

/**
 * `fail`-terminal minus the token-equality guard and minus backoff — an unconditional
 * dead-letter for callers that already hold the right to finalize (stalled-recovery over its
 * budget, ticket 08). Anti-drift: recovery never hand-writes a failed-set entry, it routes
 * through the same `finalizeFailed`.
 *
 * ARGV: prefix, wfId, jobId, reason, resultTtl, groupCap, keepFailed
 */
const MOVE_TO_FAILED = `
${MAINTAIN_GROUP}
${RELEASE_ACTIVE}
${FINALIZE_FAILED}
local prefix = ARGV[1]
local wfId = ARGV[2]
local jobId = ARGV[3]
local reason = ARGV[4]
local resultTtl = tonumber(ARGV[5])
local groupCap = tonumber(ARGV[6])
local keepFailed = tonumber(ARGV[7])

finalizeFailed(prefix, wfId, jobId, reason, "", resultTtl, groupCap, keepFailed)
return 1
`

/**
 * Token-CAS heartbeat (§9): no-op returning 0 if `lock != myToken` (the claim was recovered
 * and re-reserved elsewhere → the caller must abort). Else renew the lock PX *and* the
 * `wf:active` deadline score in lockstep so they can never diverge, keeping a healthy
 * long-running job out of the stalled-candidate window. The worker runs it on a derived
 * `min(lockMs/3, 10s)` timer.
 *
 * ARGV: prefix, wfId, jobId, token, lockMs
 */
const HEARTBEAT = `
local prefix = ARGV[1]
local wfId = ARGV[2]
local jobId = ARGV[3]
local token = ARGV[4]
local lockMs = tonumber(ARGV[5])

local wf = prefix .. ":" .. wfId
local jobKey = wf .. ":j:" .. jobId

if redis.call("GET", jobKey .. ":lock") ~= token then
  return 0
end
${NOW}
redis.call("SET", jobKey .. ":lock", token, "PX", lockMs)
redis.call("ZADD", wf .. ":active", now + lockMs, jobId)
return 1
`

/**
 * The single throttled stalled-recovery scan (§9), never folded into reserve. Gated by
 * `SET wf:stalled-check <now> NX PX interval` so only one worker per interval scans; returns
 * 0 immediately when the gate is held. Detection is a pure deadline-compare over the
 * deadline-scored active ZSET (`ZRANGEBYSCORE wf:active 0 now`, batch-capped) — no `stalled`
 * SET, no two-pass. Per candidate: a `state == "active"` fence (skips a job mid-`completing`,
 * closing the complete-vs-recover race) → the shared `releaseActive` (frees every slot and
 * unblocks the group for free) → `HINCRBY stalledCount`. Over budget ⇒ the SAME dead-letter
 * path as `moveToFailed` (`finalizeFailed`, bypassing backoff/retry); otherwise requeue at the
 * stored packed score via the shared `addWaiting`. `releaseActive` already kicks both wake lists.
 *
 * ARGV: prefix, wfId, groupCap, maxStalledCount, interval, batchSize, resultTtl, keepFailed
 */
const RECOVER_STALLED = `
${MAINTAIN_GROUP}
${RELEASE_ACTIVE}
${FINALIZE_FAILED}
${ADD_WAITING}
local prefix = ARGV[1]
local wfId = ARGV[2]
local groupCap = tonumber(ARGV[3])
local maxStalledCount = tonumber(ARGV[4])
local interval = tonumber(ARGV[5])
local batchSize = tonumber(ARGV[6])
local resultTtl = tonumber(ARGV[7])
local keepFailed = tonumber(ARGV[8])

local wf = prefix .. ":" .. wfId
${NOW}

if redis.call("SET", wf .. ":stalled-check", now, "NX", "PX", interval) == false then
  return 0
end

local candidates = redis.call("ZRANGEBYSCORE", wf .. ":active", 0, now, "LIMIT", 0, batchSize)
local recovered = 0
for i = 1, #candidates do
  local jobId = candidates[i]
  local jobKey = wf .. ":j:" .. jobId
  if redis.call("HGET", jobKey, "state") == "active" then
    releaseActive(prefix, wfId, jobId, groupCap)
    local stalledCount = redis.call("HINCRBY", jobKey, "stalledCount", 1)
    if stalledCount > maxStalledCount then
      finalizeFailed(prefix, wfId, jobId, "stalled more than allowable limit", "", resultTtl, groupCap, keepFailed)
    else
      local groupId = redis.call("HGET", jobKey, "groupId")
      local score = tonumber(redis.call("HGET", jobKey, "score"))
      addWaiting(wf, jobKey, jobId, groupId, 0, groupCap, score)
    end
    recovered = recovered + 1
  end
end
return recovered
`

/**
 * Cron firing = "JS computes next, Lua commits via CAS-on-score" (§8). The JS wake-loop tick
 * reads a due schedule, computes `nextScore = croner.nextRun(now)`, and calls this with the
 * score it saw (`expectedScore`). The CAS bails unless `ZSCORE due scheduleId == expectedScore`,
 * so a racing worker that already fired (and advanced the score) is a no-op ⇒ **exactly-once
 * across N workers with no distributed lock**. Because the next score is computed in JS *before*
 * the call, a crash before it is a no-op retry and a crash after is fully done — never a
 * half-fired state.
 *
 * On a winning CAS: re-arm `ZADD due nextScore` (atomic with the enqueue). Skip-if-running (§8):
 * if enabled and the record's `lastJobId` is still non-terminal (its hash exists and is not
 * `failed` — a completed occurrence's hash is `DEL`'d, a failed one carries `state=failed`),
 * advance the score but do NOT enqueue. Otherwise enqueue the occurrence via the shared
 * `enqueueNow` (the exact `add` path, never a parallel one) and stamp `lastJobId`/`lastFireAt`.
 * Missed-run collapses to one fire because the JS tick passed `nextRun(now)`, not the overdue
 * time.
 *
 * Returns `{"stale"}`, `{"skipped"}`, or `{"fired", jobId}`.
 * ARGV: prefix, wfId, nsId, scheduleId, expectedScore, nextScore, jobId, maxAttempts, groupCap
 */
const FIRE_SCHEDULE = `
${MAINTAIN_GROUP}
${ADD_WAITING}
${ENQUEUE_NOW}
local prefix = ARGV[1]
local wfId = ARGV[2]
local nsId = ARGV[3]
local scheduleId = ARGV[4]
local expectedScore = ARGV[5]
local nextScore = tonumber(ARGV[6])
local jobId = ARGV[7]
local maxAttempts = ARGV[8]
local groupCap = tonumber(ARGV[9])

local wf = prefix .. ":" .. wfId
local dueKey = wf .. ":schedules:due"
local scheduleKey = wf .. ":schedule:" .. scheduleId

local cur = redis.call("ZSCORE", dueKey, scheduleId)
if cur == false or cur ~= expectedScore then
  return { "stale" }
end

redis.call("ZADD", dueKey, nextScore, scheduleId)

local vals = redis.call("HMGET", scheduleKey, "data", "priority", "groupId", "skipIfRunning", "lastJobId")
local data = vals[1]
local priority = tonumber(vals[2])
local groupId = vals[3]
local skipIfRunning = vals[4]
local lastJobId = vals[5]

if skipIfRunning == "1" and lastJobId and lastJobId ~= "" then
  local lastKey = wf .. ":j:" .. lastJobId
  if redis.call("EXISTS", lastKey) == 1 and redis.call("HGET", lastKey, "state") ~= "failed" then
    return { "skipped" }
  end
end
${NOW}
enqueueNow(prefix, wf, wf .. ":j:" .. jobId, jobId, data, groupId, priority, maxAttempts, nsId, groupCap, now)
redis.call("HSET", scheduleKey, "lastJobId", jobId, "lastFireAt", now)
return { "fired", jobId }
`

export interface QueueCommands {
  enqueue: (...args: (string | number)[]) => Promise<number>
  reserve: (...args: (string | number)[]) => Promise<(string | number)[]>
  complete: (...args: (string | number)[]) => Promise<number>
  fail: (...args: (string | number)[]) => Promise<number>
  moveToFailed: (...args: (string | number)[]) => Promise<number>
  heartbeat: (...args: (string | number)[]) => Promise<number>
  recoverStalled: (...args: (string | number)[]) => Promise<number>
  fireSchedule: (...args: (string | number)[]) => Promise<(string | number)[]>
}

/** A redis connection with the queue's custom commands registered. */
export type QueueRedis = Redis & QueueCommands

const registered = new WeakSet<Redis>()

/** Register the queue's Lua commands on a connection (idempotent). */
export function registerScripts(redis: Redis): QueueRedis {
  if (!registered.has(redis)) {
    redis.defineCommand('enqueue', { numberOfKeys: 0, lua: ENQUEUE })
    redis.defineCommand('reserve', { numberOfKeys: 0, lua: RESERVE })
    redis.defineCommand('complete', { numberOfKeys: 0, lua: COMPLETE })
    redis.defineCommand('fail', { numberOfKeys: 0, lua: FAIL })
    redis.defineCommand('moveToFailed', { numberOfKeys: 0, lua: MOVE_TO_FAILED })
    redis.defineCommand('heartbeat', { numberOfKeys: 0, lua: HEARTBEAT })
    redis.defineCommand('recoverStalled', { numberOfKeys: 0, lua: RECOVER_STALLED })
    redis.defineCommand('fireSchedule', { numberOfKeys: 0, lua: FIRE_SCHEDULE })
    registered.add(redis)
  }
  return redis as QueueRedis
}
