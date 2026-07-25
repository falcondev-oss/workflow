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
 * The single source of truth for freeing a claim. Reads `groupId`+`nsId` off the job
 * hash and mirrors reserve's writes: removes the job from the three active structures,
 * deletes the lock, re-evaluates the group's `ready` membership, and kicks both wake
 * lists. Shared (included, not copied) by every finalize path.
 */
const RELEASE_ACTIVE = `
local function releaseActive(prefix, wfId, jobId, groupCap)
  local wf = prefix .. ":" .. wfId
  local jobKey = wf .. ":j:" .. jobId
  local groupId = redis.call("HGET", jobKey, "groupId")
  local nsId = redis.call("HGET", jobKey, "nsId")
  local groupJobs = wf .. ":g:" .. groupId .. ":jobs"
  local groupActive = wf .. ":g:" .. groupId .. ":active"
  local readyKey = wf .. ":ready"

  redis.call("ZREM", wf .. ":active", jobId)
  redis.call("SREM", groupActive, jobId)
  redis.call("SREM", prefix .. ":ns:" .. nsId .. ":active", wfId .. ":" .. jobId)
  redis.call("DEL", jobKey .. ":lock")

  if redis.call("ZCARD", groupJobs) > 0 and redis.call("SCARD", groupActive) < groupCap then
    local head = redis.call("ZRANGE", groupJobs, 0, 0, "WITHSCORES")
    redis.call("ZADD", readyKey, head[2], groupId)
  else
    redis.call("ZREM", readyKey, groupId)
  end

  local wfWake = wf .. ":wake"
  redis.call("LPUSH", wfWake, "1")
  redis.call("LTRIM", wfWake, 0, 0)
  local nsWake = prefix .. ":ns:" .. nsId .. ":wake"
  redis.call("LPUSH", nsWake, "1")
  redis.call("LTRIM", nsWake, 0, 0)
end
`

/**
 * Add an immediate job. `EXISTS` guard on the job hash (in-script, no TOCTOU) →
 * `JobAlreadyExists`. Stamps the FIFO tiebreak counter, writes the packed score into the
 * group ZSET, re-evaluates `ready`, and kicks the wake list.
 *
 * ARGV: prefix, wfId, nsId, jobId, data, groupId, priority, maxAttempts, groupCap
 */
const ENQUEUE = `
local prefix = ARGV[1]
local wfId = ARGV[2]
local nsId = ARGV[3]
local jobId = ARGV[4]
local data = ARGV[5]
local groupId = ARGV[6]
local priority = tonumber(ARGV[7])
local maxAttempts = ARGV[8]
local groupCap = tonumber(ARGV[9])

local wf = prefix .. ":" .. wfId
local jobKey = wf .. ":j:" .. jobId

if redis.call("EXISTS", jobKey) == 1 then
  return redis.error_reply("JobAlreadyExists")
end
${NOW}
local counter = redis.call("INCR", wf .. ":pc") % 4294967296
local score = (${PMAX} - priority) * 4294967296 + counter

redis.call("HSET", jobKey,
  "data", data, "state", "waiting", "attempts", 0,
  "maxAttempts", maxAttempts, "stalledCount", 0, "priority", priority,
  "groupId", groupId, "nsId", nsId, "createdAt", now)

local groupJobs = wf .. ":g:" .. groupId .. ":jobs"
local groupActive = wf .. ":g:" .. groupId .. ":active"
redis.call("ZADD", groupJobs, score, jobId)
redis.call("SADD", wf .. ":groups", groupId)

if redis.call("SCARD", groupActive) < groupCap then
  local head = redis.call("ZRANGE", groupJobs, 0, 0, "WITHSCORES")
  redis.call("ZADD", wf .. ":ready", head[2], groupId)
end

local wake = wf .. ":wake"
redis.call("LPUSH", wake, "1")
redis.call("LTRIM", wake, 0, 0)
return 1
`

/**
 * The hot path (ported from `prototypes/reserve.lua`). O(1) top-gates → pop head group of
 * `ready` → `ZPOPMIN` its head job → all-or-nothing claim into the three active structures
 * + lock + `state=active` → ready-set maintenance. Returns `{"maxed"}`, `{"empty"}`, or
 * `{"job", jobId, groupId, data, attempts, priority}`.
 *
 * ARGV: prefix, wfId, nsId, nsCap, wfCap, groupCap, lockMs, token
 */
const RESERVE = `
local prefix = ARGV[1]
local wfId = ARGV[2]
local nsId = ARGV[3]
local nsCap = tonumber(ARGV[4])
local wfCap = tonumber(ARGV[5])
local groupCap = tonumber(ARGV[6])
local lockMs = tonumber(ARGV[7])
local token = ARGV[8]

local wf = prefix .. ":" .. wfId
local nsActive = prefix .. ":ns:" .. nsId .. ":active"
local wfActive = wf .. ":active"
local readyKey = wf .. ":ready"

if redis.call("SCARD", nsActive) >= nsCap then return { "maxed" } end
if redis.call("ZCARD", wfActive) >= wfCap then return { "maxed" } end

local head = redis.call("ZRANGE", readyKey, 0, 0)
if #head == 0 then return { "empty" } end
local gid = head[1]

local groupJobs = wf .. ":g:" .. gid .. ":jobs"
local groupActive = wf .. ":g:" .. gid .. ":active"

local popped = redis.call("ZPOPMIN", groupJobs)
if #popped == 0 then
  redis.call("ZREM", readyKey, gid)
  return { "empty" }
end
local jobId = popped[1]

local jobKey = wf .. ":j:" .. jobId
${NOW}
local deadline = now + lockMs

redis.call("ZADD", wfActive, deadline, jobId)
redis.call("SADD", groupActive, jobId)
redis.call("SADD", nsActive, wfId .. ":" .. jobId)
redis.call("SET", jobKey .. ":lock", token, "PX", lockMs)
redis.call("HSET", jobKey, "state", "active", "deadlineAt", deadline)

if redis.call("ZCARD", groupJobs) > 0 and redis.call("SCARD", groupActive) < groupCap then
  local next = redis.call("ZRANGE", groupJobs, 0, 0, "WITHSCORES")
  redis.call("ZADD", readyKey, next[2], gid)
else
  redis.call("ZREM", readyKey, gid)
end

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

export interface QueueCommands {
  enqueue: (...args: (string | number)[]) => Promise<number>
  reserve: (...args: (string | number)[]) => Promise<string[]>
  complete: (...args: (string | number)[]) => Promise<number>
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
    registered.add(redis)
  }
  return redis as QueueRedis
}
