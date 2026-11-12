import type Redis from 'ioredis'

export const PMAX = 2 ** 21 - 1
export const UNLIMITED = Number.MAX_SAFE_INTEGER

const NOW = `
local __t = redis.call("TIME")
local now = tonumber(__t[1]) * 1000 + math.floor(tonumber(__t[2]) / 1000)
`

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

  local record = cjson.encode({ state = "failed", reason = reason, stack = stack })
  redis.call("SET", wf .. ":result:" .. jobId, record, "EX", resultTtl)
  redis.call("PUBLISH", wf .. ":done:" .. jobId, record)
end
`

const ADD_WAITING = `
local function addWaiting(wf, jobKey, jobId, groupId, priority, groupCap, readyAt, scoreArg)
  local score = scoreArg
  if not score then
    local counter = redis.call("INCR", wf .. ":pc") % 4294967296
    score = (${PMAX} - priority) * 4294967296 + counter
  end
  redis.call("HSET", jobKey, "state", "waiting", "readyAt", readyAt)
  redis.call("ZADD", wf .. ":g:" .. groupId .. ":jobs", score, jobId)
  maintainGroup(wf, groupId, groupCap)
end
`

const ENQUEUE_NOW = `
local function enqueueNow(prefix, wf, jobKey, jobId, data, groupId, priority, maxAttempts, nsId, groupCap, now)
  redis.call("HSET", jobKey,
    "data", data, "attempts", 0,
    "maxAttempts", maxAttempts, "stalledCount", 0, "priority", priority,
    "groupId", groupId, "nsId", nsId, "createdAt", now)
  addWaiting(wf, jobKey, jobId, groupId, priority, groupCap, now)
  local wake = wf .. ":wake"
  redis.call("LPUSH", wake, "1")
  redis.call("LTRIM", wake, 0, 0)
end
`

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
local tokenPrefix = ARGV[8]
local promoteCap = tonumber(ARGV[9])
local want = tonumber(ARGV[10])
local limiterCount = tonumber(ARGV[11]) or 0

local wf = prefix .. ":" .. wfId
local limiters = {}
for i = 1, limiterCount do
  local base = 11 + (i - 1) * 3
  local key = prefix .. ":ns:" .. nsId .. ":rl:" .. ARGV[base + 1]
  limiters[i] = { key, tonumber(ARGV[base + 2]), tonumber(ARGV[base + 3]) }
end
local nsActive = prefix .. ":ns:" .. nsId .. ":active"
local wfActive = wf .. ":active"
local readyKey = wf .. ":ready"
local delayedKey = wf .. ":delayed"
local scheduleDueKey = wf .. ":schedules:due"
${NOW}

local due = redis.call("ZRANGEBYSCORE", delayedKey, "-inf", now, "LIMIT", 0, promoteCap)
for i = 1, #due do
  local dj = due[i]
  redis.call("ZREM", delayedKey, dj)
  local djKey = wf .. ":j:" .. dj
  local meta = redis.call("HMGET", djKey, "groupId", "priority", "runAt")
  addWaiting(wf, djKey, dj, meta[1], tonumber(meta[2]), groupCap, meta[3])
end

local function msToHead(key)
  local next = redis.call("ZRANGE", key, 0, 0, "WITHSCORES")
  if #next == 0 then return -1 end
  local d = tonumber(next[2]) - now
  if d < 0 then return 0 end
  return d
end

local function readGate()
  local allowed = want
  local wait = math.max(0, redis.call("PTTL", wf .. ":paused"))
  if wait > 0 then allowed = 0 end
  for i = 1, #limiters do
    local key, limit = limiters[i][1], limiters[i][2]
    local paused = math.max(0, redis.call("PTTL", key .. ":paused"))
    if paused > 0 then
      allowed = 0
      if paused > wait then wait = paused end
    end
    redis.call("ZREMRANGEBYSCORE", key, "-inf", now)
    local count = redis.call("ZCARD", key)
    limiters[i][4] = count
    allowed = math.min(allowed, math.max(0, limit - count))
  end
  return allowed, wait
end

local jobs = {}
local maxed = 0
local msToLimit = -1
local allowed = nil
local wait = 0
while #jobs < want do
  if redis.call("SCARD", nsActive) >= nsCap or redis.call("ZCARD", wfActive) >= wfCap then
    maxed = 1
    break
  end
  local head = redis.call("ZRANGE", readyKey, 0, 0)
  if #head == 0 then break end
  if allowed == nil then allowed, wait = readGate() end
  if #jobs >= allowed then
    for i = 1, #limiters do
      local key, limit = limiters[i][1], limiters[i][2]
      local count = limiters[i][4] + #jobs
      if count >= limit then
        local nth = redis.call("ZRANGE", key, count - limit, count - limit, "WITHSCORES")
        wait = math.max(wait, tonumber(nth[2]) - now)
      end
    end
    maxed = 1
    msToLimit = wait
    break
  end
  local gid = head[1]
  local popped = redis.call("ZPOPMIN", wf .. ":g:" .. gid .. ":jobs")
  if #popped == 0 then
    maintainGroup(wf, gid, groupCap)
  else
    local jobId = popped[1]
    local jobKey = wf .. ":j:" .. jobId
    local deadline = now + lockMs
    local token = tokenPrefix .. ":" .. #jobs

    redis.call("ZADD", wfActive, deadline, jobId)
    redis.call("SADD", wf .. ":g:" .. gid .. ":active", jobId)
    redis.call("SADD", nsActive, wfId .. ":" .. jobId)
    redis.call("SET", jobKey .. ":lock", token, "PX", lockMs)
    redis.call("HSET", jobKey, "state", "active", "deadlineAt", deadline, "score", popped[2])
    for i = 1, #limiters do
      redis.call("ZADD", limiters[i][1], now + limiters[i][3], token)
    end

    maintainGroup(wf, gid, groupCap)

    local vals = redis.call("HMGET", jobKey, "data", "priority", "attempts", "readyAt", "createdAt")
    local steps = redis.call("HGETALL", jobKey .. ":steps")
    local queueWait = math.max(0, now - tonumber(vals[4] or vals[5]))
    local age = math.max(0, now - tonumber(vals[5]))
    jobs[#jobs + 1] = { jobId, gid, vals[1], vals[3], vals[2], token, steps, queueWait, age }
  end
end

if #jobs > 0 then
  for i = 1, #limiters do
    local last = redis.call("ZRANGE", limiters[i][1], -1, -1, "WITHSCORES")
    redis.call("PEXPIREAT", limiters[i][1], last[2])
  end
end

local dueSchedules = redis.call("ZRANGEBYSCORE", scheduleDueKey, "-inf", now, "WITHSCORES")
return { jobs, msToHead(delayedKey), msToHead(scheduleDueKey), dueSchedules, maxed, msToLimit }
`

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
redis.call("PUBLISH", wf .. ":done:" .. jobId, result)
return 1
`

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
local noRetry = tonumber(ARGV[11])

local wf = prefix .. ":" .. wfId
local jobKey = wf .. ":j:" .. jobId

if redis.call("GET", jobKey .. ":lock") ~= token then
  return 0
end

local attempts = redis.call("HINCRBY", jobKey, "attempts", 1)
local maxAttempts = tonumber(redis.call("HGET", jobKey, "maxAttempts"))

if noRetry == 0 and attempts < maxAttempts then
  redis.call("HSET", jobKey, "state", "delayed", "runAt", runAt)
  releaseActive(prefix, wfId, jobId, groupCap)
  redis.call("ZADD", wf .. ":delayed", runAt, jobId)
  return 2
end

finalizeFailed(prefix, wfId, jobId, reason, stack, resultTtl, groupCap, keepFailed)
return 1
`

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
  return { 0, {} }
end

local candidates = redis.call("ZRANGEBYSCORE", wf .. ":active", 0, now, "LIMIT", 0, batchSize)
local requeued = 0
local deadLettered = {}
for i = 1, #candidates do
  local jobId = candidates[i]
  local jobKey = wf .. ":j:" .. jobId
  if redis.call("HGET", jobKey, "state") == "active" then
    releaseActive(prefix, wfId, jobId, groupCap)
    local stalledCount = redis.call("HINCRBY", jobKey, "stalledCount", 1)
    if stalledCount > maxStalledCount then
      local vals = redis.call("HMGET", jobKey, "attempts", "createdAt")
      deadLettered[#deadLettered + 1] = { tonumber(vals[1]) + 1, math.max(0, now - tonumber(vals[2])) }
      finalizeFailed(prefix, wfId, jobId, "stalled more than allowable limit", "", resultTtl, groupCap, keepFailed)
    else
      local groupId = redis.call("HGET", jobKey, "groupId")
      local score = tonumber(redis.call("HGET", jobKey, "score"))
      addWaiting(wf, jobKey, jobId, groupId, 0, groupCap, now, score)
      requeued = requeued + 1
    end
  end
end
return { requeued, deadLettered }
`

const RATE_LIMIT = `
${MAINTAIN_GROUP}
${RELEASE_ACTIVE}
${ADD_WAITING}
local prefix = ARGV[1]
local wfId = ARGV[2]
local nsId = ARGV[3]
local jobId = ARGV[4]
local token = ARGV[5]
local ms = math.ceil(tonumber(ARGV[6]))
local limiter = ARGV[7]
local groupCap = tonumber(ARGV[8])

local wf = prefix .. ":" .. wfId
local jobKey = wf .. ":j:" .. jobId
${NOW}

local pausedKey = wf .. ":paused"
if limiter ~= "" then pausedKey = prefix .. ":ns:" .. nsId .. ":rl:" .. limiter .. ":paused" end
local untilAt = now + ms
local existing = tonumber(redis.call("GET", pausedKey) or 0)
if existing > untilAt then untilAt = existing end
redis.call("SET", pausedKey, untilAt, "PXAT", untilAt)

if redis.call("GET", jobKey .. ":lock") ~= token then
  return 0
end
releaseActive(prefix, wfId, jobId, groupCap)
local meta = redis.call("HMGET", jobKey, "groupId", "score")
addWaiting(wf, jobKey, jobId, meta[1], 0, groupCap, now, tonumber(meta[2]))
return 1
`

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
if cur == false or cur ~= expectedScore or nextScore <= tonumber(expectedScore) then
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

const RATE_LIMITER_METRICS = `
${NOW}
local values = {}
for i = 3, #ARGV do
  local key = ARGV[1] .. ":ns:" .. ARGV[2] .. ":rl:" .. ARGV[i]
  values[#values + 1] = {
    ARGV[i],
    redis.call("ZCOUNT", key, "(" .. now, "+inf"),
    math.max(0, redis.call("PTTL", key .. ":paused"))
  }
end
return values
`

const GROUP_JOBS = `
local wf = ARGV[1] .. ":" .. ARGV[2]
local groupId = ARGV[3]
local jobs = {}
local function collect(ids)
  for i = 1, #ids do
    local jobKey = wf .. ":j:" .. ids[i]
    if redis.call("HGET", jobKey, "groupId") == groupId then
      local vals = redis.call("HMGET", jobKey, "state", "data", "createdAt")
      jobs[#jobs + 1] = { ids[i], vals[1], vals[2], vals[3] }
    end
  end
end
collect(redis.call("SMEMBERS", wf .. ":g:" .. groupId .. ":active"))
collect(redis.call("ZRANGE", wf .. ":g:" .. groupId .. ":jobs", 0, -1))
collect(redis.call("ZRANGE", wf .. ":delayed", 0, -1))
return jobs
`

export interface QueueCommands {
  groupJobs: (
    ...args: string[]
  ) => Promise<
    [id: string, state: 'waiting' | 'delayed' | 'active', data: string, createdAt: string][]
  >
  rateLimiterMetrics: (
    ...args: string[]
  ) => Promise<[name: string, starts: number, pausedMs: number][]>

  enqueue: (...args: (string | number)[]) => Promise<number>
  reserve: (
    ...args: (string | number)[]
  ) => Promise<
    [
      jobs: [
        id: string,
        groupId: string,
        data: string,
        attempts: string,
        priority: string,
        token: string,
        steps: string[],
        queueWaitMs: number,
        ageMs: number,
      ][],
      msToNext: number,
      msToSchedule: number,
      due: string[],
      maxed: number,
      msToLimit: number,
    ]
  >
  rateLimit: (...args: (string | number)[]) => Promise<number>
  complete: (...args: (string | number)[]) => Promise<number>
  fail: (...args: (string | number)[]) => Promise<number>
  moveToFailed: (...args: (string | number)[]) => Promise<number>
  heartbeat: (...args: (string | number)[]) => Promise<number>
  recoverStalled: (
    ...args: (string | number)[]
  ) => Promise<[requeued: number, deadLettered: [attempts: number, ageMs: number][]]>
  fireSchedule: (...args: (string | number)[]) => Promise<(string | number)[]>
}

export type QueueRedis = Redis & QueueCommands

const registered = new WeakSet<Redis>()

export function registerScripts(redis: Redis): QueueRedis {
  if (!registered.has(redis)) {
    redis.defineCommand('enqueue', { numberOfKeys: 0, lua: ENQUEUE })
    redis.defineCommand('reserve', { numberOfKeys: 0, lua: RESERVE })
    redis.defineCommand('complete', { numberOfKeys: 0, lua: COMPLETE })
    redis.defineCommand('fail', { numberOfKeys: 0, lua: FAIL })
    redis.defineCommand('moveToFailed', { numberOfKeys: 0, lua: MOVE_TO_FAILED })
    redis.defineCommand('heartbeat', { numberOfKeys: 0, lua: HEARTBEAT })
    redis.defineCommand('recoverStalled', { numberOfKeys: 0, lua: RECOVER_STALLED })
    redis.defineCommand('rateLimiterMetrics', { numberOfKeys: 0, lua: RATE_LIMITER_METRICS })
    redis.defineCommand('groupJobs', { numberOfKeys: 0, lua: GROUP_JOBS })
    redis.defineCommand('rateLimit', { numberOfKeys: 0, lua: RATE_LIMIT })
    redis.defineCommand('fireSchedule', { numberOfKeys: 0, lua: FIRE_SCHEDULE })
    registered.add(redis)
  }
  return redis as QueueRedis
}
