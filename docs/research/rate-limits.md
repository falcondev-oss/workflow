# Rate limit algorithms and prior art

Research for [#29](https://github.com/falcondev-oss/workflow/issues/29), part of the [rate limiters map](https://github.com/falcondev-oss/workflow/issues/28). The question: how do established queues and rate-limit libraries limit job starts against a strict external quota, and what does each algorithm cost on Redis and on Postgres?

Sources are official docs and source code, read September 2026. Source links are pinned to a commit or tag where practical. Statements marked **(analysis)** are my own reasoning, not claims made by a source.

## 1. Algorithms

Notation: the external quota is `N` starts per window `W`. "Boundary overshoot" means the most starts the limiter admits in some interval of length `W`.

| Algorithm | Boundary overshoot | Burst | State per limiter | One atomic Lua call | One Postgres statement |
| --- | --- | --- | --- | --- | --- |
| Fixed window | up to `2N` in a `W` straddling two windows | `N` at the start of each window | 1 counter + TTL | yes | yes (upsert on `(limiter, window_start)`) |
| Sliding window log | none, exact | `N` | up to `N` timestamps | yes | yes, if the log is a table or array |
| Sliding window counter | approximate, can exceed `N` slightly | smoothed | 2 counters | yes | yes |
| Token bucket | up to `B + N` in a `W` when `B = N` | `B` (bucket size) | tokens + last refill time | yes | yes (`UPDATE ... RETURNING`) |
| GCRA | same envelope as a token bucket of size `burst + 1` | `burst + 1` | 1 timestamp (TAT) | yes | yes |

### Fixed window

- One counter per window, `INCR` plus `EXPIRE` on the first hit. The Redis `INCR` docs give it as the canonical rate limiter pattern and say to wrap `INCR` + conditional `EXPIRE` in Lua to avoid leaking a key without a TTL ([INCR, Pattern: rate limiter](https://redis.io/docs/latest/commands/incr/#pattern-rate-limiter)).
- Boundary overshoot: "A client could send 10 requests at second 9 and another 10 at second 11, 20 requests in 2 seconds while technically staying within a '10 per 10 seconds' limit" ([Redis rate limiting tutorial, fixed window trade-offs](https://redis.io/learn/howtos/ratelimiting#1-fixed-window-counter)).
- It is exact only if our windows line up with the provider's windows. We usually cannot know how the provider aligns them **(analysis)**.

### Sliding window log

- Store one timestamp per admitted event in a sorted set. Each check runs `ZREMRANGEBYSCORE` for entries older than `now - W`, then `ZCARD`, then `ZADD` if under the limit. The tutorial rates it "Exact", "No bursts", with "O(n) entries" of memory ([Redis tutorial, comparison table and section 2](https://redis.io/learn/howtos/ratelimiting#2-sliding-window-log)). Retry-after is `oldest + W - now` (same section).
- Cost is bounded for our use: the limiter never admits more than `N` in a window, so the log never holds more than `N` live entries. For start quotas (tens to low thousands per window) that is small **(analysis)**.
- This is the only listed algorithm that guarantees "never more than `N` in any interval of length `W`" regardless of how the provider aligns its windows **(analysis, follows from the definition)**.

### Sliding window counter

- Keep the current and previous fixed-window counts and estimate `prev * (1 - elapsed) + current`. Cloudflare describes it with `rate = 42 * ((60-15)/60) + 18` and reports "0.003% of requests have been wrongly allowed or rate limited" across 400M requests, with "only two numbers per counter" ([Cloudflare, Counting things, a lot of different things](https://blog.cloudflare.com/counting-things-a-lot-of-different-things/)).
- The estimate assumes events in the previous window were spread evenly, so it can let through more than `N` in a real sliding window. Cloudflare saw sources "allowed despite generating traffic slightly above the threshold" (same post). That rules it out for a strict quota **(analysis)**.

### Token bucket

- A bucket holds up to `max_tokens`, refilled by `tokens_per_fill` every `fill_interval`, and starts full ([Envoy `TokenBucket` proto](https://github.com/envoyproxy/envoy/blob/main/api/envoy/type/v3/token_bucket.proto)).
- A lazy-refill version stores `tokens` and `last_refill` in a hash and refills by `elapsed * rate` capped at capacity, all in one Lua script ([Redis tutorial, section 4](https://redis.io/learn/howtos/ratelimiting#4-token-bucket)).
- Envelope: in any interval `T` it admits at most `B + rate * T`. With `B = N` and `rate = N / W`, one window of length `W` can see `2N` (a full bucket plus a full refill) **(analysis)**. To stay under a strict `N per W`, use `B = 1` (a smooth rate, no burst) or accept the overshoot.
- Uses floating-point math, "which needs care to avoid drift" ([Redis tutorial, section 4 trade-offs](https://redis.io/learn/howtos/ratelimiting#4-token-bucket)).

### GCRA (generic cell rate algorithm)

- redis-cell implements GCRA as `CL.THROTTLE <key> <max_burst> <count per period> <period> [<quantity>]`. It returns limited, limit, remaining, retry-after, and reset-after ([brandur/redis-cell README](https://github.com/brandur/redis-cell)).
- State is a single timestamp, the theoretical arrival time (TAT). `emission_interval = period / count`, `delay_variation_tolerance = emission_interval * (max_burst + 1)`, `limit = max_burst + 1` ([redis-cell `src/cell/mod.rs` L62-86](https://github.com/brandur/redis-cell/blob/e8d93c2466ce8ba37037c31124a0cb4d4d1a80e/src/cell/mod.rs#L62-L86)).
- The check: `new_tat = max(tat, now) + increment`, `allow_at = new_tat - dvt`, limited when `now < allow_at`, and then `retry_after = -(now - allow_at)`. Nothing is written when the call is limited ([mod.rs L137-192](https://github.com/brandur/redis-cell/blob/e8d93c2466ce8ba37037c31124a0cb4d4d1a80e/src/cell/mod.rs#L137-L192)).
- redis-cell reads `now` from the store, not the process, so that "different nodes can operate with a consistent clock" ([`src/cell/store.rs` L26-34](https://github.com/brandur/redis-cell/blob/e8d93c2466ce8ba37037c31124a0cb4d4d1a80e/src/cell/store.rs#L26-L34)).
- GCRA is a token bucket expressed as one timestamp: the same burst envelope, no refill arithmetic, and an exact retry-after for free **(analysis)**. With `max_burst = 0` it spaces starts `W / N` apart.
- Inngest uses GCRA for both `rateLimit` and `throttle` (see section 2).

### Atomicity

- **Redis:** "Redis guarantees the script's atomic execution. While executing the script, all server activities are blocked during its entire runtime" ([Scripting with Lua](https://redis.io/docs/latest/develop/programmability/eval-intro/)). Every algorithm above fits in one script, and `RESERVE` is already one script, so the limiter check can live inside the claim loop.
- **Postgres:** each algorithm is a read-modify-write on one row (or a small log table). The claim already runs in a transaction holding a per-namespace advisory lock (map #28 notes), so a read followed by an update inside that transaction is serialized for claims in the same namespace. A limiter shared across namespaces needs its own serialization, for example `SELECT ... FOR UPDATE` on the limiter row or a guarded `UPDATE ... WHERE ... RETURNING` **(analysis)**.

## 2. Prior art

Versions read: BullMQ v6.3.6, graphile-worker v0.18.0, pg-boss 12.33.0, Hatchet v0.107.0, Inngest v1.44.0, Temporal v1.32.0, Trigger.dev docs as of September 2026.

| System | Limit on starts | Algorithm | Limited job | Reactive pause of the whole limiter |
| --- | --- | --- | --- | --- |
| BullMQ | `limiter: { max, duration }`, one key per queue | fixed window (`INCR` + `PEXPIRE`) | stays in wait | yes: `queue.rateLimit(ms)` + `RateLimitError` |
| BullMQ Pro | `group.limit`, per group | not public | group skipped, others run | per group: `worker.rateLimitGroup` |
| graphile-worker | none built in | n/a | n/a | no |
| pg-boss | none (send-side throttle only) | n/a | n/a | no |
| Hatchet | static or dynamic keys, `units` | fixed-window refill in Postgres | requeued with `requeue_after` | no (only `workflows.pause`) |
| Inngest | `rateLimit` (drops) and `throttle` (delays) | GCRA in Lua | dropped / queued FIFO | no |
| Trigger.dev | none (concurrency only) | n/a | n/a | no (`queues.pause`, manual resume) |
| Temporal | task queue activities per second | GCRA-style `simpleLimiter` in memory | stays in the matcher | set rate to 0 |

### BullMQ (open source)

- **Algorithm:** a fixed window. Each move to active runs `INCR` on `{prefix}:{queue}:limiter` and sets `PEXPIRE duration` when the counter is 1 ([prepareJobForProcessing.lua L16-27](https://github.com/taskforcesh/bullmq/blob/v6.3.6/src/commands/includes/prepareJobForProcessing.lua#L16-L27)). Before taking a job the script checks `GET limiter >= max` and returns the key's `PTTL` ([getRateLimitTTL.lua L4-17](https://github.com/taskforcesh/bullmq/blob/v6.3.6/src/commands/includes/getRateLimitTTL.lua#L4-L17), called from [moveToActive-11.lua L62-66](https://github.com/taskforcesh/bullmq/blob/v6.3.6/src/commands/moveToActive-11.lua#L62-L66)).
- **Overshoot:** the window starts at the first job, not on a clock boundary, and the key expires with no memory, so up to `2 * max` starts can land inside one `duration` **(analysis of the code above)**.
- **Scope:** "The rate limiter is global, so if you have for example 10 workers for one queue ... only 10 jobs will be processed by second" ([docs](https://docs.bullmq.io/guide/rate-limiting)). Each worker passes its own `max`/`duration` ([redis-queue-backend.ts L1628-1637](https://github.com/taskforcesh/bullmq/blob/v6.3.6/src/classes/redis-queue-backend.ts#L1628-L1637)), so workers with different options share one counter but compare it against different limits. `queue.setGlobalRateLimit` stores `max`/`duration` in the meta hash ([queue.ts L282-284](https://github.com/taskforcesh/bullmq/blob/v6.3.6/src/classes/queue.ts#L282-L284), [docs](https://docs.bullmq.io/guide/queues/global-rate-limit)). Meta `max` wins over the worker's, but the worker's `duration` wins over meta ([moveToActive-11.lua L62, L71](https://github.com/taskforcesh/bullmq/blob/v6.3.6/src/commands/moveToActive-11.lua#L62-L71)).
- **What counts:** every move from wait or prioritized to active, including the fetch-next paths inside `moveToFinished` and `moveToDelayed` ([fetchNextJob.lua L58-85](https://github.com/taskforcesh/bullmq/blob/v6.3.6/src/commands/includes/fetchNextJob.lua#L58-L85)). Retries and recovered stalled jobs count again when picked up, since `moveStalledJobsToWait` only puts them back in wait ([moveStalledJobsToWait-9.lua L95-103](https://github.com/taskforcesh/bullmq/blob/v6.3.6/src/commands/moveStalledJobsToWait-9.lua#L95-L103)). Jobs flagged for deferred failure (`defa`) skip the `INCR` (same file L96-98). Nothing is ever decremented.
- **A limited job** is never taken and "will actually stay in the waiting state" ([docs](https://docs.bullmq.io/guide/rate-limiting); [moveToActive-11.lua L65-74](https://github.com/taskforcesh/bullmq/blob/v6.3.6/src/commands/moveToActive-11.lua#L65-L74)). The worker sets `limitUntil = Date.now() + ttl` and sleeps locally ([worker.ts L893-900](https://github.com/taskforcesh/bullmq/blob/v6.3.6/src/classes/worker.ts#L893-L900)). The sleep is capped by `maximumRateLimitDelay` (default 30000 ms) so delayed jobs still get promoted ([worker.ts L876-880](https://github.com/taskforcesh/bullmq/blob/v6.3.6/src/classes/worker.ts#L876-L880)).
- **Reactive form:** `queue.rateLimit(ms)` runs `SET limiter MAX_SAFE_INTEGER PX ms` ([redis-queue-backend.ts L2748-2753](https://github.com/taskforcesh/bullmq/blob/v6.3.6/src/classes/redis-queue-backend.ts#L2748-L2753)). `worker.rateLimit` is deprecated in its favour ([worker.ts L756-771](https://github.com/taskforcesh/bullmq/blob/v6.3.6/src/classes/worker.ts#L756-L771)). The handler then throws `Worker.RateLimitError()` ([rate-limit-error.ts](https://github.com/taskforcesh/bullmq/blob/v6.3.6/src/classes/errors/rate-limit-error.ts#L1-L15)). The worker recognises it and calls `job.moveToWait(token)`, skipping failure handling ([worker.ts L1149-1153](https://github.com/taskforcesh/bullmq/blob/v6.3.6/src/classes/worker.ts#L1149-L1153)). `moveJobFromActiveToWait` releases the lock and puts the job back at the head of wait. It does not increment `attemptsMade`, but `attemptsStarted` was already incremented on activation ([moveJobFromActiveToWait-9.lua L36-67](https://github.com/taskforcesh/bullmq/blob/v6.3.6/src/commands/moveJobFromActiveToWait-9.lua#L36-L67)).
- **Footgun:** the manual pause only takes effect when a `max` is configured, because `getRateLimitTTL` does nothing without one ([getRateLimitTTL.lua L5](https://github.com/taskforcesh/bullmq/blob/v6.3.6/src/commands/includes/getRateLimitTTL.lua#L5)). The docs tell users to always set `limiter` ([docs](https://docs.bullmq.io/guide/rate-limiting)). `removeRateLimitKey()` deletes the key, which also resets the counter (same docs page).
- **Ordering and clock:** the rate limit is checked before the concurrency "maxed" check ([getQueueMetadata.lua L5-21](https://github.com/taskforcesh/bullmq/blob/v6.3.6/src/commands/includes/getQueueMetadata.lua#L5-L21)). The window lives in Redis TTLs (`PEXPIRE`/`PTTL`). Other timestamps come from the client's `Date.now()` passed in as ARGV ([promoteDelayedJobs.lua L18](https://github.com/taskforcesh/bullmq/blob/v6.3.6/src/commands/includes/promoteDelayedJobs.lua#L18)).

### BullMQ Pro groups (docs only, source is closed)

- Each group is limited on its own with `group.limit: { max, duration }`. A limited group is skipped while other groups keep running. `worker.rateLimitGroup(job, duration)` plus `throw Worker.RateLimitError()` pauses one group, and `queue.getGroupRateLimitTtl` reports the remaining time ([docs](https://docs.bullmq.io/bullmq-pro/groups/rate-limiting)). `queue.setGroupRateLimit(groupId, max, duration)` overrides the limit per group, but every worker still needs a default `group.limit` ([docs](https://docs.bullmq.io/bullmq-pro/groups/local-group-rate-limit)).
- The changelog says group jobs also count toward the global limiter, and the rate-limit delay is used as the block timeout ([changelog-v6.md L282-289](https://github.com/taskforcesh/bullmq/blob/v6.3.6/docs/gitbook/bullmq-pro/changelogs/changelog-v6.md#L282-L289), [changelog.md](https://github.com/taskforcesh/bullmq/blob/v6.3.6/docs/gitbook/bullmq-pro/changelog.md)).

### graphile-worker

- **No built-in limiter.** The claim query can skip jobs whose `flags` match: `and ((flags ?| $N::text[]) is not true)` ([src/sql/getJobs.ts L97-98](https://github.com/graphile/worker/blob/v0.18.0/src/sql/getJobs.ts#L97-L98)). The worker takes `forbiddenFlags`, so a rate limiter can live outside the database and name the flags that are currently limited.
- [graphile-worker-rate-limiter](https://github.com/politics-rewired/graphile-worker-rate-limiter) is a third-party library built on this hook, with a leaky bucket kept in Redis. It is a soft limit: the flag list is computed before the claim, not inside it.
- **Clock:** the claim uses `now()` unless `useNodeTime` is set, in which case the client passes the time as a parameter ([getJobs.ts L101](https://github.com/graphile/worker/blob/v0.18.0/src/sql/getJobs.ts#L101), used at L235 `run_at <= ${now}` and L248 `locked_at = ${now}`).

### pg-boss

- **No per-window limit on job starts.** Throttling is on the send side: `singletonSeconds` deduplicates sends through a unique index on `(name, singleton_on, key)` ([docs](https://timgit.github.io/pg-boss/)). That caps how many jobs are created, not how fast they start.
- `fail()` takes no per-call delay, so a handler cannot say "retry after `Retry-After`". Retry timing comes from the queue's `retryDelay`/backoff (same docs).
- **Clock:** every write goes through `job_now()`, whose body is `SELECT pg_catalog.now();`, with a test-only override ([src/plans.ts L199-224](https://github.com/timgit/pg-boss/blob/12.33.0/src/plans.ts#L199-L224)).

### Hatchet

- **Model:** rate limits are keys with a limit, a window, and a `units` cost per step. Keys can be static or computed per run ([docs](https://docs.hatchet.run/home/rate-limits)).
- **Implementation:** a fixed-window refill in the Postgres `"RateLimit"` table. `ListRateLimitsForTenantWithMutate` locks the due rows with `FOR UPDATE`, sets `"value" = get_refill_value(rl)` and `"lastRefill" = CURRENT_TIMESTAMP`, and returns `nextRefillAt` ([rate_limits.sql L125-163](https://github.com/hatchet-dev/hatchet/blob/v0.107.0/pkg/repository/sqlcv1/rate_limits.sql#L125-L163)). The window check uses `NOW()` (L92, L133).
- The scheduler admits work against an in-memory copy. It flushes consumed units to the database every second and caps updates at one per 500 ms "to avoid boundary conditions on 1 second polls" ([pkg/scheduling/v1/rate_limit.go L15, L72-81](https://github.com/hatchet-dev/hatchet/blob/v0.107.0/pkg/scheduling/v1/rate_limit.go#L15-L81)). So the limiter is checked outside the claim transaction.
- **Limited items** move to `v1_rate_limited_queue_items` with a `requeue_after` once the next refill is more than about 2 s away. Every retry is charged again. There is no retry-after error; the only runtime lever is pausing a workflow (not pinned to a source line).

### Inngest

- **Two primitives:** `rateLimit` drops runs over the limit ([docs](https://www.inngest.com/docs/guides/rate-limiting)). `throttle` delays them in FIFO order and allows a `burst` ([docs](https://www.inngest.com/docs/guides/throttling)). Both count run starts only.
- **Both are GCRA in Lua**, with the clock passed in from Go as `now_ns`. `limit = burst + 1`, `emission = period_ns / limit`, `dvt = emission * (burst + 1)` ([pkg/constraintapi/lua/helper/gcra.lua](https://github.com/inngest/inngest/blob/v1.44.0/pkg/constraintapi/lua/helper/gcra.lua)). The older path is [pkg/execution/ratelimit/lua/ratelimit.lua](https://github.com/inngest/inngest/blob/v1.44.0/pkg/execution/ratelimit/lua/ratelimit.lua).
- **Reactive:** `RetryAfterError` delays the retry of one step ([docs](https://www.inngest.com/docs/reference/typescript/functions/errors)). It still uses up an attempt and affects only that item. There is no way to pause the limiter itself.

### Trigger.dev

- **No run rate limit.** Queues and `concurrencyLimit` cap parallelism only ([docs](https://trigger.dev/docs/queue-concurrency)).
- **Reactive:** `catchError` can return `retryAt`, and `retry.fetch` can retry on status 429 using the response headers ([docs](https://trigger.dev/docs/errors-retrying)). Both affect one run. `queues.pause` stops a queue but never resumes it on its own ([docs](https://trigger.dev/docs/management/queues/pause)).

### Temporal

- **Model:** `maxTaskQueueActivitiesPerSecond` is a worker option, and the last poller to set it wins. The newer `UpdateTaskQueueConfig` API sets the rate on the server ([Task queue docs](https://docs.temporal.io/task-queue)).
- **Implementation:** the matching service keeps a limiter in memory per partition. `simpleLimiter` is commented as implementing "a 'GCRA' limiter", stored as a single "ready time as unix nanos" ([service/matching/matcher_data.go L681-706](https://github.com/temporalio/temporal/blob/v1.32.0/service/matching/matcher_data.go#L681-L706)). The reader also reports that a token is refunded when a task is recycled and that retries are charged again ([ratelimit_manager.go](https://github.com/temporalio/temporal/blob/v1.32.0/service/matching/ratelimit_manager.go)).
- **Reactive:** `ApplicationFailure` with `nextRetryDelay` delays one activity's retry and still uses up an attempt ([docs](https://docs.temporal.io/develop/typescript/failure-detection)). Setting the rate to 0 through the config API pauses dispatch for the whole task queue. That is the closest any of these systems gets to pausing a whole limiter, and it has no automatic resume.

### Takeaways from prior art

- Only BullMQ has "pause the whole limiter for `ms`" built in (`queue.rateLimit(ms)`), and it is built as "fill the fixed window until `now + ms`".
- Every system that counts at claim time charges retries and recovered jobs again.
- None of them refunds a start when a job is put back because of a 429. Temporal's refund covers internal task recycling only.
- Limiting at claim time with the job left in waiting (BullMQ) keeps the job's queue position and avoids extra state. Moving the job to a delayed or "rate limited" set (Hatchet) lets the scheduler compute exactly when to wake.

## 3. Clock source

### Redis `TIME`

- `TIME` returns "the current server time as a two items lists: a Unix timestamp and the amount of microseconds already elapsed in the current second", similar to `gettimeofday` ([TIME](https://redis.io/docs/latest/commands/time/)).
- Scripts may call `TIME` and then write. Effects replication is the default since Redis 5.0 and the only mode since 7.0. With it, "the restrictions on non-deterministic functions are removed. You can, for example, use the TIME ... commands inside your scripts freely at any place" ([Scripting with Lua, script replication](https://redis.io/docs/latest/develop/programmability/eval-intro/#replicating-commands-instead-of-scripts)). Before that, a write after `TIME` was blocked with an error (same page, "Scripts with deterministic writes").
- This repo already reads `TIME` once per script as "the single clock authority" ([`src/queue/scripts.ts` `NOW`](https://github.com/falcondev-oss/workflow/blob/93d8eeb/src/queue/scripts.ts#L16-L20)). `TIME` is a wall clock, so a failover to a replica with a skewed clock moves `now`. Every worker still sees the same clock, which is what a shared limiter needs **(analysis)**.

### Postgres `now()`

- "`now()` is a traditional PostgreSQL equivalent to `transaction_timestamp()`". These functions "return the start time of the current transaction, their values do not change during the transaction". `statement_timestamp()` returns the start of the current statement, and "`clock_timestamp()` returns the actual current time, and therefore its value changes even within a single SQL statement" ([Date/Time Functions, Current Date/Time](https://www.postgresql.org/docs/current/functions-datetime.html#FUNCTIONS-DATETIME-CURRENT)).
- Transaction-level advisory locks "are automatically released at the end of the transaction, and there is no explicit unlock operation" ([Explicit Locking, Advisory Locks](https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS)).
- Consequence **(analysis)**: if a claim transaction waits on the advisory lock, its `now()` is the time *before* the wait. Two claims serialized by the lock can therefore see `now()` values in the opposite order to the order they run in, and each can be up to one lock wait in the past. A limiter that writes `last_start = now()` can move backwards, and one that compares `now() - W` sees a slightly stale window. That errs toward admitting early, which is the unsafe direction for a strict quota. `clock_timestamp()` read after taking the lock is monotonic with respect to the lock order (on one server with a sane clock), so the limiter check should use it.
- graphile-worker and pg-boss both use `now()` in the claim (section 2). They have no limiter, so the difference does not matter to them. Hatchet also uses `NOW()`/`CURRENT_TIMESTAMP` in its refill query.

## 4. Implications for this repo

**(analysis throughout)**

1. **Algorithm: sliding window log** (or GCRA with `burst = 0` if spacing starts evenly is acceptable). It is the only option that never goes over "`N` per `W`" however the provider aligns its windows, and its state is bounded by `N`. Redis: a ZSET per limiter, capped with `ZREMRANGEBYSCORE ... now - W` then `ZCARD < N`. Postgres: a `limiter_start(limiter, started_at)` table, or an array column, pruned on the same condition. A fixed window like BullMQ's can start `2N` in one `W`, which is what gets a strict API to return 429s.
2. **Check it inside the `RESERVE` claim loop,** right next to the `nsCap`/`wfCap` gate (`src/queue/scripts.ts` L288). A batch claim (`want` > 1) must record one start per claimed job, not per call. When the limiter stops the batch, `reserve` should return `msUntilFree = oldest + W - now` next to `msToNext`, and the worker should fold it into the `BRPOP` timeout the same way `msToNext` is folded in (`src/queue/worker.ts` L140-144). Otherwise a limited worker spins or waits for the safety timeout. `maxed` alone is not enough: nothing kicks `wake` when a window slides.
3. **The limited job stays in waiting**, like BullMQ. No new state and no loss of queue position, and the group head is left untouched.
4. **Reactive pause = a `pausedUntil` timestamp on the limiter**, set with `max(existing, now + ms)` and checked in the same gate. This is simpler than BullMQ's "fill the counter" trick, works with any algorithm, and has no "only works if `max` is set" footgun. The handler then throws a `RateLimitError`. `fail` should requeue the job to waiting without incrementing `attempts`, which is BullMQ's `moveJobFromActiveToWait` behaviour. Whether the 429 start is refunded from the log is a spec decision: nobody else refunds it, and keeping it errs on the safe side.
5. **Counting:** every claim counts, including retries (after the `delayed` promotion) and stalled recoveries on re-claim, because each one is a real start against the external API. This matches every system surveyed.
6. **Clock:** Redis keeps using `TIME` (already the single clock authority). Postgres should read `clock_timestamp()` once, *after* taking the advisory lock, not `now()`. A limiter shared across namespaces is not covered by the per-namespace lock and needs `SELECT ... FOR UPDATE` on the limiter row inside the claim transaction.
7. **Cross-workflow wake:** a shared limiter needs its own wake list (or the namespace wake list) kicked when `pausedUntil` is set. Otherwise idle workers of other workflows keep blocking with timeouts from before the pause. Workers blocked because a window is full need no kick, only the `msUntilFree` timeout from point 2.
