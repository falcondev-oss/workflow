/**
 * PROTOTYPE — wipe me. The ten Postgres ops #20 is about, plus the private `enqueueJob`
 * helper #14 promised. Where the ticket poses a real question, both candidates live here
 * side by side (`failSplit` vs `failCase`, `stepConcat` vs `stepJsonbSet`, three `metrics*`).
 *
 * Conventions inherited and not re-litigated: every identifier schema-qualified and
 * `search_path` never touched (#13); epoch-ms at the seam, `timestamptz` internally (#14);
 * driver transactions pinned `read committed` (#14); notify channels hashed in JS (#9).
 */
import type postgres from 'postgres'
import { createHash } from 'node:crypto'

/**
 * The handle every write op takes. #12 recorded that `TransactionSql` is not assignable to
 * `Sql`; `ISql` is their shared supertype and postgres.js does export it, so the union spelled
 * out in that ticket's facts is one name after all.
 */
export type Db = postgres.ISql

export const SCHEMA = 'workflow'

/** #9: `wf_wake_<sha256(schema, nsId)[0:16]>`, always hashed, never readable-when-it-fits. */
export const wakeChannel = (nsId: string) =>
  `wf_wake_${createHash('sha256').update(`${SCHEMA}:${nsId}`).digest('hex').slice(0, 16)}`

/** #9: `wait()` keeps a per-job channel — an idle one costs nothing server-side. */
export const doneChannel = (nsId: string, wfId: string, jobId: string) =>
  `wf_done_${createHash('sha256').update(`${SCHEMA}:${nsId}:${wfId}:${jobId}`).digest('hex').slice(0, 16)}`

/** epoch-ms → timestamptz, exact int64 µs arithmetic (#14). `null` passes through. */
const ts = (sql: Db, ms: number | null) =>
  sql`case when ${ms}::bigint is null then null
      else 'epoch'::timestamptz + (${ms}::bigint * interval '1 millisecond') end`

/** timestamptz → epoch-ms. `numeric` since PG 14, so `* 1000` is exact. postgres.js hands
 *  `bigint` back as a string, so every caller of this wraps it in `Number`. */
const msOf = (col: string) => `(extract(epoch from ${col}) * 1000)::bigint`

/* ─────────────────────────────── the shared enqueue ─────────────────────────────── */

export type EnqueueArgs = {
  nsId: string
  wfId: string
  jobId: string
  data: string
  groupId: string
  priority: number
  maxAttempts: number
  /** Absolute epoch-ms, or `null` for immediately ready. */
  runAt: number | null
}

/**
 * THE single insert site and THE single wake-notify site (#14's requirement, and the reason
 * `fireSchedule` calls this rather than fusing its own insert into the CAS statement).
 *
 * One statement. `waiting` vs `delayed` is decided against `now()` rather than in JS, so a
 * `runAt` already in the past lands straight in `waiting` exactly as Lua's `runAt > now`
 * branch does — no round-trip of latency waiting for the next `reserve` to promote it.
 * `seq` is stamped only on the waiting branch, which is what `job_seq_present` demands.
 *
 * Zero rows back ⇒ `'duplicate'`: `on conflict do nothing returning` *is* the `EXISTS` guard,
 * with no error string to match (#8). `pg_notify` reads off the insert's returned row, so it
 * fires once, only on a real insert, and only on commit — which is also why `fireSchedule`
 * rolling back cannot leave a phantom wake behind.
 */
export async function enqueueJob(sql: Db, a: EnqueueArgs): Promise<'enqueued' | 'duplicate'> {
  const rows = await sql`
    with args as (
      select ${ts(sql, a.runAt)} as run_at
    ),
    ins as (
      insert into workflow.job
        (wf_id, id, ns_id, group_id, data, state, priority, seq, max_attempts, run_at)
      select ${a.wfId}, ${a.jobId}, ${a.nsId}, ${a.groupId}, ${a.data},
             case when args.run_at is null or args.run_at <= now() then 'waiting' else 'delayed' end,
             ${a.priority}::int,
             case when args.run_at is null or args.run_at <= now()
                  then nextval('workflow.job_seq') end,
             ${a.maxAttempts}::int,
             case when args.run_at > now() then args.run_at end
      from args
      on conflict (wf_id, id) do nothing
      returning 1
    )
    select pg_notify(${wakeChannel(a.nsId)}, ${a.wfId}) from ins
  `
  return rows.length > 0 ? 'enqueued' : 'duplicate'
}

/* ────────────────────────────── the shared finalize ────────────────────────────── */

/**
 * The result write, the one fragment `complete`, `fail`'s dead-letter branch and #15's
 * recovery dead-letter all carry. **Upsert, not insert** (#15): `complete` deletes the job
 * row, so the id is reusable immediately while the result outlives it by `resultTtl`.
 *
 * It is a fragment rather than a statement because all three finalizers are single
 * statements — sharing the *call site* the way #14 shared `enqueueJob` is not available when
 * the shared part has to be a CTE of the caller's own statement.
 */
export const resultUpsert = (sql: Db, wfId: string, jobId: string, record: string, ttlMs: number) => sql`
  insert into workflow.result (wf_id, job_id, record, expires_at)
  select ${wfId}, ${jobId}, ${record}, now() + (${ttlMs}::bigint * interval '1 millisecond')
  from fin
  on conflict (wf_id, job_id) do update
    set record = excluded.record, expires_at = excluded.expires_at
  returning 1
`

/**
 * Token-guarded commit, one statement: delete the job row, upsert the result, wake the
 * `wait()` caller, kick the namespace (Redis got that last one free from `releaseActive`;
 * Postgres fires nothing unless the statement says so — the same catch #15 made for the
 * janitor).
 *
 * `lock_token = $token` is the whole guard. `state = 'active'` would be redundant: the token
 * is null on every other state, which is exactly what `complete`-vs-recover races on.
 */
export async function complete(
  sql: Db,
  a: {
    nsId: string
    wfId: string
    jobId: string
    token: string
    record: string
    /** The `wait()` payload — the record itself, or a bare marker past #9's 7999-byte cap. */
    published: string
    resultTtlMs: number
  },
): Promise<'committed' | 'stale-token'> {
  const rows = await sql`
    with fin as (
      delete from workflow.job
      where wf_id = ${a.wfId} and id = ${a.jobId} and lock_token = ${a.token}
      returning 1
    ),
    res as (${resultUpsert(sql, a.wfId, a.jobId, a.record, a.resultTtlMs)})
    select pg_notify(${doneChannel(a.nsId, a.wfId, a.jobId)}, ${a.published}),
           pg_notify(${wakeChannel(a.nsId)}, '*')
    from fin
  `
  return rows.length > 0 ? 'committed' : 'stale-token'
}

export type FailArgs = {
  nsId: string
  wfId: string
  jobId: string
  token: string
  reason: string
  stack: string
  /** Absolute epoch-ms for the retry, from the shared backoff policy. */
  retryAt: number
  record: string
  published: string
  resultTtlMs: number
  nonRecoverable: boolean
}

export type FailOutcome = 'requeued' | 'dead-lettered' | 'stale-token'

/**
 * CANDIDATE A — two statements split by predicate, one transaction, pipelined.
 *
 * The predicates are exact complements, so the pair is disjoint by construction and both can
 * be sent unconditionally: whichever matches decides the outcome, and if the requeue matches
 * it nulls `lock_token`, which independently disarms the dead-letter. Both zero ⇒ the claim
 * was recovered ⇒ `'stale-token'`.
 *
 * The requeue branch nulls `seq` — it lands in `delayed`, not `waiting`, so `job_seq_present`
 * demands it. That is the opposite of #15's recovery requeue, which is the one transition in
 * the system that carries `seq` across a state change.
 */
export async function failSplit(sql: postgres.Sql, a: FailArgs): Promise<FailOutcome> {
  return sql.begin('isolation level read committed', async (tx) => {
    const [requeued, deadLettered] = await Promise.all([
      tx`
        with fin as (
          update workflow.job set
            attempts = attempts + 1,
            state = 'delayed',
            run_at = ${ts(tx, a.retryAt)},
            seq = null,
            lock_token = null,
            deadline_at = null
          where wf_id = ${a.wfId} and id = ${a.jobId} and lock_token = ${a.token}
            and not ${a.nonRecoverable} and attempts + 1 < max_attempts
          returning 1
        )
        select pg_notify(${wakeChannel(a.nsId)}, '*') from fin
      `,
      tx`
        with fin as (
          update workflow.job set
            attempts = attempts + 1,
            state = 'failed',
            seq = null,
            lock_token = null,
            deadline_at = null,
            run_at = null,
            steps = '{}',
            finished_on = now(),
            failed_reason = ${a.reason},
            stacktrace = ${a.stack}
          where wf_id = ${a.wfId} and id = ${a.jobId} and lock_token = ${a.token}
            and (${a.nonRecoverable} or attempts + 1 >= max_attempts)
          returning 1
        ),
        res as (${resultUpsert(tx, a.wfId, a.jobId, a.record, a.resultTtlMs)})
        select pg_notify(${doneChannel(a.nsId, a.wfId, a.jobId)}, ${a.published}),
               pg_notify(${wakeChannel(a.nsId)}, '*')
        from fin
      `,
    ])
    if (requeued.length > 0 && deadLettered.length > 0) throw new Error('BOTH BRANCHES MATCHED')
    if (requeued.length > 0) return 'requeued' as const
    return deadLettered.length > 0 ? ('dead-lettered' as const) : ('stale-token' as const)
  })
}

/**
 * CANDIDATE B — one statement, the branch as a `case` in every column.
 *
 * One round-trip and no transaction, at the cost of repeating the retry predicate in eight
 * places (a `retry` CTE would be a second row read of the same row) and of a `RETURNING` that
 * has to re-derive which branch it took from the new row's `state`.
 */
export async function failCase(sql: Db, a: FailArgs): Promise<FailOutcome> {
  const retry = sql`not ${a.nonRecoverable} and attempts + 1 < max_attempts`
  const rows = await sql`
    with fin as (
      update workflow.job set
        attempts = attempts + 1,
        state = case when ${retry} then 'delayed' else 'failed' end,
        run_at = case when ${retry} then ${ts(sql, a.retryAt)} end,
        seq = null,
        lock_token = null,
        deadline_at = null,
        steps = case when ${retry} then steps else '{}' end,
        finished_on = case when ${retry} then null else now() end,
        failed_reason = case when ${retry} then null else ${a.reason} end,
        stacktrace = case when ${retry} then null else ${a.stack} end
      where wf_id = ${a.wfId} and id = ${a.jobId} and lock_token = ${a.token}
      returning state = 'failed' as dead
    ),
    res as (
      insert into workflow.result (wf_id, job_id, record, expires_at)
      select ${a.wfId}, ${a.jobId}, ${a.record},
             now() + (${a.resultTtlMs}::bigint * interval '1 millisecond')
      from fin where fin.dead
      on conflict (wf_id, job_id) do update
        set record = excluded.record, expires_at = excluded.expires_at
      returning 1
    )
    select fin.dead,
           pg_notify(${wakeChannel(a.nsId)}, '*'),
           case when fin.dead
                then pg_notify(${doneChannel(a.nsId, a.wfId, a.jobId)}, ${a.published}) end
    from fin
  `
  if (rows.length === 0) return 'stale-token'
  return rows[0]!.dead ? 'dead-lettered' : 'requeued'
}

/* ──────────────────────────────────── plain ops ──────────────────────────────────── */

/** Renew a claim. `deadline_at` is in no index — that is what keeps this HOT (#8). */
export async function heartbeat(
  sql: Db,
  a: { wfId: string; jobId: string; token: string; lockMs: number },
): Promise<'renewed' | 'stale-token'> {
  const rows = await sql`
    update workflow.job
    set deadline_at = now() + (${a.lockMs}::bigint * interval '1 millisecond')
    where wf_id = ${a.wfId} and id = ${a.jobId} and lock_token = ${a.token}
    returning 1
  `
  return rows.length > 0 ? 'renewed' : 'stale-token'
}

/** CANDIDATE A — `||`. No path syntax anywhere, so a step name is an opaque key. */
export async function stepConcat(
  sql: Db,
  a: { wfId: string; jobId: string; stepName: string; value: string },
): Promise<void> {
  await sql`
    update workflow.job
    set steps = steps || jsonb_build_object(${a.stepName}::text, ${a.value}::text)
    where wf_id = ${a.wfId} and id = ${a.jobId}
  `
}

/** CANDIDATE B — `jsonb_set` with a one-element `text[]` path. */
export async function stepJsonbSet(
  sql: Db,
  a: { wfId: string; jobId: string; stepName: string; value: string },
): Promise<void> {
  await sql`
    update workflow.job
    set steps = jsonb_set(steps, array[${a.stepName}], to_jsonb(${a.value}::text), true)
    where wf_id = ${a.wfId} and id = ${a.jobId}
  `
}

/** The expiry predicate is the semantics; the janitor sweep is only garbage collection (#15). */
export async function getResult(sql: Db, a: { wfId: string; jobId: string }): Promise<string | null> {
  const rows = await sql<{ record: string }[]>`
    select record from workflow.result
    where wf_id = ${a.wfId} and job_id = ${a.jobId} and expires_at > now()
  `
  return rows[0]?.record ?? null
}

/** Omitting `last_job_id`/`last_fire_at` from the update list preserves them — structurally
 *  the same thing Redis' "HSET only the listed fields" does for free (#14). */
export async function upsertSchedule(
  sql: Db,
  a: {
    wfId: string
    scheduleId: string
    pattern: string
    tz: string
    data: string
    priority: number
    groupId: string
    skipIfRunning: boolean
    nextRun: number
  },
): Promise<void> {
  await sql`
    insert into workflow.schedule
      (wf_id, schedule_id, pattern, tz, data, priority, group_id, skip_if_running, next_run)
    values (${a.wfId}, ${a.scheduleId}, ${a.pattern}, ${a.tz}, ${a.data}, ${a.priority},
            ${a.groupId}, ${a.skipIfRunning}, ${ts(sql, a.nextRun)})
    on conflict (wf_id, schedule_id) do update set
      pattern = excluded.pattern, tz = excluded.tz, data = excluded.data,
      priority = excluded.priority, group_id = excluded.group_id,
      skip_if_running = excluded.skip_if_running, next_run = excluded.next_run
  `
}

export async function removeSchedule(sql: Db, a: { wfId: string; scheduleId: string }): Promise<void> {
  await sql`delete from workflow.schedule where wf_id = ${a.wfId} and schedule_id = ${a.scheduleId}`
}

export type ScheduleInfo = {
  scheduleId: string
  pattern: string
  tz: string
  nextRun: number
  lastFireAt: number | null
  lastJobId: string | null
}

/** `(next_run, schedule_id)` is contractual (#14) — it is what Redis' `zrange` score-then-member
 *  ordering means, ties included. */
export async function getSchedules(sql: Db, a: { wfId: string }): Promise<ScheduleInfo[]> {
  const rows = await sql<
    {
      schedule_id: string
      pattern: string
      tz: string
      next_run: string
      last_fire_at: string | null
      last_job_id: string | null
    }[]
  >`
    select schedule_id, pattern, tz,
           ${sql.unsafe(msOf('next_run'))} as next_run,
           ${sql.unsafe(msOf('last_fire_at'))} as last_fire_at,
           last_job_id
    from workflow.schedule
    where wf_id = ${a.wfId}
    order by next_run, schedule_id
  `
  return rows.map((r) => ({
    scheduleId: r.schedule_id,
    pattern: r.pattern,
    tz: r.tz,
    nextRun: Number(r.next_run),
    lastFireAt: r.last_fire_at === null ? null : Number(r.last_fire_at),
    lastJobId: r.last_job_id,
  }))
}

export type QueueMetrics = { active: number; waiting: number; delayed: number }

/** CANDIDATE A — one statement, three scalar subqueries, one per partial index. `ns_id` is
 *  passed because `job_active_idx` leads with it and the driver instance knows it. */
export async function metricsSubqueries(
  sql: Db,
  a: { nsId: string; wfId: string },
): Promise<QueueMetrics> {
  const [row] = await sql<{ active: number; waiting: number; delayed: number }[]>`
    select
      (select count(*)::int from workflow.job
        where ns_id = ${a.nsId} and wf_id = ${a.wfId} and state = 'active') as active,
      (select count(*)::int from workflow.job
        where wf_id = ${a.wfId} and state = 'waiting') as waiting,
      (select count(*)::int from workflow.job
        where wf_id = ${a.wfId} and state = 'delayed') as delayed
  `
  return row!
}

/** CANDIDATE A′ — the same, without `ns_id`, to price the leading column of `job_active_idx`. */
export async function metricsSubqueriesNoNs(sql: Db, a: { wfId: string }): Promise<QueueMetrics> {
  const [row] = await sql<{ active: number; waiting: number; delayed: number }[]>`
    select
      (select count(*)::int from workflow.job
        where wf_id = ${a.wfId} and state = 'active') as active,
      (select count(*)::int from workflow.job
        where wf_id = ${a.wfId} and state = 'waiting') as waiting,
      (select count(*)::int from workflow.job
        where wf_id = ${a.wfId} and state = 'delayed') as delayed
  `
  return row!
}

/** CANDIDATE B — one pass over the workflow's rows with filtered aggregates. Reads like the
 *  obvious answer; the plan is the question. */
export async function metricsFilter(sql: Db, a: { wfId: string }): Promise<QueueMetrics> {
  const [row] = await sql<{ active: number; waiting: number; delayed: number }[]>`
    select
      count(*) filter (where state = 'active')::int  as active,
      count(*) filter (where state = 'waiting')::int as waiting,
      count(*) filter (where state = 'delayed')::int as delayed
    from workflow.job where wf_id = ${a.wfId}
  `
  return row!
}

/** CANDIDATE C — Redis' shape: three separate reads. Here they are three round-trips. */
export async function metricsThreeTrips(
  sql: postgres.Sql,
  a: { nsId: string; wfId: string },
): Promise<QueueMetrics> {
  const [[active], [waiting], [delayed]] = await Promise.all([
    sql<{ n: number }[]>`select count(*)::int as n from workflow.job
      where ns_id = ${a.nsId} and wf_id = ${a.wfId} and state = 'active'`,
    sql<{ n: number }[]>`select count(*)::int as n from workflow.job
      where wf_id = ${a.wfId} and state = 'waiting'`,
    sql<{ n: number }[]>`select count(*)::int as n from workflow.job
      where wf_id = ${a.wfId} and state = 'delayed'`,
  ])
  return { active: active!.n, waiting: waiting!.n, delayed: delayed!.n }
}
