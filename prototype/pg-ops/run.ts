/**
 * PROTOTYPE — wipe me.  `pnpm proto:pg-ops`
 *
 * Scratch Postgres 18 on :15499 (see README.md). Answers, in order:
 *   A. `enqueueJob` — one statement, one insert site, one notify site; delayed branch; duplicate;
 *      composed inside `fireSchedule`'s transaction; bulk-notify collapse.
 *   B. `fail` — split-by-predicate vs one `case` statement: same row state? same outcomes?
 *   C. `complete` and the shared finalize fragment, including #15's reusable-id collision.
 *   D. `getMetrics` — one statement or three, and what `ns_id` is worth.
 *   E. `setStepData` — `||` vs `jsonb_set`, hostile step names, concurrent writers, O(n²).
 *   F. `heartbeat` — is it really a HOT update?
 *   G. `getSchedules` — is `(next_run, schedule_id)` served by `schedule_due_idx`?
 */
import { randomUUID } from 'node:crypto'
import { readFile } from 'node:fs/promises'
import postgres from 'postgres'
import {
  complete,
  doneChannel,
  enqueueJob,
  failCase,
  failSplit,
  getResult,
  getSchedules,
  heartbeat,
  metricsFilter,
  metricsSubqueries,
  metricsSubqueriesNoNs,
  metricsThreeTrips,
  removeSchedule,
  stepConcat,
  stepJsonbSet,
  upsertSchedule,
  wakeChannel,
} from './queries'

const DB_URL = 'postgres://postgres:proto@localhost:15499/proto'
const sql = postgres(DB_URL, { max: 20, onnotice: () => {} })
const listener = postgres(DB_URL, { max: 2, onnotice: () => {} })

const NS = 'ns1'
const WF = 'wf1'
const TTL = 300_000

const h = (s: string) => console.log(`\n\x1b[1m── ${s}\x1b[0m`)
let failures = 0
const ok = (c: boolean, s: string) => {
  if (!c) failures++
  console.log(`  ${c ? '\x1b[32m✓' : '\x1b[31m✗'} ${s}\x1b[0m`)
}
const info = (s: string) => console.log(`    \x1b[2m${s}\x1b[0m`)
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

async function resetSchema() {
  await sql.unsafe(await readFile(new globalThis.URL('schema.sql', import.meta.url), 'utf8'))
}

/** Collect every notification on a channel for the life of the run. */
const heard: Record<string, string[]> = {}
async function listen(channel: string) {
  heard[channel] ??= []
  await listener.listen(channel, (payload) => heard[channel]!.push(payload))
}
const drain = async (channel: string) => {
  await sleep(60)
  const got = heard[channel]!.slice()
  heard[channel]!.length = 0
  return got
}

const row = async (jobId: string) =>
  (
    await sql<any[]>`select *, (extract(epoch from run_at)*1000)::bigint as run_at_ms
                     from workflow.job where wf_id = ${WF} and id = ${jobId}`
  )[0]

const claim = async (jobId: string, token: string) => {
  await sql`update workflow.job set state='active', lock_token=${token},
            deadline_at = now() + interval '30 seconds' where wf_id=${WF} and id=${jobId}`
}

const enqueueArgs = (jobId: string, over: Partial<Parameters<typeof enqueueJob>[1]> = {}) => ({
  nsId: NS,
  wfId: WF,
  jobId,
  data: '{"n":1}',
  groupId: 'g1',
  priority: 0,
  maxAttempts: 3,
  runAt: null,
  ...over,
})

const failArgs = (jobId: string, token: string, over: Partial<Parameters<typeof failCase>[1]> = {}) => ({
  nsId: NS,
  wfId: WF,
  jobId,
  token,
  reason: 'boom',
  stack: 'Error: boom\n  at x',
  retryAt: Date.now() + 60_000,
  record: JSON.stringify({ state: 'failed', reason: 'boom', stack: 'Error: boom' }),
  published: JSON.stringify({ state: 'failed', reason: 'boom', stack: 'Error: boom' }),
  resultTtlMs: TTL,
  nonRecoverable: false,
  ...over,
})

function stats(ts: number[]) {
  ts.sort((a, b) => a - b)
  const p = (q: number) => ts[Math.min(ts.length - 1, Math.floor(ts.length * q))]!.toFixed(2)
  return `p50 ${p(0.5)}ms  p95 ${p(0.95)}ms`
}
async function time(n: number, fn: () => Promise<unknown>) {
  await fn()
  const ts: number[] = []
  for (let i = 0; i < n; i++) {
    const t0 = performance.now()
    await fn()
    ts.push(performance.now() - t0)
  }
  return stats(ts)
}
const explain = async (q: string, params: unknown[] = []) =>
  (await sql.unsafe(`explain (analyze, buffers) ${q}`, params as any[]))
    .map((r: any) => r['QUERY PLAN'])
    .join('\n      ')

/* ══════════════════════════════ A. enqueueJob ══════════════════════════════ */

async function sectionA() {
  h('A. enqueueJob — the one insert site, the one notify site')
  await resetSchema()
  await listen(wakeChannel(NS))
  await drain(wakeChannel(NS))

  ok((await enqueueJob(sql, enqueueArgs('j-now'))) === 'enqueued', 'immediate → enqueued')
  let r = await row('j-now')
  ok(r.state === 'waiting' && r.seq !== null && r.run_at === null, 'immediate → waiting, seq stamped, run_at null')
  ok((await drain(wakeChannel(NS))).join() === WF, 'wake notified once, payload = wfId')

  const future = Date.now() + 3_600_000
  ok((await enqueueJob(sql, enqueueArgs('j-later', { runAt: future }))) === 'enqueued', 'delayed → enqueued')
  r = await row('j-later')
  ok(r.state === 'delayed' && r.seq === null && Number(r.run_at_ms) === future,
    'delayed → delayed, seq null, run_at exact epoch-ms round-trip')
  ok((await drain(wakeChannel(NS))).length === 1, 'delayed enqueue also kicks wake (Redis parity)')

  await enqueueJob(sql, enqueueArgs('j-past', { runAt: Date.now() - 60_000 }))
  r = await row('j-past')
  ok(r.state === 'waiting' && r.seq !== null, 'runAt in the past → straight to waiting, like Lua `runAt > now`')

  const seqBefore = (await sql`select last_value from workflow.job_seq`)[0]!.last_value
  await enqueueJob(sql, enqueueArgs('j-later2', { runAt: future }))
  const seqAfter = (await sql`select last_value from workflow.job_seq`)[0]!.last_value
  ok(String(seqBefore) === String(seqAfter), 'delayed branch does not evaluate nextval (no burnt seq)')

  await drain(wakeChannel(NS))
  ok((await enqueueJob(sql, enqueueArgs('j-now', { data: 'OVERWRITE?' }))) === 'duplicate', 'duplicate → duplicate')
  ok((await row('j-now')).data === '{"n":1}', 'duplicate left the live row untouched')
  ok((await drain(wakeChannel(NS))).length === 0, 'duplicate fires no wake')

  // fireSchedule's composition: the same function, a TransactionSql, notify only on commit.
  await sql.begin('isolation level read committed', async (tx) => {
    await tx`update workflow.schedule set next_run = next_run` // stand-in for the CAS
    ok((await enqueueJob(tx, enqueueArgs('j-tx'))) === 'enqueued', 'composes inside a transaction (fireSchedule stmt 2)')
    ok((await drain(wakeChannel(NS))).length === 0, 'no wake before commit')
  })
  ok((await drain(wakeChannel(NS))).length === 1, 'wake delivered on commit')

  await sql
    .begin('isolation level read committed', async (tx) => {
      await enqueueJob(tx, enqueueArgs('j-rollback'))
      throw new Error('rollback')
    })
    .catch(() => {})
  ok((await row('j-rollback')) === undefined, 'rollback → no row')
  ok((await drain(wakeChannel(NS))).length === 0, 'rollback → no phantom wake')

  await sql.begin('isolation level read committed', async (tx) => {
    for (let i = 0; i < 5; i++) await enqueueJob(tx, enqueueArgs(`j-bulk${i}`))
  })
  ok((await drain(wakeChannel(NS))).length === 1, '5 enqueues in one tx collapse to 1 notification')

  await sql.begin('isolation level read committed', async (tx) => {
    await enqueueJob(tx, enqueueArgs('j-bulkA', { wfId: WF }))
    await enqueueJob(tx, { ...enqueueArgs('j-bulkB'), wfId: 'wf2' })
  })
  ok((await drain(wakeChannel(NS))).length === 2, 'different payloads in one tx do NOT collapse')

  info(`cost: ${await time(200, () => enqueueJob(sql, enqueueArgs(randomUUID())))}`)
}

/* ══════════════════════════════ B. fail ══════════════════════════════ */

const FAIL_COLS = [
  'state', 'attempts', 'seq', 'lock_token', 'deadline_at', 'run_at_ms',
  'steps', 'finished_on', 'failed_reason', 'stacktrace',
] as const
const shape = (r: any) =>
  JSON.stringify(Object.fromEntries(FAIL_COLS.map((c) => [c, c === 'finished_on' ? r[c] !== null : r[c]])))

async function seedClaimed(jobId: string, token: string, attempts: number, maxAttempts: number) {
  await sql`delete from workflow.job where wf_id=${WF} and id=${jobId}`
  await sql`
    insert into workflow.job (wf_id, id, ns_id, group_id, data, steps, state, priority, seq,
                              attempts, max_attempts, deadline_at, lock_token)
    values (${WF}, ${jobId}, ${NS}, 'g1', '{"n":1}', '{"step-a":"memo"}', 'active', 0,
            nextval('workflow.job_seq'), ${attempts}, ${maxAttempts},
            now() + interval '30 seconds', ${token})`
}

async function sectionB() {
  h('B. fail — split by predicate (A) vs one `case` statement (B)')
  await listen(doneChannel(NS, WF, 'f1'))

  const cases = [
    { name: 'retry budget left → requeued', attempts: 0, max: 3, nr: false, want: 'requeued' },
    { name: 'last attempt → dead-lettered', attempts: 2, max: 3, nr: false, want: 'dead-lettered' },
    { name: 'maxAttempts 1 → dead-lettered', attempts: 0, max: 1, nr: false, want: 'dead-lettered' },
    { name: 'nonRecoverable with budget → dead-lettered', attempts: 0, max: 9, nr: true, want: 'dead-lettered' },
  ] as const

  for (const c of cases) {
    const tok = randomUUID()
    const fixed = { nonRecoverable: c.nr, retryAt: Date.now() + 60_000 }
    await seedClaimed('f1', tok, c.attempts, c.max)
    const gotA = await failSplit(sql, failArgs('f1', tok, fixed))
    const shapeA = shape(await row('f1'))
    await sql`delete from workflow.result where wf_id=${WF} and job_id='f1'`

    await seedClaimed('f1', tok, c.attempts, c.max)
    const gotB = await failCase(sql, failArgs('f1', tok, fixed))
    const shapeB = shape(await row('f1'))

    ok(gotA === c.want && gotB === c.want, `${c.name} (A=${gotA} B=${gotB})`)
    ok(shapeA === shapeB, `  → identical row state`)
    if (shapeA !== shapeB) info(`A ${shapeA}\n    B ${shapeB}`)
  }

  const tok = randomUUID()
  await seedClaimed('f1', tok, 0, 3)
  const r = await row('f1')
  ok(r.steps['step-a'] === 'memo', 'seeded memo present')
  await failSplit(sql, failArgs('f1', tok))
  ok((await row('f1')).steps['step-a'] === 'memo', 'requeue preserves `steps`')
  ok((await row('f1')).seq === null, 'requeue nulls `seq` (it lands in delayed, not waiting)')

  await seedClaimed('f1', tok, 2, 3)
  await drain(doneChannel(NS, WF, 'f1'))
  await failSplit(sql, failArgs('f1', tok))
  const dead = await row('f1')
  ok(Object.keys(dead.steps).length === 0, 'dead-letter clears `steps`')
  ok(dead.seq === null && dead.finished_on !== null && dead.failed_reason === 'boom', 'dead-letter stamps the failure')
  ok((await getResult(sql, { wfId: WF, jobId: 'f1' })) !== null, 'dead-letter writes the result')
  ok((await drain(doneChannel(NS, WF, 'f1'))).length === 1, 'dead-letter notifies the done channel')

  await seedClaimed('f1', tok, 0, 3)
  await drain(wakeChannel(NS))
  await failSplit(sql, failArgs('f1', tok))
  ok((await drain(wakeChannel(NS))).length === 1, 'requeue kicks wake (Redis got this from releaseActive)')

  await seedClaimed('f1', tok, 0, 3)
  ok((await failSplit(sql, failArgs('f1', 'wrong-token'))) === 'stale-token', 'A: stale token → stale-token')
  ok((await failCase(sql, failArgs('f1', 'wrong-token'))) === 'stale-token', 'B: stale token → stale-token')
  ok((await row('f1')).attempts === 0, 'stale token changed nothing')

  // Candidate A's one structural risk: the two statements are not one row lock, so a janitor
  // recovery can land in the gap. It can only land there when statement 1 matched NOTHING —
  // a matching statement 1 holds the row to the end of the transaction.
  await seedClaimed('f-race', tok, 2, 3) // exhausted ⇒ statement 1 will not match
  const raced = await sql.begin('isolation level read committed', async (tx) => {
    const requeued = await tx`
      update workflow.job set attempts = attempts + 1, state = 'delayed', seq = null,
        lock_token = null, deadline_at = null
      where wf_id = ${WF} and id = 'f-race' and lock_token = ${tok} and attempts + 1 < max_attempts
      returning 1`
    // …the janitor recovers the claim right here, on another connection
    await sql`update workflow.job set state='waiting', lock_token=null, deadline_at=null,
              stalled_count = stalled_count + 1
              where wf_id=${WF} and id='f-race' and state='active'`
    const dead = await tx`
      update workflow.job set state = 'failed', seq = null, lock_token = null, finished_on = now()
      where wf_id = ${WF} and id = 'f-race' and lock_token = ${tok}
      returning 1`
    return requeued.length > 0 ? 'requeued' : dead.length > 0 ? 'dead-lettered' : 'stale-token'
  })
  ok(raced === 'stale-token', 'recovery landing between the split pair → stale-token, not a double write')
  ok((await row('f-race')).state === 'waiting', '  → the recovered job is left exactly as recovery left it')

  // Cost, with the seeding outside the measurement.
  const bench = async (n: number, tag: string, fn: (id: string, token: string) => Promise<unknown>) => {
    const token = randomUUID()
    const ids = Array.from({ length: n }, (_, i) => `${tag}${i}`)
    for (const id of ids) await seedClaimed(id, token, 0, 3)
    const ts: number[] = []
    for (const id of ids) {
      const t0 = performance.now()
      await fn(id, token)
      ts.push(performance.now() - t0)
    }
    return stats(ts)
  }
  info(`cost A (split, two statements in one tx): ${await bench(100, 'ba', (id, t) => failSplit(sql, failArgs(id, t)))}`)
  info(`cost B (one case statement):              ${await bench(100, 'bb', (id, t) => failCase(sql, failArgs(id, t)))}`)
  info(`  bare `+'`select 1`'+`:                       ${await time(100, () => sql`select 1`)}`)
  info(`  empty transaction (BEGIN/COMMIT only):  ${await time(100, () => sql.begin('isolation level read committed', (tx) => tx`select 1`))}`)
}

/* ══════════════════════════════ C. complete & the shared finalize ══════════════════════════════ */

async function sectionC() {
  h('C. complete — one statement, and the finalize fragment it shares with fail')
  const tok = randomUUID()
  await seedClaimed('c1', tok, 0, 3)
  await listen(doneChannel(NS, WF, 'c1'))
  await drain(doneChannel(NS, WF, 'c1'))
  await drain(wakeChannel(NS))

  const record = JSON.stringify({ state: 'completed', value: 'v' })
  ok((await complete(sql, { nsId: NS, wfId: WF, jobId: 'c1', token: tok, record, published: record, resultTtlMs: TTL })) === 'committed', 'commit → committed')
  ok((await row('c1')) === undefined, 'job row deleted (id immediately reusable)')
  ok((await getResult(sql, { wfId: WF, jobId: 'c1' })) === record, 'result stored')
  ok((await drain(doneChannel(NS, WF, 'c1'))).join() === record, 'done channel carries the record')
  ok((await drain(wakeChannel(NS))).length === 1, 'complete kicks wake (frees a ns slot)')

  await seedClaimed('c2', tok, 0, 3)
  ok((await complete(sql, { nsId: NS, wfId: WF, jobId: 'c2', token: 'wrong', record, published: record, resultTtlMs: TTL })) === 'stale-token', 'stale token → stale-token')
  ok((await row('c2')) !== undefined, 'stale complete left the job alone')
  ok((await getResult(sql, { wfId: WF, jobId: 'c2' })) === null, 'stale complete wrote no result')

  // #15's reusable-id collision, end to end: complete X, re-add X, dead-letter it.
  const tok2 = randomUUID()
  await seedClaimed('reuse', tok2, 0, 3)
  await complete(sql, { nsId: NS, wfId: WF, jobId: 'reuse', token: tok2, record: '{"state":"completed","value":"first"}', published: 'x', resultTtlMs: TTL })
  await seedClaimed('reuse', tok2, 2, 3)
  const outcome = await failSplit(sql, failArgs('reuse', tok2))
  ok(outcome === 'dead-lettered', 'id reuse: second life dead-letters without a PK violation')
  ok((await getResult(sql, { wfId: WF, jobId: 'reuse' }))?.includes('failed') === true, 'result upsert overwrote the stale record')

  // Expiry is the read's predicate, not the sweep's job.
  await sql`update workflow.result set expires_at = now() - interval '1 second' where wf_id=${WF} and job_id='reuse'`
  ok((await getResult(sql, { wfId: WF, jobId: 'reuse' })) === null, 'an unswept expired result still reads as gone')

  // #9's 7999-byte cap is an ERROR, not truncation — and the notify is a CTE of `complete`'s own
  // statement, so an oversized payload does not "lose a notification", it fails the commit.
  const tok3 = randomUUID()
  await seedClaimed('big', tok3, 0, 3)
  const huge = JSON.stringify({ state: 'completed', value: 'x'.repeat(9000) })
  const err = await complete(sql, { nsId: NS, wfId: WF, jobId: 'big', token: tok3, record: huge, published: huge, resultTtlMs: TTL })
    .then(() => null, (e: Error) => e.message)
  ok(err !== null, `oversized publish payload throws: ${err?.slice(0, 60)}`)
  ok((await row('big')) !== undefined, '  → and the job is NOT completed, so the cap check must run before the statement')
  ok((await complete(sql, { nsId: NS, wfId: WF, jobId: 'big', token: tok3, record: huge, published: '1', resultTtlMs: TTL })) === 'committed',
    'a 9KB record stores fine when only the PAYLOAD falls back to a marker')
  ok((await getResult(sql, { wfId: WF, jobId: 'big' }))?.length === huge.length, '  → the stored record is the full 9KB')

  info(`cost complete: ${await time(100, async () => {
    await seedClaimed('cperf', tok, 0, 3)
    return complete(sql, { nsId: NS, wfId: WF, jobId: 'cperf', token: tok, record, published: record, resultTtlMs: TTL })
  })}`)
}

/* ══════════════════════════════ D. getMetrics ══════════════════════════════ */

async function seedMetrics(waiting: number, delayed: number, active: number, failed: number) {
  await sql`truncate workflow.job`
  await sql`insert into workflow.job (wf_id, id, ns_id, group_id, data, state, priority, seq, max_attempts)
            select ${WF}, 'w'||i, ${NS}, 'g'||i, '{}', 'waiting', 0, nextval('workflow.job_seq'), 3
            from generate_series(1, ${waiting}) i`
  await sql`insert into workflow.job (wf_id, id, ns_id, group_id, data, state, priority, run_at, max_attempts)
            select ${WF}, 'd'||i, ${NS}, 'g'||i, '{}', 'delayed', 0, now() + interval '1 hour', 3
            from generate_series(1, ${delayed}) i`
  await sql`insert into workflow.job (wf_id, id, ns_id, group_id, data, state, priority, seq, max_attempts, lock_token, deadline_at)
            select ${WF}, 'a'||i, ${NS}, 'g'||i, '{}', 'active', 0, nextval('workflow.job_seq'), 3, 't'||i, now()+interval '30 seconds'
            from generate_series(1, ${active}) i`
  await sql`insert into workflow.job (wf_id, id, ns_id, group_id, data, state, priority, max_attempts, finished_on, failed_reason)
            select ${WF}, 'f'||i, ${NS}, 'g'||i, '{}', 'failed', 0, 3, now(), 'x'
            from generate_series(1, ${failed}) i`
  // a second workflow in the same namespace, so the per-wf filters have something to exclude
  await sql`insert into workflow.job (wf_id, id, ns_id, group_id, data, state, priority, seq, max_attempts)
            select 'wf2', 'x'||i, ${NS}, 'g'||i, '{}', 'waiting', 0, nextval('workflow.job_seq'), 3
            from generate_series(1, ${waiting}) i`
  // ...and a second NAMESPACE holding a large active set, which is the only thing that can
  // price `job_active_idx`'s leading column.
  await sql`insert into workflow.job (wf_id, id, ns_id, group_id, data, state, priority, seq, max_attempts, lock_token, deadline_at)
            select 'wf3', 'n'||i, 'ns2', 'g'||i, '{}', 'active', 0, nextval('workflow.job_seq'), 3, 't'||i, now()+interval '30 seconds'
            from generate_series(1, 20000) i`
  await sql`analyze workflow.job`
}

async function sectionD() {
  h('D. getMetrics — one statement or three, and what `ns_id` buys')
  await seedMetrics(50_000, 20_000, 400, 30_000)
  const want = { active: 400, waiting: 50_000, delayed: 20_000 }
  const j = JSON.stringify(want)
  ok(JSON.stringify(await metricsSubqueries(sql, { nsId: NS, wfId: WF })) === j, 'A  subqueries + ns_id correct')
  ok(JSON.stringify(await metricsSubqueriesNoNs(sql, { wfId: WF })) === j, "A′ subqueries without ns_id correct")
  ok(JSON.stringify(await metricsFilter(sql, { wfId: WF })) === j, 'B  filtered aggregate correct')
  ok(JSON.stringify(await metricsThreeTrips(sql, { nsId: NS, wfId: WF })) === j, 'C  three round-trips correct')

  info('cold — straight after the bulk load, before any vacuum (empty visibility map):')
  info(`  A  3 subqueries (+ns_id): ${await time(10, () => metricsSubqueries(sql, { nsId: NS, wfId: WF }))}`)
  info(`  A' 3 subqueries (no ns):  ${await time(10, () => metricsSubqueriesNoNs(sql, { wfId: WF }))}`)
  await sql`vacuum (analyze) workflow.job`
  info('warm — after a vacuum, i.e. the steady state #8 tuned autovacuum for:')
  info(`A  one statement, 3 subqueries (+ns_id): ${await time(30, () => metricsSubqueries(sql, { nsId: NS, wfId: WF }))}`)
  info(`A′ one statement, 3 subqueries (no ns):  ${await time(30, () => metricsSubqueriesNoNs(sql, { wfId: WF }))}`)
  info(`B  one statement, count(*) filter:       ${await time(10, () => metricsFilter(sql, { wfId: WF }))}`)
  info(`C  three round-trips:                    ${await time(30, () => metricsThreeTrips(sql, { nsId: NS, wfId: WF }))}`)

  console.log('\n    A plan:\n      ' + (await explain(`
    select (select count(*)::int from workflow.job where ns_id=$1 and wf_id=$2 and state='active') as active,
           (select count(*)::int from workflow.job where wf_id=$2 and state='waiting') as waiting,
           (select count(*)::int from workflow.job where wf_id=$2 and state='delayed') as delayed`, [NS, WF])))
  console.log('\n    A′ active-count plan (no ns_id):\n      ' + (await explain(
    `select count(*)::int from workflow.job where wf_id=$1 and state='active'`, [WF])))
  console.log('\n    B plan:\n      ' + (await explain(`
    select count(*) filter (where state='active')::int, count(*) filter (where state='waiting')::int,
           count(*) filter (where state='delayed')::int from workflow.job where wf_id=$1`, [WF])))
}

/* ══════════════════════════════ E. setStepData ══════════════════════════════ */

async function sectionE() {
  h('E. setStepData — `||` vs `jsonb_set`')
  await sql`truncate workflow.job`
  const mk = async (id: string) => {
    await sql`insert into workflow.job (wf_id, id, ns_id, group_id, data, state, priority, seq, max_attempts)
              values (${WF}, ${id}, ${NS}, 'g1', '{}', 'active', 0, nextval('workflow.job_seq'), 3)`
  }
  const hostile = ['plain', 'a.b', 'x[0]', '{"k":1}', '', 'naïve 🔑', '"quoted"', 'a'.repeat(500), '$.a', '0']

  for (const [name, fn] of [['||', stepConcat], ['jsonb_set', stepJsonbSet]] as const) {
    await sql`delete from workflow.job where wf_id=${WF} and id='s1'`
    await mk('s1')
    let allBack = true
    for (const k of hostile) await fn(sql, { wfId: WF, jobId: 's1', stepName: k, value: `v:${k}` })
    const steps = (await row('s1')).steps
    for (const k of hostile) if (steps[k] !== `v:${k}`) allBack = false
    ok(allBack && Object.keys(steps).length === hostile.length,
      `${name}: all ${hostile.length} hostile step names round-trip verbatim`)
    await fn(sql, { wfId: WF, jobId: 's1', stepName: 'plain', value: 'overwritten' })
    ok((await row('s1')).steps.plain === 'overwritten', `${name}: re-writing a step overwrites`)
  }

  // Concurrent writers of DIFFERENT steps on the same row — the silent-lost-step class.
  await sql`delete from workflow.job where wf_id=${WF} and id='s2'`
  await mk('s2')
  await Promise.all(
    Array.from({ length: 32 }, (_, i) =>
      stepConcat(sql, { wfId: WF, jobId: 's2', stepName: `k${i}`, value: `v${i}` })),
  )
  ok(Object.keys((await row('s2')).steps).length === 32, '32 concurrent `||` writers lose nothing (READ COMMITTED re-reads)')

  // A step written for a job that no longer exists: Redis' HSET would resurrect a phantom hash.
  const before = (await sql`select count(*)::int as n from workflow.job`)[0]!.n
  await stepConcat(sql, { wfId: WF, jobId: 'gone', stepName: 'k', value: 'v' })
  ok((await sql`select count(*)::int as n from workflow.job`)[0]!.n === before, 'writing a step for a dead job is a no-op, not a phantom row')

  // O(n²): 200 steps × 2KB on one row.
  await sql`delete from workflow.job where wf_id=${WF} and id='s3'`
  await mk('s3')
  const big = 'x'.repeat(2048)
  const t0 = performance.now()
  for (let i = 0; i < 200; i++) await stepConcat(sql, { wfId: WF, jobId: 's3', stepName: `k${i}`, value: big })
  const quad = performance.now() - t0
  const sz = (await sql`select pg_size_pretty(pg_table_size('workflow.job')) as s`)[0]!.s
  info(`200 steps × 2KB on one row: ${quad.toFixed(0)}ms total, table now ${sz} (the accepted O(n²), #8)`)

  await sql`delete from workflow.job where wf_id=${WF} and id='s4'`
  await mk('s4')
  info(`cost || :        ${await time(200, () => stepConcat(sql, { wfId: WF, jobId: 's4', stepName: 'k', value: 'v' }))}`)
  info(`cost jsonb_set: ${await time(200, () => stepJsonbSet(sql, { wfId: WF, jobId: 's4', stepName: 'k', value: 'v' }))}`)
}

/* ══════════════════════════════ F. heartbeat ══════════════════════════════ */

async function sectionF() {
  h('F. heartbeat — is `deadline_at` unindexed actually buying a HOT update?')
  await sql`truncate workflow.job`
  const tok = randomUUID()
  await seedClaimed('hb', tok, 0, 3)
  ok((await heartbeat(sql, { wfId: WF, jobId: 'hb', token: tok, lockMs: 30_000 })) === 'renewed', 'valid token → renewed')
  ok((await heartbeat(sql, { wfId: WF, jobId: 'hb', token: 'nope', lockMs: 30_000 })) === 'stale-token', 'stale token → stale-token')

  await sql`select pg_stat_force_next_flush()`
  const before = (await sql<any[]>`select n_tup_upd, n_tup_hot_upd from pg_stat_user_tables where relname='job'`)[0]
  const t = await time(1000, () => heartbeat(sql, { wfId: WF, jobId: 'hb', token: tok, lockMs: 30_000 }))
  await sql`select pg_stat_force_next_flush()`
  const after = (await sql<any[]>`select n_tup_upd, n_tup_hot_upd from pg_stat_user_tables where relname='job'`)[0]
  const upd = Number(after.n_tup_upd) - Number(before.n_tup_upd)
  const hot = Number(after.n_tup_hot_upd) - Number(before.n_tup_hot_upd)
  ok(hot / upd > 0.95, `HOT ratio ${(100 * hot / upd).toFixed(1)}% over ${upd} heartbeat updates`)
  info(`cost: ${t}`)
}

/* ══════════════════════════════ G. getSchedules ══════════════════════════════ */

async function sectionG() {
  h('G. getSchedules — ordering contract and the index that serves it')
  await sql`truncate workflow.schedule`
  const base = Date.now() + 60_000
  // ties on next_run, inserted out of order, so only the (next_run, schedule_id) contract sorts them
  for (const [id, at] of [['s-c', base], ['s-a', base], ['s-b', base], ['s-z', base - 1000], ['s-y', base + 1000]] as const)
    await upsertSchedule(sql, {
      wfId: WF, scheduleId: id, pattern: '* * * * *', tz: 'Europe/Berlin', data: '{}',
      priority: 0, groupId: id, skipIfRunning: true, nextRun: at,
    })
  const got = await getSchedules(sql, { wfId: WF })
  ok(got.map((s) => s.scheduleId).join() === 's-z,s-a,s-b,s-c,s-y', 'ordered by (next_run, schedule_id), ties lexical')
  ok(got[1]!.nextRun === base, 'nextRun round-trips as exact epoch-ms')
  ok(got[0]!.lastFireAt === null && got[0]!.lastJobId === null, 'never-fired schedule reads null bookkeeping')

  await sql`update workflow.schedule set last_job_id='j9', last_fire_at=now() where schedule_id='s-a'`
  await upsertSchedule(sql, {
    wfId: WF, scheduleId: 's-a', pattern: '*/5 * * * *', tz: 'UTC', data: '{"v":2}',
    priority: 1, groupId: 'g', skipIfRunning: false, nextRun: base + 5000,
  })
  const again = (await getSchedules(sql, { wfId: WF })).find((s) => s.scheduleId === 's-a')!
  ok(again.pattern === '*/5 * * * *' && again.lastJobId === 'j9', 'upsert replaces config, preserves last-fire bookkeeping')

  await removeSchedule(sql, { wfId: WF, scheduleId: 's-a' })
  await removeSchedule(sql, { wfId: WF, scheduleId: 's-a' })
  ok((await getSchedules(sql, { wfId: WF })).length === 4, 'removeSchedule is idempotent')

  for (const n of [20, 500, 20_000]) {
    await sql`truncate workflow.schedule`
    await sql`insert into workflow.schedule (wf_id, schedule_id, pattern, tz, data, priority, group_id, skip_if_running, next_run)
              select ${WF}, 'bulk'||i, '* * * * *', 'UTC', '{}', 0, 'g', true, now() + (i * interval '1 second')
              from generate_series(1, ${n}) i`
    await sql`analyze workflow.schedule`
    info(`${String(n).padStart(6)} schedules: ${await time(20, () => getSchedules(sql, { wfId: WF }))}`)
  }
  console.log('\n    plan at 20k:\n      ' + (await explain(
    `select schedule_id, pattern, tz, next_run, last_fire_at, last_job_id from workflow.schedule
     where wf_id=$1 order by next_run, schedule_id`, [WF])))
  // Would carrying schedule_id in the index remove the sort?
  await sql`create index schedule_due_idx2 on workflow.schedule (wf_id, next_run, schedule_id)`
  await sql`analyze workflow.schedule`
  console.log('\n    plan at 20k with (wf_id, next_run, schedule_id):\n      ' + (await explain(
    `select schedule_id, pattern, tz, next_run, last_fire_at, last_job_id from workflow.schedule
     where wf_id=$1 order by next_run, schedule_id`, [WF])))
  // …and does it still serve reserve's due read, which is what the index actually exists for?
  console.log('\n    reserve due-schedule read:\n      ' + (await explain(
    `select schedule_id, next_run, pattern, tz from workflow.schedule
     where wf_id=$1 and next_run <= now() order by next_run limit 500`, [WF])))
  await sql`drop index workflow.schedule_due_idx2`
}

/* ══════════════════════════════ main ══════════════════════════════ */

try {
  await sectionA()
  await sectionB()
  await sectionC()
  await sectionD()
  await sectionE()
  await sectionF()
  await sectionG()
  console.log(failures === 0 ? '\n\x1b[32mall assertions passed\x1b[0m' : `\n\x1b[31m${failures} FAILED\x1b[0m`)
} finally {
  await listener.end()
  await sql.end()
}
