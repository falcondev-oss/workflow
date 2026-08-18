# PROTOTYPE — the remaining ten Postgres ops (#20)

Throwaway. Answers [#20](https://github.com/falcondev-oss/workflow/issues/20) against a real Postgres 18. Not shipped code — the validated decisions go into the spec, this branch is the primary source.

```sh
docker run -d --name wf-proto-pg -e POSTGRES_PASSWORD=proto -e POSTGRES_DB=proto \
  -p 15499:5432 postgres:18-alpine
pnpm proto:pg-ops
docker rm -f wf-proto-pg
```

- `schema.sql` — the DDL locked in #8, with #15's `result_expires_idx` correction and without the `job_group_head_idx` #12 rejected.
- `queries.ts` — `enqueueJob` and the ten ops. Where the ticket poses a real question both candidates sit side by side: `failSplit` vs `failCase`, `stepConcat` vs `stepJsonbSet`, `metricsSubqueries` / `metricsFilter` / `metricsThreeTrips`.
- `run.ts` — correctness assertions first (including the races and the hostile inputs), then cost, then `EXPLAIN (ANALYZE, BUFFERS)`.

Measured on one dev machine against a containerised PG 18 with `synchronous_commit = on`; treat absolute ms as machine-specific and the ratios as the finding.
