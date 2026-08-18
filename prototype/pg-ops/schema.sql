-- PROTOTYPE — wipe me. The DDL locked in #8, verbatim, with #15's `result_expires_idx`
-- correction applied and #12's rejected `job_group_head_idx` left out.

drop schema if exists workflow cascade;

create schema if not exists workflow;

create sequence if not exists workflow.job_seq as bigint;

create table if not exists workflow.job (
  wf_id         text        not null,
  id            text        not null,
  ns_id         text        not null,
  group_id      text        not null,
  data          text        not null,
  steps         jsonb       not null default '{}',
  state         text        not null check (state in ('waiting','delayed','active','failed')),
  priority      int         not null check (priority between 0 and 2097151),
  seq           bigint,
  attempts      int         not null default 0,
  max_attempts  int         not null,
  stalled_count int         not null default 0,
  created_at    timestamptz not null default now(),
  run_at        timestamptz,
  deadline_at   timestamptz,
  lock_token    text,
  finished_on   timestamptz,
  failed_reason text,
  stacktrace    text,
  primary key (wf_id, id),
  constraint job_seq_present check ((state in ('waiting','active')) = (seq is not null))
) with (fillfactor = 85, autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 100);

create index if not exists job_waiting_idx on workflow.job (wf_id, priority desc, seq)
  include (group_id) where state = 'waiting';

create index if not exists job_active_idx on workflow.job (ns_id, wf_id, group_id)
  where state = 'active';

create index if not exists job_delayed_idx on workflow.job (wf_id, run_at) where state = 'delayed';
create index if not exists job_failed_idx  on workflow.job (wf_id, finished_on) where state = 'failed';

create table if not exists workflow.result (
  wf_id      text        not null,
  job_id     text        not null,
  record     text        not null,
  expires_at timestamptz not null,
  primary key (wf_id, job_id)
);
-- #15: global sweep, so no leading wf_id.
create index if not exists result_expires_idx on workflow.result (expires_at);

create table if not exists workflow.schedule (
  wf_id           text        not null,
  schedule_id     text        not null,
  pattern         text        not null,
  tz              text        not null,
  data            text        not null,
  priority        int         not null check (priority between 0 and 2097151),
  group_id        text        not null,
  skip_if_running boolean     not null,
  -- #14: only ever written from an epoch-ms parameter, never from now(), so the sub-ms
  -- component is always zero and the CAS round-trips exactly.
  next_run        timestamptz not null,
  last_fire_at    timestamptz,
  last_job_id     text,
  primary key (wf_id, schedule_id)
);
create index if not exists schedule_due_idx on workflow.schedule (wf_id, next_run);
