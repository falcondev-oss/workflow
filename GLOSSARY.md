# Workflow

Durable, type-safe queue workers. Workflows run jobs whose steps are persisted, so a retry replays the job and skips finished steps.

## Workflows

**Definition**:
A workflow without its handler: its id, schema and options. Enough to enqueue, schedule and inspect jobs, not to run them.
_Avoid_: Job type, contract

**Handler**:
The `run` function a worker calls for each job of a workflow.
_Avoid_: Processor, body

## Rate limits

**Rate limiter**:
A named budget of at most `limit` starts in any span of `window` milliseconds, shared by every workflow in one namespace that uses it.
_Avoid_: Throttle, token bucket, per-workflow limit

**Start**:
One claim of a job by a worker. Retries and re-claims after stalled recovery are starts too.
_Avoid_: Token, run, attempt

**Pause**:
A span during which a rate limiter admits no starts, set by a handler when an external service reports its limit is reached. In a workflow that uses no rate limiter, the pause stops that workflow alone. The job that sets a pause uses no attempt.
_Avoid_: Backoff, throttle, snooze
