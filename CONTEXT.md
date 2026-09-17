# Workflow

Durable, type-safe queue workers. Workflows run jobs whose steps are persisted, so a retry replays the job and skips finished steps.

## Rate limits

**Rate limiter**:
A named budget of at most `limit` starts in any span of `window` milliseconds, shared by every workflow that uses it.
_Avoid_: Throttle, token bucket, per-workflow limit

**Start**:
One claim of a job by a worker. Retries and re-claims after stalled recovery are starts too.
_Avoid_: Token, run, attempt
