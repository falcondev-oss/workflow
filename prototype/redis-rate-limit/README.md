# PROTOTYPE: Redis reserve with a rate limiter

Throwaway. Answers [Redis reserve with a rate limiter](https://github.com/falcondev-oss/workflow/issues/33). Never merge this branch.

The real changes are in `src/queue/scripts.ts` (`RESERVE`, new `RATE_LIMIT`) and `src/queue/worker.ts`. The workflow layer only passes options through; the typed API is specified elsewhere.

```sh
pnpm tsx prototype/redis-rate-limit/harness.ts         # scenarios with printed state and checks
pnpm tsx prototype/redis-rate-limit/brpop-precision.ts # BRPOP timeout lateness at hz 10 and 100
pnpm bench                                             # adds three "limiter open" drain scenarios
```

## Keys

| Key                                   | Type   | Holds                                                              |
| ------------------------------------- | ------ | ------------------------------------------------------------------ |
| `<prefix>:ns:<nsId>:rl:<name>`        | ZSET   | one member per start (the claim token), scored by `start + window` |
| `<prefix>:ns:<nsId>:rl:<name>:paused` | string | `pausedUntil` in ms, set with `PXAT pausedUntil`                   |
| `<prefix>:<wfId>:paused`              | string | the workflow's own pause (workflow without limiters)               |

The log key's TTL is set to its highest score after each batch, so it disappears once all its starts have expired.

## Harness results

- One `reserve` claims up to the tightest limiter's remaining budget, then returns `maxed = 1` and `msToLimit`.
- 2 workflows × 3 processes on one limiter of 20 per 500 ms: 300 starts in 7035 ms (ideal 7000), never more than 20 in any window.
- `limit: 1, window: 100`: gaps of 100 to 105 ms.
- A pause of 400 ms blocks the other workflow on the limiter; the paused job reruns first in its group with attempt 0 and its steps.
- Pods on different windows: the short-window pod does not prune the long-window pod's starts. The reverse order allows 6 starts in 250 ms, the looser budget the spec accepts during a rolling deploy.
- Boundary: a batch at `T` and another at `T + window` both show up in a 500 ms span of local handler times. That is correct: the window is half-open.

## Bench (best of 3, ops/s)

| Scenario                    | base `b9c427d` (2 runs) | prototype (2 runs) |
| --------------------------- | ----------------------- | ------------------ |
| drain c=50, no limiter      | 15202 / 14394           | 15198 / 16155      |
| drain c=50, 1 open limiter  | n/a                     | 15315 / 15472      |
| drain c=50, 3 open limiters | n/a                     | 14311 / 14002      |
| drain c=1, no limiter       | 3348 / 3129             | 3031 / 3284        |
| drain c=1, 1 open limiter   | n/a                     | 3214 / 3310        |
