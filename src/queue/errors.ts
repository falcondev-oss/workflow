export class JobAlreadyExistsError extends Error {
  constructor(jobId: string) {
    super(`Job already exists: ${jobId}`)
    this.name = 'JobAlreadyExistsError'
  }
}

export class ResultExpiredError extends Error {
  constructor(jobId: string) {
    super(`Result expired or never existed for job: ${jobId}`)
    this.name = 'ResultExpiredError'
  }
}

/**
 * Thrown by a handler for a failure that retrying cannot fix — the job is dead-lettered
 * immediately, skipping the remaining `maxAttempts` budget. Only the thrown error itself is
 * checked; wrapping one in another error's `cause` does not skip retries.
 */
export class NonRecoverableError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options)
    this.name = 'NonRecoverableError'
  }
}

export class TimeoutError extends Error {
  constructor(jobId: string) {
    super(`Timed out waiting for job: ${jobId}`)
    this.name = 'TimeoutError'
  }
}
