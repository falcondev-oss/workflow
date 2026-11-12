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

export class NonRecoverableError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options)
    this.name = 'NonRecoverableError'
  }
}

export class RateLimitError extends Error {
  constructor(
    readonly ms: number,
    readonly limiter = '',
  ) {
    super(`Rate limited for ${ms} ms`)
    this.name = 'RateLimitError'
  }
}

export class TimeoutError extends Error {
  constructor(jobId: string) {
    super(`Timed out waiting for job: ${jobId}`)
    this.name = 'TimeoutError'
  }
}
