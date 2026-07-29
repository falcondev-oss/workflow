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

export class TimeoutError extends Error {
  constructor(jobId: string) {
    super(`Timed out waiting for job: ${jobId}`)
    this.name = 'TimeoutError'
  }
}
