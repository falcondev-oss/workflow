import type { StandardSchemaV1 } from '@standard-schema/spec'
import { UnrecoverableError } from 'bullmq'

export class WorkflowInputError extends UnrecoverableError {
  issues

  constructor(message: string, issues: readonly StandardSchemaV1.Issue[]) {
    super(message)
    this.issues = issues
  }
}
