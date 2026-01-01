import type { Attributes } from '@opentelemetry/api'
import { SpanStatusCode, trace } from '@opentelemetry/api'

export function getTracer() {
  return trace.getTracer('falcondev-oss-workflow')
}

export async function runWithTracing<T>(spanName: string, attributes: Attributes, fn: () => T) {
  return getTracer().startActiveSpan(spanName, async (span) => {
    try {
      span.setAttributes(attributes)
      const result = await fn()
      span.setStatus({
        code: SpanStatusCode.OK,
      })
      return result
    } catch (err_) {
      const err = err_ as Error
      span.recordException(err)
      span.setStatus({
        code: SpanStatusCode.ERROR,
        message: err.message,
      })
      throw err_
    } finally {
      span.end()
    }
  })
}
