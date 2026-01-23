import type { Context, Span, SpanOptions } from '@opentelemetry/api'
import { SpanStatusCode, trace } from '@opentelemetry/api'

export function getTracer() {
  return trace.getTracer('falcondev-oss-workflow')
}

export async function runWithTracing<T>(
  spanName: string,
  options: SpanOptions,
  fn: (span: Span) => T,
  context?: Context,
) {
  const span = getTracer().startSpan(spanName, options, context)
  try {
    const result = await fn(span)
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
}
