import { parse, stringify } from 'superjson'

/** superjson → opaque string. The queue backbone is string-only; all encoding lives here. */
export function serialize<T>(data: T): string {
  return stringify(data)
}

export function deserialize<T>(data: string): T {
  return parse<T>(data)
}
