import type { SuperJSONResult } from 'superjson'
import type { Tagged } from 'type-fest'
import { deserialize as _deserialize, serialize as _serialize } from 'superjson'

export type Serialized<T> = Tagged<SuperJSONResult, 'data', T>

export function serialize<T>(data: T): Serialized<T> {
  return _serialize(data) as Serialized<T>
}

export function deserialize<T>(data: Serialized<T>): T {
  return _deserialize(data)
}
