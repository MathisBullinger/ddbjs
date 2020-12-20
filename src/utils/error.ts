export function assert(cond: boolean, error: Error) {
  if (cond) return
  throw error
}

export class ReturnValueError extends Error {
  public readonly name = 'ReturnValueError'

  constructor(value: string, op: string) {
    super(`invalid return value '${value}' for ${op} operation`)
  }
}
