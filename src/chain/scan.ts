import BaseChain, { Config } from './base'
import * as expr from '../expression'
import type { Schema, Field, Projected, ScFields } from '../types'

export class Scan<
  T extends Schema<any>,
  S extends string | number | symbol = Field<T>
> extends BaseChain<Projected<ScFields<T>, S>[], Config<T>, { limit: true }> {
  constructor(config: Config<T>) {
    super(config, { limit: true })
  }

  async execute() {
    const { items } = await this.batchExec('scan')
    this.resolve(items)
  }

  [Symbol.asyncIterator] = this.batchIter<ScFields<T>>('scan')

  public get expr(): AWS.DynamoDB.ScanInput {
    const params = {
      TableName: this.config.table,
      Limit: this.config.limit,
    }
    Object.assign(params, expr.project(...(this.config.selection ?? [])))
    return params
  }

  public select<Fields extends string>(...fields: Fields[]): Scan<T, Fields> {
    return this.clone({ selection: fields }) as any
  }
}
