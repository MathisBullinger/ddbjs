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
    const params: AWS.DynamoDB.ScanInput = {
      TableName: this.config.table,
      Limit: this.config.limit,
    }
    Object.assign(params, expr.project(...(this.config.selection ?? [])))
    const { items } = await this.batchExec('scan', params)
    this.resolve(items)
  }

  public select<Fields extends string>(...fields: Fields[]): Scan<T, Fields> {
    return this.clone({ selection: fields }) as any
  }
}
