import BaseChain, { Config } from './base'
import * as expr from '../expression'
import type { Schema, Field, Projected, ScFields } from '../types'

type ScanConfig<T extends Schema<any>> = Config<T> & {
  selection?: any[]
}

export class Scan<
  T extends Schema<any>,
  S extends string | number | symbol = Field<T>
> extends BaseChain<
  Projected<ScFields<T>, S>[],
  ScanConfig<T>,
  { limit: true }
> {
  constructor(config: ScanConfig<T>) {
    super(config, { limit: true })
  }

  async execute() {
    const params: AWS.DynamoDB.ScanInput = {
      TableName: this.config.table,
      Limit: this.config.limit,
    }
    Object.assign(params, expr.project(...(this.config.selection ?? [])))
    this.resolve(await this.batchExec('scan', params))
  }

  public select<Fields extends string>(...fields: Fields[]): Scan<T, Fields> {
    return this.clone({ selection: fields }) as any
  }
}
