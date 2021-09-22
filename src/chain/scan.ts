import BaseChain, { Config } from './base'
import { decode } from '../utils/convert'
import * as expr from '../expression'
import type { Schema, Field, Projected, ScFields } from '../types'

type ScanConfig<T extends Schema<any>> = Config<T> & {
  limit?: number
  selection?: any[]
}

export class Scan<
  T extends Schema<any>,
  S extends string | number | symbol = Field<T>
> extends BaseChain<Projected<ScFields<T>, S>[], ScanConfig<T>> {
  constructor(config: ScanConfig<T>) {
    super(config, {})
  }

  async execute() {
    const params: AWS.DynamoDB.ScanInput = {
      TableName: this.config.table,
      Limit: this.config.limit,
    }
    Object.assign(params, expr.project(...(this.config.selection ?? [])))

    const items: any[] = []

    do {
      super.log('scan', params)
      const { Items, LastEvaluatedKey } = await this.config.client
        .scan(params)
        .promise()
      items.push(...(Items ?? []))
      params.ExclusiveStartKey = LastEvaluatedKey
      if (params.Limit) params.Limit -= Items?.length ?? 0
    } while (params.ExclusiveStartKey && (params.Limit ?? Infinity > 0))

    this.resolve(items.map(decode) as any)
  }

  public limit(limit: number) {
    return this.clone({ limit })
  }

  public select<Fields extends string>(...fields: Fields[]): Scan<T, Fields> {
    return this.clone({ selection: fields }) as any
  }
}
