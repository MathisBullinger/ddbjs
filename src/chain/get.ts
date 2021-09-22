import BaseChain, { Config } from './base'
import { decode } from '../utils/convert'
import * as expr from '../expression'
import type { ScFields, Schema, Field, Projected } from '../types'

type GetCon<T extends Schema<any>> = Config<T> & {
  key: any
  consistent?: boolean
  selection?: any[]
}

export class Get<
  T extends Schema<any>,
  S extends string | number | symbol = Field<T>
> extends BaseChain<Projected<ScFields<T>, S>, GetCon<T>> {
  constructor(config: GetCon<T>) {
    super(config, {})
  }

  async execute() {
    const params: Partial<AWS.DynamoDB.GetItemInput> = {
      TableName: this.config.table,
      Key: this.config.key,
      ConsistentRead: this.config.consistent ?? false,
    }
    super.log('get', params)
    Object.assign(params, expr.project(...(this.config.selection ?? [])))
    const { Item } = await this.config.client.get(params as any).promise()
    this.resolve(decode(Item) as any)
  }

  public select<Fields extends string>(...fields: Fields[]): Get<T, Fields> {
    return this.clone({ selection: fields }) as any
  }

  public strong() {
    return this.clone({ strong: true })
  }
}
