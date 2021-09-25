import BaseChain, { Config } from './base'
import { decode } from '../utils/convert'
import * as expr from '../expression'
import type { ScFields, Schema, Field, Projected } from '../types'

type GetCon<T extends Schema<any>> = Config<T> & {
  key: any
}

export class Get<
  T extends Schema<any>,
  S extends string | number | symbol = Field<T>
> extends BaseChain<Projected<ScFields<T>, S>, GetCon<T>, { strong: true }> {
  constructor(config: GetCon<T>) {
    super(config, { strong: true })
  }

  async execute() {
    const params = this.expr
    this.log('get', params)
    const { Item } = await this.config.client.get(params as any).promise()
    this.resolve(decode(Item) as any)
  }

  public get expr(): AWS.DynamoDB.GetItemInput {
    const params = this.createInput({ Key: this.config.key })
    Object.assign(params, expr.project(...(this.config.selection ?? [])))
    return params
  }

  public select<Fields extends string>(...fields: Fields[]): Get<T, Fields> {
    return this.clone({ selection: fields }) as any
  }
}
