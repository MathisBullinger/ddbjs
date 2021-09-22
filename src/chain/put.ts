import type { Config } from './base'
import ConditionChain from './condition'
import { decode } from '../utils/convert'
import { assert, ReturnValueError } from '../utils/error'
import { oneOf } from '../utils/array'
import * as expr from '../expression'
import type { Schema, ScItem } from '../types'

type ReturnType = 'NONE' | 'NEW' | 'OLD'

type PutConfig<T extends Schema<any>, R extends ReturnType> = Config<T> & {
  return: R
  item: ScItem<T>
}

export class Put<
  T extends Schema<any>,
  R extends ReturnType
> extends ConditionChain<
  R extends 'NONE' ? undefined : ScItem<T>,
  PutConfig<T, R>,
  { cast: true }
> {
  constructor(config: PutConfig<T, R>) {
    super(config, { cast: true })
  }

  async execute() {
    const params: AWS.DynamoDB.DocumentClient.PutItemInput = {
      TableName: this.config.table,
      Item: this.makeSets(this.config.item),
      ...(this.config.return === 'OLD' && { ReturnValues: 'ALL_OLD' }),
    }

    Object.assign(params, expr.merge(params as any, this.buildCondition()))
    super.log('put', params)

    const { Attributes } = await this.config.client.put(params).promise()

    const result = decode(
      this.config.return === 'NEW'
        ? params.Item
        : this.config.return === 'OLD'
        ? Attributes
        : undefined
    )
    this.resolve(result as any)
  }

  returning<R extends ReturnType>(v: R): Put<T, R> {
    assert(oneOf(v, 'NEW', 'OLD', 'NONE'), new ReturnValueError(v, 'insert'))
    return this.clone({ return: v as any }) as any
  }

  public ifNotExists() {
    let chain = this
    for (const k of this.keyFields)
      chain = chain.if(k, '<>', (this.config.item as any)[k]) as any
    return chain
  }
}
