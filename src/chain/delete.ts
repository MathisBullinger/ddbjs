import type { Config } from './base'
import ConditionChain from './condition'
import { decode } from '../utils/convert'
import { assert, ReturnValueError } from '../utils/error'
import { oneOf } from '../utils/array'
import * as expr from '../expression'
import type { Schema, ScItem } from '../types'

type ReturnType = 'NONE' | 'OLD'

type DelConfig<T extends Schema<any>, R extends ReturnType> = Config<T> & {
  return: R
  key: any
}

export class Delete<
  T extends Schema<any>,
  R extends ReturnType
> extends ConditionChain<
  R extends 'NONE' ? undefined : ScItem<T>,
  DelConfig<T, R> & { verb: 'if' },
  {}
> {
  constructor(config: DelConfig<T, R>) {
    super({ ...config, verb: 'if' }, {})
  }

  async execute() {
    const params: AWS.DynamoDB.DeleteItemInput = {
      TableName: this.config.table,
      Key: this.config.key,
      ...(this.config.return === 'OLD' && {
        ReturnValues: 'ALL_OLD',
      }),
    }
    Object.assign(params, expr.merge(params as any, this.buildCondition()))
    super.log('delete', params)

    const { Attributes } = await this.config.client.delete(params).promise()

    const result = decode(this.config.return === 'OLD' ? Attributes : undefined)
    this.resolve(result as any)
  }

  returning<R extends ReturnType>(v: R): Delete<T, R> {
    assert(oneOf(v, 'NONE', 'OLD'), new ReturnValueError(v, 'delete'))
    return this.clone({ return: v as any }) as any
  }
}
