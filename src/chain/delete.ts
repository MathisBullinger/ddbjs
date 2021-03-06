import BaseChain from './base'
import { decode } from '../utils/convert'
import { assert, ReturnValueError } from '../utils/error'
import { oneOf } from '../utils/array'
import type { Fields, DBItem } from '../types'

type ReturnType = 'NONE' | 'OLD'

export class DeletionChain<
  T extends Fields,
  R extends ReturnType,
  F = R extends 'NONE' ? undefined : DBItem<T>
> extends BaseChain<F, T> {
  constructor(
    fields: T,
    client: AWS.DynamoDB.DocumentClient,
    private readonly params: AWS.DynamoDB.DocumentClient.DeleteItemInput,
    private readonly returnType: ReturnType = 'NONE',
    debug?: boolean
  ) {
    super(fields, client, debug)
  }

  async execute() {
    const params: AWS.DynamoDB.DeleteItemInput = {
      ...this.params,
      ...(this.returnType === 'OLD' && {
        ReturnValues: 'ALL_OLD',
      }),
    }
    super.log('delete', params)

    const { Attributes } = await this.client.delete(params).promise()

    const result: F = decode(
      this.returnType === 'OLD' ? Attributes : undefined
    ) as any

    this.resolve(result)
  }

  returning<R extends ReturnType>(v: R): DeletionChain<T, R> {
    assert(oneOf(v, 'NONE', 'OLD'), new ReturnValueError(v, 'delete'))
    return this.clone(this.fields, this._debug, v)
  }

  protected clone(
    fields = this.fields,
    debug = this._debug,
    returnType = this.returnType
  ) {
    return new DeletionChain(
      fields,
      this.client,
      this.params,
      returnType,
      debug
    ) as any
  }
}
