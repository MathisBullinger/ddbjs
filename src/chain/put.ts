import { decode } from '../utils/convert'
import BaseChain from './base'
import { assert, ReturnValueError } from '../utils/error'
import { oneOf } from '../utils/array'

type ReturnType = 'NONE' | 'NEW' | 'OLD' | 'UPDATED_OLD' | 'UPDATED_NEW'

type Item<T extends Fields> = { [K in keyof T]: SchemaValue<T[K]> } &
  Record<string, any>

export default class PutChain<
  T extends Fields,
  R extends ReturnType,
  F = R extends 'NONE' ? undefined : Item<T>
> extends BaseChain<F> {
  constructor(
    private readonly fields: T,
    protected readonly client: AWS.DynamoDB.DocumentClient,
    private readonly params: AWS.DynamoDB.DocumentClient.PutItemInput,
    private readonly returnType: ReturnType = 'NONE'
  ) {
    super(client)
  }

  async execute() {
    const { Attributes } = await this.client
      .put({
        ...this.params,
        ...(this.returnType === 'OLD' && {
          ReturnValues: 'ALL_OLD',
        }),
      })
      .promise()

    const result: F = decode(
      this.returnType === 'NEW'
        ? this.params.Item
        : this.returnType === 'OLD'
        ? Attributes
        : undefined
    ) as any

    this.resolve(result)
  }

  returning<R extends ReturnType>(v: R): PutChain<T, R> {
    assert(oneOf(v, 'NEW', 'OLD', 'NONE'), new ReturnValueError(v, 'insert'))
    return new PutChain(this.fields, this.client, this.params, v)
  }
}
