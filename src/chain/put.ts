import BaseChain from './base'
import { decode } from '../utils/convert'
import { assert, ReturnValueError } from '../utils/error'
import { oneOf } from '../utils/array'
import type { Fields, DBItem } from '../types'

type ReturnType = 'NONE' | 'NEW' | 'OLD'

export class PutChain<
  T extends Fields,
  R extends ReturnType,
  F = R extends 'NONE' ? undefined : DBItem<T>
> extends BaseChain<F, T> {
  constructor(
    fields: T,
    client: AWS.DynamoDB.DocumentClient,
    private readonly keyFields: string[],
    private readonly params: AWS.DynamoDB.DocumentClient.PutItemInput,
    private readonly returnType: ReturnType = 'NONE',
    private readonly existCheck = false
  ) {
    super(fields, client)
  }

  async execute() {
    this.params.Item = this.makeSets(this.params.Item)

    if (this.existCheck) {
      const conditions: string[] = []
      for (const k of this.keyFields) {
        const name = `:${k}`
        ;(this.params.ExpressionAttributeValues ??= {})[
          name
        ] = this.params.Item[k]
        conditions.push(`${k}<>${name}`)
      }
      this.params.ConditionExpression = conditions.join(' AND ')
    }

    const params = {
      ...this.params,
      ...(this.returnType === 'OLD' && {
        ReturnValues: 'ALL_OLD',
      }),
    }
    super.log('put', params)

    const { Attributes } = await this.client.put(params).promise()

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
    return new PutChain(
      this.fields,
      this.client,
      this.keyFields,
      this.params,
      v
    )
  }

  public ifNotExists(): PutChain<T, R> {
    return this.clone(this.fields, true)
  }

  public cast = super._cast.bind(this)

  protected clone(fields = this.fields, existsCheck?: boolean) {
    return new PutChain(
      fields,
      this.client,
      this.keyFields,
      this.params,
      this.returnType,
      existsCheck
    ) as any
  }
}
