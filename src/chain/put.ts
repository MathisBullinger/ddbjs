import { decode } from '../utils/convert'
import BaseChain from './base'
import { assert, ReturnValueError } from '../utils/error'
import { oneOf } from '../utils/array'

type ReturnType = 'NONE' | 'NEW' | 'OLD' | 'UPDATED_OLD' | 'UPDATED_NEW'

type Item<T extends Fields> = { [K in keyof T]: SchemaValue<T[K]> }

export default class PutChain<T extends Fields> extends BaseChain<Item<T>> {
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

    const result: Item<T> = decode(
      this.returnType === 'NEW'
        ? this.params.Item
        : this.returnType === 'OLD'
        ? Attributes
        : undefined
    ) as any

    this.resolve(result)
  }

  returning(v: ReturnType): PutChain<T> {
    assert(oneOf(v, 'NEW', 'OLD', 'NONE'), new ReturnValueError(v, 'insert'))
    return new PutChain(this.fields, this.client, this.params, v)
  }
}
