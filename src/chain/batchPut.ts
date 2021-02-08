import BaseChain from './base'
import { batch } from '../utils/array'
import type { Schema, Fields, Item } from '../types'

export class BatchPutChain<
  T extends Schema<F>,
  F extends Fields = Omit<T, 'key'>
> extends BaseChain<undefined, T> {
  constructor(
    private readonly schema: T,
    client: AWS.DynamoDB.DocumentClient,
    private readonly table: string,
    private readonly items: Item<F, T['key']>[]
  ) {
    super(schema, client)
  }

  async execute() {
    await Promise.all(batch(this.items, 25).map(batch => this.put(batch)))
    this.resolve(undefined as any)
  }

  private async put(items?: any[]) {
    if (!items?.length) return

    const { UnprocessedItems } = await this.client
      .batchWrite({
        RequestItems: {
          [this.table]: items.map(Item => ({ PutRequest: { Item } })),
        },
      })
      .promise()

    await this.put(UnprocessedItems?.[this.table])
  }

  protected clone(): this {
    return new BatchPutChain(
      this.schema,
      this.client,
      this.table,
      this.items
    ) as any
  }
}
