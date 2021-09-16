import BaseChain from './base'
import { batch } from '../utils/array'
import type { Schema, Fields, Item, KeySym } from '../types'

export class BatchPut<
  T extends Schema<F>,
  F extends Fields = Omit<T, KeySym>
> extends BaseChain<undefined, T> {
  constructor(
    private readonly schema: T,
    client: AWS.DynamoDB.DocumentClient,
    table: string,
    private readonly items: Item<F, T[KeySym]>[],
    debug?: boolean
  ) {
    super(schema, client, table, debug)
  }

  async execute() {
    await Promise.all(batch(this.items, 25).map(batch => this.put(batch)))
    this.resolve(undefined as any)
  }

  private async put(items?: any[]) {
    if (!items?.length) return

    const payload = {
      RequestItems: {
        [this.table]: items.map(Item => ({ PutRequest: { Item } })),
      },
    }
    super.log('batchWrite', payload)

    const { UnprocessedItems } = await this.client.batchWrite(payload).promise()

    await this.put(UnprocessedItems?.[this.table])
  }

  protected clone(schema = this.schema, debug = this._debug): this {
    return new BatchPut(
      schema,
      this.client,
      this.table,
      this.items,
      debug
    ) as any
  }
}
