import BaseChain, { Config } from './base'
import { batch } from '../utils/array'
import type { Schema, ScItem } from '../types'

type BatchPutConfig<T extends Schema<any>> = Config<T> & { items: ScItem<T>[] }

export class BatchPut<T extends Schema<any>> extends BaseChain<
  undefined,
  BatchPutConfig<T>
> {
  constructor(config: BatchPutConfig<T>) {
    super(config, {})
  }

  async execute() {
    await Promise.all(
      batch(this.config.items, 25).map(batch => this.put(batch))
    )
    this.resolve(undefined as any)
  }

  private async put(items?: any[]) {
    if (!items?.length) return

    const payload = {
      RequestItems: {
        [this.config.table]: items.map(Item => ({ PutRequest: { Item } })),
      },
    }
    super.log('batchWrite', payload)

    const { UnprocessedItems } = await this.config.client
      .batchWrite(payload)
      .promise()

    await this.put(UnprocessedItems?.[this.config.table])
  }
}
