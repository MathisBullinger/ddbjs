import BaseChain, { Config } from './base'
import { batch } from '../utils/array'
import type { Schema } from '../types'

type BatchDelConfig<T extends Schema<any>> = Config<T> & { keys: any[] }

export class BatchDelete<T extends Schema<any>> extends BaseChain<
  undefined,
  BatchDelConfig<T>
> {
  constructor(config: BatchDelConfig<T>) {
    super(config, {})
  }

  async execute() {
    await Promise.all(
      batch(this.config.keys, 25).map(batch => this.delete(batch))
    )
    this.resolve(undefined as any)
  }

  private async delete(keys: any[]) {
    await this.config.client
      .batchWrite({
        RequestItems: {
          [this.config.table]: keys.map(Key => ({
            DeleteRequest: { Key },
          })),
        },
      })
      .promise()
  }
}
