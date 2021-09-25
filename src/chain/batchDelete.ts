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
      this.expr.map(v => this.config.client.batchWrite(v).promise())
    )
    this.resolve(undefined as any)
  }

  public get expr(): AWS.DynamoDB.BatchWriteItemInput[] {
    return batch(this.config.keys, 25).map(keys => ({
      RequestItems: {
        [this.config.table]: keys.map(Key => ({
          DeleteRequest: { Key },
        })),
      },
    }))
  }
}
