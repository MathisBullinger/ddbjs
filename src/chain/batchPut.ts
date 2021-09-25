import BaseChain, { Config } from './base'
import { batch } from '../utils/array'
import type { Schema, ScItem } from '../types'
import clone from 'snatchblock/clone'
import { omit } from 'snatchblock'

type BatchPutConfig<T extends Schema<any>> = Config<T> & { items: ScItem<T>[] }

export class BatchPut<T extends Schema<any>> extends BaseChain<
  undefined,
  BatchPutConfig<T>
> {
  constructor(config: BatchPutConfig<T>) {
    super(config, {})
  }

  async execute() {
    await Promise.all(this.expr.map(v => this.put(v)))
    this.resolve(undefined as any)
  }

  public get expr(): AWS.DynamoDB.BatchWriteItemInput[] {
    return batch(this.config.items, 25).map(v => ({
      RequestItems: {
        [this.config.table]: v.map(Item => ({ PutRequest: { Item } })),
      },
    }))
  }

  private async put(params: AWS.DynamoDB.BatchWriteItemInput) {
    const items = params.RequestItems[this.config.table]
    if (!items?.length) return

    this.log('batchWrite', params)

    const { UnprocessedItems } = await this.config.client
      .batchWrite(params)
      .promise()

    await this.put({
      ...omit(params, 'RequestItems'),
      RequestItems: {
        [this.config.table]: UnprocessedItems?.[this.config.table] ?? [],
      },
    })
  }
}
