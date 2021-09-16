import BaseChain from './base'
import { batch } from '../utils/array'
import type { Fields } from '../types'

export class BatchDelete<T extends Fields> extends BaseChain<undefined, T> {
  constructor(
    schema: T,
    client: AWS.DynamoDB.DocumentClient,
    table: string,
    private readonly keys: any[],
    debug?: boolean
  ) {
    super(schema, client, table, debug)
  }

  async execute() {
    await Promise.all(batch(this.keys, 25).map(batch => this.delete(batch)))
    this.resolve(undefined as any)
  }

  private async delete(keys: any[]) {
    await this.client
      .batchWrite({
        RequestItems: {
          [this.table]: keys.map(Key => ({
            DeleteRequest: { Key },
          })),
        },
      })
      .promise()
  }

  protected clone(fields = this.fields, debug = this._debug) {
    return new BatchDelete(
      fields,
      this.client,
      this.table,
      this.keys,
      debug
    ) as any
  }
}
