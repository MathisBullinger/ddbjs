import BaseChain from './base'
import { decode } from '../utils/convert'
import type { Fields, DBItem } from '../types'

export class GetChain<T extends Fields> extends BaseChain<DBItem<T>, T> {
  constructor(
    fields: T,
    client: AWS.DynamoDB.DocumentClient,
    private readonly table: string,
    private readonly key: any
  ) {
    super(fields, client)
  }

  async execute() {
    const { Item } = await this.client
      .get({ TableName: this.table, Key: this.key })
      .promise()

    this.resolve(decode(Item) as any)
  }

  protected clone(fields = this.fields): this {
    return new GetChain(fields, this.client, this.table, this.key) as any
  }
}
