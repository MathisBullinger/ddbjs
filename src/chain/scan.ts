import BaseChain from './base'
import { decode } from '../utils/convert'
import type { Fields, DBItem } from '../types'

export class ScanChain<T extends Fields> extends BaseChain<DBItem<T>[], T> {
  constructor(
    fields: T,
    client: AWS.DynamoDB.DocumentClient,
    private readonly table: string
  ) {
    super(fields, client)
  }

  async execute() {
    const params: AWS.DynamoDB.ScanInput = { TableName: this.table }
    const items: any[] = []

    do {
      const { Items, LastEvaluatedKey } = await this.client
        .scan(params)
        .promise()
      params.ExclusiveStartKey = LastEvaluatedKey
      items.push(...(Items ?? []))
    } while (params.ExclusiveStartKey)

    this.resolve(items.map(decode) as any)
  }

  protected clone(): this {
    return new ScanChain(this.fields, this.client, this.table) as any
  }
}
