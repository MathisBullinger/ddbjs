import BaseChain from './base'
import { decode } from '../utils/convert'
import * as expr from '../expression'
import type { Fields, DBItem } from '../types'

export class ScanChain<T extends Fields> extends BaseChain<DBItem<T>[], T> {
  constructor(
    fields: T,
    client: AWS.DynamoDB.DocumentClient,
    private readonly table: string,
    private readonly _limit?: number,
    private readonly selected?: string[]
  ) {
    super(fields, client)
  }

  async execute() {
    const params: AWS.DynamoDB.ScanInput = {
      TableName: this.table,
      Limit: this._limit,
    }
    Object.assign(params, expr.project(...(this.selected ?? [])))

    const items: any[] = []

    do {
      const { Items, LastEvaluatedKey } = await this.client
        .scan(params)
        .promise()
      items.push(...(Items ?? []))
      params.ExclusiveStartKey = LastEvaluatedKey
      if (params.Limit) params.Limit -= Items?.length ?? 0
    } while (params.ExclusiveStartKey && (params.Limit ?? Infinity > 0))

    this.resolve(items.map(decode) as any)
  }

  public limit(n: number): this {
    return this.clone(this.fields, n)
  }

  public select(...fields: string[]): this {
    return this.clone(this.fields, this._limit, fields)
  }

  protected clone(
    fields = this.fields,
    limit = this._limit,
    selected = this.selected
  ): this {
    return new ScanChain(
      fields,
      this.client,
      this.table,
      limit,
      selected
    ) as any
  }
}
