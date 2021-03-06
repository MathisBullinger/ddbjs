import BaseChain from './base'
import { decode } from '../utils/convert'
import * as expr from '../expression'
import type { Fields, DBItem } from '../types'

export class GetChain<T extends Fields> extends BaseChain<DBItem<T>, T> {
  constructor(
    fields: T,
    client: AWS.DynamoDB.DocumentClient,
    private readonly table: string,
    private readonly key: any,
    private readonly selected?: string[],
    private readonly consistent = false,
    debug?: boolean
  ) {
    super(fields, client, debug)
  }

  async execute() {
    const params: Partial<AWS.DynamoDB.GetItemInput> = {
      TableName: this.table,
      Key: this.key,
      ConsistentRead: this.consistent,
    }
    super.log('get', params)

    Object.assign(params, expr.project(...(this.selected ?? [])))

    const { Item } = await this.client.get(params as any).promise()

    this.resolve(decode(Item) as any)
  }

  public select(...fields: string[]): this {
    return this.clone(this.fields, this._debug, fields)
  }

  public strong(): this {
    return this.clone(this.fields, this._debug, this.selected, true)
  }

  protected clone(
    fields = this.fields,
    debug = this._debug,
    selected?: string[],
    consistent = this.consistent
  ): this {
    return new GetChain(
      fields,
      this.client,
      this.table,
      this.key,
      selected,
      consistent,
      debug
    ) as any
  }
}
