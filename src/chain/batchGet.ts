import BaseChain from './base'
import type * as AWS from 'aws-sdk'
import { decode } from '../utils/convert'
import type { Fields, DBItem } from '../types'

const batch = <T>(arr: T[], batchSize: number): T[][] =>
  Array(Math.ceil(arr.length / batchSize))
    .fill(0)
    .map((_, i) => arr.slice(i * batchSize, (i + 1) * batchSize))

export default class BatchGetChain<T extends Fields> extends BaseChain<
  DBItem<T>[],
  T
> {
  constructor(
    fields: T,
    client: AWS.DynamoDB.DocumentClient,
    private readonly table: string,
    private readonly keys: any[],
    private readonly shouldSort: boolean = false
  ) {
    super(fields, client)
  }

  async execute() {
    const items = (
      await Promise.all(batch(this.keys, 100).map(v => this.read(v)))
    ).flat()

    this.resolve(
      (this.shouldSort
        ? BatchGetChain.sortByKeys(this.keys, items)
        : items
      ).map(decode) as any
    )
  }

  private async read(Keys?: any[]): Promise<any[]> {
    if (!Keys?.length) return []

    const { Responses, UnprocessedKeys } = await this.client
      .batchGet({
        RequestItems: {
          [this.table]: {
            Keys,
          },
        },
      })
      .promise()

    return [
      ...(Responses?.[this.table] ?? []),
      ...(await this.read(UnprocessedKeys?.[this.table]?.Keys)),
    ]
  }

  public sort() {
    return this.clone(this.fields, true)
  }

  private static sortByKeys(keys: any[], items: any[]) {
    const remaining = [...items]
    const sorted = []

    for (const key of keys) {
      const props = Object.entries(key)
      const i = remaining.findIndex(item =>
        props.every(([k, v]) => item[k] === v)
      )
      if (i === -1) continue
      sorted.push(remaining.splice(i, 1)[0])
    }

    sorted.push(...remaining)
    return sorted
  }

  protected clone(fields = this.fields, sort = false): this {
    return new BatchGetChain(
      fields,
      this.client,
      this.table,
      this.keys,
      sort
    ) as any
  }
}
