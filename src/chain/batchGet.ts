import BaseChain from './base'
import { decode } from '../utils/convert'
import * as expr from '../expression'
import { batch } from '../utils/array'
import type { Schema, Fields, DBItem, KeySym } from '../types'

export class BatchGetChain<
  T extends Schema<F>,
  F extends Fields = Omit<T, KeySym>
> extends BaseChain<DBItem<F>[], F> {
  constructor(
    private readonly schema: T,
    client: AWS.DynamoDB.DocumentClient,
    private readonly table: string,
    private readonly keys: any[],
    private readonly shouldSort: boolean = false,
    private readonly selected?: string[],
    private readonly removeFields: string[] = [],
    debug?: boolean
  ) {
    super(schema, client, debug)
  }

  async execute() {
    const selected = [...(this.selected ?? [])]

    const params = expr.project(...selected)

    const items = (
      await Promise.all(
        batch(this.keys, 100).map(v => this.read(v, params as any))
      )
    ).flat()

    this.resolve(
      this.removeKeys(
        this.shouldSort ? BatchGetChain.sortByKeys(this.keys, items) : items
      ).map(decode) as any
    )
  }

  public select(...fields: string[]): this {
    return this.clone(this.schema, this._debug, this.shouldSort, fields)
  }

  private async read(
    Keys?: any[],
    params?: Partial<AWS.DynamoDB.BatchGetItemInput>
  ): Promise<any[]> {
    if (!Keys?.length) return []

    const payload = {
      RequestItems: {
        [this.table]: {
          Keys,
          ...params,
        },
      },
    }
    super.log('batchGet', payload)

    const { Responses, UnprocessedKeys } = await this.client
      .batchGet(payload)
      .promise()

    return [
      ...(Responses?.[this.table] ?? []),
      ...(await this.read(UnprocessedKeys?.[this.table]?.Keys)),
    ]
  }

  public sort() {
    const keyFields = [this.schema[BaseChain.key!]].flat() as string[]
    return this.clone(
      this.schema,
      this._debug,
      true,
      this.selected && [...this.selected, ...keyFields],
      this.selected && keyFields.filter(v => !this.selected!.includes(v))
    )
  }

  private removeKeys<T>(items: T[]): T[] {
    if (!this.removeFields?.length) return items
    return items.map(
      v =>
        Object.fromEntries(
          Object.entries(v).map(([k, v]) =>
            this.removeFields.includes(k) ? [] : [k, v]
          )
        ) as T
    )
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

  protected clone(
    schema = this.schema,
    debug = this._debug,
    sort = false,
    selected = this.selected,
    remove = this.removeFields
  ): this {
    return new BatchGetChain(
      schema as any,
      this.client,
      this.table,
      this.keys,
      sort,
      selected,
      remove,
      debug
    ) as any
  }
}
