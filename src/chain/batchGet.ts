import BaseChain, { Config } from './base'
import { decode } from '../utils/convert'
import * as expr from '../expression'
import { batch } from '../utils/array'
import type { Schema, Fields, ScFields, Projected, Field } from '../types'
import omit from 'snatchblock/omit'

type BatchGetConfig<T extends Schema<any>> = Config<T> & {
  keys: any[]
  sort?: boolean
  removeFields?: any[]
}

export class BatchGet<
  T extends Schema<any>,
  S extends string | number | symbol = Field<T>
> extends BaseChain<Projected<ScFields<T>, S>[], BatchGetConfig<T>> {
  constructor(config: BatchGetConfig<T>) {
    super(config, {})
  }

  async execute() {
    const items = (await Promise.all(this.expr.map(v => this.read(v)))).flat()

    this.resolve(
      this.removeKeys(
        this.config.sort ? BatchGet.sortByKeys(this.config.keys, items) : items
      ).map(decode) as any
    )
  }

  public get expr(): AWS.DynamoDB.BatchGetItemInput[] {
    const params = expr.project(...(this.config.selection ?? []))
    return batch(this.config.keys, 100).map(Keys => ({
      RequestItems: {
        [this.config.table]: {
          Keys,
          ...params,
        },
      },
    }))
  }

  public select<Fields extends string>(
    ...fields: Fields[]
  ): BatchGet<T, Fields> {
    return this.clone({ selection: fields }) as any
  }

  private async read(params: AWS.DynamoDB.BatchGetItemInput): Promise<any[]> {
    const keys = params.RequestItems[this.config.table].Keys
    if (!keys.length) return []

    this.log('batchGet', params)

    const { Responses, UnprocessedKeys } = await this.config.client
      .batchGet(params)
      .promise()

    return [
      ...(Responses?.[this.config.table] ?? []),
      ...(await this.read({
        ...omit(params, 'RequestItems'),
        RequestItems: {
          [this.config.table]: {
            Keys: UnprocessedKeys?.[this.config.table]?.Keys ?? [],
          },
        },
      })),
    ]
  }

  public sort() {
    const keyFields = [
      (this.config.schema as Schema<Fields>)[BaseChain.key!],
    ].flat()
    return this.clone({
      sort: true,
      ...(this.config.selection && {
        selection: [...this.config.selection, ...keyFields],
        removeFields: keyFields.filter(
          v => !this.config.selection!.includes(v)
        ),
      }),
    })
  }

  private removeKeys<T>(items: T[]): T[] {
    if (!this.config.removeFields?.length) return items
    return items.map(
      v =>
        Object.fromEntries(
          Object.entries(v).map(([k, v]) =>
            this.config.removeFields!.includes(k) ? [] : [k, v]
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
}
